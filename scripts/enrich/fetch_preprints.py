#!/usr/bin/env python3
"""
fetch_preprints.py — Download PDFs from preprint servers for DB entries or raw identifiers.

Supported servers: arXiv, bioRxiv, medRxiv, SSRN (via S2 openAccessPdf only).

Resolution strategy:
  Tier 1 — Semantic Scholar openAccessPdf field (covers all servers + PMC + some publishers)
  Tier 2 — Direct URL construction (arXiv, bioRxiv, medRxiv fallback only)

Usage:
  python3 scripts/py.py scripts/enrich/fetch_preprints.py --id ID [ID ...]
  python3 scripts/py.py scripts/enrich/fetch_preprints.py --paper ID [ID ...]
  python3 scripts/py.py scripts/enrich/fetch_preprints.py --all-external
  python3 scripts/py.py scripts/enrich/fetch_preprints.py --all-stubs

Options:
  --dry-run       Show what would be downloaded, no actual downloads
  --force         Re-attempt previously failed or succeeded downloads
  --max N         Cap total downloads per run (default: 50)
  --output-dir    Override output directory (default: pdf-staging/)

Environment:
  S2_API_KEY  — Semantic Scholar API key; also read from project.yaml (apis.semantic_scholar.key)
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import normalize_doi, export_json, get_s2_api_key

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
FETCH_LOG_FILE = ROOT / "data" / "db" / "preprint_fetch_log.json"
DEFAULT_OUTPUT_DIR = ROOT / "pdf-staging"

S2_BASE_URL = "https://api.semanticscholar.org/graph/v1"

USER_AGENT = "PaperClaw/1.0 (academic research tool; mailto:user@example.com)"

# Rate limits per source (seconds between requests)
RATE_LIMITS = {
    "arxiv": 3.0,
    "biorxiv": 1.0,
    "medrxiv": 1.0,
}


# ---------------------------------------------------------------------------
# S2 API helpers (copied from fetch_forward_citations.py)
# ---------------------------------------------------------------------------

def s2_request(path: str, api_key: str | None, retries: int = 3) -> dict | None:
    """Make a GET request to the S2 API. Returns parsed JSON or None on 404."""
    url = f"{S2_BASE_URL}{path}"
    headers = {"Accept": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key

    delay = 5.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 400:
                print(f"  S2 HTTP 400 (Bad Request) — skipping")
                return None
            if e.code in (429, 500, 502, 503, 504):
                if attempt < retries - 1:
                    print(f"  S2 HTTP {e.code} — retrying in {delay:.0f}s...")
                    time.sleep(delay)
                    delay *= 2
                    continue
                else:
                    print(f"  S2 HTTP {e.code} — rate limited after {retries} retries, skipping")
                    return None
            raise
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                print(f"  S2 network error ({e}) — retrying in {delay:.0f}s...")
                time.sleep(delay)
                delay *= 2
                continue
            raise

    return None


def rate_sleep(api_key: str | None):
    """Sleep to respect S2 rate limits."""
    time.sleep(0.12 if api_key else 1.05)


# ---------------------------------------------------------------------------
# Identifier classification
# ---------------------------------------------------------------------------

# Patterns for arXiv IDs: new-style YYMM.NNNNN or old-style archive/YYMMNNN
ARXIV_ID_RE = re.compile(r"^(\d{4}\.\d{4,5}(v\d+)?)$|^([a-z\-]+/\d{7}(v\d+)?)$")
ARXIV_DOI_RE = re.compile(r"^10\.48550/arxiv\.(.+)$", re.IGNORECASE)
BIORXIV_DOI_RE = re.compile(r"^10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d+)", re.IGNORECASE)
MEDRXIV_DOI_RE = re.compile(r"^10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d+)", re.IGNORECASE)
SSRN_DOI_RE = re.compile(r"^10\.2139/ssrn\.", re.IGNORECASE)
DB_PAPER_ID_RE = re.compile(r"^[a-z][a-z0-9_]*_\d{4}_[a-z]")


def classify_identifier(s: str) -> tuple[str, str]:
    """
    Returns (kind, normalized) where kind is one of:
      arxiv_id, doi, url, db_paper_id, unknown
    """
    s = s.strip()

    # URL
    if s.startswith("http://") or s.startswith("https://"):
        return "url", s

    # arXiv bare ID
    if ARXIV_ID_RE.match(s):
        # strip version suffix for canonical ID
        clean = re.sub(r"v\d+$", "", s)
        return "arxiv_id", clean

    # DOI (starts with 10.)
    if s.startswith("10."):
        return "doi", s.lower()

    # DB paper ID
    if DB_PAPER_ID_RE.match(s):
        return "db_paper_id", s

    return "unknown", s


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------

def _arxiv_id_from_doi(doi: str) -> str | None:
    m = ARXIV_DOI_RE.match(doi)
    if m:
        return re.sub(r"v\d+$", "", m.group(1))
    return None


def _arxiv_pdf_url(arxiv_id: str) -> str:
    return f"https://arxiv.org/pdf/{arxiv_id}"


def _biorxiv_pdf_url(doi: str) -> str:
    return f"https://www.biorxiv.org/content/{doi}.full.pdf"


def _medrxiv_pdf_url(doi: str) -> str:
    return f"https://www.medrxiv.org/content/{doi}.full.pdf"


def _s2_open_access_pdf(identifier: str, id_type: str, api_key: str | None) -> tuple[str | None, str | None]:
    """
    Query S2 for openAccessPdf.
    id_type: "doi", "s2id", or "arxiv"
    Returns (url, status_message).
    """
    fields = "openAccessPdf,externalIds,title"

    if id_type == "doi":
        encoded = urllib.parse.quote(identifier, safe="")
        path = f"/paper/DOI:{encoded}?fields={fields}"
    elif id_type == "s2id":
        path = f"/paper/{identifier}?fields={fields}"
    elif id_type == "arxiv":
        path = f"/paper/ARXIV:{identifier}?fields={fields}"
    else:
        return None, "unknown id_type"

    result = s2_request(path, api_key)
    rate_sleep(api_key)

    if not result:
        return None, "not_found_in_s2"

    oa = result.get("openAccessPdf")
    if oa and oa.get("url"):
        return oa["url"], "s2_open_access"

    return None, "not_open_access"


def resolve_download_url(
    identifier: str,
    kind: str,
    paper: dict | None,
    api_key: str | None,
) -> tuple[str | None, str, dict]:
    """
    Map any identifier to (download_url, source_label, extra_metadata).
    Returns (None, reason, {}) if no URL can be found.

    source_label values: s2_open_access, arxiv_direct, biorxiv_direct,
                         medrxiv_direct, not_found, not_open_access
    """
    doi = None
    s2_paper_id = None
    arxiv_id = None

    # Extract what we know from the identifier itself
    if kind == "arxiv_id":
        arxiv_id = identifier
    elif kind == "doi":
        doi = identifier
        aid = _arxiv_id_from_doi(doi)
        if aid:
            arxiv_id = aid
    elif kind == "url":
        # Extract arXiv ID from URL
        m = re.search(r"arxiv\.org/(?:abs|pdf)/([^\s?/]+)", identifier)
        if m:
            arxiv_id = re.sub(r"v\d+$", "", m.group(1))
            kind = "arxiv_id"
        # Extract DOI from doi.org URL
        m2 = re.search(r"doi\.org/(.+)$", identifier)
        if m2:
            doi = m2.group(1).lower()
            aid = _arxiv_id_from_doi(doi)
            if aid:
                arxiv_id = aid
    elif kind == "db_paper_id":
        # Merge paper metadata
        if paper:
            doi = normalize_doi(paper.get("doi"))
            s2_paper_id = paper.get("s2_paper_id")
            if doi:
                aid = _arxiv_id_from_doi(doi)
                if aid:
                    arxiv_id = aid

    # Tier 1: S2 openAccessPdf
    if s2_paper_id:
        url, status = _s2_open_access_pdf(s2_paper_id, "s2id", api_key)
        if url:
            return url, "s2_open_access", {}
        if status == "not_open_access":
            print(f"  S2: paper found but no open-access PDF")
    elif arxiv_id:
        url, status = _s2_open_access_pdf(arxiv_id, "arxiv", api_key)
        if url:
            return url, "s2_open_access", {}
        if status == "not_open_access":
            print(f"  S2: arXiv paper found but no open-access PDF")
    elif doi:
        url, status = _s2_open_access_pdf(doi, "doi", api_key)
        if url:
            return url, "s2_open_access", {}
        if status == "not_open_access":
            print(f"  S2: paper found but no open-access PDF")
    else:
        print(f"  No DOI, S2 ID, or arXiv ID — cannot query S2")
        status = "no_identifier"

    # Tier 2: Direct URL construction (fallback)
    if arxiv_id:
        print(f"  Falling back to direct arXiv URL for {arxiv_id}")
        return _arxiv_pdf_url(arxiv_id), "arxiv_direct", {"arxiv_id": arxiv_id}

    if doi:
        if re.match(r"10\.1101/", doi):
            # Disambiguate bioRxiv vs medRxiv: try both, return bioRxiv first
            # (medRxiv DOIs use same prefix 10.1101; we can't easily distinguish without S2)
            print(f"  Falling back to direct bioRxiv URL for {doi}")
            return _biorxiv_pdf_url(doi), "biorxiv_direct", {"doi": doi}

        if SSRN_DOI_RE.match(doi):
            print(f"  SSRN is login-walled — only available via S2 openAccessPdf")
            return None, "not_open_access", {}

    if status == "not_found_in_s2":
        return None, "not_found", {}

    return None, "not_open_access", {}


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def download_pdf(url: str, dest_path: Path, source: str, dry_run: bool = False) -> tuple[bool, str]:
    """
    Download a PDF from url to dest_path.
    Returns (success, message).
    Validates %PDF magic bytes. Atomic write via temp file.
    """
    if dry_run:
        return True, f"[dry-run] would download from {url}"

    rate_key = None
    if "arxiv.org" in url:
        rate_key = "arxiv"
    elif "biorxiv.org" in url:
        rate_key = "biorxiv"
    elif "medrxiv.org" in url:
        rate_key = "medrxiv"

    headers = {"User-Agent": USER_AGENT}
    delay = 5.0

    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = resp.read()

            # Validate PDF magic bytes
            if not data[:1024].lstrip().startswith(b"%PDF"):
                return False, f"response is not a PDF (got {data[:20]!r}...)"

            # Atomic write
            tmp = dest_path.with_suffix(".tmp")
            tmp.write_bytes(data)
            tmp.rename(dest_path)

            if rate_key:
                time.sleep(RATE_LIMITS[rate_key])

            return True, f"downloaded {len(data):,} bytes"

        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False, f"HTTP 404 not found"
            if e.code == 429:
                if attempt < 2:
                    print(f"  HTTP 429 — retrying in {delay:.0f}s...")
                    time.sleep(delay)
                    delay *= 2
                    continue
                return False, f"HTTP 429 rate limited after 3 retries"
            return False, f"HTTP {e.code}"
        except urllib.error.URLError as e:
            if attempt < 2:
                print(f"  Network error ({e}) — retrying in {delay:.0f}s...")
                time.sleep(delay)
                delay *= 2
                continue
            return False, f"network error: {e}"
        except Exception as e:
            return False, f"unexpected error: {e}"

    return False, "max retries exceeded"


# ---------------------------------------------------------------------------
# Fetch log
# ---------------------------------------------------------------------------

def load_fetch_log() -> dict:
    if FETCH_LOG_FILE.exists():
        return json.loads(FETCH_LOG_FILE.read_text())
    return {"fetches": {}}


def save_fetch_log(log: dict):
    export_json(log, FETCH_LOG_FILE)


def should_attempt(key: str, log: dict, force: bool, output_dir: Path) -> tuple[bool, str]:
    """
    Returns (should_attempt, reason).
    Skips if: already downloaded (PDF exists) unless --force,
              or last attempt was successful unless --force,
              or last attempt was not_found/not_open_access unless --force.
    """
    if force:
        return True, "force"

    entry = log["fetches"].get(key)
    if not entry:
        return True, "never attempted"

    result = entry.get("result", "")

    # If we successfully downloaded before, check if file still exists
    if result == "ok":
        pdf_filename = entry.get("pdf_filename", "")
        if pdf_filename:
            pdf_path = output_dir / pdf_filename
            already_path = ROOT / "data" / "pdfs" / pdf_filename
            if pdf_path.exists() or already_path.exists():
                return False, f"already downloaded ({pdf_filename})"
        return True, "previous download missing, retrying"

    # Skip permanently-failed cases without --force
    if result in ("not_found", "not_open_access"):
        return False, f"previously: {result}"

    # Retry transient failures
    return True, "retrying previous failure"


# ---------------------------------------------------------------------------
# PDF filename derivation
# ---------------------------------------------------------------------------

def sanitize_filename(s: str) -> str:
    """Make a string safe for use as a filename."""
    s = re.sub(r"[^\w\.\-]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:100]


def derive_filename(key: str, kind: str, paper: dict | None) -> str:
    """
    Derive PDF filename.
    - DB paper IDs → {paper_id}.pdf (seamless adopt_import.py matching)
    - Raw identifiers → sanitized version
    """
    if kind == "db_paper_id" and paper:
        return f"{key}.pdf"
    return f"{sanitize_filename(key)}.pdf"


# ---------------------------------------------------------------------------
# Target collection
# ---------------------------------------------------------------------------

def collect_targets_from_db(
    papers: dict,
    mode: str,
    paper_ids: list[str] | None = None,
) -> list[tuple[str, str, dict]]:
    """
    Returns list of (key, kind, paper_dict) tuples.
    mode: "paper", "all-external", "all-stubs"
    """
    targets = []

    if mode == "paper":
        for pid in (paper_ids or []):
            if pid not in papers:
                print(f"WARNING: '{pid}' not found in papers.json — skipping")
                continue
            targets.append((pid, "db_paper_id", papers[pid]))

    elif mode == "all-external":
        for pid, p in papers.items():
            if p.get("type") == "external_owned":
                targets.append((pid, "db_paper_id", p))

    elif mode == "all-stubs":
        for pid, p in papers.items():
            if p.get("type") == "stub":
                # Only bother if we have some way to resolve a URL
                has_doi = bool(p.get("doi"))
                has_s2 = bool(p.get("s2_paper_id"))
                if has_doi or has_s2:
                    targets.append((pid, "db_paper_id", p))

    return targets


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def process_target(
    key: str,
    kind: str,
    paper: dict | None,
    output_dir: Path,
    log: dict,
    api_key: str | None,
    dry_run: bool,
    force: bool,
) -> str:
    """
    Process one download target. Returns result string.
    """
    attempt_ok, reason = should_attempt(key, log, force, output_dir)
    if not attempt_ok:
        print(f"  SKIP: {reason}")
        return "skipped"

    # Resolve download URL
    url, source, meta = resolve_download_url(key, kind, paper, api_key)

    if not url:
        print(f"  No download URL found: {source}")
        log["fetches"][key] = {
            "last_attempted": str(date.today()),
            "result": source,  # "not_found" or "not_open_access"
        }
        return source

    print(f"  URL ({source}): {url}")

    filename = derive_filename(key, kind, paper)
    dest = output_dir / filename

    success, msg = download_pdf(url, dest, source, dry_run=dry_run)

    if dry_run:
        print(f"  {msg}")
        print(f"  Would save as: {filename}")
        return "dry_run"

    if success:
        print(f"  Saved: {filename} ({msg})")
        log["fetches"][key] = {
            "last_attempted": str(date.today()),
            "result": "ok",
            "source": source,
            "url": url,
            "pdf_filename": filename,
        }
        return "ok"
    else:
        print(f"  FAILED: {msg}")
        # Clean up partial file if it exists
        if dest.exists():
            dest.unlink()
        log["fetches"][key] = {
            "last_attempted": str(date.today()),
            "result": "download_failed",
            "source": source,
            "url": url,
            "error": msg,
        }
        return "download_failed"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download PDFs from preprint servers into pdf-staging/."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--id", nargs="+", metavar="ID",
                       help="Direct identifiers: arXiv IDs, DOIs, URLs")
    group.add_argument("--paper", nargs="+", metavar="ID",
                       help="DB paper IDs (stubs or external_owned)")
    group.add_argument("--all-external", action="store_true",
                       help="Download for all external_owned entries lacking PDFs")
    group.add_argument("--all-stubs", action="store_true",
                       help="Download for stubs with DOIs or S2 IDs")

    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be downloaded, no actual downloads")
    parser.add_argument("--force", action="store_true",
                        help="Re-attempt previously failed or succeeded downloads")
    parser.add_argument("--max", type=int, default=50, metavar="N",
                        help="Cap total downloads per run (default: 50)")
    parser.add_argument("--output-dir", type=Path, default=None, metavar="DIR",
                        help="Override output directory (default: pdf-staging/)")
    parser.add_argument("--api-key", default=None,
                        help="S2 API key (overrides project.yaml and env var)")
    args = parser.parse_args()

    api_key = args.api_key or get_s2_api_key()
    print(f"S2_API_KEY: {'found' if api_key else 'not set (1 req/sec)'}")
    if args.dry_run:
        print("DRY RUN — no files will be written")

    output_dir = args.output_dir or DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output dir: {output_dir}")

    # Load DB if needed
    papers = {}
    if args.paper or args.all_external or args.all_stubs:
        if not PAPERS_FILE.exists():
            print(f"ERROR: {PAPERS_FILE} not found", file=sys.stderr)
            sys.exit(1)
        db = json.loads(PAPERS_FILE.read_text())
        papers = db["papers"]
        print(f"Loaded {len(papers):,} papers from DB")

    # Build target list
    if args.id:
        targets = []
        for raw in args.id:
            kind, normalized = classify_identifier(raw)
            if kind == "unknown":
                print(f"WARNING: cannot classify '{raw}' — skipping")
                continue
            targets.append((normalized, kind, None))
    elif args.paper:
        targets = collect_targets_from_db(papers, "paper", args.paper)
    elif args.all_external:
        targets = collect_targets_from_db(papers, "all-external")
    else:  # all_stubs
        targets = collect_targets_from_db(papers, "all-stubs")

    print(f"Targets: {len(targets)}")
    if args.max and len(targets) > args.max:
        print(f"Capping at {args.max} (use --max to change)")
        targets = targets[:args.max]

    log = load_fetch_log()
    counts = {"ok": 0, "skipped": 0, "not_found": 0, "not_open_access": 0,
              "download_failed": 0, "dry_run": 0}

    for i, (key, kind, paper) in enumerate(targets, 1):
        title = paper.get("title", "") if paper else ""
        display = title[:70] if title else key
        print(f"\n[{i}/{len(targets)}] {display}")
        if title and key != display:
            print(f"  ID: {key}")

        result = process_target(
            key=key,
            kind=kind,
            paper=paper,
            output_dir=output_dir,
            log=log,
            api_key=api_key,
            dry_run=args.dry_run,
            force=args.force,
        )
        counts[result] = counts.get(result, 0) + 1

        if not args.dry_run:
            save_fetch_log(log)

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Summary:")
    print(f"  Downloaded:       {counts['ok']}")
    print(f"  Skipped (cached): {counts['skipped']}")
    print(f"  Not found in S2:  {counts['not_found']}")
    print(f"  Not open access:  {counts['not_open_access']}")
    print(f"  Download failed:  {counts['download_failed']}")
    if args.dry_run:
        print(f"  Would download:   {counts['dry_run']}")

    if counts["ok"] > 0 and not args.dry_run:
        print(f"\nNext step: run /ingest to extract and integrate the downloaded PDFs")


if __name__ == "__main__":
    main()
