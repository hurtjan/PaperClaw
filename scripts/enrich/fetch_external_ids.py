#!/usr/bin/env python3
"""
fetch_external_ids.py — Resolve external identifiers (S2 ID, arXiv, PubMed,
PMC, DOI, open-access URL) via Semantic Scholar and write them back to
papers.json + DuckDB.

Three modes:
  DOI mode (default)    — POST /paper/batch for papers that have a DOI
  Title-search mode     — GET /paper/search for papers without DOI/S2 ID,
                          prioritised by type (owned first) then citation count
  S2-enrich mode        — POST /paper/batch by S2 paper ID for papers that
                          already have an S2 ID but are missing openAccessPdf

Usage:
  python3 scripts/py.py scripts/enrich/fetch_external_ids.py [options]
  python3 scripts/py.py scripts/enrich/fetch_external_ids.py --title-search [options]

Options:
  --title-search    Search by title for papers that have no DOI and no S2 ID
  --s2-enrich       Fetch openAccessPdf + externalIds for papers that have S2 ID
  --force           Re-fetch even for papers that already have s2_paper_id
  --dry-run         Show what would be processed, no writes
  --batch-size N    DOI-mode batch size, max 500 (default 500)
  --limit N         Cap total papers to process per run
  --api-key KEY     S2 API key (overrides project.yaml and env var)

Environment:
  S2_API_KEY  — optional Semantic Scholar API key (higher rate limits)
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
DB_FILE = ROOT / "data" / "db" / "lit.duckdb"
RESULTS_FILE = ROOT / "data" / "tmp" / "s2_external_ids.json"
S2_BASE_URL = "https://api.semanticscholar.org/graph/v1"

ARXIV_DOI_RE = re.compile(r"^10\.48550/arxiv\.(.+)$", re.IGNORECASE)
BIORXIV_DOI_RE = re.compile(r"^10\.1101/", re.IGNORECASE)
SSRN_DOI_RE = re.compile(r"^10\.2139/ssrn\.", re.IGNORECASE)

S2_FIELDS = "externalIds,openAccessPdf"
S2_SEARCH_FIELDS = "paperId,title,authors,year,externalIds,openAccessPdf"

TYPE_PRIORITY = {"owned": 0, "external_owned": 1, "stub": 2}


# ---------------------------------------------------------------------------
# S2 API helpers
# ---------------------------------------------------------------------------

def _s2_request(url, api_key, body=None, retries=3):
    headers = {"Accept": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    if body is not None:
        headers["Content-Type"] = "application/json"
    method = "POST" if body is not None else "GET"
    delay = 5.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 400:
                print(f"  S2 HTTP 400 — skipping")
                return None
            if e.code in (429, 500, 502, 503, 504):
                if attempt < retries - 1:
                    print(f"  S2 HTTP {e.code} — retrying in {delay:.0f}s...")
                    time.sleep(delay)
                    delay *= 2
                    continue
                print(f"  S2 HTTP {e.code} — failed after {retries} retries")
                return None
            raise
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                print(f"  Network error ({e}) — retrying in {delay:.0f}s...")
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return None


def rate_sleep(api_key, batch=False):
    if batch:
        time.sleep(1.0 if api_key else 3.5)
    else:
        time.sleep(0.12 if api_key else 1.05)


# ---------------------------------------------------------------------------
# DOI batch mode
# ---------------------------------------------------------------------------

def s2_batch_request(doi_list, api_key):
    url = f"{S2_BASE_URL}/paper/batch?fields={S2_FIELDS}"
    body = json.dumps({"ids": [f"DOI:{d}" for d in doi_list]}).encode("utf-8")
    result = _s2_request(url, api_key, body=body)
    if isinstance(result, list):
        return result
    return [None] * len(doi_list)


# ---------------------------------------------------------------------------
# Title search mode
# ---------------------------------------------------------------------------

def _title_similarity(a, b):
    from rapidfuzz.fuzz import ratio
    return ratio(a.lower(), b.lower()) / 100.0


def _first_lastname(authors_raw):
    if not authors_raw:
        return ""
    if isinstance(authors_raw, list):
        first = str(authors_raw[0])
    else:
        first = str(authors_raw).split(";")[0].strip()
    if "," in first:
        return first.split(",")[0].strip().lower()
    parts = first.split()
    return parts[-1].lower() if parts else ""


def s2_title_search(paper, api_key):
    """
    Search S2 by title, verify match, return parsed external IDs or None.
    Verification: title similarity >= 0.85 AND (year matches OR author lastname matches).
    """
    title = (paper.get("title") or "").strip()
    if not title:
        return None

    encoded = urllib.parse.quote(title, safe="")
    url = f"{S2_BASE_URL}/paper/search?query={encoded}&fields={S2_SEARCH_FIELDS}&limit=5"
    result = _s2_request(url, api_key)
    rate_sleep(api_key)

    if not result:
        return None

    candidates = result.get("data", []) if isinstance(result, dict) else []
    if not candidates:
        return None

    our_year = str(paper.get("year", "")).strip()
    our_lastname = _first_lastname(paper.get("authors", []))

    for candidate in candidates:
        cand_title = (candidate.get("title") or "").strip()
        if _title_similarity(title, cand_title) < 0.85:
            continue
        cand_year = str(candidate.get("year") or "").strip()
        cand_authors = candidate.get("authors") or []
        cand_lastname = _first_lastname([a.get("name", "") for a in cand_authors])

        year_ok = our_year and cand_year and our_year == cand_year
        author_ok = our_lastname and cand_lastname and our_lastname in cand_lastname

        if year_ok or author_ok:
            return parse_external_ids(candidate)

    return None


# ---------------------------------------------------------------------------
# Parse S2 result
# ---------------------------------------------------------------------------

def parse_external_ids(s2_result):
    if not s2_result:
        return {}

    out = {}
    ext = s2_result.get("externalIds") or {}

    s2_id = s2_result.get("paperId")
    if s2_id:
        out["s2_paper_id"] = s2_id

    arxiv = ext.get("ArXiv")
    if arxiv:
        out["arxiv_id"] = arxiv

    pubmed = ext.get("PubMed")
    if pubmed:
        out["pubmed_id"] = str(pubmed)

    pmc = ext.get("PubMedCentral")
    if pmc:
        out["pmc_id"] = str(pmc)

    dblp = ext.get("DBLP")
    if dblp:
        out["dblp_id"] = dblp

    # DOI found via S2 (useful when paper had none)
    s2_doi = ext.get("DOI")
    if s2_doi:
        out["doi_from_s2"] = s2_doi.lower()

    oa = s2_result.get("openAccessPdf")
    if oa and oa.get("url"):
        out["open_access_url"] = oa["url"]

    doi = (s2_doi or "").lower()
    oa_url = out.get("open_access_url", "")

    if arxiv:
        out["preprint_server"] = "arxiv"
    elif BIORXIV_DOI_RE.match(doi):
        out["preprint_server"] = "medrxiv" if "medrxiv.org" in oa_url else "biorxiv"
    elif SSRN_DOI_RE.match(doi):
        out["preprint_server"] = "ssrn"
    elif "arxiv.org" in oa_url:
        out["preprint_server"] = "arxiv"
    elif "biorxiv.org" in oa_url:
        out["preprint_server"] = "biorxiv"
    elif "medrxiv.org" in oa_url:
        out["preprint_server"] = "medrxiv"

    return out


def extract_arxiv_from_doi(doi):
    m = ARXIV_DOI_RE.match(doi)
    return re.sub(r"v\d+$", "", m.group(1)) if m else None


# ---------------------------------------------------------------------------
# DuckDB patching
# ---------------------------------------------------------------------------

NEW_COLUMNS = [
    ("s2_id", "VARCHAR"),
    ("arxiv_id", "VARCHAR"),
    ("pubmed_id", "VARCHAR"),
    ("pmc_id", "VARCHAR"),
    ("preprint_server", "VARCHAR"),
    ("open_access_url", "VARCHAR"),
]


def patch_duckdb(updates):
    try:
        import duckdb
    except ImportError:
        print("  WARNING: duckdb not installed — skipping DB patch")
        return

    if not DB_FILE.exists():
        print(f"  WARNING: {DB_FILE} not found — skipping DB patch")
        return

    con = duckdb.connect(str(DB_FILE))

    existing = {
        r[0] for r in
        con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='papers'").fetchall()
    }
    for col_name, col_type in NEW_COLUMNS:
        if col_name not in existing:
            con.execute(f"ALTER TABLE papers ADD COLUMN {col_name} {col_type}")
            print(f"  Added column papers.{col_name}")

    if not updates:
        con.close()
        return

    rows = [
        (pid,
         vals.get("s2_paper_id", ""),
         vals.get("arxiv_id", ""),
         vals.get("pubmed_id", ""),
         vals.get("pmc_id", ""),
         vals.get("preprint_server", ""),
         vals.get("open_access_url", ""),
         vals.get("doi_from_s2", ""),
        )
        for pid, vals in updates.items()
    ]

    con.execute("""
        CREATE TEMP TABLE _ext_ids (
            paper_id VARCHAR, s2_id VARCHAR, arxiv_id VARCHAR,
            pubmed_id VARCHAR, pmc_id VARCHAR, preprint_server VARCHAR,
            open_access_url VARCHAR, doi VARCHAR
        )
    """)
    con.executemany("INSERT INTO _ext_ids VALUES (?,?,?,?,?,?,?,?)", rows)
    con.execute("""
        UPDATE papers SET
            s2_id           = NULLIF(e.s2_id, ''),
            arxiv_id        = NULLIF(e.arxiv_id, ''),
            pubmed_id       = NULLIF(e.pubmed_id, ''),
            pmc_id          = NULLIF(e.pmc_id, ''),
            preprint_server = NULLIF(e.preprint_server, ''),
            open_access_url = NULLIF(e.open_access_url, ''),
            doi             = CASE WHEN papers.doi IS NULL AND e.doi != ''
                                   THEN e.doi ELSE papers.doi END
        FROM _ext_ids e
        WHERE papers.paper_id = e.paper_id
    """)
    total_s2 = con.execute("SELECT COUNT(*) FROM papers WHERE s2_id IS NOT NULL").fetchone()[0]
    con.execute("DROP TABLE _ext_ids")
    con.close()
    print(f"  DuckDB: {len(updates)} rows updated, {total_s2} total with s2_id")


def load_citation_counts():
    try:
        import duckdb
        if not DB_FILE.exists():
            return {}
        con = duckdb.connect(str(DB_FILE))
        rows = con.execute("SELECT paper_id, cited_by_count FROM citation_counts").fetchall()
        con.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Resolve external IDs (S2, arXiv, PubMed, DOI) via Semantic Scholar."
    )
    parser.add_argument("--title-search", action="store_true",
                        help="Search by title for papers without DOI/S2 ID")
    parser.add_argument("--s2-enrich", action="store_true",
                        help="Fetch openAccessPdf/externalIds for papers that have s2_paper_id but no open_access_url")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even for papers that already have s2_paper_id")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed, no writes")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="DOI-mode batch size, max 500 (default 500)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total papers to process")
    parser.add_argument("--api-key", default=None,
                        help="S2 API key (overrides project.yaml and env var)")
    args = parser.parse_args()

    api_key = args.api_key or get_s2_api_key()
    print(f"S2_API_KEY: {'found' if api_key else 'not set (1 req/sec)'}")
    if args.dry_run:
        print("DRY RUN — no writes")

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found", file=sys.stderr)
        sys.exit(1)
    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    print(f"Loaded {len(papers):,} papers")

    all_updates = {}

    # ── DOI mode ──────────────────────────────────────────────────────────
    if not args.title_search and not args.s2_enrich:
        arxiv_from_doi = 0
        for pid, p in papers.items():
            doi = p.get("doi", "")
            if doi and not p.get("arxiv_id"):
                aid = extract_arxiv_from_doi(doi)
                if aid:
                    p["arxiv_id"] = aid
                    if not p.get("preprint_server"):
                        p["preprint_server"] = "arxiv"
                    arxiv_from_doi += 1
        if arxiv_from_doi:
            print(f"Extracted {arxiv_from_doi} arXiv IDs from existing arXiv DOIs")

        targets = [
            (pid, normalize_doi(p.get("doi")))
            for pid, p in papers.items()
            if normalize_doi(p.get("doi")) and (args.force or not p.get("s2_paper_id"))
        ]
        print(f"Papers with DOIs needing S2 lookup: {len(targets)}")
        if args.limit:
            targets = targets[:args.limit]

        if not targets:
            print("Nothing to do in DOI mode.")
        elif args.dry_run:
            batch_size = min(args.batch_size, 500)
            print(f"Would fetch {len(targets)} papers in {(len(targets) + batch_size - 1) // batch_size} batches")
        else:
            batch_size = min(args.batch_size, 500)
            total_batches = (len(targets) + batch_size - 1) // batch_size
            found = not_found = 0

            for batch_idx in range(total_batches):
                batch = targets[batch_idx * batch_size:(batch_idx + 1) * batch_size]
                dois = [t[1] for t in batch]
                print(f"\nBatch {batch_idx + 1}/{total_batches} ({len(batch)} papers)...")
                results = s2_batch_request(dois, api_key)

                for (pid, doi), result in zip(batch, results):
                    parsed = parse_external_ids(result)
                    if parsed:
                        p = papers[pid]
                        for k, v in parsed.items():
                            p[k] = v
                        if not p.get("arxiv_id"):
                            aid = extract_arxiv_from_doi(doi)
                            if aid:
                                p["arxiv_id"] = aid
                        if not p.get("preprint_server") and BIORXIV_DOI_RE.match(doi):
                            oa_url = p.get("open_access_url", "")
                            p["preprint_server"] = "medrxiv" if "medrxiv.org" in oa_url else "biorxiv"
                        all_updates[pid] = parsed
                        found += 1
                    else:
                        not_found += 1

                if batch_idx < total_batches - 1:
                    rate_sleep(api_key, batch=True)

            print(f"\nS2 results: {found} found, {not_found} not found")

        for pid, p in papers.items():
            if pid not in all_updates and p.get("arxiv_id"):
                all_updates[pid] = {k: p.get(k, "") for k in
                                    ("s2_paper_id", "arxiv_id", "pubmed_id",
                                     "pmc_id", "preprint_server", "open_access_url")}

    # ── Title-search mode ─────────────────────────────────────────────────
    elif args.title_search:
        if not api_key:
            print("WARNING: --title-search works best with an S2 API key")

        citation_counts = load_citation_counts()

        candidates = [
            (pid, p)
            for pid, p in papers.items()
            if not normalize_doi(p.get("doi"))
            and (args.force or not p.get("s2_paper_id"))
            and (p.get("title") or "").strip()
        ]
        candidates.sort(key=lambda t: (
            TYPE_PRIORITY.get(t[1].get("type", "stub"), 9),
            -citation_counts.get(t[0], 0),
        ))

        print(f"Papers without DOI/S2 needing title search: {len(candidates)}")
        if args.limit:
            candidates = candidates[:args.limit]
            print(f"Capped at {args.limit}")

        if args.dry_run:
            print(f"Would search {len(candidates)} papers (owned/external_owned first)")
            for pid, p in candidates[:10]:
                cited = citation_counts.get(pid, 0)
                print(f"  [{p.get('type','?')} cited:{cited:>4}] {(p.get('title') or '')[:65]}")
            if len(candidates) > 10:
                print(f"  ... and {len(candidates) - 10} more")
            return

        found = not_found = 0
        for i, (pid, p) in enumerate(candidates, 1):
            cited = citation_counts.get(pid, 0)
            print(f"[{i}/{len(candidates)}] {(p.get('title') or '')[:60]} (cited:{cited})")

            parsed = s2_title_search(p, api_key)
            if parsed:
                for k, v in parsed.items():
                    p[k] = v
                if not p.get("doi") and parsed.get("doi_from_s2"):
                    p["doi"] = parsed["doi_from_s2"]
                    print(f"  → DOI: {parsed['doi_from_s2']}")
                all_updates[pid] = parsed
                found += 1
                ids = []
                if parsed.get("s2_paper_id"):
                    ids.append(f"S2:{parsed['s2_paper_id'][:8]}")
                if parsed.get("arxiv_id"):
                    ids.append(f"arXiv:{parsed['arxiv_id']}")
                if parsed.get("pubmed_id"):
                    ids.append(f"PMID:{parsed['pubmed_id']}")
                if parsed.get("open_access_url"):
                    ids.append("OA:yes")
                print(f"  ✓ {' | '.join(ids)}")
            else:
                not_found += 1
                print(f"  – not found")

            if i % 100 == 0:
                export_json(db, PAPERS_FILE)
                patch_duckdb(all_updates)
                all_updates = {}
                print(f"  [checkpoint at {i}]")

        print(f"\nTitle search: {found} found, {not_found} not found")

    # ── S2-enrich mode: batch fetch OA/externalIds by S2 paper ID ──────────
    elif args.s2_enrich:
        targets_enrich = [
            (pid, p["s2_paper_id"])
            for pid, p in papers.items()
            if p.get("s2_paper_id") and (args.force or not p.get("open_access_url"))
        ]
        print(f"Papers with S2 ID missing open_access_url: {len(targets_enrich)}")
        if args.limit:
            targets_enrich = targets_enrich[:args.limit]

        if args.dry_run:
            n_batches = (len(targets_enrich) + 499) // 500
            print(f"Would fetch {len(targets_enrich)} papers in {n_batches} batches")
            return

        batch_size = min(args.batch_size, 500)
        total_batches = (len(targets_enrich) + batch_size - 1) // batch_size
        found = not_found = 0

        for batch_idx in range(total_batches):
            batch = targets_enrich[batch_idx * batch_size:(batch_idx + 1) * batch_size]
            s2_ids = [t[1] for t in batch]
            url = S2_BASE_URL + "/paper/batch?fields=" + S2_FIELDS
            body = json.dumps({"ids": s2_ids}).encode("utf-8")
            print(f"Batch {batch_idx + 1}/{total_batches} ({len(batch)} papers)...")
            results = _s2_request(url, api_key, body=body)
            if not isinstance(results, list):
                results = [None] * len(batch)

            for (pid, s2_id), result in zip(batch, results):
                parsed = parse_external_ids(result)
                if parsed:
                    p = papers[pid]
                    for k, v in parsed.items():
                        if v:
                            p[k] = v
                    all_updates[pid] = parsed
                    found += 1
                else:
                    not_found += 1

            if batch_idx < total_batches - 1:
                rate_sleep(api_key, batch=True)

        print(f"S2-enrich: {found} updated, {not_found} not found")

    # ── Save & patch ──────────────────────────────────────────────────────
    if args.dry_run:
        return

    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    export_json({
        "date": str(date.today()),
        "mode": "title_search" if args.title_search else "doi_batch",
        "updates": {pid: vals for pid, vals in all_updates.items()},
    }, RESULTS_FILE)

    export_json(db, PAPERS_FILE)
    print("papers.json updated")

    print("\nPatching DuckDB...")
    patch_duckdb(all_updates)

    # ── Summary ───────────────────────────────────────────────────────────
    counts = {k: sum(1 for p in papers.values() if p.get(k))
              for k in ("s2_paper_id", "arxiv_id", "pubmed_id", "pmc_id", "preprint_server")}
    servers = {}
    for p in papers.values():
        srv = p.get("preprint_server")
        if srv:
            servers[srv] = servers.get(srv, 0) + 1

    print(f"\nTotal coverage:")
    print(f"  S2 paper ID:     {counts['s2_paper_id']:,}")
    print(f"  arXiv ID:        {counts['arxiv_id']:,}")
    print(f"  PubMed ID:       {counts['pubmed_id']:,}")
    print(f"  PMC ID:          {counts['pmc_id']:,}")
    print(f"  Preprint server: {counts['preprint_server']:,}")
    for srv, cnt in sorted(servers.items(), key=lambda x: -x[1]):
        print(f"    {srv}: {cnt}")

    print(f"\nNext: python3 ../scripts/py.py ../scripts/build/sync_query.py")


if __name__ == "__main__":
    main()
