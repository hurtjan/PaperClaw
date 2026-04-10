#!/usr/bin/env python3
"""
fetch_forward_citations.py — Fetch papers that cite owned papers via Semantic Scholar API.

Discovers forward citations (papers published AFTER the owned paper that cite it),
saves raw S2 results to data/tmp/s2_forward_results.json, and persists S2 IDs on
owned papers. A subsequent apply_forward.py step creates stubs, then /clean-db
deduplicates.

Usage:
  python3 scripts/py.py scripts/enrich/fetch_forward_citations.py --paper ID [ID ...] [options]
  python3 scripts/py.py scripts/enrich/fetch_forward_citations.py --all [options]

Options:
  --force              Re-fetch even if fetched within 30 days
  --dry-run            Simulate without writing to disk
  --max-per-paper N    Cap forward citations per paper (default: unlimited)

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
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import (normalize_doi, export_json, is_owned, get_s2_api_key)

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
S2_FETCH_LOG = ROOT / "data" / "db" / "s2_fetch_log.json"
S2_RESULTS_FILE = ROOT / "data" / "tmp" / "s2_forward_results.json"
S2_BASE_URL = "https://api.semanticscholar.org/graph/v1"

# Skip papers fetched within this many days (unless --force)
CACHE_DAYS = 30

# ---------------------------------------------------------------------------
# S2 API helpers
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
                print(f"  S2 HTTP 400 (Bad Request) — skipping this request")
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


def fetch_s2_paper_id(doi: str, api_key: str | None) -> str | None:
    """Resolve a DOI to an S2 paper ID. Returns None if not found."""
    encoded = urllib.parse.quote(doi, safe="")
    result = s2_request(f"/paper/DOI:{encoded}?fields=paperId,externalIds,title", api_key)
    if result and result.get("paperId"):
        return result["paperId"]
    return None


def search_s2_by_title(paper: dict, api_key: str | None) -> str | None:
    """
    Search S2 by title and verify the top result matches this paper.
    Returns S2 paper ID if a confident match is found, else None.

    NOTE: The S2 /paper/search endpoint requires an API key for reliable use.
    Without S2_API_KEY, this will almost always be rate-limited (429).

    Verification: title similarity >= 0.85 AND (year matches OR author lastname matches).
    """
    from rapidfuzz.fuzz import ratio as fuzz_ratio

    if not api_key:
        print(f"  NOTE: title search requires S2_API_KEY — set it for reliable results")

    title = paper.get("title", "").strip()
    if not title:
        return None

    encoded = urllib.parse.quote(title, safe="")
    fields = "paperId,title,authors,year,externalIds"
    result = s2_request(f"/paper/search?query={encoded}&fields={fields}&limit=5", api_key)
    if not result:
        return None

    candidates = result.get("data", [])
    if not candidates:
        return None

    paper_year = str(paper.get("year", "")).strip()

    our_authors = paper.get("authors", [])
    our_lastname = ""
    if our_authors:
        first = str(our_authors[0])
        our_lastname = (first.split(",")[0].strip() if "," in first
                        else (first.split() or [""])[-1]).lower()

    for candidate in candidates:
        cand_title = candidate.get("title") or ""
        cand_year = str(candidate.get("year", "")).strip()
        cand_s2_id = candidate.get("paperId", "")

        if not cand_title or not cand_s2_id:
            continue

        title_sim = fuzz_ratio(title.lower(), cand_title.lower()) / 100.0
        if title_sim < 0.85:
            continue

        year_match = paper_year and cand_year and paper_year == cand_year

        cand_authors = candidate.get("authors", [])
        author_match = False
        if our_lastname and cand_authors:
            cand_first_name = cand_authors[0].get("name", "")
            cand_lastname = cand_first_name.rsplit(" ", 1)[-1].lower() if cand_first_name else ""
            author_match = our_lastname == cand_lastname

        if year_match or author_match:
            print(f"  Title search match: '{cand_title[:70]}' "
                  f"(sim={title_sim:.2f}, year={'✓' if year_match else '✗'}, "
                  f"author={'✓' if author_match else '✗'})")
            return cand_s2_id

    return None


def fetch_all_citations(s2_paper_id: str, api_key: str | None, max_results: int | None) -> list[dict]:
    """Fetch all forward citations for an S2 paper ID, paginating as needed."""
    fields = "title,authors,year,abstract,externalIds,journal,publicationVenue,citationCount"
    all_data = []
    offset = 0
    limit = 1000

    while True:
        path = f"/paper/{s2_paper_id}/citations?fields={fields}&offset={offset}&limit={limit}"
        page = s2_request(path, api_key)
        rate_sleep(api_key)

        if not page:
            break
        data = page.get("data", [])
        if not data:
            break

        all_data.extend(data)

        if max_results and len(all_data) >= max_results:
            all_data = all_data[:max_results]
            break

        next_offset = page.get("next")
        if next_offset is None:
            break
        offset = next_offset

    return all_data


# ---------------------------------------------------------------------------
# Data conversion helpers
# ---------------------------------------------------------------------------

def s2_author_to_project(s2_author: dict) -> str:
    """Convert S2 {"name": "Jean-Francois Mercure"} → "Mercure, Jean-Francois"."""
    name = s2_author.get("name", "").strip()
    if not name:
        return name
    parts = name.rsplit(" ", 1)
    return f"{parts[1]}, {parts[0]}" if len(parts) == 2 else name


def s2_entry_to_record(s2_entry: dict) -> dict:
    """Convert one S2 citations-endpoint entry to a project-style dict."""
    citing = s2_entry.get("citingPaper", {})

    authors = [s2_author_to_project(a) for a in citing.get("authors", [])]

    ext_ids = citing.get("externalIds") or {}
    doi = normalize_doi(ext_ids.get("DOI"))

    venue = citing.get("publicationVenue") or {}
    journal_obj = citing.get("journal") or {}
    journal = venue.get("name") or journal_obj.get("name") or ""

    return {
        "title": citing.get("title") or "",
        "authors": authors,
        "year": citing.get("year"),
        "journal": journal,
        "doi": doi,
        "s2_paper_id": citing.get("paperId"),
        "abstract": citing.get("abstract") or "",
    }


# ---------------------------------------------------------------------------
# Fetch log
# ---------------------------------------------------------------------------

def load_fetch_log() -> dict:
    if S2_FETCH_LOG.exists():
        return json.loads(S2_FETCH_LOG.read_text())
    return {"fetches": {}}


def save_fetch_log(log: dict):
    export_json(log, S2_FETCH_LOG)


def should_fetch(paper_id: str, log: dict, force: bool) -> bool:
    if force:
        return True
    entry = log["fetches"].get(paper_id)
    if not entry:
        return True
    last_fetched = entry.get("last_fetched", "")
    if not last_fetched:
        return True
    try:
        last_date = datetime.strptime(last_fetched, "%Y-%m-%d").date()
        return (date.today() - last_date).days >= CACHE_DAYS
    except ValueError:
        return True


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def _resolve_and_persist_s2_id(
    paper_id: str,
    paper: dict,
    api_key: str | None,
    dry_run: bool,
    log: dict,
) -> str | None:
    """
    Resolve S2 paper ID via DOI (fast path) or title search (fallback).
    Persists s2_paper_id to the paper dict if found and not dry_run.
    Returns the S2 paper ID or None if not found.
    """
    doi = normalize_doi(paper.get("doi"))
    s2_paper_id = paper.get("s2_paper_id")

    if not s2_paper_id:
        if doi:
            print(f"  Resolving DOI {doi} → S2 paper ID...")
            s2_paper_id = fetch_s2_paper_id(doi, api_key)
            rate_sleep(api_key)

        if not s2_paper_id:
            if doi:
                print(f"  DOI not found in S2 — falling back to title search...")
            else:
                print(f"  No DOI — searching S2 by title...")
            rate_sleep(api_key)
            s2_paper_id = search_s2_by_title(paper, api_key)
            rate_sleep(api_key)

        if not s2_paper_id:
            print(f"  SKIP: not found in S2 (tried {'DOI + title search' if doi else 'title search'})")
            log["fetches"][paper_id] = {
                "last_fetched": str(date.today()),
                "result": "not_in_s2",
                "new_added": 0,
            }
            save_fetch_log(log)
            return None

        print(f"  S2 paper ID: {s2_paper_id}")
        if not dry_run:
            paper["s2_paper_id"] = s2_paper_id
    else:
        print(f"  S2 paper ID (cached): {s2_paper_id}")

    return s2_paper_id


def process_paper_raw(
    paper_id: str,
    paper: dict,
    api_key: str | None,
    max_per_paper: int | None,
    dry_run: bool,
    log: dict,
) -> dict | None:
    """
    Fetch S2 data for one owned paper (default mode).
    Returns a raw result dict for s2_forward_results.json, or None if not found in S2.
    Persists s2_paper_id on the owned paper dict in place.
    """
    print(f"\n[{paper_id}]")

    s2_paper_id = _resolve_and_persist_s2_id(paper_id, paper, api_key, dry_run, log)
    if not s2_paper_id:
        return None

    print(f"  Fetching forward citations from S2...")
    s2_data = fetch_all_citations(s2_paper_id, api_key, max_per_paper)
    print(f"  S2 returned {len(s2_data)} citing paper(s)")

    citing_papers = []
    failed = 0
    for entry in s2_data:
        record = s2_entry_to_record(entry)
        if record.get("title"):
            citing_papers.append(record)
        else:
            failed += 1

    if failed:
        print(f"  Skipped (no title): {failed}")
    print(f"  Valid records: {len(citing_papers)}")

    log["fetches"][paper_id] = {
        "last_fetched": str(date.today()),
        "result": "ok",
        "s2_paper_id": s2_paper_id,
        "citing_count": len(citing_papers),
    }
    if not dry_run:
        save_fetch_log(log)

    return {
        "owned_paper_id": paper_id,
        "s2_paper_id": s2_paper_id,
        "citing_papers": citing_papers,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch forward citations for owned papers via Semantic Scholar API."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--paper", nargs="+", metavar="ID",
                       help="One or more owned paper IDs to query")
    group.add_argument("--all", action="store_true",
                       help="Query all owned papers")
    parser.add_argument("--stubs", action="store_true",
                        help="Allow stub papers (must have DOI or S2 ID)")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if already fetched within 30 days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without modifying any files")
    parser.add_argument("--max-per-paper", type=int, default=None, metavar="N",
                        help="Limit forward citations fetched per paper")
    parser.add_argument("--api-key", default=None,
                        help="S2 API key (overrides project.yaml and env var)")
    args = parser.parse_args()

    api_key = args.api_key or get_s2_api_key()
    print(f"S2_API_KEY: {'found (authenticated rate limit)' if api_key else 'not set (1 req/sec)'}")
    if args.dry_run:
        print("DRY RUN — no changes will be written to disk")

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found", file=sys.stderr)
        sys.exit(1)

    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    def _eligible(paper):
        if is_owned(paper):
            return True
        if args.stubs and paper.get("type") == "stub":
            if paper.get("doi") or paper.get("semantic_scholar_id"):
                return True
        return False

    if args.all:
        target_ids = [pid for pid, p in papers.items() if _eligible(p)]
        label = "owned" + (" + stubs with DOI/S2ID" if args.stubs else "")
        print(f"Targeting all {len(target_ids)} {label} papers")
    else:
        target_ids = args.paper
        for pid in target_ids:
            if pid not in papers:
                print(f"ERROR: '{pid}' not found in papers.json", file=sys.stderr)
                sys.exit(1)
            if not _eligible(papers[pid]):
                ptype = papers[pid].get("type")
                if ptype == "stub" and not args.stubs:
                    print(f"ERROR: '{pid}' is a stub — use --stubs to allow",
                          file=sys.stderr)
                elif ptype == "stub":
                    print(f"ERROR: '{pid}' is a stub without DOI or S2 ID",
                          file=sys.stderr)
                else:
                    print(f"ERROR: '{pid}' is type '{ptype}', not 'owned'",
                          file=sys.stderr)
                sys.exit(1)

    log = load_fetch_log()
    skipped = 0
    raw_results = []

    for paper_id in target_ids:
        if not should_fetch(paper_id, log, args.force):
            last = log["fetches"][paper_id].get("last_fetched", "?")
            print(f"[{paper_id}] SKIP: fetched {last} (use --force to re-fetch)")
            skipped += 1
            continue

        result = process_paper_raw(
            paper_id=paper_id,
            paper=papers[paper_id],
            api_key=api_key,
            max_per_paper=args.max_per_paper,
            dry_run=args.dry_run,
            log=log,
        )
        if result:
            raw_results.append(result)

    fetched = len(target_ids) - skipped
    total_citing = sum(len(r["citing_papers"]) for r in raw_results)

    if not args.dry_run:
        # Persist S2 IDs added to owned papers during fetch
        owned_count = sum(1 for p in papers.values() if is_owned(p))
        stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
        db["metadata"]["last_updated"] = str(date.today())
        db["metadata"]["owned_count"] = owned_count
        db["metadata"]["stub_count"] = stub_count
        export_json(db, PAPERS_FILE, description="S2 ID resolution for forward citations")

        S2_RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        export_json(raw_results, S2_RESULTS_FILE, track=False)
        print(f"\nSaved {total_citing} citing papers from {len(raw_results)} owned paper(s) "
              f"to {S2_RESULTS_FILE}")
    else:
        print(f"\n[DRY RUN] Would save {total_citing} citing papers from {len(raw_results)} "
              f"owned paper(s) to {S2_RESULTS_FILE}")

    print(f"\nSummary:")
    print(f"  Papers queried:          {fetched}")
    print(f"  Papers skipped (cached): {skipped}")
    print(f"  Total citing papers:     {total_citing}")
    if not args.dry_run and raw_results:
        print(f"\nNEXT: python3 scripts/py.py scripts/link/apply_forward.py")


if __name__ == "__main__":
    main()
