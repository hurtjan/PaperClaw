#!/usr/bin/env python3
"""
fetch_forward_citations.py — Fetch papers that cite owned papers via Semantic Scholar API.

Discovers forward citations (papers published AFTER the owned paper that cite it),
adds them to data/db/papers.json as stub entries with discovered_via="s2_forward",
and records them in forward_cited_by on the owned paper.

Usage:
  .venv/bin/python3 scripts/enrich/fetch_forward_citations.py --paper ID [ID ...] [options]
  .venv/bin/python3 scripts/enrich/fetch_forward_citations.py --all [options]

Options:
  --force              Re-fetch even if fetched within 30 days
  --dry-run            Simulate without writing to data/db/papers.json
  --max-per-paper N    Cap forward citations per paper (default: unlimited)

Environment:
  S2_API_KEY  — optional Semantic Scholar API key (enables ~8 req/sec vs 1 req/sec)
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

from litdb import (PaperIndex, find_candidates_indexed, normalize_doi, export_json,
                   TITLE_STOP_WORDS, transliterate, normalize_title, derive_author_lastnames,
                   is_owned)

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
S2_FETCH_LOG = ROOT / "data" / "db" / "s2_fetch_log.json"
S2_BASE_URL = "https://api.semanticscholar.org/graph/v1"

# Skip papers fetched within this many days (unless --force)
CACHE_DAYS = 30

# Minimum score to consider an S2 paper a match against an existing DB entry
MATCH_SCORE_THRESHOLD = 3


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


def generate_paper_id(title: str, authors: list, year, existing_ids: set) -> str:
    """
    Generate {first_author_lastname}_{year}_{first_significant_title_word}.
    Appends _2, _3, ... on collision.
    """
    if authors:
        first = str(authors[0])
        lastname = first.split(",")[0].strip() if "," in first else (first.split() or ["unknown"])[-1]
    else:
        lastname = "unknown"

    lastname = re.sub(r"[^a-z0-9]", "_", transliterate(lastname).lower()).strip("_")
    lastname = re.sub(r"_+", "_", lastname) or "unknown"

    yr = str(year).strip() if year else "0000"

    title_word = "paper"
    if title:
        text = re.sub(r"[^\w\s]", " ", transliterate(title).lower())
        text = re.sub(r"\s+", " ", text).strip()
        for word in text.split():
            if word and word not in TITLE_STOP_WORDS and not word.isdigit():
                title_word = word
                break

    base = f"{lastname}_{yr}_{title_word}"
    if base not in existing_ids:
        return base
    for suffix in range(2, 1000):
        candidate = f"{base}_{suffix}"
        if candidate not in existing_ids:
            return candidate
    return f"{base}_x"


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

def process_paper(
    paper_id: str,
    paper: dict,
    papers: dict,
    index: PaperIndex,
    api_key: str | None,
    max_per_paper: int | None,
    dry_run: bool,
    log: dict,
) -> dict:
    """
    Fetch and integrate forward citations for one owned paper.
    Returns stats: {new, matched, failed, s2_paper_id}.
    """
    stats = {"new": 0, "matched": 0, "failed": 0, "s2_paper_id": None}
    doi = normalize_doi(paper.get("doi"))

    print(f"\n[{paper_id}]")

    # Step 1: Resolve → S2 paper ID (DOI fast path, title search fallback)
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
            return stats

        print(f"  S2 paper ID: {s2_paper_id}")
        if not dry_run:
            paper["s2_paper_id"] = s2_paper_id
    else:
        print(f"  S2 paper ID (cached): {s2_paper_id}")

    stats["s2_paper_id"] = s2_paper_id

    # Step 2: Fetch all citing papers from S2
    print(f"  Fetching forward citations from S2...")
    s2_data = fetch_all_citations(s2_paper_id, api_key, max_per_paper)
    print(f"  S2 returned {len(s2_data)} citing paper(s)")

    # Step 3: Deduplicate and integrate into graph
    existing_ids = set(papers.keys())
    cited_by = paper.setdefault("forward_cited_by", [])

    for s2_entry in s2_data:
        record = s2_entry_to_record(s2_entry)

        if not record["title"]:
            stats["failed"] += 1
            continue

        candidates = find_candidates_indexed(record, index, min_score=MATCH_SCORE_THRESHOLD)

        if candidates:
            best = candidates[0]
            matched_id = best["id"]
            matched = papers.get(matched_id, {})

            if not dry_run:
                if record.get("s2_paper_id") and not matched.get("s2_paper_id"):
                    matched["s2_paper_id"] = record["s2_paper_id"]
                if matched.get("discovered_via") == "s2_forward":
                    if paper_id not in matched.get("cites", []):
                        matched.setdefault("cites", []).append(paper_id)
                    if matched_id not in cited_by:
                        cited_by.append(matched_id)

            stats["matched"] += 1

        else:
            new_id = generate_paper_id(record["title"], record["authors"], record["year"], existing_ids)
            existing_ids.add(new_id)

            new_entry = {
                "id": new_id,
                "type": "stub",
                "title": record["title"],
                "authors": record["authors"],
                "year": record["year"],
                "journal": record["journal"],
                "doi": record["doi"],
                "abstract": record.get("abstract") or "",
                "cites": [paper_id],
                "cited_by": [],
                "discovered_via": "s2_forward",
            }
            if record.get("s2_paper_id"):
                new_entry["s2_paper_id"] = record["s2_paper_id"]

            if not dry_run:
                papers[new_id] = new_entry
                if new_id not in cited_by:
                    cited_by.append(new_id)
                index.papers.append(new_entry)
                index.by_id[new_id] = new_entry
                nd = normalize_doi(new_entry.get("doi"))
                if nd:
                    index.by_doi.setdefault(nd, []).append(new_entry)

            stats["new"] += 1

    print(f"  New: {stats['new']}, Matched: {stats['matched']}, Failed: {stats['failed']}")
    print(f"  forward_cited_by total for {paper_id}: {len(cited_by)}")

    log["fetches"][paper_id] = {
        "last_fetched": str(date.today()),
        "result": "ok",
        "s2_paper_id": s2_paper_id,
        "new_added": stats["new"],
        "matched": stats["matched"],
        "cited_by_count": len(cited_by),
    }
    if not dry_run:
        save_fetch_log(log)

    return stats


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
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if already fetched within 30 days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without modifying data/db/papers.json")
    parser.add_argument("--max-per-paper", type=int, default=None, metavar="N",
                        help="Limit forward citations fetched per paper")
    args = parser.parse_args()

    api_key = os.environ.get("S2_API_KEY")
    print(f"S2_API_KEY: {'found (authenticated rate limit)' if api_key else 'not set (1 req/sec)'}")
    if args.dry_run:
        print("DRY RUN — no changes will be written to disk")

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found", file=sys.stderr)
        sys.exit(1)

    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    if args.all:
        target_ids = [pid for pid, p in papers.items() if is_owned(p)]
        print(f"Targeting all {len(target_ids)} owned papers")
    else:
        target_ids = args.paper
        for pid in target_ids:
            if pid not in papers:
                print(f"ERROR: '{pid}' not found in papers.json", file=sys.stderr)
                sys.exit(1)
            if not is_owned(papers[pid]):
                print(f"ERROR: '{pid}' is type '{papers[pid].get('type')}', not 'owned'",
                      file=sys.stderr)
                sys.exit(1)

    log = load_fetch_log()
    total_new = total_matched = total_failed = skipped = 0

    for paper_id in target_ids:
        if not should_fetch(paper_id, log, args.force):
            last = log["fetches"][paper_id].get("last_fetched", "?")
            print(f"[{paper_id}] SKIP: fetched {last} (use --force to re-fetch)")
            skipped += 1
            continue

        index = PaperIndex(list(papers.values()))

        stats = process_paper(
            paper_id=paper_id,
            paper=papers[paper_id],
            papers=papers,
            index=index,
            api_key=api_key,
            max_per_paper=args.max_per_paper,
            dry_run=args.dry_run,
            log=log,
        )
        total_new += stats["new"]
        total_matched += stats["matched"]
        total_failed += stats["failed"]

    if not args.dry_run:
        owned_count = sum(1 for p in papers.values() if is_owned(p))
        stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
        db["metadata"]["last_updated"] = str(date.today())
        db["metadata"]["owned_count"] = owned_count
        db["metadata"]["stub_count"] = stub_count

        export_json(db, PAPERS_FILE,
                    description=f"S2 forward: {total_new} new citations, {total_matched} matched")
        print(f"\nSaved papers.json: {owned_count} owned + {stub_count} stub = "
              f"{owned_count + stub_count} total")

        result_index = __import__("subprocess").run(
            [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
            cwd=ROOT, capture_output=True, text=True,
        )
        if result_index.returncode == 0:
            print("Index rebuilt (data/db/contexts.json)")
        else:
            print(f"WARNING: index rebuild failed:\n{result_index.stderr.strip()}", file=sys.stderr)

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Summary:")
    print(f"  Papers queried:          {len(target_ids) - skipped}")
    print(f"  Papers skipped (cached): {skipped}")
    print(f"  New stub entries:  {total_new}")
    print(f"  Matched existing:        {total_matched}")
    print(f"  Failed (no title/etc):   {total_failed}")


if __name__ == "__main__":
    main()
