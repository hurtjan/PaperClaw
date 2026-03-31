#!/usr/bin/env python3
"""
apply_forward.py — Create stubs for S2 forward citations.

Reads s2_forward_results.json, creates stubs for citing papers not already in
the DB, and wires forward_cited_by/cites edges. Papers already in the DB
(matched by DOI or S2 ID) get edges wired without creating a duplicate stub.

All new stubs are marked dedup_pending=True so /clean-db picks them up.

Reads:  data/tmp/s2_forward_results.json
Writes: data/db/papers.json

Usage:
  .venv/bin/python3 scripts/link/apply_forward.py
"""

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import normalize_doi, export_json, is_owned, generate_paper_id

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
S2_RESULTS_FILE = ROOT / "data" / "tmp" / "s2_forward_results.json"


def _build_lookup(papers: dict) -> tuple[dict[str, str], dict[str, str]]:
    """Build DOI→id and S2 ID→id lookup dicts for exact-match skipping."""
    doi_map: dict[str, str] = {}
    s2_map: dict[str, str] = {}
    for pid, p in papers.items():
        doi = normalize_doi(p.get("doi"))
        if doi:
            doi_map[doi] = pid
        s2 = p.get("s2_paper_id")
        if s2:
            s2_map[s2] = pid
    return doi_map, s2_map


def main():
    if not S2_RESULTS_FILE.exists():
        print(f"ERROR: {S2_RESULTS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    s2_results = json.loads(S2_RESULTS_FILE.read_text())
    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    doi_map, s2_map = _build_lookup(papers)

    print(f"Loaded {len(papers)} papers from papers.json")
    print(f"Processing {len(s2_results)} owned paper(s)")

    total_matched = total_new = total_enriched = 0

    for result in s2_results:
        owned_id = result["owned_paper_id"]
        citing_papers = result.get("citing_papers", [])

        if owned_id not in papers:
            print(f"\nWARNING: owned paper '{owned_id}' not in papers.json — skipping")
            continue

        owned_paper = papers[owned_id]
        cited_by = owned_paper.setdefault("forward_cited_by", [])

        print(f"\n[{owned_id}] {len(citing_papers)} citing paper(s)")

        existing_ids = set(papers.keys())
        paper_matched = paper_new = paper_enriched = 0

        for record in citing_papers:
            s2_id = record.get("s2_paper_id", "")
            if not s2_id:
                continue

            doi = normalize_doi(record.get("doi"))

            # Exact-match: check if already in DB by S2 ID or DOI
            matched_id = s2_map.get(s2_id) or (doi_map.get(doi) if doi else None)

            if matched_id and matched_id in papers:
                matched = papers[matched_id]
                # Enrich with s2_paper_id if not already set
                if s2_id and not matched.get("s2_paper_id"):
                    matched["s2_paper_id"] = s2_id
                    s2_map[s2_id] = matched_id
                    paper_enriched += 1
                # Wire forward_cited_by on owned paper
                if matched_id not in cited_by:
                    cited_by.append(matched_id)
                # Wire cites on the citing paper
                if owned_id not in matched.get("cites", []):
                    matched.setdefault("cites", []).append(owned_id)
                paper_matched += 1
                continue

            # Create stub
            new_id = generate_paper_id(
                record.get("title", ""),
                record.get("authors", []),
                record.get("year"),
                existing_ids,
            )
            existing_ids.add(new_id)

            stub = {
                "id": new_id,
                "type": "stub",
                "title": record.get("title", ""),
                "authors": record.get("authors", []),
                "year": record.get("year"),
                "journal": record.get("journal", ""),
                "doi": normalize_doi(record.get("doi")),
                "abstract": record.get("abstract", ""),
                "cites": [owned_id],
                "cited_by": [],
                "discovered_via": "s2_forward",
                "dedup_pending": True,
            }
            if s2_id:
                stub["s2_paper_id"] = s2_id
                s2_map[s2_id] = new_id

            papers[new_id] = stub
            if new_id not in cited_by:
                cited_by.append(new_id)
            if doi:
                doi_map[doi] = new_id
            paper_new += 1

        print(f"  Matched: {paper_matched}, New stubs: {paper_new}, "
              f"S2 IDs enriched: {paper_enriched}")
        print(f"  forward_cited_by total: {len(cited_by)}")

        total_matched += paper_matched
        total_new += paper_new
        total_enriched += paper_enriched

    # Update metadata
    owned_count = sum(1 for p in papers.values() if is_owned(p))
    stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
    db["metadata"]["last_updated"] = str(date.today())
    db["metadata"]["owned_count"] = owned_count
    db["metadata"]["stub_count"] = stub_count

    export_json(db, PAPERS_FILE,
                description=f"forward citations: {total_new} new stubs, {total_matched} matched")
    print(f"\nUpdated papers.json: {owned_count} owned + {stub_count} stub "
          f"= {owned_count + stub_count} total")
    print(f"Total: matched={total_matched}, new stubs={total_new}, enriched={total_enriched}")

    # Rebuild index
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
        cwd=ROOT, capture_output=True, text=True,
    )
    if result.returncode == 0:
        print("Index rebuilt (data/db/contexts.json)")
    else:
        print(f"ERROR: index rebuild failed:\n{result.stderr.strip()}", file=sys.stderr)

    # Consistency check
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "check_db.py")],
        cwd=ROOT, capture_output=True, text=True,
    )
    print(result.stdout.strip())

    print("\nDONE — forward citations applied.")
    if total_new > 0:
        dedup_count = sum(1 for p in papers.values() if p.get("dedup_pending"))
        print(f"  {dedup_count} papers marked dedup_pending — run /clean-db to deduplicate")


if __name__ == "__main__":
    main()
