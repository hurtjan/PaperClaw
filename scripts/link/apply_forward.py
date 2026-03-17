#!/usr/bin/env python3
"""
apply_forward.py — Apply forward citation link decisions to papers.json.

Reads resolved decisions from forward_resolved.txt + raw S2 data from
s2_forward_results.json, then wires forward_cited_by/cites relationships
and creates stubs for new papers.

Decision logic:
  - s2_id in resolved with match_id  → enrich existing paper, wire relationships
  - s2_id in resolved with "new"     → create stub
  - s2_id NOT in resolved file       → was NEW (no DB candidates) → auto-create stub

Reads:  data/tmp/forward_resolved.txt (optional), data/tmp/s2_forward_results.json
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
RESOLVED_FILE = ROOT / "data" / "tmp" / "forward_resolved.txt"


def parse_resolved(path: Path) -> dict[str, dict[str, str]]:
    """
    Parse forward_resolved.txt.
    Returns: {owned_paper_id: {s2_id: match_paper_id_or_"new"}}
    """
    decisions: dict[str, dict[str, str]] = {}
    current_owned = None

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("OWNED_PAPER:"):
                current_owned = line.split(":", 1)[1].strip()
                decisions.setdefault(current_owned, {})
            else:
                # Strip inline comments
                line = line.split("#")[0].strip()
                if not line:
                    continue
                parts = line.split(",", 1)
                if len(parts) == 2 and current_owned is not None:
                    s2_id_raw = parts[0].strip()
                    match_id = parts[1].strip()
                    # Strip s2: prefix if present
                    s2_id = s2_id_raw[3:] if s2_id_raw.startswith("s2:") else s2_id_raw
                    decisions[current_owned][s2_id] = match_id

    return decisions


def main():
    if not S2_RESULTS_FILE.exists():
        print(f"ERROR: {S2_RESULTS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    # Load resolved decisions (optional — if missing, all citations are treated as NEW)
    if RESOLVED_FILE.exists():
        decisions = parse_resolved(RESOLVED_FILE)
        print(f"Loaded decisions from {RESOLVED_FILE}")
    else:
        decisions = {}
        print("No forward_resolved.txt found — treating all unmatched citations as new stubs")

    s2_results = json.loads(S2_RESULTS_FILE.read_text())
    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

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
        owned_decisions = decisions.get(owned_id, {})

        print(f"\n[{owned_id}] {len(citing_papers)} citing paper(s)")

        existing_ids = set(papers.keys())
        paper_matched = paper_new = paper_enriched = 0

        for record in citing_papers:
            s2_id = record.get("s2_paper_id", "")
            if not s2_id:
                continue

            decision = owned_decisions.get(s2_id)

            if decision and decision != "new":
                # Matched to an existing paper
                matched_id = decision
                if matched_id not in papers:
                    print(f"  WARNING: resolved match '{matched_id}' not in papers.json "
                          f"— creating stub instead (s2:{s2_id})")
                    decision = None  # fall through to stub creation
                else:
                    matched = papers[matched_id]
                    # Enrich with s2_paper_id if not already set
                    if s2_id and not matched.get("s2_paper_id"):
                        matched["s2_paper_id"] = s2_id
                        paper_enriched += 1
                    # Wire forward_cited_by on owned paper
                    if matched_id not in cited_by:
                        cited_by.append(matched_id)
                    # Wire cites on the citing paper
                    if owned_id not in matched.get("cites", []):
                        matched.setdefault("cites", []).append(owned_id)
                    paper_matched += 1
                    continue

            # Create stub: either explicit "new" decision, no decision (was NEW), or
            # matched paper not found
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
            }
            if s2_id:
                stub["s2_paper_id"] = s2_id

            papers[new_id] = stub
            if new_id not in cited_by:
                cited_by.append(new_id)
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
    if total_new > 0 or total_matched > 0:
        print("NEXT: Run .venv/bin/python3 scripts/link/link_authors.py")
    else:
        print("STOP: no new papers added — author linking not needed")


if __name__ == "__main__":
    main()
