#!/usr/bin/env python3
"""
Incremental Step 1: Rank candidates for matching a new extraction against papers.json.

Usage: .venv/bin/python3 scripts/link/link_paper.py data/extractions/{id}.json

Writes: data/tmp/link_candidates.json
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import find_candidates_indexed, PaperIndex, score_match, export_json

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "link_candidates.json"


def main():
    if len(sys.argv) < 2:
        print("Usage: .venv/bin/python3 scripts/link/link_paper.py data/extractions/{id}.json")
        sys.exit(1)

    extraction_path = Path(sys.argv[1])
    if not extraction_path.is_absolute():
        extraction_path = ROOT / extraction_path

    with open(extraction_path) as f:
        extraction = json.load(f)

    with open(PAPERS_FILE) as f:
        db = json.load(f)

    all_papers = list(db["papers"].values())
    paper_index = PaperIndex(all_papers)
    print(f"Loaded {len(all_papers)} papers from papers.json")

    citations = extraction.get("citations", [])
    print(f"Processing {len(citations)} citations from {extraction['id']}")

    auto_matched = []
    needs_judgment = []
    new_citations = []

    for cit in citations:
        candidates = find_candidates_indexed(cit, paper_index)

        if not candidates:
            new_citations.append({
                "citation_id": cit.get("id", ""),
                "citation_title": cit.get("title", ""),
                "citation_year": str(cit.get("year", "")),
                "citation_authors": cit.get("authors", []),
                "citation_doi": cit.get("doi"),
            })
        elif candidates[0]["score"] > 6:
            top = candidates[0]
            auto_matched.append({
                "citation_id": cit.get("id", ""),
                "citation_title": cit.get("title", ""),
                "candidate_id": top["id"],
                "candidate_title": top["title"],
                "score": top["score"],
                "signals": top["signals"],
            })
        else:
            needs_judgment.append({
                "citation_id": cit.get("id", ""),
                "citation_title": cit.get("title", ""),
                "citation_year": str(cit.get("year", "")),
                "citation_authors": cit.get("authors", []),
                "citation_doi": cit.get("doi"),
                "candidates": candidates,
            })

    # Version detection: check if owned paper matches existing stub
    cited_only_papers = [p for p in all_papers if p.get("type") == "stub"]
    version_candidates = []
    for existing in cited_only_papers:
        s, sigs, sim = score_match(extraction, existing)
        if s >= 2:
            version_candidates.append({
                "existing_id": existing.get("id", ""),
                "score": s, "signals": sigs,
                "title_similarity": round(sim, 3),
                "existing_title": existing.get("title", ""),
                "existing_authors": existing.get("authors", []),
                "existing_year": existing.get("year"),
            })
    version_candidates.sort(key=lambda x: x["score"], reverse=True)

    output = {
        "from_paper": extraction["id"],
        "from_paper_metadata": {
            "id": extraction["id"],
            "title": extraction.get("title", ""),
            "authors": extraction.get("authors", []),
            "year": extraction.get("year"),
            "journal": extraction.get("journal"),
            "doi": extraction.get("doi"),
        },
        "existing_papers_count": len(all_papers),
        "auto_matched": auto_matched,
        "needs_judgment": needs_judgment,
        "new_citations": new_citations,
        "version_candidates": version_candidates,
    }

    export_json(output, OUTPUT_FILE)

    print(f"\nResults:")
    print(f"  Auto-matched (score > 6): {len(auto_matched)}")
    print(f"  Needs judgment (score 1-3): {len(needs_judgment)}")
    print(f"  New (no candidates): {len(new_citations)}")
    print(f"  Version candidates: {len(version_candidates)}")
    print(f"Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
