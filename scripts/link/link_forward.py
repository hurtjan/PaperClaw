#!/usr/bin/env python3
"""
link_forward.py — Score forward citation candidates against the DB.

Reads:  data/tmp/s2_forward_results.json
Writes: data/tmp/forward_candidates.txt

Usage:
  .venv/bin/python3 scripts/link/link_forward.py [owned_paper_id ...]
  (with no args, processes all owned papers in s2_forward_results.json)
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import find_candidates_indexed, PaperIndex

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
S2_RESULTS_FILE = ROOT / "data" / "tmp" / "s2_forward_results.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "forward_candidates.txt"

AUTO_MATCH_THRESHOLD = 6  # score > 6 → AUTO_MATCHED


def format_candidates_txt(all_blocks: list[dict]) -> str:
    lines = ["DIRECTION: forward", ""]

    for block in all_blocks:
        owned_id = block["owned_paper_id"]
        auto = block["auto_matched"]
        needs = block["needs_judgment"]
        new = block["new_citations"]

        lines.append(f"OWNED_PAPER: {owned_id}")
        lines.append("")

        lines.append(f"=== AUTO_MATCHED [{len(auto)}] ===")
        for entry in auto:
            lines.append(f"--- [s2:{entry['s2_id']}] \"{entry['title']}\" "
                         f"({entry['year'] or '?'}) ---")
            if entry["authors"]:
                lines.append(f"  Authors: {'; '.join(str(a) for a in entry['authors'][:3])}")
            if entry["journal"]:
                lines.append(f"  Journal: {entry['journal']}")
            if entry["doi"]:
                lines.append(f"  DOI: {entry['doi']}")
            m = entry["best_match"]
            lines.append(f"  → Best match: {m['id']} (score={m['score']})")
            lines.append(f"    Title: \"{m['title']}\"")
            if m.get("authors"):
                lines.append(f"    Authors: {'; '.join(str(a) for a in m['authors'][:2])}")
            if m.get("year"):
                lines.append(f"    Year: {m['year']}")
            lines.append("")

        lines.append(f"=== NEEDS_JUDGMENT [{len(needs)}] ===")
        for entry in needs:
            lines.append(f"--- [s2:{entry['s2_id']}] \"{entry['title']}\" "
                         f"({entry['year'] or '?'}) ---")
            if entry["authors"]:
                lines.append(f"  Authors: {'; '.join(str(a) for a in entry['authors'][:3])}")
            if entry["journal"]:
                lines.append(f"  Journal: {entry['journal']}")
            if entry["doi"]:
                lines.append(f"  DOI: {entry['doi']}")
            for i, cand in enumerate(entry["candidates"], 1):
                lines.append(f"  Candidate {i}: {cand['id']} (score={cand['score']})")
                lines.append(f"    Title: \"{cand['title']}\"")
                if cand.get("authors"):
                    lines.append(f"    Authors: {'; '.join(str(a) for a in cand['authors'][:2])}")
                if cand.get("year"):
                    lines.append(f"    Year: {cand['year']}")
            lines.append("")

        lines.append(f"=== NEW [{len(new)}] ===")
        for entry in new:
            lines.append(f"--- [s2:{entry['s2_id']}] \"{entry['title']}\" "
                         f"({entry['year'] or '?'}) ---")
            if entry["authors"]:
                lines.append(f"  Authors: {'; '.join(str(a) for a in entry['authors'][:3])}")
            lines.append(f"  (no DB matches found)")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def main():
    target_ids = set(sys.argv[1:]) if len(sys.argv) > 1 else None

    if not S2_RESULTS_FILE.exists():
        print(f"ERROR: {S2_RESULTS_FILE} not found. "
              f"Run fetch_forward_citations.py first.", file=sys.stderr)
        sys.exit(1)

    if not PAPERS_FILE.exists():
        print(f"ERROR: {PAPERS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    s2_results = json.loads(S2_RESULTS_FILE.read_text())
    db = json.loads(PAPERS_FILE.read_text())
    all_papers = list(db["papers"].values())
    index = PaperIndex(all_papers)

    print(f"Loaded {len(all_papers)} papers from papers.json")
    print(f"Processing {len(s2_results)} owned paper(s) from s2_forward_results.json")

    if target_ids:
        s2_results = [r for r in s2_results if r["owned_paper_id"] in target_ids]
        print(f"Filtered to {len(s2_results)} target paper(s)")

    all_blocks = []
    total_auto = total_needs = total_new = 0

    for result in s2_results:
        owned_id = result["owned_paper_id"]
        citing_papers = result.get("citing_papers", [])

        print(f"\n[{owned_id}] {len(citing_papers)} citing paper(s) to classify")

        auto_matched = []
        needs_judgment = []
        new_citations = []

        for record in citing_papers:
            s2_id = record.get("s2_paper_id", "")
            if not s2_id:
                continue

            candidates = find_candidates_indexed(record, index)

            if not candidates:
                new_citations.append({
                    "s2_id": s2_id,
                    "title": record.get("title", ""),
                    "authors": record.get("authors", []),
                    "year": record.get("year"),
                    "journal": record.get("journal", ""),
                    "doi": record.get("doi"),
                })
            elif candidates[0]["score"] > AUTO_MATCH_THRESHOLD:
                auto_matched.append({
                    "s2_id": s2_id,
                    "title": record.get("title", ""),
                    "authors": record.get("authors", []),
                    "year": record.get("year"),
                    "journal": record.get("journal", ""),
                    "doi": record.get("doi"),
                    "best_match": candidates[0],
                })
            else:
                needs_judgment.append({
                    "s2_id": s2_id,
                    "title": record.get("title", ""),
                    "authors": record.get("authors", []),
                    "year": record.get("year"),
                    "journal": record.get("journal", ""),
                    "doi": record.get("doi"),
                    "candidates": candidates[:3],
                })

        print(f"  AUTO_MATCHED: {len(auto_matched)}, "
              f"NEEDS_JUDGMENT: {len(needs_judgment)}, "
              f"NEW: {len(new_citations)}")

        total_auto += len(auto_matched)
        total_needs += len(needs_judgment)
        total_new += len(new_citations)

        all_blocks.append({
            "owned_paper_id": owned_id,
            "auto_matched": auto_matched,
            "needs_judgment": needs_judgment,
            "new_citations": new_citations,
        })

    txt = format_candidates_txt(all_blocks)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(txt)

    print(f"\nTotal: AUTO_MATCHED={total_auto}, NEEDS_JUDGMENT={total_needs}, NEW={total_new}")
    print(f"Written to {OUTPUT_FILE}")

    if total_auto > 0 or total_needs > 0:
        print("NEXT: forward-citation-linker")
    else:
        print("STOP: no candidates to review — all citations are NEW (will be auto-created as stubs)")
        print("Run: .venv/bin/python3 scripts/link/apply_forward.py")


if __name__ == "__main__":
    main()
