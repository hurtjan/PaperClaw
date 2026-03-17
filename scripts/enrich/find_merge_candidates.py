#!/usr/bin/env python3
"""
Find fuzzy-match candidates when merging an external PaperClaw corpus.

Compares external owned papers against local DB using the same scoring logic
as the cross-reference linker. Writes data/tmp/merge_candidates.txt.

Usage:
  .venv/bin/python3 scripts/enrich/find_merge_candidates.py <source_dir>
  .venv/bin/python3 scripts/enrich/find_merge_candidates.py <source_dir> --name <label>

Exit codes:
  0 — no fuzzy candidates (merge can proceed without agent review)
  2 — fuzzy candidates found (agent review required)
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import find_candidates_indexed, PaperIndex

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "merge_candidates.txt"


def first_author_last(authors):
    if not authors:
        return ""
    first = str(authors[0])
    if "," in first:
        return first.split(",")[0].strip()
    parts = first.split()
    return parts[-1] if parts else ""


def format_paper_line(paper_id, paper):
    """Format: paper_id: Title, LastName, Year [doi:...]"""
    title = paper.get("title", "") or ""
    authors = paper.get("authors", [])
    year = paper.get("year")
    doi = paper.get("doi")
    last = first_author_last(authors)
    parts = [p for p in [title, last, str(year) if year else ""] if p]
    body = ", ".join(parts)
    if doi:
        body += f" [doi:{doi}]"
    return f"{paper_id}: {body}"


def main():
    parser = argparse.ArgumentParser(description="Find fuzzy merge candidates")
    parser.add_argument("source_dir", help="Path to external PaperClaw root or db_imports dir")
    parser.add_argument("--name", help="Label for this import (defaults to source dirname)")
    args = parser.parse_args()

    source = Path(args.source_dir).resolve()
    if not source.exists():
        print(f"ERROR: source dir not found: {source}", file=sys.stderr)
        sys.exit(1)

    name = args.name or source.name

    # Source path resolution (same as merge_db.py)
    source_papers_file = source / "data" / "db" / "papers.json"
    if not source_papers_file.exists():
        alt = source / "papers.json"
        if alt.exists():
            source_papers_file = alt

    if not source_papers_file.exists():
        print(f"ERROR: papers.json not found in {source}", file=sys.stderr)
        sys.exit(1)

    source_db = json.loads(source_papers_file.read_text())
    source_papers = source_db.get("papers", {})

    local_db = json.loads(PAPERS_FILE.read_text())
    local_papers = local_db["papers"]
    local_list = list(local_papers.values())
    local_index = PaperIndex(local_list)
    local_by_id = {p["id"]: p for p in local_list if p.get("id")}

    print(f"Local papers:  {len(local_papers)}")
    print(f"Source papers: {len(source_papers)}")

    exact_matches = 0
    auto_matched = []
    needs_judgment = []
    new_papers = []

    for ext_id, ext_paper in source_papers.items():
        # Only fuzzy-check owned papers — stubs are added as-is in the merge loop
        if ext_paper.get("type") != "owned":
            continue

        if ext_id in local_papers:
            exact_matches += 1
            continue

        candidates = find_candidates_indexed(ext_paper, local_index)

        if not candidates:
            new_papers.append(ext_id)
        elif candidates[0]["score"] > 6:
            top = candidates[0]
            local_p = local_by_id.get(top["id"], {
                "id": top["id"], "title": top.get("title", ""),
                "authors": top.get("authors", []), "year": top.get("year"),
            })
            auto_matched.append({
                "ext_id": ext_id,
                "ext_paper": ext_paper,
                "candidate": top,
                "local_paper": local_p,
            })
        else:
            needs_judgment.append({
                "ext_id": ext_id,
                "ext_paper": ext_paper,
                "candidates": candidates,
            })

    # Build output text
    lines = []
    lines.append(f"FROM_SOURCE: {name}")
    lines.append(f"# Exact ID matches (handled automatically): {exact_matches}")
    lines.append(f"# New papers with no local candidates (added automatically): {len(new_papers)}")
    lines.append("")

    lines.append(f"=== AUTO_MATCHED [{len(auto_matched)}] ===")
    lines.append("# score > 6 — verify each. Accept or override to 'new'.")
    for entry in auto_matched:
        ext_id = entry["ext_id"]
        cand = entry["candidate"]
        local_p = entry["local_paper"]
        ext_line = format_paper_line(ext_id, entry["ext_paper"])
        local_line = format_paper_line(local_p.get("id", cand["id"]), local_p)
        signals = ",".join(cand.get("signals", []))
        lines.append(f"{ext_line} | {cand['score']} {local_line} | signals=[{signals}]")
    lines.append("")

    lines.append(f"=== NEEDS_JUDGMENT [{len(needs_judgment)}] ===")
    lines.append("# score 1-6 — decide: match to candidate or mark as 'new'.")
    for entry in needs_judgment:
        ext_id = entry["ext_id"]
        ext_line = format_paper_line(ext_id, entry["ext_paper"])
        cand_parts = []
        for c in entry["candidates"]:
            local_p = local_by_id.get(c["id"], {
                "id": c["id"], "title": c.get("title", ""),
                "authors": c.get("authors", []), "year": c.get("year"),
            })
            cand_parts.append(f"{c['score']} {format_paper_line(c['id'], local_p)}")
        lines.append(f"{ext_line} | {' | '.join(cand_parts)}")
    lines.append("")

    txt = "\n".join(lines) + "\n"
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(txt)

    print(f"\nResults:")
    print(f"  Exact ID matches:         {exact_matches}")
    print(f"  Auto-matched (score > 6): {len(auto_matched)}")
    print(f"  Needs judgment (score 1-6): {len(needs_judgment)}")
    print(f"  New (no candidates):      {len(new_papers)}")
    print(f"\nWritten to {OUTPUT_FILE}")

    if auto_matched or needs_judgment:
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
