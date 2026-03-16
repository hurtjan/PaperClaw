#!/usr/bin/env python3
"""
Incremental Step 1: Rank candidates for matching a new extraction against papers.json.

Usage: .venv/bin/python3 scripts/link/link_paper.py data/extractions/{id}.json

Writes: data/tmp/link_candidates.txt
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import find_candidates_indexed, PaperIndex, score_match

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "link_candidates.txt"


def first_author_last(authors):
    if not authors:
        return ""
    first = str(authors[0])
    if ", " in first:
        return first.split(",")[0].strip()
    parts = first.split()
    return parts[-1] if parts else ""


def format_cit_line(cit_id, title, authors, year, journal, doi):
    """Format: cit_id: Title, LastName, Year, Journal [doi:...]"""
    last = first_author_last(authors)
    parts = [p for p in [title, last, str(year) if year else "", journal] if p]
    body = ", ".join(parts)
    if doi:
        body += f" [doi:{doi}]"
    return f"{cit_id}: {body}"


def format_db_paper_inline(paper, score=None):
    """Format: [score ]paper_id: Title, LastName, Year, Journal [doi:...]"""
    pid = paper.get("id", "")
    title = paper.get("title", "")
    authors = paper.get("authors", [])
    year = paper.get("year", "")
    journal = paper.get("journal", "")
    doi = paper.get("doi", "")
    last = first_author_last(authors)
    parts = [p for p in [title, last, str(year) if year else "", journal] if p]
    body = ", ".join(parts)
    if doi:
        body += f" [doi:{doi}]"
    prefix = f"{score} " if score is not None else ""
    return f"{prefix}{pid}: {body}"


def format_candidates_txt(output, papers_by_id):
    lines = []
    lines.append(f"FROM_PAPER: {output['from_paper']}")
    lines.append("")

    auto = output["auto_matched"]
    needs = output["needs_judgment"]
    new = output["new_citations"]
    versions = output["version_candidates"]

    lines.append(f"=== AUTO_MATCHED [{len(auto)}] ===")
    for entry in auto:
        cit_line = format_cit_line(
            entry["citation_id"], entry["citation_title"],
            entry["citation_authors"], entry["citation_year"],
            entry["citation_journal"], entry["citation_doi"],
        )
        cand_paper = papers_by_id.get(entry["candidate_id"], {"id": entry["candidate_id"], "title": ""})
        cand_line = format_db_paper_inline(cand_paper, entry["score"])
        lines.append(f"{cit_line} | {cand_line}")
    lines.append("")

    lines.append(f"=== NEEDS_JUDGMENT [{len(needs)}] ===")
    for entry in needs:
        cit_line = format_cit_line(
            entry["citation_id"], entry["citation_title"],
            entry["citation_authors"], entry["citation_year"],
            entry["citation_journal"], entry["citation_doi"],
        )
        cand_parts = [format_db_paper_inline(c, c.get("score")) for c in entry.get("candidates", [])]
        if cand_parts:
            lines.append(f"{cit_line} | {' | '.join(cand_parts)}")
        else:
            lines.append(cit_line)
    lines.append("")

    lines.append(f"=== NEW [{len(new)}] ===")
    for entry in new:
        lines.append(format_cit_line(
            entry["citation_id"], entry["citation_title"],
            entry["citation_authors"], entry["citation_year"],
            entry["citation_journal"], entry["citation_doi"],
        ))
    lines.append("")

    lines.append(f"=== VERSION_CANDIDATES [{len(versions)}] ===")
    for v in versions:
        existing_id = v["existing_id"]
        paper = papers_by_id.get(existing_id, {
            "id": existing_id,
            "title": v.get("existing_title", ""),
            "authors": v.get("existing_authors", []),
            "year": v.get("existing_year"),
            "journal": "",
        })
        cand_line = format_db_paper_inline(paper)
        lines.append(f"{cand_line} | score={v['score']}, sim={v['title_similarity']}")

    return "\n".join(lines) + "\n"


def main():
    if len(sys.argv) < 2:
        print("Usage: .venv/bin/python3 scripts/link/link_paper.py data/extractions/{id}.json")
        sys.exit(1)

    extraction_path = Path(sys.argv[1])
    if not extraction_path.is_absolute():
        extraction_path = ROOT / extraction_path

    with open(extraction_path) as f:
        extraction = json.load(f)

    if not PAPERS_FILE.exists():
        PAPERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PAPERS_FILE.write_text('{"metadata": {}, "papers": {}}')
    with open(PAPERS_FILE) as f:
        db = json.load(f)

    all_papers = list(db["papers"].values())
    papers_by_id = {p["id"]: p for p in all_papers}
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
                "citation_journal": cit.get("journal", ""),
                "citation_doi": cit.get("doi"),
            })
        elif candidates[0]["score"] > 6:
            top = candidates[0]
            auto_matched.append({
                "citation_id": cit.get("id", ""),
                "citation_title": cit.get("title", ""),
                "citation_year": str(cit.get("year", "")),
                "citation_authors": cit.get("authors", []),
                "citation_journal": cit.get("journal", ""),
                "citation_doi": cit.get("doi"),
                "candidate_id": top["id"],
                "score": top["score"],
            })
        else:
            needs_judgment.append({
                "citation_id": cit.get("id", ""),
                "citation_title": cit.get("title", ""),
                "citation_year": str(cit.get("year", "")),
                "citation_authors": cit.get("authors", []),
                "citation_journal": cit.get("journal", ""),
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
        "auto_matched": auto_matched,
        "needs_judgment": needs_judgment,
        "new_citations": new_citations,
        "version_candidates": version_candidates,
    }

    txt = format_candidates_txt(output, papers_by_id)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(txt)

    print(f"\nResults:")
    print(f"  Auto-matched (score > 6): {len(auto_matched)}")
    print(f"  Needs judgment (score 1-3): {len(needs_judgment)}")
    print(f"  New (no candidates): {len(new_citations)}")
    print(f"  Version candidates: {len(version_candidates)}")
    print(f"Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
