#!/usr/bin/env python3
"""
Bootstrap Stage 1: Read extraction JSONs, group citation candidates.

Outputs: data/tmp/candidates.json
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
OUTPUT_FILE = ROOT / "data" / "tmp" / "candidates.json"

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import normalize_doi, score_match


def main():
    extractions = []
    for path in sorted(EXTRACTIONS_DIR.glob("*.json")):
        name = path.name
        if any(x in name for x in ['.contexts.', '.analysis.', '.sections.', '.refs.']):
            continue
        with open(path) as f:
            data = json.load(f)
        data["_extraction_file"] = path.name
        extractions.append(data)

    print(f"Loaded {len(extractions)} extraction files")

    owned_papers = [{
        "id": ext["id"],
        "title": ext.get("title", ""),
        "authors": ext.get("authors", []),
        "year": ext.get("year"),
        "journal": ext.get("journal", ""),
        "doi": normalize_doi(ext.get("doi")),
        "abstract": ext.get("abstract", ""),
        "source_file": ext.get("source_file", ""),
        "extraction_file": f"data/extractions/{ext['_extraction_file']}",
    } for ext in extractions]

    all_citations = []
    for ext in extractions:
        from_id = ext["id"]
        for cit in ext.get("citations", []):
            all_citations.append({
                "from_paper": from_id,
                "id": cit.get("id", ""),
                "citation_key": cit.get("citation_key", ""),
                "authors": cit.get("authors", []),
                "author_lastnames": cit.get("author_lastnames", []),
                "year": str(cit.get("year", "")).strip(),
                "title": cit.get("title", ""),
                "title_normalized": cit.get("title_normalized", ""),
                "journal": cit.get("journal", ""),
                "doi": normalize_doi(cit.get("doi")),
            })

    print(f"Total raw citations: {len(all_citations)}")

    by_id = defaultdict(list)
    for cit in all_citations:
        if cit["id"]:
            by_id[cit["id"]].append(cit)

    # Secondary conflict detection
    secondary_conflicts = set()

    by_author_year = defaultdict(list)
    for cit in all_citations:
        if cit.get("author_lastnames") and cit["year"]:
            key = f"{cit['author_lastnames'][0]}_{cit['year']}"
            by_author_year[key].append(cit["id"])

    for key, ids in by_author_year.items():
        if len(set(ids)) > 1:
            secondary_conflicts.update(ids)

    by_doi = defaultdict(list)
    for cit in all_citations:
        if cit["doi"]:
            by_doi[cit["doi"]].append(cit["id"])
    for doi, ids in by_doi.items():
        if len(set(ids)) > 1:
            secondary_conflicts.update(ids)

    # Build groups
    candidate_groups = []
    singletons = []

    for cit_id, entries in by_id.items():
        from_papers = list(set(e["from_paper"] for e in entries))
        needs_resolution = cit_id in secondary_conflicts
        group = {
            "id": cit_id,
            "cited_from": from_papers,
            "count": len(entries),
            "needs_resolution": needs_resolution,
            "representative": max(entries, key=lambda e: len(e.get("title", ""))),
            "all_entries": entries,
        }
        if len(from_papers) > 1 or needs_resolution:
            candidate_groups.append(group)
        else:
            singletons.append(group)

    print(f"Unique citation IDs: {len(by_id)}")
    print(f"Candidate groups: {len(candidate_groups)}")
    print(f"Singletons: {len(singletons)}")
    print(f"Secondary conflicts: {len(secondary_conflicts)}")

    output = {
        "owned_papers": owned_papers,
        "candidate_groups": candidate_groups,
        "singletons": singletons,
        "stats": {
            "owned_count": len(owned_papers),
            "total_raw_citations": len(all_citations),
            "unique_citation_ids": len(by_id),
            "candidate_groups": len(candidate_groups),
            "singletons": len(singletons),
            "secondary_conflicts": len(secondary_conflicts),
        },
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
