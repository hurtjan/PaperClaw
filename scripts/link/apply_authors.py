#!/usr/bin/env python3
"""
Apply resolved author decisions to authors.json.

Reads:  data/tmp/author_candidates.json, data/tmp/author_resolved.json, data/db/papers.json, data/db/authors.json
Writes: data/db/authors.json
"""

import json
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import export_json

CANDIDATES_FILE = ROOT / "data" / "tmp" / "author_candidates.json"
RESOLVED_FILE = ROOT / "data" / "tmp" / "author_resolved.json"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"


def main():
    with open(CANDIDATES_FILE) as f:
        candidates = json.load(f)
    with open(RESOLVED_FILE) as f:
        resolved = json.load(f)
    with open(PAPERS_FILE) as f:
        papers = json.load(f)["papers"]

    if AUTHORS_FILE.exists():
        with open(AUTHORS_FILE) as f:
            authors_data = json.load(f)
    else:
        authors_data = {"metadata": {}, "persons": {}, "institutions": {}, "processed_papers": []}

    persons = authors_data.get("persons", {})
    institutions = authors_data.get("institutions", {})
    processed = set(authors_data.get("processed_papers", []))

    decisions = resolved.get("decisions", {})
    overrides = resolved.get("overrides", {})
    new_paper_ids = candidates["new_paper_ids"]

    author_map = {}
    for entry in candidates.get("authors", []):
        author_string = entry["author_string"]
        for paper_id in entry["paper_ids"]:
            key = (author_string, paper_id)
            if "auto" in entry:
                author_map[key] = overrides.get(author_string, entry["auto"])
            elif "candidates" in entry:
                if author_string in decisions:
                    author_map[key] = decisions[author_string]
            elif "new" in entry:
                author_map[key] = decisions.get(author_string, entry["new"])

    updated = created = 0
    for (author_string, paper_id), entity_id in author_map.items():
        if entity_id in persons:
            person = persons[entity_id]
            if paper_id not in person["papers"]:
                person["papers"].append(paper_id)
                person["papers"].sort()
                person["paper_count"] = len(person["papers"])
            if author_string not in person["name_variants"]:
                person["name_variants"].append(author_string)
                person["name_variants"].sort()
            paper_type = papers.get(paper_id, {}).get("type")
            if paper_type in ("owned", "external_owned") and paper_id not in person.get("owned_papers", []):
                person.setdefault("owned_papers", []).append(paper_id)
                person["owned_paper_count"] = len(person["owned_papers"])
            if len(author_string.split(",")[-1].strip()) > len(person["canonical_name"].split(",")[-1].strip()):
                person["canonical_name"] = author_string
            updated += 1
        else:
            paper_type = papers.get(paper_id, {}).get("type")
            owned_papers = [paper_id] if paper_type in ("owned", "external_owned") else []
            persons[entity_id] = {
                "id": entity_id, "canonical_name": author_string,
                "name_variants": [author_string], "grouping_key": "",
                "papers": [paper_id], "owned_papers": owned_papers,
                "paper_count": 1, "owned_paper_count": len(owned_papers),
                "coauthors": [],
            }
            created += 1

    # Institutional authors
    for paper_id in new_paper_ids:
        for name_str in papers.get(paper_id, {}).get("authors", []):
            if "," in name_str:
                continue
            norm = name_str.strip()
            if not norm or norm == "et al.":
                continue
            inst_id = re.sub(r"[^a-z0-9_]", "", norm.lower().replace(" ", "_"))
            inst_id = re.sub(r"_+", "_", inst_id).strip("_")
            if not inst_id:
                continue
            if inst_id in institutions:
                if paper_id not in institutions[inst_id]["papers"]:
                    institutions[inst_id]["papers"].append(paper_id)
                    institutions[inst_id]["paper_count"] = len(institutions[inst_id]["papers"])
            else:
                institutions[inst_id] = {
                    "id": inst_id, "name": norm, "type": "institutional",
                    "papers": [paper_id], "paper_count": 1,
                }

    # Rebuild coauthors
    paper_to_authors = defaultdict(set)
    for aid, author in persons.items():
        for pid in author["papers"]:
            paper_to_authors[pid].add(aid)

    affected = set()
    for paper_id in new_paper_ids:
        affected.update(paper_to_authors.get(paper_id, set()))

    for aid in affected:
        coauthor_ids = set()
        for pid in persons[aid]["papers"]:
            coauthor_ids.update(paper_to_authors.get(pid, set()))
        coauthor_ids.discard(aid)
        persons[aid]["coauthors"] = sorted(coauthor_ids)

    processed.update(new_paper_ids)

    authors_data["persons"] = dict(sorted(persons.items()))
    authors_data["institutions"] = dict(sorted(institutions.items()))
    authors_data["processed_papers"] = sorted(processed)
    authors_data["metadata"] = {
        "person_count": len(persons), "institution_count": len(institutions),
        "processed_paper_count": len(processed), "last_updated": str(date.today()),
    }

    export_json(authors_data, AUTHORS_FILE)
    print(f"Updated: {updated}, Created: {created}, Total: {len(persons)} persons, {len(institutions)} institutions")


if __name__ == "__main__":
    main()
