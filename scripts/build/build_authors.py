#!/usr/bin/env python3
"""
Build data/db/authors.json from data/db/papers.json.

Usage:
  .venv/bin/python3 scripts/build/build_authors.py
  .venv/bin/python3 scripts/build/build_authors.py --stats
"""

import json
import re
import sys
import argparse
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"

NAME_PREFIXES = {"van", "von", "de", "di", "le", "la", "del", "den", "der", "het", "du"}


def _extract_initials(firstname: str) -> str:
    if not firstname:
        return ""
    parts = re.split(r"[-\s.]+", firstname.strip())
    return "".join(p[0].lower() for p in parts if p)


def parse_author_name(name_str: str) -> dict | None:
    name_str = name_str.strip()
    if not name_str or "," not in name_str:
        return None

    parts = name_str.split(",", 1)
    lastname = parts[0].strip()
    firstname = parts[1].strip() if len(parts) > 1 else ""
    if not lastname:
        return None

    lastname_lower = lastname.lower()
    initials = _extract_initials(firstname)
    first_initial = initials[0] if initials else ""
    key = f"{lastname_lower}_{initials}" if initials else lastname_lower

    clean = firstname.replace(".", "").replace("-", " ").strip()
    first_word = clean.split()[0] if clean else ""
    firstname_part = first_word.lower() if len(first_word) > 1 else initials

    author_id = f"{lastname_lower}_{firstname_part}" if firstname_part else lastname_lower
    author_id = re.sub(r"[^a-z0-9_]", "", author_id.replace(" ", "_").replace("-", "_"))
    author_id = re.sub(r"_+", "_", author_id).strip("_")

    return {
        "lastname": lastname, "firstname": firstname,
        "initials": initials, "key": key,
        "author_id": author_id, "original": name_str,
    }


def build_authors(papers: dict) -> dict:
    groups = defaultdict(list)
    institutions = defaultdict(list)

    for paper_id, paper in papers.items():
        for name_str in paper.get("authors", []):
            parsed = parse_author_name(name_str)
            if parsed is None:
                norm = name_str.strip()
                if norm and norm != "et al.":
                    institutions[norm].append(paper_id)
            else:
                groups[parsed["key"]].append((parsed, paper_id))

    # Merge single-initial keys into more-specific keys when unambiguous
    single_initial_keys = [k for k in groups if re.match(r".+_[a-z]$", k)]
    for short_key in single_initial_keys:
        longer = [k for k in groups if k != short_key and k.startswith(short_key)]
        if len(longer) == 1:
            groups[longer[0]].extend(groups[short_key])
            del groups[short_key]

    persons = {}
    for key, entries in groups.items():
        variants = set()
        paper_ids = set()
        author_ids = set()

        for parsed, paper_id in entries:
            variants.add(parsed["original"])
            paper_ids.add(paper_id)
            author_ids.add(parsed["author_id"])

        best_variant = max(variants, key=lambda v: (len(v.split(",")[-1].strip()), len(v)))
        best_id = max(author_ids, key=len)

        if best_id in persons:
            counter = 2
            base_id = best_id
            while best_id in persons:
                best_id = f"{base_id}_{counter}"
                counter += 1

        paper_list = sorted(paper_ids)
        owned_papers = [pid for pid in paper_list if papers[pid].get("type") in ("owned", "external_owned")]

        persons[best_id] = {
            "id": best_id, "canonical_name": best_variant,
            "name_variants": sorted(variants), "grouping_key": key,
            "papers": paper_list, "owned_papers": owned_papers,
            "paper_count": len(paper_list), "owned_paper_count": len(owned_papers),
            "coauthors": [],
        }

    # Resolve coauthors
    paper_to_authors = defaultdict(set)
    for author_id, author in persons.items():
        for paper_id in author["papers"]:
            paper_to_authors[paper_id].add(author_id)

    for author_id, author in persons.items():
        coauthor_ids = set()
        for paper_id in author["papers"]:
            coauthor_ids.update(paper_to_authors[paper_id])
        coauthor_ids.discard(author_id)
        author["coauthors"] = sorted(coauthor_ids)

    inst_entities = {}
    for name, paper_ids in institutions.items():
        inst_id = re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_").replace("&", "and"))
        inst_id = re.sub(r"_+", "_", inst_id).strip("_")
        if not inst_id:
            continue
        unique_papers = sorted(set(paper_ids))
        inst_entities[inst_id] = {
            "id": inst_id, "name": name, "type": "institutional",
            "papers": unique_papers, "paper_count": len(unique_papers),
        }

    return {
        "metadata": {
            "person_count": len(persons),
            "institution_count": len(inst_entities),
        },
        "persons": dict(sorted(persons.items())),
        "institutions": dict(sorted(inst_entities.items())),
        "processed_papers": sorted(papers.keys()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    papers = json.loads(PAPERS_FILE.read_text())["papers"]
    data = build_authors(papers)

    AUTHORS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(AUTHORS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    m = data["metadata"]
    print(f"Wrote authors.json: {m['person_count']} persons, {m['institution_count']} institutions")

    if args.stats:
        by_count = sorted(data["persons"].values(), key=lambda a: -a["paper_count"])
        print(f"\nTop 15 by paper count:")
        for a in by_count[:15]:
            print(f"  {a['canonical_name']:30s}  {a['paper_count']} papers")


if __name__ == "__main__":
    main()
