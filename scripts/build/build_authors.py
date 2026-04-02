#!/usr/bin/env python3
"""
Build data/db/authors.json from data/db/papers.json.

Supports incremental mode: only processes papers added since last build
and merges new author entries into the existing authors.json.
Use --force for a full rebuild.

Usage:
  python3 scripts/py.py scripts/build/build_authors.py
  python3 scripts/py.py scripts/build/build_authors.py --force
  python3 scripts/py.py scripts/build/build_authors.py --stats
"""

import json
import re
import sys
import argparse
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
from litdb import export_json, fast_loads

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"
BUILD_META_FILE = ROOT / "data" / "db" / ".authors_build_meta.json"

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


def _load_build_meta() -> dict:
    """Load stored paper IDs from last build."""
    if BUILD_META_FILE.exists():
        try:
            return fast_loads(BUILD_META_FILE.read_text())
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_build_meta(paper_ids: set):
    """Save processed paper IDs for incremental tracking."""
    BUILD_META_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(BUILD_META_FILE, "w") as f:
        json.dump({"processed_paper_ids": sorted(paper_ids)}, f)


def _incremental_update(papers: dict, new_paper_ids: set, existing_authors: dict) -> dict:
    """Merge authors from new papers into existing authors.json.

    Adds new name variants, papers, and coauthors without rebuilding everything.
    """
    persons = existing_authors.get("persons", {})
    institutions = existing_authors.get("institutions", {})

    # Parse new papers' authors
    new_groups = defaultdict(list)
    new_institutions = defaultdict(list)

    for paper_id in new_paper_ids:
        paper = papers.get(paper_id)
        if not paper:
            continue
        for name_str in paper.get("authors", []):
            parsed = parse_author_name(name_str)
            if parsed is None:
                norm = name_str.strip()
                if norm and norm != "et al.":
                    new_institutions[norm].append(paper_id)
            else:
                new_groups[parsed["key"]].append((parsed, paper_id))

    # Merge into existing persons by grouping_key
    key_to_id = {p["grouping_key"]: aid for aid, p in persons.items() if "grouping_key" in p}

    for key, entries in new_groups.items():
        if key in key_to_id:
            # Update existing author
            aid = key_to_id[key]
            author = persons[aid]
            paper_set = set(author["papers"])
            variant_set = set(author["name_variants"])
            for parsed, paper_id in entries:
                paper_set.add(paper_id)
                variant_set.add(parsed["original"])
            author["papers"] = sorted(paper_set)
            author["name_variants"] = sorted(variant_set)
            author["paper_count"] = len(author["papers"])
            author["owned_papers"] = [pid for pid in author["papers"]
                                       if papers.get(pid, {}).get("type") in ("owned", "external_owned")]
            author["owned_paper_count"] = len(author["owned_papers"])
            author["canonical_name"] = max(variant_set,
                                            key=lambda v: (len(v.split(",")[-1].strip()), len(v)))
        else:
            # New author group
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
            owned_papers = [pid for pid in paper_list if papers.get(pid, {}).get("type") in ("owned", "external_owned")]
            persons[best_id] = {
                "id": best_id, "canonical_name": best_variant,
                "name_variants": sorted(variants), "grouping_key": key,
                "papers": paper_list, "owned_papers": owned_papers,
                "paper_count": len(paper_list), "owned_paper_count": len(owned_papers),
                "coauthors": [],
            }
            key_to_id[key] = best_id

    # Merge institutions
    inst_name_to_id = {inst["name"]: iid for iid, inst in institutions.items()}
    for name, paper_ids in new_institutions.items():
        if name in inst_name_to_id:
            iid = inst_name_to_id[name]
            existing_papers = set(institutions[iid]["papers"])
            existing_papers.update(paper_ids)
            institutions[iid]["papers"] = sorted(existing_papers)
            institutions[iid]["paper_count"] = len(institutions[iid]["papers"])
        else:
            inst_id = re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_").replace("&", "and"))
            inst_id = re.sub(r"_+", "_", inst_id).strip("_")
            if inst_id:
                unique_papers = sorted(set(paper_ids))
                institutions[inst_id] = {
                    "id": inst_id, "name": name, "type": "institutional",
                    "papers": unique_papers, "paper_count": len(unique_papers),
                }

    # Rebuild coauthors (fast: only touch authors with new papers)
    paper_to_authors = defaultdict(set)
    for author_id, author in persons.items():
        for paper_id in author["papers"]:
            paper_to_authors[paper_id].add(author_id)

    touched_authors = set()
    for paper_id in new_paper_ids:
        touched_authors.update(paper_to_authors.get(paper_id, set()))

    for author_id in touched_authors:
        if author_id not in persons:
            continue
        author = persons[author_id]
        coauthor_ids = set()
        for paper_id in author["papers"]:
            coauthor_ids.update(paper_to_authors.get(paper_id, set()))
        coauthor_ids.discard(author_id)
        author["coauthors"] = sorted(coauthor_ids)

    all_paper_ids = set()
    for a in persons.values():
        all_paper_ids.update(a["papers"])
    for inst in institutions.values():
        all_paper_ids.update(inst["papers"])

    return {
        "metadata": {
            "person_count": len(persons),
            "institution_count": len(institutions),
        },
        "persons": dict(sorted(persons.items())),
        "institutions": dict(sorted(institutions.items())),
        "processed_papers": sorted(papers.keys()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--force", action="store_true", help="Full rebuild, ignore cached state")
    args = parser.parse_args()

    papers = fast_loads(PAPERS_FILE.read_text())["papers"]

    # Check for incremental build
    old_meta = {} if args.force else _load_build_meta()
    old_paper_ids = set(old_meta.get("processed_paper_ids", []))
    current_paper_ids = set(papers.keys())
    new_paper_ids = current_paper_ids - old_paper_ids
    removed_paper_ids = old_paper_ids - current_paper_ids

    # If nothing changed at all, skip the rebuild entirely
    if not args.force and not new_paper_ids and not removed_paper_ids and old_paper_ids:
        print(f"No paper changes, skipping author rebuild ({len(current_paper_ids)} papers cached)")
        return

    can_incremental = (
        not args.force
        and AUTHORS_FILE.exists()
        and new_paper_ids
        and not removed_paper_ids
        and old_paper_ids  # must have a previous build
    )

    if can_incremental:
        existing_authors = fast_loads(AUTHORS_FILE.read_text())
        data = _incremental_update(papers, new_paper_ids, existing_authors)
        print(f"Incremental: {len(new_paper_ids)} new papers merged into authors")
    else:
        data = build_authors(papers)
        if not args.force and old_paper_ids:
            print(f"Full rebuild ({len(removed_paper_ids)} papers removed since last build)")
        else:
            print(f"Full rebuild")

    m = data["metadata"]

    # Skip patch tracking for full rebuilds (too expensive)
    track = can_incremental
    export_json(data, AUTHORS_FILE, track=track,
                description=f"build authors.json: {m['person_count']} persons, {m['institution_count']} institutions")

    # Save build metadata
    _save_build_meta(current_paper_ids)

    print(f"Wrote authors.json: {m['person_count']} persons, {m['institution_count']} institutions")

    if args.stats:
        by_count = sorted(data["persons"].values(), key=lambda a: -a["paper_count"])
        print(f"\nTop 15 by paper count:")
        for a in by_count[:15]:
            print(f"  {a['canonical_name']:30s}  {a['paper_count']} papers")


if __name__ == "__main__":
    main()
