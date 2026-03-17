#!/usr/bin/env python3
"""
Incremental author linking: score unprocessed authors against existing entities.

Usage: .venv/bin/python3 scripts/link/link_authors.py [--paper PAPER_ID ...]
Writes: data/tmp/author_candidates.json, data/tmp/author_candidates.txt
"""

import json
import re
import sys
import argparse
from collections import defaultdict
from rapidfuzz.fuzz import ratio as rapidfuzz_ratio
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import export_json

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "author_candidates.json"
TXT_OUTPUT_FILE = ROOT / "data" / "tmp" / "author_candidates.txt"


def _extract_initials(firstname: str) -> str:
    if not firstname:
        return ""
    parts = re.split(r"[-\s.]+", firstname.strip())
    return "".join(p[0].lower() for p in parts if p)


def parse_author(name_str: str) -> dict | None:
    name_str = name_str.strip()
    if not name_str or "," not in name_str:
        return None
    parts = name_str.split(",", 1)
    lastname = parts[0].strip()
    firstname = parts[1].strip() if len(parts) > 1 else ""
    if not lastname:
        return None
    initials = _extract_initials(firstname)
    clean = firstname.replace(".", "").replace("-", " ").strip()
    first_word = clean.split()[0] if clean else ""
    return {
        "original": name_str, "lastname": lastname,
        "lastname_lower": lastname.lower(), "firstname": firstname,
        "full_firstname": first_word.lower() if len(first_word) > 1 else "",
        "initials": initials,
    }


def score_author_match(parsed: dict, existing: dict) -> tuple[int, list[str]]:
    score = 0
    signals = []

    p_ln = parsed["lastname_lower"]
    e_name = existing.get("canonical_name", "")
    e_ln = e_name.split(",")[0].strip().lower() if "," in e_name else existing.get("id", "").split("_")[0]

    if p_ln != e_ln:
        return 0, []

    score += 1
    signals.append("lastname_match")

    p_init = parsed["initials"]
    e_initials = set()
    for variant in existing.get("name_variants", []):
        vp = parse_author(variant)
        if vp:
            e_initials.add(vp["initials"])

    if p_init and e_initials:
        if p_init in e_initials:
            score += 2
            signals.append("initials_exact")
        elif any(p_init.startswith(ei) or ei.startswith(p_init) for ei in e_initials):
            score += 1
            signals.append("initials_prefix")
        else:
            return 0, []

    p_fn = parsed["full_firstname"]
    if p_fn:
        e_fns = set()
        for variant in existing.get("name_variants", []):
            vp = parse_author(variant)
            if vp and vp["full_firstname"]:
                e_fns.add(vp["full_firstname"])
        if e_fns:
            if p_fn in e_fns:
                score += 2
                signals.append("firstname_match")
            elif any(rapidfuzz_ratio(p_fn, efn) > 85 for efn in e_fns):
                score += 1
                signals.append("firstname_similar")
            else:
                score -= 1
                signals.append("firstname_conflict")

    return score, signals


def _make_pseudo_entity(entry: dict) -> dict:
    return {
        "id": entry["_suggested_id"],
        "canonical_name": entry["author_string"],
        "name_variants": [entry["author_string"]],
    }


def format_author_candidates_txt(new_paper_ids, auto_matched_map, batch_grouped_map,
                                  needs_judgment_map, new_authors_map,
                                  existing_persons, papers):
    lines = []
    lines.append(f"NEW_PAPERS: {', '.join(new_paper_ids)}")
    lines.append("")

    def get_venues(person, max_venues=5):
        venues = []
        seen = set()
        for pid in person.get("papers", []):
            j = papers.get(pid, {}).get("journal", "")
            if j and j not in seen:
                venues.append(j)
                seen.add(j)
            if len(venues) >= max_venues:
                break
        return venues

    def resolve_coauthors(coauthor_ids, max_n=5):
        names = []
        for cid in coauthor_ids[:max_n]:
            p = existing_persons.get(cid)
            names.append(p["canonical_name"] if p else cid)
        return names

    # ── AUTO_MATCHED ──────────────────────────────────────────────────────────
    lines.append(f"=== AUTO_MATCHED [{len(auto_matched_map)}] ===")
    for entry in auto_matched_map.values():
        cand = entry["candidate"]
        score_str = f"score: {cand['score']}, {'+'.join(cand['signals'])}"
        papers_str = ", ".join(entry["paper_ids"])
        lines.append(f"{entry['author_string']} (papers: {papers_str}) -> {cand['author_id']} [{score_str}]")
        person = existing_persons.get(cand["author_id"])
        if person:
            variants_str = " | ".join(person.get("name_variants", [])) + f" | {person.get('paper_count', 0)} papers"
            lines.append(f"  Variants: {variants_str}")
            coauthor_names = resolve_coauthors(person.get("coauthors", []))
            if coauthor_names:
                lines.append(f"  Coauthors: {'; '.join(coauthor_names)}")
            venues = get_venues(person)
            if venues:
                lines.append(f"  Venues: {', '.join(venues)}")
    lines.append("")

    # ── BATCH_GROUPED ─────────────────────────────────────────────────────────
    lines.append(f"=== BATCH_GROUPED [{len(batch_grouped_map)}] ===")
    for entry in batch_grouped_map.values():
        score_str = f"score: {entry['_score']}, {'+'.join(entry['_signals'])}"
        papers_str = ", ".join(entry["paper_ids"])
        lines.append(f"{entry['author_string']} (papers: {papers_str}) -> {entry['_suggested_id']} [{score_str}]")
        lines.append(f"  Grouped with: {entry['_primary_string']}")
    lines.append("")

    # ── NEEDS_JUDGMENT ────────────────────────────────────────────────────────
    lines.append(f"=== NEEDS_JUDGMENT [{len(needs_judgment_map)}] ===")
    for entry in needs_judgment_map.values():
        papers_str = ", ".join(entry["paper_ids"])
        lines.append(f"{entry['author_string']} (papers: {papers_str})")

        new_coauthors = set()
        for pid in entry["paper_ids"]:
            for a in papers.get(pid, {}).get("authors", []):
                if a.strip() != entry["author_string"]:
                    new_coauthors.add(a.strip())
        if new_coauthors:
            lines.append(f"  New paper coauthors: {'; '.join(sorted(new_coauthors))}")

        for i, cand in enumerate(entry["candidates"], 1):
            author_id = cand["author_id"]
            score_str = f"score: {cand['score']}, {'+'.join(cand['signals'])}"
            lines.append(f"  Candidate {i}: {cand['canonical_name']} ({author_id}) [{score_str}]")
            person = existing_persons.get(author_id)
            if person:
                variants_str = " | ".join(person.get("name_variants", [])) + f" | {person.get('paper_count', 0)} papers"
                lines.append(f"    Variants: {variants_str}")

                cand_coauthor_names = set()
                for cid in person.get("coauthors", []):
                    cp = existing_persons.get(cid)
                    if cp:
                        cand_coauthor_names.add(cp["canonical_name"])
                        for v in cp.get("name_variants", []):
                            cand_coauthor_names.add(v)

                overlap = sorted(new_coauthors & cand_coauthor_names)
                non_overlap_ids = [c for c in person.get("coauthors", [])
                                   if existing_persons.get(c, {}).get("canonical_name") not in overlap]

                if overlap:
                    lines.append(f"    Coauthor overlap: {'; '.join(overlap)}")
                else:
                    lines.append(f"    Coauthor overlap: (none)")

                other_names = resolve_coauthors(non_overlap_ids)
                if other_names:
                    lines.append(f"    Other coauthors: {'; '.join(other_names)}")

                venues = get_venues(person)
                if venues:
                    lines.append(f"    Venues: {', '.join(venues)}")
    lines.append("")

    # ── NEW ───────────────────────────────────────────────────────────────────
    lines.append(f"=== NEW [{len(new_authors_map)}] ===")
    for entry in new_authors_map.values():
        papers_str = ", ".join(entry["paper_ids"])
        covers = entry.get("_batch_covers", [])
        if covers:
            lines.append(
                f"{entry['author_string']} (papers: {papers_str}) -> suggested: {entry['_suggested_id']}"
                f" [BATCH PRIMARY: also covers {', '.join(covers)}]"
            )
        else:
            lines.append(f"{entry['author_string']} (papers: {papers_str}) -> suggested: {entry['_suggested_id']}")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", nargs="+", metavar="PAPER_ID")
    args = parser.parse_args()

    if not PAPERS_FILE.exists():
        PAPERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PAPERS_FILE.write_text('{"metadata": {}, "papers": {}}')
    with open(PAPERS_FILE) as f:
        papers = json.load(f)["papers"]

    authors_data = None
    existing_persons = {}
    if AUTHORS_FILE.exists():
        with open(AUTHORS_FILE) as f:
            authors_data = json.load(f)
        existing_persons = authors_data.get("persons", {})

    if args.paper:
        new_paper_ids = [pid for pid in args.paper if pid in papers]
    elif authors_data:
        tracked = set(authors_data.get("processed_papers", []))
        new_paper_ids = [pid for pid in papers if pid not in tracked]
    else:
        new_paper_ids = list(papers.keys())

    if not new_paper_ids:
        print("No new papers to process. STOP — pipeline complete.")
        return

    print(f"Processing {len(new_paper_ids)} paper(s) against {len(existing_persons)} existing authors")

    lastname_index = defaultdict(list)
    for aid, author in existing_persons.items():
        cname = author.get("canonical_name", "")
        if "," in cname:
            lastname_index[cname.split(",")[0].strip().lower()].append(author)

    auto_matched_map = {}
    needs_judgment_map = {}
    new_authors_map = {}

    for paper_id in new_paper_ids:
        for name_str in papers[paper_id].get("authors", []):
            parsed = parse_author(name_str)
            if parsed is None:
                continue

            candidates = []
            for existing in lastname_index.get(parsed["lastname_lower"], []):
                score, signals = score_author_match(parsed, existing)
                if score > 0:
                    candidates.append({
                        "author_id": existing["id"],
                        "canonical_name": existing["canonical_name"],
                        "paper_count": existing["paper_count"],
                        "score": score, "signals": signals,
                    })
            candidates.sort(key=lambda x: -x["score"])

            if not candidates:
                if name_str not in new_authors_map:
                    new_authors_map[name_str] = {"author_string": name_str, "paper_ids": [],
                                                  "lastname": parsed["lastname"],
                                                  "lastname_lower": parsed["lastname_lower"],
                                                  "initials": parsed["initials"],
                                                  "full_firstname": parsed["full_firstname"]}
                new_authors_map[name_str]["paper_ids"].append(paper_id)
            elif candidates[0]["score"] >= 3:
                if name_str not in auto_matched_map:
                    auto_matched_map[name_str] = {"author_string": name_str, "paper_ids": [],
                                                   "candidate": candidates[0]}
                auto_matched_map[name_str]["paper_ids"].append(paper_id)
            else:
                if name_str not in needs_judgment_map:
                    needs_judgment_map[name_str] = {"author_string": name_str, "paper_ids": [],
                                                     "candidates": candidates[:5]}
                needs_judgment_map[name_str]["paper_ids"].append(paper_id)

    # Generate suggested IDs for new authors
    existing_ids = set(existing_persons.keys())
    taken: set[str] = set()
    for entry in new_authors_map.values():
        lastname = entry["lastname"].lower()
        suffix = entry["full_firstname"] or entry["initials"] or "unknown"
        base = f"{lastname}_{suffix}"
        candidate_id = base
        n = 2
        while candidate_id in existing_ids or candidate_id in taken:
            candidate_id = f"{base}_{n}"
            n += 1
        entry["_suggested_id"] = candidate_id
        taken.add(candidate_id)

    # Within-batch self-matching (greedy primary assignment)
    lastname_groups: dict[str, list] = defaultdict(list)
    for entry in new_authors_map.values():
        lastname_groups[entry["lastname"].lower()].append(entry)

    batch_grouped_map: dict[str, dict] = {}

    for group in lastname_groups.values():
        if len(group) < 2:
            continue
        # Most informative first: longest full_firstname, then most paper_ids
        group.sort(key=lambda e: (-len(e.get("full_firstname", "")), -len(e["paper_ids"])))

        primaries: list[dict] = []
        to_delete: list[str] = []

        for entry in group:
            best_score = 0
            best_primary = None
            best_signals: list[str] = []
            for primary in primaries:
                pseudo = _make_pseudo_entity(primary)
                s, sigs = score_author_match(entry, pseudo)
                if s > best_score:
                    best_score = s
                    best_primary = primary
                    best_signals = sigs

            if best_primary is not None and best_score >= 3 and "firstname_conflict" not in best_signals:
                name_str = entry["author_string"]
                batch_grouped_map[name_str] = {
                    "author_string": name_str,
                    "paper_ids": list(entry["paper_ids"]),
                    "_batch_grouped": True,
                    "_suggested_id": best_primary["_suggested_id"],
                    "_primary_string": best_primary["author_string"],
                    "_score": best_score,
                    "_signals": best_signals,
                }
                best_primary["paper_ids"] = best_primary["paper_ids"] + entry["paper_ids"]
                best_primary.setdefault("_batch_covers", []).append(name_str)
                to_delete.append(name_str)
            else:
                primaries.append(entry)

        for name_str in to_delete:
            del new_authors_map[name_str]

    # Assemble JSON output: regular_auto + needs_judgment + new + batch_grouped_auto
    # (new must precede batch_grouped so primary entities exist before secondaries update them)
    authors_out = []
    for entry in auto_matched_map.values():
        authors_out.append({"author_string": entry["author_string"],
                           "paper_ids": entry["paper_ids"], "auto": entry["candidate"]["author_id"]})
    for entry in needs_judgment_map.values():
        compact = [f"{c['author_id']}: {c['canonical_name']} ({c['paper_count']} papers)"
                   for c in entry["candidates"]]
        authors_out.append({"author_string": entry["author_string"],
                           "paper_ids": entry["paper_ids"], "candidates": compact})
    for entry in new_authors_map.values():
        authors_out.append({"author_string": entry["author_string"],
                           "paper_ids": entry["paper_ids"], "new": entry["_suggested_id"]})
    for entry in batch_grouped_map.values():
        authors_out.append({"author_string": entry["author_string"],
                           "paper_ids": entry["paper_ids"], "auto": entry["_suggested_id"],
                           "_batch_grouped": True})

    export_json({"new_paper_ids": new_paper_ids, "authors": authors_out}, OUTPUT_FILE)

    txt = format_author_candidates_txt(
        new_paper_ids, auto_matched_map, batch_grouped_map,
        needs_judgment_map, new_authors_map, existing_persons, papers
    )
    TXT_OUTPUT_FILE.write_text(txt)

    print(f"Auto: {len(auto_matched_map)}, Batch-grouped: {len(batch_grouped_map)}, "
          f"Judgment: {len(needs_judgment_map)}, New: {len(new_authors_map)}")
    print(f"Written to {TXT_OUTPUT_FILE}")
    print("NEXT: Use the Read tool to read data/tmp/author_candidates.txt")


if __name__ == "__main__":
    main()
