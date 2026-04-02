#!/usr/bin/env python3
"""Repair stale alias references and re-enrich canonicals from their aliases.

Usage: python3 scripts/py.py scripts/build/repair_aliases.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))

from litdb import fast_loads, export_json
from db import _build_alias_remap, resolve_aliases_in_edges, repair_bidi_sql

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"

ENRICHABLE_FIELDS = (
    "doi", "s2_paper_id", "forward_cited_by", "abstract",
    "journal", "authors", "year", "title",
)
TYPE_PRIORITY = {"owned": 3, "external_owned": 2, "stub": 1}


def _reenrich_canonicals(papers, dry_run=False):
    """Re-enrich canonical papers from their aliases.

    Fills missing metadata fields and upgrades type (stub → external_owned)
    when an alias has richer data. Returns (enriched_count, type_upgrades).
    """
    enriched_count = 0
    type_upgrades = 0
    for pid, p in papers.items():
        aliases = p.get("aliases", [])
        if not aliases:
            continue
        changed = False
        # Field enrichment
        for f in ENRICHABLE_FIELDS:
            if p.get(f) is not None and p.get(f) != "" and p.get(f) != []:
                continue
            for alias_id in aliases:
                alias = papers.get(alias_id, {})
                val = alias.get(f)
                if val is not None and val != "" and val != []:
                    if not dry_run:
                        p[f] = val
                    changed = True
                    break
        # Type upgrade (stub → external_owned, but never → owned)
        canon_priority = TYPE_PRIORITY.get(p.get("type", "stub"), 0)
        for alias_id in aliases:
            alias = papers.get(alias_id, {})
            alias_type = alias.get("type", "stub")
            alias_priority = TYPE_PRIORITY.get(alias_type, 0)
            if alias_priority > canon_priority and alias_type != "owned":
                if not dry_run:
                    p["type"] = alias_type
                canon_priority = alias_priority
                changed = True
                type_upgrades += 1
                break
        if changed:
            enriched_count += 1
    return enriched_count, type_upgrades


def main():
    parser = argparse.ArgumentParser(description="Repair stale alias references")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    print(f"Loaded {len(papers)} papers")

    remap = _build_alias_remap(papers)
    print(f"Alias remap: {len(remap)} entries")

    rewritten = 0
    edges = 0
    if remap:
        # Step 1: Resolve alias references
        rewritten = resolve_aliases_in_edges(papers, remap)
        print(f"Step 1 — alias resolution: {rewritten} papers rewritten")

        # Step 2: Bidi repair (skips superseded papers)
        print("Step 2 — bidi repair (this may take a moment)...")
        edges = repair_bidi_sql(papers)
        print(f"Step 2 — bidi repair: {edges} edges added")
    else:
        print("No aliases to resolve — skipping Steps 1-2.")

    # Step 3: Re-enrich canonicals from their aliases
    enriched, type_ups = _reenrich_canonicals(papers, dry_run=args.dry_run)
    print(f"Step 3 — re-enrich canonicals: {enriched} papers enriched, {type_ups} type upgrades")

    if not remap and enriched == 0:
        print("Nothing to repair.")
        return

    if args.dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    desc_parts = []
    if remap:
        desc_parts.append(f"resolve {len(remap)} aliases, {rewritten} papers rewritten")
    if enriched:
        desc_parts.append(f"re-enrich {enriched} canonicals ({type_ups} type upgrades)")
    export_json(db, PAPERS_FILE,
                description=f"repair_aliases: {', '.join(desc_parts)}")
    print(f"\nSaved to {PAPERS_FILE}")


if __name__ == "__main__":
    main()
