#!/usr/bin/env python3
"""One-time repair: resolve stale alias references in cites/cited_by arrays.

Usage: .venv/bin/python3 scripts/build/repair_aliases.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))

from litdb import fast_loads, export_json
from db import _build_alias_remap, resolve_aliases_in_edges, repair_bidi_sql

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def main():
    parser = argparse.ArgumentParser(description="Repair stale alias references")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    print(f"Loaded {len(papers)} papers")

    remap = _build_alias_remap(papers)
    print(f"Alias remap: {len(remap)} entries")
    if not remap:
        print("No aliases to resolve.")
        return

    # Step 1: Resolve alias references
    rewritten = resolve_aliases_in_edges(papers, remap)
    print(f"Step 1 — alias resolution: {rewritten} papers rewritten")

    # Step 2: Bidi repair (skips superseded papers)
    print("Step 2 — bidi repair (this may take a moment)...")
    edges = repair_bidi_sql(papers)
    print(f"Step 2 — bidi repair: {edges} edges added")

    # Stats
    c = papers.get("tong_zhang_2019_committed", {})
    if c:
        print(f"\nSample: tong_zhang_2019_committed")
        print(f"  cited_by: {len(c.get('cited_by', []))}")
        print(f"  cites: {len(c.get('cites', []))}")

    if args.dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    export_json(db, PAPERS_FILE,
                description=f"repair_aliases: resolve {len(remap)} aliases, {rewritten} papers rewritten")
    print(f"\nSaved to {PAPERS_FILE}")


if __name__ == "__main__":
    main()
