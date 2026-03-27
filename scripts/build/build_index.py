#!/usr/bin/env python3
"""
Build data/db/contexts.json from extractions.

Supports incremental mode: only re-processes extractions whose mtime changed
since the last build. Use --force for a full rebuild.

Usage:
  .venv/bin/python3 scripts/build/build_index.py
  .venv/bin/python3 scripts/build/build_index.py --force
"""

import json
import os
import re
from datetime import date
from pathlib import Path
from collections import defaultdict

import sys
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
from litdb import export_json, fast_loads

EXTRACTIONS_DIR = ROOT / "data" / "extractions"
EXTERNAL_DIR = ROOT / "data" / "db_imports"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
INDEX_FILE = ROOT / "data" / "db" / "contexts.json"
BUILD_META_FILE = ROOT / "data" / "db" / ".contexts_build_meta.json"

SKIP_PATTERNS = [
    r'\.analysis\.json$', r'\.contexts(\.\d+)?\.json$',
    r'\.sections(\.\d+)?\.json$', r'\.refs\.json$',
]


def is_main_extraction(filename):
    return not any(re.search(p, filename) for p in SKIP_PATTERNS)


def _file_stat(path: Path) -> tuple[int, int]:
    """Return (mtime_ns, size) for a file."""
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return (0, 0)


def _load_build_meta() -> dict:
    """Load stored extraction file stats from last build."""
    if BUILD_META_FILE.exists():
        try:
            return fast_loads(BUILD_META_FILE.read_text())
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_build_meta(meta: dict):
    """Save extraction file stats for incremental tracking."""
    BUILD_META_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(BUILD_META_FILE, "w") as f:
        json.dump(meta, f)


def _extract_contexts(ext_path: Path, superseded_ids: set) -> tuple[str, list[dict]]:
    """Extract citation contexts from one extraction file.

    Returns (citing_id, list_of_context_entries).
    """
    ext = fast_loads(ext_path.read_text())
    citing_id = ext["id"]
    if citing_id in superseded_ids:
        return citing_id, []

    entries = []
    for cit in ext.get("citations", []):
        cited_id = cit.get("id", "")
        for ctx in cit.get("contexts", []):
            entries.append({
                "citing": citing_id, "cited": cited_id,
                "cited_title": cit.get("title", ""),
                "purpose": ctx.get("purpose", ""),
                "section": ctx.get("section", ""),
                "quote": ctx.get("quote", ""),
                "explanation": ctx.get("explanation", ""),
            })
    return citing_id, entries


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build contexts.json from extractions")
    parser.add_argument("--force", action="store_true", help="Full rebuild, ignore cached state")
    args = parser.parse_args()

    if not PAPERS_FILE.exists():
        PAPERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PAPERS_FILE.write_text('{"metadata": {}, "papers": {}}')
    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    superseded_ids = {pid for pid, p in papers.items() if p.get("superseded_by")}

    local = [f for f in sorted(EXTRACTIONS_DIR.glob("*.json")) if is_main_extraction(f.name)]
    external = [f for f in sorted(EXTERNAL_DIR.glob("*/extractions/*.json"))
                if is_main_extraction(f.name)] if EXTERNAL_DIR.exists() else []
    all_extractions = local + external

    # Incremental: check which extractions changed since last build
    old_meta = {} if args.force else _load_build_meta()
    new_meta: dict[str, list] = {}
    changed_files: list[Path] = []
    unchanged_citing_ids: list[str] = []

    for ext_path in all_extractions:
        key = str(ext_path)
        stat = _file_stat(ext_path)
        new_meta[key] = list(stat)
        if not args.force and old_meta.get(key) == list(stat):
            # File unchanged — reuse cached contexts
            unchanged_citing_ids.append(ext_path.stem)
        else:
            changed_files.append(ext_path)

    # Check for deleted extraction files
    old_keys = set(old_meta.keys())
    new_keys = set(new_meta.keys())
    deleted_keys = old_keys - new_keys

    # If nothing changed at all, skip the rebuild entirely
    if not args.force and not changed_files and not deleted_keys and old_meta:
        print(f"No extractions changed, skipping rebuild ({len(unchanged_citing_ids)} cached)")
        return

    can_incremental = (
        not args.force
        and INDEX_FILE.exists()
        and not deleted_keys
        and changed_files
        and unchanged_citing_ids
    )

    if can_incremental:
        # Incremental: load existing contexts, remove entries from changed files, re-extract
        existing = fast_loads(INDEX_FILE.read_text())
        by_cited = defaultdict(list)

        # Keep entries from unchanged extractions
        changed_citing_set = {f.stem for f in changed_files}
        for cited_id, entries in existing.get("by_cited", {}).items():
            for e in entries:
                if e.get("citing", "") not in changed_citing_set:
                    by_cited[cited_id].append(e)

        # Re-extract changed files
        for ext_path in changed_files:
            citing_id, entries = _extract_contexts(ext_path, superseded_ids)
            for e in entries:
                by_cited[e["cited"]].append(e)

        print(f"Incremental: {len(changed_files)} changed, {len(unchanged_citing_ids)} cached")
    else:
        # Full rebuild
        by_cited = defaultdict(list)
        for ext_path in all_extractions:
            citing_id, entries = _extract_contexts(ext_path, superseded_ids)
            for e in entries:
                by_cited[e["cited"]].append(e)
        print(f"Full rebuild: {len(all_extractions)} extractions")

    # Build by_purpose index
    by_purpose = defaultdict(list)
    for entries in by_cited.values():
        for e in entries:
            by_purpose[e.get("purpose", "")].append(e)

    citation_counts = {pid: len(p.get("cited_by", [])) for pid, p in papers.items()}

    owned_papers = sorted([
        {
            "id": p["id"], "title": p["title"],
            "authors": p.get("authors", []), "year": p.get("year"),
            "journal": p.get("journal"), "doi": p.get("doi"),
            "cites_count": len(p.get("cites", [])),
            "cited_by_count": len(p.get("cited_by", [])),
            "cited_by": p.get("cited_by", []),
        }
        for p in papers.values()
        if p.get("type") in ("owned", "external_owned")
    ], key=lambda x: x["id"])

    index = {
        "generated": str(date.today()),
        "owned_count": len(owned_papers),
        "total_papers": len(papers),
        "owned_papers": owned_papers,
        "by_cited": dict(by_cited),
        "by_purpose": dict(by_purpose),
        "citation_counts": citation_counts,
    }

    total_contexts = sum(len(v) for v in by_cited.values())

    # Skip patch tracking for full rebuilds (too expensive, not useful for rollback)
    track = not args.force and can_incremental
    export_json(index, INDEX_FILE, track=track,
                description=f"build contexts.json: {len(owned_papers)} owned, {total_contexts} contexts")

    # Save build metadata for next incremental run
    _save_build_meta(new_meta)

    print(f"Contexts: {len(owned_papers)} owned, {total_contexts} contexts, {len(by_purpose)} purpose types")


if __name__ == "__main__":
    main()
