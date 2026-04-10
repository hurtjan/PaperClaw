#!/usr/bin/env python3
"""
One-time migration: move flat data/text/*.txt files into staging subdirectories.

For each non-part .txt file sitting in the flat data/text/ root:
  - If a merged extraction exists (has extraction_meta with passes_completed) → done/
  - If an extraction JSON exists but without extraction_meta                  → in_process/
  - If no extraction JSON matches at all                                      → staging/
Part files (*.part*.txt) follow their parent stem.

Usage:
  python3 scripts/py.py scripts/ingest/migrate_text_staging.py [--dry-run]
"""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import ensure_text_dirs, TEXT_DIR, ROOT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_papers_db() -> dict:
    """Load papers.json and return the nested papers dict."""
    papers_path = ROOT / "data" / "db" / "papers.json"
    with open(papers_path) as f:
        raw = json.load(f)
    return raw.get("papers", raw)


def build_text_stem_index(papers: dict) -> dict:
    """Map text-file stem → paper dict for owned papers that have a text_file."""
    index = {}
    for _pid, p in papers.items():
        if not isinstance(p, dict):
            continue
        tf = p.get("text_file")
        if tf and p.get("type") in ("owned", "external_owned"):
            stem = Path(tf).stem
            index[stem] = p
    return index


def build_source_file_index(extractions_dir: Path) -> dict:
    """Map source_file stem → extraction JSON path by scanning extraction files."""
    index = {}
    for ef in extractions_dir.iterdir():
        if not ef.name.endswith(".json") or ef.name.startswith("_"):
            continue
        try:
            with open(ef) as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                continue
            sf = data.get("source_file", "")
            if sf:
                sf_stem = Path(sf).stem
                index[sf_stem] = ef
        except (json.JSONDecodeError, OSError):
            pass
    return index


def check_extraction_has_meta(ext_path: Path) -> bool:
    """Return True if the extraction JSON has extraction_meta.passes_completed."""
    try:
        with open(ext_path) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return False
        meta = data.get("extraction_meta")
        return bool(meta and meta.get("passes_completed"))
    except (json.JSONDecodeError, OSError):
        return False


def classify_text_file(
    stem: str,
    text_stem_index: dict,
    source_file_index: dict,
    extractions_dir: Path,
) -> str:
    """Determine the target stage for a text file stem.

    Returns 'done', 'in_process', or 'staging'.
    """
    # Strategy 1: Look up in papers.json via text_file stem
    paper = text_stem_index.get(stem)
    if paper:
        em = paper.get("extraction_meta")
        if em and em.get("passes_completed"):
            return "done"
        # papers.json says no meta — check extraction file directly
        ef = paper.get("extraction_file")
        if ef:
            ext_path = Path(ef) if Path(ef).is_absolute() else ROOT / ef
            if ext_path.exists():
                if check_extraction_has_meta(ext_path):
                    return "done"
                else:
                    return "in_process"
        # Paper exists but no extraction file at all
        return "staging"

    # Strategy 2: Check if an extraction JSON references this stem via source_file
    if stem in source_file_index:
        ext_path = source_file_index[stem]
        if check_extraction_has_meta(ext_path):
            return "done"
        else:
            return "in_process"

    # Strategy 3: Direct name match in extractions/
    direct_ext = extractions_dir / f"{stem}.json"
    if direct_ext.exists():
        if check_extraction_has_meta(direct_ext):
            return "done"
        else:
            return "in_process"

    # No extraction found at all
    return "staging"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="One-time migration: move flat text files into staging subdirectories."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without moving files.",
    )
    args = parser.parse_args()

    ensure_text_dirs()

    extractions_dir = ROOT / "data" / "extractions"

    # Build lookup indices
    papers = load_papers_db()
    text_stem_index = build_text_stem_index(papers)
    source_file_index = build_source_file_index(extractions_dir)

    # Gather all .txt files sitting in the flat text root (not in subdirectories)
    all_flat_txt = sorted(
        f for f in TEXT_DIR.iterdir()
        if f.is_file() and f.suffix == ".txt"
    )

    # Separate main files from part files
    main_files = [f for f in all_flat_txt if ".part" not in f.stem]
    part_files = [f for f in all_flat_txt if ".part" in f.stem]

    # Classify each main file
    # stem → target stage
    stage_map: dict[str, str] = {}
    for f in main_files:
        stem = f.stem
        stage = classify_text_file(stem, text_stem_index, source_file_index, extractions_dir)
        stage_map[stem] = stage

    # Map part files to their parent stem
    # e.g. "foo.part1" → parent stem "foo"
    part_parent: dict[str, str] = {}
    for f in part_files:
        # filename like "foo.part1.txt" → stem is "foo.part1", parent is "foo"
        name_no_ext = f.stem  # "foo.part1"
        # Find the .partN boundary
        idx = name_no_ext.find(".part")
        if idx >= 0:
            parent_stem = name_no_ext[:idx]
        else:
            parent_stem = name_no_ext
        part_parent[f.name] = parent_stem

    # Counters
    counts = {"staging": 0, "in_process": 0, "done": 0}
    part_count = 0

    # Move main files
    for f in main_files:
        stem = f.stem
        target_stage = stage_map[stem]
        target_dir = TEXT_DIR / target_stage
        target_path = target_dir / f.name

        if args.dry_run:
            print(f"  [dry-run] {f.name} → {target_stage}/")
        else:
            shutil.move(str(f), str(target_path))

        counts[target_stage] += 1

    # Move part files
    for f in part_files:
        parent_stem = part_parent[f.name]
        target_stage = stage_map.get(parent_stem)

        if target_stage is None:
            # Orphan part file — parent wasn't found; default to staging
            target_stage = "staging"

        target_dir = TEXT_DIR / target_stage
        target_path = target_dir / f.name

        if args.dry_run:
            print(f"  [dry-run] {f.name} → {target_stage}/ (part of {parent_stem})")
        else:
            shutil.move(str(f), str(target_path))

        part_count += 1

    # Summary
    prefix = "[dry-run] " if args.dry_run else ""
    print(f"\n{prefix}Migration complete:")
    print(f"  → staging:    {counts['staging']:>4} files")
    print(f"  → in_process: {counts['in_process']:>4} files")
    print(f"  → done:       {counts['done']:>4} files")
    print(f"  → skipped:    {part_count:>4} part files (moved with parent)")


if __name__ == "__main__":
    main()
