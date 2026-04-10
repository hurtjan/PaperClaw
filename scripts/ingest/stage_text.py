#!/usr/bin/env python3
"""
CLI for text-file stage transitions.

Move text files between staging directories and inspect stage status.

Usage:
  python3 scripts/py.py scripts/ingest/stage_text.py <stem> <target_stage>
  python3 scripts/py.py scripts/ingest/stage_text.py --list [staging|in_process|done|legacy|all]
  python3 scripts/py.py scripts/ingest/stage_text.py --status
  python3 scripts/py.py scripts/ingest/stage_text.py --batch <target_stage> stem1 stem2 ...
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import (
    resolve_text_file,
    move_to_stage,
    get_text_stage,
    ensure_text_dirs,
    TEXT_DIR,
    TEXT_STAGES,
)


def cmd_status() -> None:
    """Print file counts per stage."""
    ensure_text_dirs()
    total = 0
    counts: list[tuple[str, int]] = []

    for stage in TEXT_STAGES:
        n = len(list((TEXT_DIR / stage).glob("*.txt")))
        counts.append((stage, n))
        total += n

    # Legacy: files sitting in the flat root of TEXT_DIR
    legacy_files = [f for f in TEXT_DIR.glob("*.txt") if f.is_file()]
    legacy_n = len(legacy_files)
    counts.append(("legacy", legacy_n))
    total += legacy_n

    # Determine label width for alignment
    label_width = max(len(s) for s, _ in counts) + 1  # +1 for the colon
    count_width = len(str(total))

    for stage, n in counts:
        label = f"{stage}:"
        print(f"  {label:<{label_width}} {n:>{count_width}} files")
    print(f"  {'total:':<{label_width}} {total:>{count_width}} files")


def cmd_list(stage_filter: str) -> None:
    """List files in a stage (or all stages)."""
    ensure_text_dirs()

    stages_to_list: list[tuple[str, Path]] = []
    if stage_filter == "all":
        for stage in TEXT_STAGES:
            stages_to_list.append((stage, TEXT_DIR / stage))
        stages_to_list.append(("legacy", TEXT_DIR))
    elif stage_filter == "legacy":
        stages_to_list.append(("legacy", TEXT_DIR))
    elif stage_filter in TEXT_STAGES:
        stages_to_list.append((stage_filter, TEXT_DIR / stage_filter))
    else:
        print(f"Error: unknown stage '{stage_filter}'. Choose from: {', '.join(TEXT_STAGES)}, legacy, all", file=sys.stderr)
        sys.exit(1)

    for stage_name, stage_dir in stages_to_list:
        files = sorted(stage_dir.glob("*.txt"))
        if stage_name == "legacy":
            # Only direct children, not subdirectory files
            files = sorted(f for f in stage_dir.glob("*.txt") if f.parent == stage_dir)
        for f in files:
            print(f.name)


def cmd_move(stem: str, target_stage: str) -> None:
    """Move a single file to a target stage."""
    ensure_text_dirs()
    try:
        new_path = move_to_stage(stem, target_stage)
        rel = new_path.relative_to(TEXT_DIR)
        print(f"Moved {stem}.txt -> {rel}")
    except FileNotFoundError:
        print(f"Error: text file not found for '{stem}'", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_batch(target_stage: str, stems: list[str]) -> None:
    """Move multiple files to a target stage."""
    ensure_text_dirs()
    moved = 0
    skipped = 0
    errors = 0

    for stem in stems:
        try:
            new_path = move_to_stage(stem, target_stage)
            rel = new_path.relative_to(TEXT_DIR)
            print(f"Moved {stem}.txt -> {rel}")
            moved += 1
        except FileNotFoundError:
            print(f"Warning: text file not found for '{stem}', skipping", file=sys.stderr)
            errors += 1
        except ValueError as e:
            print(f"Warning: {e}", file=sys.stderr)
            errors += 1

    print(f"\nBatch complete: {moved} moved, {errors} errors, {len(stems)} total")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Text-file stage transitions.",
        usage="%(prog)s [--status | --list STAGE | --batch TARGET stem ...| STEM TARGET]",
    )
    parser.add_argument("--status", action="store_true", help="Show file counts per stage")
    parser.add_argument("--list", metavar="STAGE", nargs="?", const="all",
                        help="List files in a stage (staging, in_process, done, legacy, all)")
    parser.add_argument("--batch", metavar="TARGET", help="Batch move stems to TARGET stage")
    parser.add_argument("args", nargs="*", help="Positional: STEM TARGET or (with --batch) stems")

    opts = parser.parse_args()

    if opts.status:
        cmd_status()
    elif opts.list is not None:
        cmd_list(opts.list)
    elif opts.batch:
        if not opts.args:
            parser.error("--batch requires at least one stem")
        cmd_batch(opts.batch, opts.args)
    elif len(opts.args) == 2:
        stem, target_stage = opts.args
        cmd_move(stem, target_stage)
    elif len(opts.args) == 0:
        parser.print_help()
        sys.exit(1)
    else:
        parser.error("Expected STEM TARGET (2 positional args), or use --batch / --list / --status")


if __name__ == "__main__":
    main()
