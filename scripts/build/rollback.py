#!/usr/bin/env python3
"""
View DB change history and roll back incremental JSON patches.

Usage:
  .venv/bin/python3 scripts/build/rollback.py                      # show last 10 changes
  .venv/bin/python3 scripts/build/rollback.py --last 1             # undo last change
  .venv/bin/python3 scripts/build/rollback.py --last 3             # undo last 3 changes
  .venv/bin/python3 scripts/build/rollback.py --dry-run --last 2   # preview rollback
  .venv/bin/python3 scripts/build/rollback.py --prune --keep-last 50  # clean old patches
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
from litdb import export_json

MANIFEST_FILE = ROOT / "data" / "db_history" / "manifest.jsonl"


def load_manifest() -> list[dict]:
    """Return all manifest entries, oldest first."""
    if not MANIFEST_FILE.exists():
        return []
    entries = []
    for line in MANIFEST_FILE.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return entries


def save_manifest(entries: list[dict]) -> None:
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_FILE, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def show_history(entries: list[dict], n: int = 10) -> None:
    recent = entries[-n:] if len(entries) > n else entries
    if not recent:
        print("No history recorded yet.")
        return
    print(f"Last {len(recent)} change(s)  (newest last):\n")
    for i, e in enumerate(recent, start=len(entries) - len(recent) + 1):
        stats = e.get("stats", {})
        ops = stats.get("patch_size_ops", "?")
        print(f"  [{i:3d}] {e['timestamp']}  {e['source']:<20}  {Path(e['file']).name}  ({ops} ops)")
        print(f"        {e['description']}")


def do_rollback(entries: list[dict], n: int, dry_run: bool) -> None:
    if not entries:
        print("No history to roll back.")
        return

    to_undo = entries[-n:]
    if not to_undo:
        print(f"Not enough history entries to roll back {n}.")
        return

    print(f"{'[DRY RUN] ' if dry_run else ''}Rolling back {len(to_undo)} change(s):")

    for entry in reversed(to_undo):
        patch_path = ROOT / entry["patch_file"]
        if not patch_path.exists():
            print(f"  ERROR: patch file missing: {patch_path}", file=sys.stderr)
            sys.exit(1)

        patch_doc = json.loads(patch_path.read_text())
        reverse_patch = patch_doc["reverse_patch"]
        target = ROOT / entry["file"]

        print(f"  Reverting: {entry['description']}")
        print(f"    File: {entry['file']}")
        print(f"    Ops:  {len(reverse_patch)}")

        if not dry_run:
            try:
                import jsonpatch
            except ImportError:
                print("ERROR: jsonpatch not installed. Run: .venv/bin/pip install jsonpatch",
                      file=sys.stderr)
                sys.exit(1)

            current_data = json.loads(target.read_text())
            patched = jsonpatch.apply_patch(current_data, reverse_patch)
            export_json(patched, target, track=False)
            print(f"    Done.")

    if not dry_run:
        remaining = entries[:-n]
        save_manifest(remaining)
        print(f"\nRolled back {len(to_undo)} change(s). Manifest updated.")
    else:
        print(f"\n[DRY RUN] No files were modified.")


def do_prune(entries: list[dict], keep_last: int) -> None:
    if len(entries) <= keep_last:
        print(f"Nothing to prune: {len(entries)} entries, keeping last {keep_last}.")
        return

    to_remove = entries[:-keep_last] if keep_last > 0 else entries
    removed_files = 0
    for entry in to_remove:
        patch_path = ROOT / entry["patch_file"]
        if patch_path.exists():
            patch_path.unlink()
            removed_files += 1

    remaining = entries[-keep_last:] if keep_last > 0 else []
    save_manifest(remaining)
    print(f"Pruned {len(to_remove)} entries ({removed_files} patch files deleted). "
          f"{len(remaining)} entries remain.")


def main() -> None:
    parser = argparse.ArgumentParser(description="View and roll back DB patch history")
    parser.add_argument("--last", type=int, metavar="N",
                        help="Roll back the last N changes")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview rollback without modifying files")
    parser.add_argument("--show", type=int, default=10, metavar="N",
                        help="Number of recent entries to show (default: 10)")
    parser.add_argument("--prune", action="store_true",
                        help="Delete old patch files, keeping --keep-last entries")
    parser.add_argument("--keep-last", type=int, default=50, metavar="N",
                        help="Number of entries to keep when pruning (default: 50)")
    args = parser.parse_args()

    entries = load_manifest()

    if args.prune:
        do_prune(entries, args.keep_last)
    elif args.last is not None:
        do_rollback(entries, args.last, args.dry_run)
    else:
        show_history(entries, args.show)


if __name__ == "__main__":
    main()
