#!/usr/bin/env python3
"""
Parse duplicate resolution decisions and apply merges.

Usage:
  .venv/bin/python3 scripts/build/apply_duplicates.py [--dry-run]

Reads:  data/tmp/duplicate_resolved.txt
Writes: data/tmp/duplicate_merge_plan.json
Runs:   scripts/build/merge_duplicates.py [--dry-run]
        scripts/build/build_duckdb.py
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import export_json, fast_loads

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
RESOLVED_FILE = ROOT / "data" / "tmp" / "duplicate_resolved.txt"
MERGE_PLAN_FILE = ROOT / "data" / "tmp" / "duplicate_merge_plan.json"


def parse_resolved(path: Path) -> list[tuple]:
    """
    Parse duplicate_resolved.txt.
    Returns list of (action, canonical_id, alias_ids).
    Format: merge canonical alias1 [alias2 ...]  OR  skip canonical alias1 [alias2 ...]
    """
    decisions = []
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        action = parts[0].lower()
        if action not in ("merge", "skip"):
            continue
        canonical = parts[1]
        aliases = parts[2:]
        decisions.append((action, canonical, aliases))
    return decisions


def main():
    parser = argparse.ArgumentParser(description="Apply duplicate resolution decisions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    parser.add_argument("--record-skips", metavar="FILE",
                        help="Append skipped group pairs to this file (for iterative --full scanning)")
    args = parser.parse_args()

    if not RESOLVED_FILE.exists():
        print(f"ERROR: {RESOLVED_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    decisions = parse_resolved(RESOLVED_FILE)

    merges = []
    skipped = 0
    all_decision_ids: set[str] = set()

    for action, canonical_id, alias_ids in decisions:
        all_decision_ids.add(canonical_id)
        all_decision_ids.update(alias_ids)
        if action == "skip":
            skipped += 1
            continue
        merges.append({"canonical_id": canonical_id, "alias_ids": alias_ids})

    print(f"Decisions: {len(merges)} merge(s), {skipped} skip(s)")

    # Record skipped group pairs to skip file for iterative --full scanning
    if args.record_skips and not args.dry_run:
        skip_lines = []
        for action, canonical_id, alias_ids in decisions:
            if action == "skip":
                pids = [canonical_id] + alias_ids
                for i in range(len(pids)):
                    for j in range(i + 1, len(pids)):
                        a, b = sorted([pids[i], pids[j]])
                        skip_lines.append(f"{a}|||{b}")
        if skip_lines:
            with open(args.record_skips, "a") as f:
                f.write("\n".join(skip_lines) + "\n")
            print(f"Recorded {len(skip_lines)} skipped pair(s) → {args.record_skips}")

    if not merges:
        print("No merges to apply.")
        print("STOP — duplicate resolution complete.")
        sys.exit(0)

    # Write merge plan
    merge_plan = {"merges": merges}
    MERGE_PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
    MERGE_PLAN_FILE.write_text(json.dumps(merge_plan, indent=2))
    print(f"Wrote merge plan: {len(merges)} group(s)")

    # Run merge_duplicates.py
    cmd = [sys.executable, str(ROOT / "scripts" / "build" / "merge_duplicates.py")]
    if args.dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)
        print("ERROR: merge_duplicates.py failed.", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("STOP — duplicate resolution complete.")
        sys.exit(0)

    # Clear dedup_pending flags for all processed papers (merged and skipped)
    all_processed_ids = all_decision_ids

    if all_processed_ids and PAPERS_FILE.exists():
        db = fast_loads(PAPERS_FILE.read_text())
        papers_db = db["papers"]
        cleared = 0
        for pid in all_processed_ids:
            if pid in papers_db and papers_db[pid].pop("dedup_pending", None) is not None:
                cleared += 1
        if cleared:
            export_json(db, PAPERS_FILE,
                        description=f"clear dedup_pending: {cleared} paper(s) processed")
            print(f"Cleared dedup_pending on {cleared} paper(s)")

    # Rebuild DuckDB query index
    rebuild_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "build" / "build_duckdb.py"),
    ]
    result = subprocess.run(rebuild_cmd, cwd=ROOT, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)
        print("WARN: build_duckdb.py failed (non-fatal)", file=sys.stderr)

    print("STOP — duplicate resolution complete.")


if __name__ == "__main__":
    main()
