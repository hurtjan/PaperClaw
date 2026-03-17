#!/usr/bin/env python3
"""
Parse duplicate resolution decisions and apply merges.

Usage:
  .venv/bin/python3 scripts/build/apply_duplicates.py [--dry-run]

Reads:  data/tmp/duplicate_resolved.txt
        data/tmp/duplicate_candidates.json
Writes: data/tmp/duplicate_merge_plan.json
Runs:   scripts/build/merge_duplicates.py [--dry-run]
        scripts/query/duckdb_query.py rebuild
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

RESOLVED_FILE = ROOT / "data" / "tmp" / "duplicate_resolved.txt"
CANDIDATES_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.json"
MERGE_PLAN_FILE = ROOT / "data" / "tmp" / "duplicate_merge_plan.json"


def parse_resolved(path: Path) -> dict:
    """
    Parse duplicate_resolved.txt.
    Returns {group_id: ("merge", canonical_id) | ("skip",)}
    """
    decisions = {}
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line:
            continue
        m = re.match(r"GROUP\s+(\d+):\s+(merge|skip)\s*(\S+)?", line, re.IGNORECASE)
        if m:
            group_id = int(m.group(1))
            action = m.group(2).lower()
            canonical = m.group(3) if m.group(3) else None
            if action == "merge":
                if not canonical:
                    print(f"ERROR: GROUP {group_id} has merge but no canonical_id",
                          file=sys.stderr)
                    sys.exit(1)
                decisions[group_id] = ("merge", canonical)
            else:
                decisions[group_id] = ("skip",)
    return decisions


def main():
    parser = argparse.ArgumentParser(description="Apply duplicate resolution decisions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    args = parser.parse_args()

    if not RESOLVED_FILE.exists():
        print(f"ERROR: {RESOLVED_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    if not CANDIDATES_FILE.exists():
        print(f"ERROR: {CANDIDATES_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    decisions = parse_resolved(RESOLVED_FILE)
    candidates = json.loads(CANDIDATES_FILE.read_text())

    # Build group_id -> paper_ids mapping
    group_paper_ids: dict = {}
    for group in candidates.get("groups", []):
        gid = group["group_id"]
        group_paper_ids[gid] = [p["id"] for p in group["papers"]]

    merges = []
    skipped = 0

    for group_id, decision in sorted(decisions.items()):
        if decision[0] == "skip":
            skipped += 1
            continue

        _, canonical_id = decision
        paper_ids = group_paper_ids.get(group_id)
        if not paper_ids:
            print(f"WARN: GROUP {group_id} not found in candidates, skipping",
                  file=sys.stderr)
            continue

        if canonical_id not in paper_ids:
            print(
                f"ERROR: canonical {canonical_id} not in GROUP {group_id} "
                f"paper IDs: {paper_ids}",
                file=sys.stderr,
            )
            sys.exit(1)

        alias_ids = [pid for pid in paper_ids if pid != canonical_id]
        merges.append({"canonical_id": canonical_id, "alias_ids": alias_ids})

    print(f"Decisions: {len(merges)} merge(s), {skipped} skip(s)")

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

    # Rebuild DuckDB query index
    rebuild_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "query" / "duckdb_query.py"),
        "rebuild",
    ]
    result = subprocess.run(rebuild_cmd, cwd=ROOT, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)
        print("WARN: duckdb_query.py rebuild failed (non-fatal)", file=sys.stderr)

    print("STOP — duplicate resolution complete.")


if __name__ == "__main__":
    main()
