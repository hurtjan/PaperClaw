#!/usr/bin/env python3
"""Build a duplicate merge plan for the _v2 injected stub from Phase E.

Reads:  data/tmp/duplicate_candidates.json
Writes: data/tmp/duplicate_merge_plan.json
Prints: "Merge plan: {canonical_id} <- {alias_ids}"
"""
import json
from pathlib import Path

candidates = json.loads(Path("data/tmp/duplicate_candidates.json").read_text())
target_group = None
for g in candidates["groups"]:
    ids = [p["id"] for p in g["papers"]]
    v2s = [pid for pid in ids if pid.endswith("_v2")]
    if v2s:
        canonical_id = g["recommended_canonical"]
        alias_ids = [pid for pid in ids if pid != canonical_id]
        target_group = {"canonical_id": canonical_id, "alias_ids": alias_ids}
        break
if not target_group:
    print("ERROR: could not find injected pair in candidate groups")
    raise SystemExit(1)
plan = {"merges": [target_group]}
Path("data/tmp/duplicate_merge_plan.json").write_text(json.dumps(plan, indent=2))
print(f"Merge plan: {target_group['canonical_id']} <- {target_group['alias_ids']}")
