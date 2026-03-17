---
description: Find and merge duplicate papers in the DB. Usage: /clean-db
---

You are running the duplicate detection and merge pipeline for the PaperClaw literature database.

Arguments: $ARGUMENTS

## What this does

Detects papers that may refer to the same work but have different titles, DOIs, or years (e.g., preprint vs. published version). Uses shared authors and shared citations as signals — these persist even when metadata differs. Groups and ranks candidates by confidence, then has a dedicated agent review and apply merges conservatively.

## Rules

- Always use `.venv/bin/python3` for scripts.
- All changes are tracked as JSON Patch deltas and are fully rollbackable.
- Show full paper titles in user-facing output. Use (Author, Year) for short mentions.
- The `duplicate-resolver` agent applies conservative judgment: when in doubt, it skips. Rollback is available if a merge needs to be undone.

---

## Argument parsing

Parse `$ARGUMENTS` for an optional `--threshold N` value. If present, it will be passed to the agent.

---

## Run the duplicate-resolver agent

Invoke the `duplicate-resolver` agent with the following prompt:

```
Run the full duplicate detection and merge pipeline.
[If --threshold N was specified: Pass --threshold N to find_duplicates.py in Step 1.]
```

The agent will:
1. Run `find_duplicates.py` (with `--threshold N` if specified)
2. Read `data/tmp/duplicate_candidates.txt` and review all groups
3. Write `data/tmp/duplicate_resolved.txt` with merge/skip decisions
4. Run `apply_duplicates.py` to apply merges and rebuild indexes

---

## Report results

After the agent completes, report the outcome to the user:

- How many duplicate groups were found
- How many groups were merged vs. skipped
- Confirmation that DB was validated and indexes rebuilt (or any errors)
- Rollback instructions: `To undo: .venv/bin/python3 scripts/build/rollback.py --last 1`

If no groups were found, report: "No duplicate candidates found above the threshold." and suggest running with `--threshold 3.0` for a broader search.
