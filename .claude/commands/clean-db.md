---
description: Find and merge duplicate papers in the DB. Usage: /clean-db
---

You are running the duplicate detection and merge pipeline for the PaperClaw literature database.

Arguments: $ARGUMENTS

## What this does

Detects papers that may refer to the same work but have different titles, DOIs, or years (e.g., preprint vs. published version). Uses shared authors and shared citations as signals — these persist even when metadata differs. Groups and ranks candidates by confidence, then merges with user approval.

## Rules

- Always use `.venv/bin/python3` for scripts.
- No agents needed — pure Python pipeline.
- **Never auto-merge without user confirmation.** Always show proposed merges and wait for explicit approval.
- All changes are tracked as JSON Patch deltas and are fully rollbackable.
- Show full paper titles in user-facing output. Use (Author, Year) for short mentions.

---

## PHASE 1 — Detect Duplicates

Run the detection script:

```bash
.venv/bin/python3 scripts/build/find_duplicates.py
```

Read `data/tmp/duplicate_candidates.json`.

If `groups_found == 0`, report: "No duplicate candidates found above the default threshold." Suggest running with `--threshold 3.0` for a broader search, then stop.

Present the groups organized by confidence (high first). For each group show:

```
## Group N (HIGH/MEDIUM confidence, max score: X.X)
Recommended canonical: <type> — "<Full Title>" (Author, Year) [DOI if present]

Papers in group:
  1. "<Full Title>" (Author1, Author2, Year) [type] DOI: ...
     Abstract: "first 200 chars or (none)"
     Cites: N  |  Cited by: N

  2. "<Full Title>" (Author1, Author2, Year) [type] DOI: ...
     Abstract: "first 200 chars or (none)"
     Cites: N  |  Cited by: N

Signals: <list of signal names from pairwise_scores>
Shared authors: <lastname list>
Shared citers: <first 5 IDs> (+N more)
Shared cites:  <first 5 IDs> (+N more)
```

List all groups before prompting the user.

---

## PHASE 2 — User Selection

Ask the user which groups to merge:

```
Which groups would you like to merge?
  all           — merge all N groups
  high          — merge only high-confidence groups (N groups)
  1,3,5         — specific group numbers
  none          — cancel

You can also override the recommended canonical for any group, e.g.:
  "2 keep paper_b" means group 2, but use paper_b as canonical instead.
```

Wait for the user's response before proceeding.

If user says "none" or equivalent, stop without further changes.

---

## PHASE 3 — Execute Merge

Based on user selection, build the merge plan. For each selected group, use the `recommended_canonical` unless the user specified an override.

Write `data/tmp/duplicate_merge_plan.json`:

```json
{
  "merges": [
    {"canonical_id": "paper_a", "alias_ids": ["paper_b"]},
    {"canonical_id": "paper_c", "alias_ids": ["paper_d", "paper_e"]}
  ]
}
```

**Preview first:**

```bash
.venv/bin/python3 scripts/build/merge_duplicates.py --dry-run
```

Show the dry-run output to the user. Include:
- Which fields will be enriched on each canonical
- How many graph edges will be consolidated
- How many papers will have references rewritten

Ask for confirmation: "Ready to apply these N merge(s)? (yes/no)"

If user confirms, run:

```bash
.venv/bin/python3 scripts/build/merge_duplicates.py
```

If user declines, stop without writing.

---

## PHASE 4 — Validate

Run the DB consistency checker:

```bash
.venv/bin/python3 scripts/build/check_db.py
```

If exit code is non-zero, report the errors and provide rollback instructions:

```
The merge produced DB errors. To undo:
  .venv/bin/python3 scripts/build/rollback.py --last 1
```

If successful, rebuild the query DB:

```bash
.venv/bin/python3 scripts/query/duckdb_query.py rebuild
```

Report the outcome:
- N duplicate group(s) merged
- N alias(es) now point to their canonical via superseded_by
- N graph edges consolidated
- DB validated (no errors)
- To undo: `.venv/bin/python3 scripts/build/rollback.py --last 1`
