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

Parse `$ARGUMENTS` for an optional `--threshold N` value. If present, pass it to `find_duplicates.py` in Step 1.

---

## Step 1: Run duplicate detection

Run the detection script **directly** (not inside an agent):

```bash
.venv/bin/python3 scripts/build/find_duplicates.py [--threshold N if specified]
```

Check the exit code and output:
- **Exit code 0** (no groups found): Report "No duplicate candidates found above the threshold." and suggest `--threshold 3.0` for a broader search. **Stop here.**
- **Exit code 2** (groups found): Parse the stdout for a `FILES: N` line. This tells you how many TXT files were generated. Continue to Step 2.

---

## Step 2: Invoke duplicate-resolver agent(s)

### Single file (FILES: 1, or no FILES line)

Invoke **one** `duplicate-resolver` agent:

```
Review duplicate candidates and apply merges.
Candidate file: data/tmp/duplicate_candidates.txt
Output file: data/tmp/duplicate_resolved.txt
Skip Step 1 — detection has already been run.
```

The agent will read candidates, write decisions, and run `apply_duplicates.py`. Once it completes, go to **Report results**.

### Multiple files (FILES: N where N > 1)

Invoke **N** `duplicate-resolver` agents **in parallel** (all Agent tool calls in a single message). Each agent handles one file.

For agent K (K = 1 to N):

```
Review duplicate candidates and write merge/skip decisions.
Candidate file: data/tmp/duplicate_candidates_K.txt
Output file: data/tmp/duplicate_resolved_K.txt
Skip Step 1 — detection has already been run.
Skip Step 4 — do NOT run apply_duplicates.py. The caller will handle it.
```

After **all** agents complete:

1. **Concatenate** the resolved files (use Bash):
   ```bash
   cat data/tmp/duplicate_resolved_1.txt data/tmp/duplicate_resolved_2.txt [... up to N] > data/tmp/duplicate_resolved.txt
   ```
   List all N files explicitly in numerical order.

2. **Apply** the merged decisions:
   ```bash
   .venv/bin/python3 scripts/build/apply_duplicates.py
   ```

---

## Report results

After completion, report the outcome to the user:

- How many duplicate groups were found
- How many groups were merged vs. skipped
- Confirmation that DB was validated and indexes rebuilt (or any errors)
- Rollback instructions: `To undo: .venv/bin/python3 scripts/build/rollback.py --last 1`

If no groups were found, report: "No duplicate candidates found above the threshold." and suggest running with `--threshold 3.0` for a broader search.
