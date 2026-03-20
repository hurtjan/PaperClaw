---
description: Find and merge duplicate papers in the DB, then link authors. Usage: /clean-db
---

You are running the unified deduplication and author linking pipeline for the PaperClaw literature database.

Arguments: $ARGUMENTS

## What this does

Detects papers that may refer to the same work but have different IDs (e.g., preprint vs. published, different citation formats). Uses title similarity, DOI/S2 ID matches, shared authors, and shared citations as signals. Auto-merges confident matches, has an agent review ambiguous groups, then links author entities.

## Rules

- Always use `.venv/bin/python3` for scripts.
- All changes are tracked as JSON Patch deltas and are fully rollbackable.
- Show full paper titles in user-facing output. Use (Author, Year) for short mentions.
- The `duplicate-resolver` agent applies conservative judgment: when in doubt, it skips.

---

## Argument parsing

Parse `$ARGUMENTS` for an optional `--threshold N` value. If present, pass it to `find_matches.py` in Step 1.

---

## Step 1: Run match detection

Run the detection script **directly** (not inside an agent):

```bash
.venv/bin/python3 scripts/build/find_matches.py [--threshold N if specified]
```

`find_matches.py` applies auto-merges directly during this step (pairs with S2 ID or DOI match AND title similarity > 90%).

Check the exit code and output:
- **Exit code 0** (no groups found): Report auto-merge results (if any) and "No judgment groups remaining." Suggest `--threshold 2.0` for a broader search. **Skip to Step 3.**
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

The agent will read candidates, write decisions, and run `apply_duplicates.py`. Once it completes, go to **Step 3**.

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

## Step 3: Author linking

Run the author linker:

```bash
.venv/bin/python3 scripts/link/link_authors.py
```

Check the output:
- If it says **"No new papers to process. STOP — pipeline complete."**: Skip to Step 4.
- Parse `Judgment: N` and `FILES: N` from the output.

### If Judgment == 0: skip agent review, go directly to apply

```bash
.venv/bin/python3 scripts/link/apply_authors.py
```

### If FILES: 1 (single file)

Invoke **one** `author-resolver` agent:

```
Resolve ambiguous author matches.
Candidate file: data/tmp/author_candidates.txt
Output file: data/tmp/author_resolved.txt
```

The agent reads candidates, writes decisions, and runs `apply_authors.py`.

### If FILES: N (multiple files, N > 1)

Invoke **N** `author-resolver` agents **in parallel** (all Agent tool calls in a single message).

For agent K (K = 1 to N):

```
Resolve ambiguous author matches.
Candidate file: data/tmp/author_candidates_K.txt
Output file: data/tmp/author_resolved_K.txt
Skip apply — do NOT run apply_authors.py. The caller will handle it.
```

After **all** agents complete:

1. **Concatenate** the resolved files:
   ```bash
   cat data/tmp/author_resolved_1.txt data/tmp/author_resolved_2.txt [... up to N] > data/tmp/author_resolved.txt
   ```

2. **Apply** the merged decisions:
   ```bash
   .venv/bin/python3 scripts/link/apply_authors.py
   ```

---

## Step 4: Final rebuild + report

```bash
.venv/bin/python3 scripts/query/duckdb_query.py rebuild
```

Report the outcome to the user:

- How many auto-merges were applied
- How many judgment groups were found, merged vs. skipped
- How many authors were linked
- Rollback instructions: `To undo: .venv/bin/python3 scripts/build/rollback.py --last 1`

If no groups were found and no authors needed linking, report: "Database is clean — no duplicates or unlinked authors found."
