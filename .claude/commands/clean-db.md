---
description: Find and merge duplicate papers in the DB, then link authors. Usage: /clean-db
---

You are running the unified deduplication and author linking pipeline for the PaperClaw literature database.

Arguments: $ARGUMENTS

## What this does

Detects papers that may refer to the same work but have different IDs (e.g., preprint vs. published, different citation formats). Uses title similarity, DOI/S2 ID matches, shared authors, and shared citations as signals. Auto-merges confident matches, has an agent review ambiguous groups, then links author entities.

## Rules

- Always use `python3 scripts/py.py` for scripts.
- All changes are tracked as JSON Patch deltas and are fully rollbackable.
- Show full paper titles in user-facing output. Use (Author, Year) for short mentions.
- The `duplicate-resolver` agent applies conservative judgment: when in doubt, it skips.

---

## Argument parsing

Parse `$ARGUMENTS` for optional values:
- `--threshold N`: pass to `find_matches.py` in Step 1
- `--full`: activates iterative full-scan mode (see Step 1)

---

## Step 0a: Clear stale decision files

Before anything else, remove leftover decision files from previous runs so agents start with a clean slate:

```bash
python3 -c "from pathlib import Path; [f.unlink() for p in ['duplicate_candidates','duplicate_resolved','author_candidates','author_resolved'] for f in Path('data/tmp').glob(p+'*.txt')]"
```

This prevents agents from inheriting old skip/merge decisions and attempting deep-review reads on papers that have no local content.

---

## Step 0: Repair stale alias references

Before detecting duplicates, resolve any stale alias references left by previous merges or imports:

```bash
python3 scripts/py.py scripts/build/repair_aliases.py
```

If it reports "No aliases to resolve", the DB is clean — continue to Step 1. Otherwise it will rewrite stale references and repair bidirectional edges. This is fast and safe (tracked + rollbackable).

---

## Step 1: Run match detection

`find_matches.py` applies auto-merges during this step (pairs with S2 ID or DOI match AND title similarity > 90%).

### Normal mode (no `--full`)

```bash
python3 scripts/py.py scripts/build/find_matches.py [--threshold N if specified]
```

Check the exit code and output:
- **Exit code 0** (no groups found): Report auto-merge results (if any) and "No judgment groups remaining." Suggest `--threshold 2.5` for a broader search. **Skip to Step 3.**
- **Exit code 2** (groups found): Parse the stdout for a `FILES: N` line. This tells you how many TXT files were generated. Continue to Step 2.

### `--full` mode — iterative batched scan

Scores ALL pairs regardless of `dedup_pending`, processes them in ranked batches of 50 (highest-score first). Stops when a batch produces fewer than 5 merges — at that point the DB is clean at this score level.

**On the first iteration only**, clear the skip file:

```bash
python3 scripts/test/test_helpers.py rm data/tmp/full_scan_decided.txt
```

**Each iteration:**

Run detection, passing the skip file so already-decided pairs are excluded:

```bash
python3 scripts/py.py scripts/build/find_matches.py --full --skip-file data/tmp/full_scan_decided.txt [--threshold N if specified]
```

- **Exit code 0**: no pairs remain → skip to Step 3
- **Exit code 2**: parse the `FILES: N` line. Continue to Step 2 — but regardless of FILES count, always instruct the agent(s) to **skip running apply_duplicates.py** (the caller handles it). After the agent(s) complete, concatenate if N > 1, then:

  ```bash
  python3 scripts/py.py scripts/build/apply_duplicates.py --record-skips data/tmp/full_scan_decided.txt
  ```

  Parse `Decisions: X merge(s)` from the apply output.

  - **If `X >= 5`**: the DB still has duplicates at this score level — repeat this iteration (do NOT clear the skip file again)
  - **If `X < 5`**: the batch was mostly skips, DB is clean — continue to Step 3

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
Skip Step 5 — do NOT run apply_duplicates.py. The caller will handle it.
```

After **all** agents complete:

1. **Concatenate** the resolved files:
   ```bash
   python3 scripts/test/test_helpers.py cat-to data/tmp/duplicate_resolved.txt data/tmp/duplicate_resolved_1.txt data/tmp/duplicate_resolved_2.txt [... up to N]
   ```
   List all N files explicitly in numerical order.

2. **Apply** the merged decisions:
   ```bash
   python3 scripts/py.py scripts/build/apply_duplicates.py
   ```

---

## Step 3: Author linking

Run the author linker:

```bash
python3 scripts/py.py scripts/link/link_authors.py
```

Check the output:
- If it says **"No new papers to process. STOP — pipeline complete."**: Skip to Step 4.
- Parse `Judgment: N` and `FILES: N` from the output.

### If Judgment == 0: skip agent review, go directly to apply

```bash
python3 scripts/py.py scripts/link/apply_authors.py
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
   python3 scripts/test/test_helpers.py cat-to data/tmp/author_resolved.txt data/tmp/author_resolved_1.txt data/tmp/author_resolved_2.txt [... up to N]
   ```

2. **Apply** the merged decisions:
   ```bash
   python3 scripts/py.py scripts/link/apply_authors.py
   ```

---

## Step 4: Final rebuild + sync query subproject

```bash
python3 scripts/py.py scripts/build/build_duckdb.py
```

If the `query/` subdirectory exists, sync the fresh database to the query subproject:

```bash
python3 query/sync.py
```

Report the outcome to the user:

- How many auto-merges were applied
- How many judgment groups were found, merged vs. skipped
- How many authors were linked
- Rollback instructions: `To undo: python3 scripts/py.py scripts/build/rollback.py --last 1`

If no groups were found and no authors needed linking, report: "Database is clean — no duplicates or unlinked authors found."
