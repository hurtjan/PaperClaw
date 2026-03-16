---
description: Merge an external PaperClaw database into the local DB. Usage: /merge <source_dir> [--name <label>] [--enrich] [--force]
---

You are merging an external PaperClaw corpus into the local database.

Arguments: $ARGUMENTS

- `<source_dir>`: Path to external PaperClaw root or `data/db_imports/<name>/` directory.
- `--name <label>`: Label for this import (defaults to source directory name).
- `--enrich`: Enrich local papers with external metadata where local fields are missing. Upgrades local stubs to `external_owned` when external has full data.
- `--force`: Overwrite existing `external_owned` entries from a previous import.

## What this does

Imports papers from an external PaperClaw database:
- External `owned` papers become `external_owned` (no local PDF/extraction, but full metadata).
- External `stub`/`cited_only` papers become local `stub` entries.
- Fuzzy-matches papers with different IDs that refer to the same work (e.g., `smith_2021_scaling` vs `smith_2021_scale`) and merges them rather than duplicating.
- With `--enrich`: fills in missing metadata (doi, s2_paper_id, abstract, forward_cited_by) on existing local papers from external data without overwriting local file paths or extractions.
- Merges citation contexts from external `contexts.json`.
- Enforces bidirectional cites/cited_by consistency, removes dangling refs, deduplicates.
- Strips derived fields (`author_lastnames`, `title_normalized`, `discovered_via`) not in local schema.
- Links newly-added papers' authors incrementally using coauthor-overlap disambiguation (`link_authors.py` → optional agent review → `apply_authors.py`).

## Rules

- Always use `.venv/bin/python3` for scripts.
- Parse `--name`, `--enrich`, `--force` from user arguments and pass through to scripts.

---

## Step 1 — Find fuzzy candidates

```
.venv/bin/python3 scripts/enrich/find_merge_candidates.py <source_dir> [--name <label>]
```

This script exits with code **0** if no fuzzy candidates exist (proceed directly to Step 3), or **2** if fuzzy candidates were found (agent review needed).

Parse the output to confirm: look for `Auto-matched (score > 6): N` and `Needs judgment (score 1-6): N` in stdout. If both are 0 (or exit code is 0), skip to Step 3.

---

## Step 2 — Agent review (only if Step 1 found candidates)

Invoke the `merge-resolver` agent to review `data/tmp/merge_candidates.txt` and write `data/tmp/merge_resolved.txt`.

The agent reads the candidates file and decides for each fuzzy match: accept (merge into existing local paper) or reject (`new` — add as separate entry).

---

## Step 3 — Run merge

```
.venv/bin/python3 scripts/enrich/merge_db.py <source_dir> [--name <label>] [--enrich] [--force] [--resolved data/tmp/merge_resolved.txt]
```

- Pass `--resolved data/tmp/merge_resolved.txt` **only if** the agent ran in Step 2 (i.e., exit code from Step 1 was 2).
- Pass `--name`, `--enrich`, `--force` as provided by the user.

The script handles:
- Fuzzy-merge decisions from `merge_resolved.txt` (if provided)
- Source path resolution (tries `<source_dir>/data/db/papers.json`, falls back to `<source_dir>/papers.json`)
- Paper merging with type-aware logic
- Context merging (deduplicates by citing/cited pair)
- Copying reference files to `data/db_imports/<name>/`

---

## Step 4 — Link authors for newly-added papers

### Step 4a — Run incremental author linker

```
.venv/bin/python3 scripts/link/link_authors.py
```

`link_authors.py` auto-detects which papers are new (not yet in `processed_papers`). Parse its output:
- If it says **"No new papers to process"**: skip Steps 4b and 4c entirely.
- Look for `Judgment: N` in the output. If N == 0: skip Step 4b, go directly to Step 4c.
- If N > 0: proceed to Step 4b.

### Step 4b — Agent review (only if NEEDS_JUDGMENT entries exist)

Invoke the `author-resolver` agent to review `data/tmp/author_candidates.json` and write `data/tmp/author_resolved.json`.

### Step 4c — Apply author decisions

```
.venv/bin/python3 scripts/link/apply_authors.py
```

---

## Step 5 — Validate

Run the DB consistency checker:

```
.venv/bin/python3 scripts/build/check_db.py
```

If exit code is non-zero, report the errors to the user. Common issues:
- **Bidi errors**: cites/cited_by not symmetric — may indicate a bug in merge repair logic.
- **Dangling refs**: references to paper IDs not in the DB — may indicate incomplete source data.
- **Invalid type**: `cited_only` or other non-standard types leaked through — merge should convert these to `stub`.
- **Missing source_db**: `external_owned` entry without provenance — merge should always set this.

If exit code is 0, report success with paper counts and context totals.

---

## Step 6 — Summary

Report to the user:
- Total papers (owned + external_owned + stubs)
- Fuzzy-merged count (if any)
- Contexts merged
- Any warnings from check_db (orphaned stubs are expected for forward_cited_by papers)

**Paths:** `data/db/papers.json`, `data/db/contexts.json`, `data/db/authors.json`, `data/db_imports/`
