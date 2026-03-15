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
- With `--enrich`: fills in missing metadata (doi, s2_paper_id, abstract, forward_cited_by) on existing local papers from external data without overwriting local file paths or extractions.
- Merges citation contexts from external `contexts.json`.
- Enforces bidirectional cites/cited_by consistency, removes dangling refs, deduplicates.
- Strips derived fields (`author_lastnames`, `title_normalized`, `discovered_via`) not in local schema.
- Rebuilds authors index and contexts index after merge.

## Rules

- Always use `.venv/bin/python3` for scripts.
- No agents needed — this is a pure Python pipeline.

---

## Step 1 — Run merge

```
.venv/bin/python3 scripts/enrich/merge_db.py <source_dir> [--name <label>] [--enrich] [--force]
```

Parse the flags from the user's arguments and pass them through. The script handles:
- Source path resolution (tries `<source_dir>/data/db/papers.json`, falls back to `<source_dir>/papers.json`)
- Paper merging with type-aware logic
- Context merging (deduplicates by citing/cited pair)
- Copying reference files to `data/db_imports/<name>/`
- Rebuilding authors index

---

## Step 2 — Validate

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

## Step 3 — Summary

Report to the user:
- Total papers (owned + external_owned + stubs)
- Contexts merged
- Any warnings from check_db (orphaned stubs are expected for forward_cited_by papers)

**Paths:** `data/db/papers.json`, `data/db/contexts.json`, `data/db/authors.json`, `data/db_imports/`
