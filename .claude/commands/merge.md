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

Imports papers from an external PaperClaw database (naive add):
- External `owned` papers become `external_owned` (no local PDF/extraction, but full metadata).
- External `stub`/`cited_only` papers become local `stub` entries.
- With `--enrich`: fills in missing metadata (doi, s2_paper_id, abstract, forward_cited_by) on existing local papers from external data without overwriting local file paths or extractions.
- Merges citation contexts from external `contexts.json`.
- Enforces bidirectional cites/cited_by consistency, removes dangling refs, deduplicates.
- Strips derived fields (`author_lastnames`, `title_normalized`, `discovered_via`) not in local schema.

Duplicate resolution and author linking are handled by `/clean-db` after the merge.

## Rules

- Always use `.venv/bin/python3` for scripts.
- Parse `--name`, `--enrich`, `--force` from user arguments and pass through to scripts.

---

## Step 1 — Run naive merge

```
.venv/bin/python3 scripts/enrich/merge_db.py <source_dir> [--name <label>] [--enrich] [--force]
```

The script handles:
- Source path resolution (tries `<source_dir>/data/db/papers.json`, falls back to `<source_dir>/papers.json`)
- Paper merging with type-aware logic (on ID collision: enrichment or skip)
- Context merging (deduplicates by citing/cited pair)
- Copying reference files to `data/db_imports/<name>/`

---

## Step 2 — Validate

Run the DB consistency checker:

```
.venv/bin/python3 scripts/build/check_db.py
```

If exit code is non-zero, report the errors to the user. If exit code is 0, report success with paper counts.

---

## Step 3 — Deduplication & author linking

Run `/clean-db` to resolve any duplicates introduced by the merge and link authors for newly-added papers.

---

## Step 4 — Summary

Report to the user:
- Total papers (owned + external_owned + stubs)
- Papers added/enriched/skipped
- Contexts merged
- Duplicates resolved and authors linked (from /clean-db)

**Paths:** `data/db/papers.json`, `data/db/contexts.json`, `data/db/authors.json`, `data/db_imports/`
