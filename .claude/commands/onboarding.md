---
description: First-run setup and project orientation. Checks environment health, guides through any missing setup steps, and orients the user on what to do next.
---

You are helping a user get set up with PaperClaw and oriented on how to use it.

## Step 1 — Environment check

Check the following, running checks in parallel where possible:

1. **Python venv** — does `.venv/` exist?
   - If not: run `python3 -m venv .venv` then `.venv/bin/pip install -r requirements.txt`
   - If it exists but packages may be stale: run `.venv/bin/pip install -r requirements.txt` (pip will skip already-satisfied deps)

2. **DuckDB FTS extension** — does `.duckdb_extensions/` exist?
   - If not: run `.venv/bin/python3 scripts/build/install_fts.py`

3. **Data directories** — do `data/db/`, `data/pdfs/`, `data/text/`, `data/extractions/`, `data/tmp/`, and `pdf-staging/` exist?
   - Create any that are missing with `mkdir -p`

Report what was found and what (if anything) was fixed.

## Step 2 — Project state

After the environment is ready, assess the current state of the database:

- Run `.venv/bin/python3 scripts/build/check_db.py` if `data/db/papers.json` exists, to get a summary of what's in the DB.
- Check `pdf-staging/` for any PDFs waiting to be ingested.

Then give the user a brief, plain-English status:
- How many papers are in the database (if any), broken down by type (owned / external / stubs)
- Whether there are PDFs waiting in staging

## Step 3 — What to do next

Based on the state above, suggest the most useful next step:

- **Empty database, no PDFs in staging:** "Drop PDFs into `pdf-staging/` and run `/ingest` to get started."
- **Empty database, PDFs in staging:** "You have N PDFs ready — run `/ingest` to extract and add them to the database."
- **Database has papers, PDFs in staging:** "You have N PDFs waiting — run `/ingest` to add them."
- **Database has papers, nothing in staging:** "Your database is set up with N papers. Try `/query <your question>` to search your literature."

Finally, give a one-line reminder of the four main commands: `/ingest`, `/query`, `/merge`, `/pull-citing`.
