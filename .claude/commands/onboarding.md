---
description: First-run setup, user profile, skill guide, and project orientation. Checks environment health, sets up user profile, explains skills and workflow, and orients the user on what to do next. Auto-triggers if no user profile exists.
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

## Step 2 — User setup

Read `project.yaml` (or note that it doesn't exist yet).

**If no `user:` key exists:**
- Ask the user for their **name only** — do NOT ask for research focus, institution, affiliation, or any other profile fields. Explain that their name is used to tag database change history (JSON Patch deltas) so edits can be attributed and rolled back by author.
- Once they answer, write to `project.yaml` under the `user:` key using this Python snippet (adapt values as needed):

```python
import yaml, pathlib, datetime
p = pathlib.Path("project.yaml")
data = yaml.safe_load(p.read_text()) if p.exists() else {}
data["user"] = {
    "name": "NAME",
    "onboarded_at": str(datetime.date.today()),
}
p.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True))
```

Run this with `.venv/bin/python3 -c "..."` (inline the snippet).

**If `user:` already exists:**
- Greet the user by name.
- Offer to update their profile if they'd like, but do not re-ask for their details unprompted.
- Continue to Step 3.

## Step 3 — Extraction defaults

Check whether `ingest_defaults.md` exists in the memory directory.

**If it exists:** Show the current setting (e.g. "Default passes: 1, 2, 4") and offer to change it, but do not re-prompt unprompted.

**If it does not exist:** Explain the extraction passes and ask which to run by default:

> **Extraction depth:** When you ingest a PDF, PaperClaw runs up to 4 extraction passes. Which should be your default?
>
> | Pass | What it extracts |
> |---|---|
> | 1 | Metadata + full reference list (required) |
> | 2 | Citation contexts — where and why each reference is cited |
> | 3 | Research questions, methodology, claims, keywords, topics |
> | 4 | Section headings, per-section summaries, annotated text |
>
> **Default: all 4 passes** (full extraction). Say "1 2" or "1 2 4" if you want a lighter default.

Unless the user specifies otherwise, save all 4 passes as the default. Save the choice to `ingest_defaults.md` in the memory directory:

```markdown
---
name: ingest_defaults
description: User's preferred default extraction passes for /ingest
type: feedback
---

Default extraction passes: [1, 2, 3, 4]

**Why:** User chose these during onboarding.
**How to apply:** Use these passes in /ingest Phase 2 unless overridden by --passes flag.
```

Also add a pointer to `MEMORY.md` in the memory directory if one is not already present.

## Step 4 — Skills overview

Present the available skills grouped by workflow phase. Use a clean table or grouped list:

**Building your database**
- `/ingest` — Extract, link, and integrate new PDFs from `pdf-staging/` into the database.
- `/merge` — Import an external PaperClaw database into your local corpus.

**Querying**
- `/query` — Ask natural-language questions about your literature corpus.

**Expanding your corpus**
- `/pull-citing` — Fetch papers that cite your owned papers from Semantic Scholar (forward citations).
- `/fetch-preprints` — Download open-access PDFs from arXiv, bioRxiv, and medRxiv (SSRN only if Semantic Scholar has a cached copy — SSRN itself is login-walled).

**Maintenance**
- `/onboarding` — Re-run setup, update your profile, or re-read this orientation.
- `/test` — Run the end-to-end pipeline test to verify everything works.

## Step 5 — Usage guide + Semantic Scholar explainer

Show the core workflow as a numbered list:

1. Drop PDFs into `pdf-staging/`
2. Run `/ingest` — extracts text, identifies references, links citations, rebuilds the query index
3. Run `/query` — ask questions about your literature in natural language
4. Run `/pull-citing` — discovers papers published *after* yours that cite them (forward citations)
5. Repeat: drop newly discovered PDFs into staging, ingest, query

Then explain Semantic Scholar integration:

> **Semantic Scholar integration**
>
> Two skills connect to the Semantic Scholar API to expand your corpus beyond what PDF extraction can see:
>
> - **`/pull-citing`** finds papers published *after* your ingested papers that cite them — forward citations that simply don't exist in any PDF's reference list.
> - **`/fetch-preprints`** downloads open-access PDFs from arXiv, bioRxiv, and medRxiv so you can ingest them directly. SSRN papers are only available if Semantic Scholar has a cached open-access copy (SSRN itself requires login).
>
> Both work without authentication, but setting the `S2_API_KEY` environment variable gives you higher rate limits for large corpora.

## Step 6 — Project state

Assess the current state of the database:

- Run `.venv/bin/python3 scripts/build/check_db.py` if `data/db/papers.json` exists, to get a summary of what's in the DB.
- Check `pdf-staging/` for any PDFs waiting to be ingested.

Give the user a brief, plain-English status:
- How many papers are in the database (if any), broken down by type (owned / external / stubs)
- Whether there are PDFs waiting in staging

## Step 7 — What to do next + example queries

Based on the project state, suggest the most useful next step:

- **Empty database, no PDFs in staging:** "Drop PDFs into `pdf-staging/` and run `/ingest` to get started."
- **Empty database, PDFs in staging:** "You have N PDFs ready — run `/ingest` to extract and add them to the database."
- **Database has papers, PDFs in staging:** "You have N PDFs waiting — run `/ingest` to add them."
- **Database has papers, nothing in staging:** "Your database is set up with N papers. Try `/query <your question>` to explore your literature."

Then show example queries that demonstrate the power of the cross-referenced corpus.

**Cross-corpus analysis:**
- "Which papers discuss [topic], and what methodologies do they use?"
- "What are the main claims about [topic] across my literature? Do any contradict each other?"
- "Compare the data sources used by papers on [topic] — which datasets come up most often?"

**Citation graph intelligence:**
- "Which references appear in the bibliographies of at least 3 different papers? Those are the foundational works."
- "What do (Author A, Year) and (Author B, Year) both cite? What do those shared references tell us about their common theoretical basis?"
- "Which papers cite (Author, Year) as contrasting evidence vs. supporting evidence?"

**Gap discovery:**
- "Which papers are most heavily referenced but don't have PDFs in the corpus yet? Those are the ones I should read next."
- "Run PageRank on the citation graph — which highly-connected papers am I missing?"
- "Find papers from before 2010 that are cited as foundational by multiple papers but that I haven't ingested."

**Network & authorship:**
- "Which authors appear across the most papers? Who are the key figures in this literature?"
- "Do any authors cite each other? Show me the mutual citation patterns."
- "Are there clusters of papers that cite each other heavily but don't connect to the rest?"
