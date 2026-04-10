---
description: "Semantic Scholar integration: forward citations, external ID enrichment, preprint discovery. Usage: /fetch-s2 [forward|enrich|title-search|all] [paper_id ...]"
---

You are enriching the literature database via the Semantic Scholar API.

Arguments: $ARGUMENTS

Subcommands:
- `forward` — Fetch forward citations (papers that cite owned papers)
- `enrich` — Batch-resolve DOIs → S2 IDs, arXiv, PubMed, open-access URLs
- `title-search` — Find S2 IDs + DOIs for papers that have no DOI (by title matching)
- `s2-enrich` — Fetch openAccessPdf/externalIds for papers that already have S2 IDs
- `all` — Run enrich → s2-enrich → forward → title-search in order
- No args — show status and suggest what to run

Optional paper IDs to limit scope.

## Rules

- Always use `python3 scripts/py.py` for scripts.
- S2 API key is read from `project.yaml` (`apis.semantic_scholar.key`), env var `S2_API_KEY`, or `--api-key` flag.
- If no key is configured, warn the user and link to https://www.semanticscholar.org/product/api#api-key-form
- After any write operation, run `/clean-db` if stubs were created (forward mode), or `sync_query.py` otherwise.

---

## Query-dir handoff

Before running, check if `query/data/pull_citing.txt` exists (written by the query subproject's `request-pull` command). If it does:
1. Read the paper IDs from the file (one per line).
2. Show the user which papers were queued and ask whether to proceed.
3. Use those IDs as `--paper` arguments to the forward citation fetch.
4. Delete `query/data/pull_citing.txt` after successful fetching.

---

## Status (no args)

Show current coverage by querying the database:

```sql
SELECT
    COUNT(*) as total,
    COUNT(s2_id) as has_s2,
    COUNT(CASE WHEN doi IS NOT NULL AND s2_id IS NULL THEN 1 END) as doi_without_s2,
    COUNT(CASE WHEN doi IS NULL AND s2_id IS NULL THEN 1 END) as no_doi_no_s2,
    COUNT(arxiv_id) as has_arxiv,
    COUNT(pubmed_id) as has_pubmed,
    COUNT(open_access_url) as has_oa,
    COUNT(preprint_server) as has_preprint
FROM papers
```

Then suggest the most useful next step based on gaps.

---

## Enrich — DOI batch resolution

Resolves DOIs to S2 IDs, arXiv/PubMed/PMC IDs, and open-access URLs via `POST /paper/batch`.

```
python3 scripts/py.py scripts/enrich/fetch_external_ids.py [--limit N] [--force]
```

- Batches of 500, ~20 seconds for 10k papers with API key
- Also extracts arXiv IDs from arXiv DOIs (`10.48550/arxiv.*`) without any API call
- Skips papers that already have `s2_paper_id` unless `--force`

---

## S2-enrich — open-access URLs for existing S2 IDs

For papers that already have an S2 ID (e.g. from forward citation fetch) but are missing `openAccessPdf`:

```
python3 scripts/py.py scripts/enrich/fetch_external_ids.py --s2-enrich [--limit N] [--force]
```

---

## Title-search — papers without DOIs

Searches S2 by title for papers that have no DOI and no S2 ID. Prioritised: owned → external_owned → stubs (by citation count descending). Match verification: title similarity ≥ 0.85 AND (year match OR author lastname match).

```
python3 scripts/py.py scripts/enrich/fetch_external_ids.py --title-search [--limit N]
```

- 1 API call per paper (not batchable) — slower
- Finds DOIs, S2 IDs, arXiv IDs for stub entries
- Checkpoints every 100 papers
- For large runs, use `--limit 500` per session

---

## Forward — forward citation discovery

Fetch papers that cite your owned papers via S2 citation API. Creates stub entries and wires citation edges.

### Step 1: Fetch S2 data

```
python3 scripts/py.py scripts/enrich/fetch_forward_citations.py [--paper ID ...| --all] [--force] [--max-per-paper N]
```

- Resolves S2 paper IDs (via DOI then title search fallback)
- Fetches all citing papers from Semantic Scholar
- Saves raw results to `data/tmp/s2_forward_results.json`
- Caches results for 30 days (use `--force` to re-fetch)

### Step 2: Create stubs

```
python3 scripts/py.py scripts/link/apply_forward.py
```

Creates stubs for citing papers not already in the DB. New stubs are marked `dedup_pending=True`.

### Step 3: Deduplicate

Run `/clean-db` to merge duplicate stubs and link authors.

---

## All — full pipeline

Run in order:

1. **Enrich** — resolve DOIs → S2 IDs + external IDs
2. **S2-enrich** — fetch openAccessPdf for papers with S2 IDs
3. **Forward** — fetch forward citations for owned papers
4. **Title-search** — resolve remaining no-DOI papers (limit 500)
5. Rebuild DuckDB + sync query environment

After each step, show progress and continue.

---

## After any mode

Sync the query database:

```
python3 scripts/py.py scripts/build/sync_query.py
```

If stubs were created (forward mode), remind the user to run `/clean-db` first.
