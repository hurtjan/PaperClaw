---
description: Fetch papers that cite your owned papers from Semantic Scholar. Usage: /pull-citing [forward|ids] [paper_id ...]
---

You are fetching citing papers and enriching the literature database with Semantic Scholar data.

Arguments: $ARGUMENTS

- `forward` (or no args): Fetch forward citations for owned papers.
- `ids`: Backfill S2 paper IDs on papers missing them.
- Optional paper IDs to limit scope.

## Rules

- Always use `.venv/bin/python3` for scripts.
- Recommend setting `S2_API_KEY` env var for title search and higher rate limits.

---

## Forward Citations

Fetch citing papers via S2, create stubs, then deduplicate with /clean-db.

### Step 1: Fetch S2 data

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py [--paper ID ...| --all] [--force] [--max-per-paper N]
```

- Resolves S2 paper IDs (via DOI then title search fallback)
- Fetches all citing papers from Semantic Scholar
- Saves raw results to `data/tmp/s2_forward_results.json`
- Persists resolved S2 IDs on owned papers in `papers.json`
- Caches results for 30 days (use `--force` to re-fetch)

### Step 2: Create stubs

```
.venv/bin/python3 scripts/link/apply_forward.py
```

Creates stubs for all citing papers not already in the DB (exact-match by DOI/S2 ID). New stubs are marked `dedup_pending=True`. Wires `forward_cited_by` edges on owned papers.

### Step 3: Deduplicate and link authors

Run `/clean-db` to find and merge duplicate stubs, then link authors.

---

## S2 ID Backfill

Resolve Semantic Scholar paper IDs for owned papers that don't have one yet. Uses DOI lookup (fast path) or title search (fallback). S2 IDs enable forward citation discovery.

The forward citation script resolves S2 IDs automatically as part of its workflow. To backfill IDs without fetching citations, use `--dry-run`:

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py --all --dry-run
```

This resolves and prints S2 IDs for all owned papers without writing citation data. To persist the resolved IDs, run without `--dry-run`:

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py --all
```

- To limit to specific papers: `--paper <id1> <id2>`
- Requires `S2_API_KEY` for title search (DOI lookups work without it)
