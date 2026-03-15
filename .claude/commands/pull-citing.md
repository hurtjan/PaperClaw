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
- No agents needed — these are pure Python scripts.
- Recommend setting `S2_API_KEY` env var for title search and higher rate limits.

---

## Forward Citations

Fetch all papers that cite each owned paper via the S2 API:

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py [paper_id ...]
```

- Adds discovered papers as `stub` entries with `discovered_via="s2_forward"`
- Caches results for 30 days
- Requires `S2_API_KEY` for title search and higher rate limits

---

## S2 ID Backfill

Resolve Semantic Scholar paper IDs for owned papers that don't have one yet. Uses DOI lookup (fast path) or title search (fallback). S2 IDs enable forward citation discovery and richer cross-referencing.

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

