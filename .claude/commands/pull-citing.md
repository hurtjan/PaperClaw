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
- Always invoke agents via the Agent tool, never via Bash.

---

## Forward Citations

Fetch and integrate papers that cite each owned paper via the S2 API, with agent-reviewed matching.

### Step 1: Fetch S2 data

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py [--paper ID ...| --all] [--force] [--max-per-paper N]
```

- Resolves S2 paper IDs (via DOI then title search fallback)
- Fetches all citing papers from Semantic Scholar
- Saves raw results to `data/tmp/s2_forward_results.json`
- Persists resolved S2 IDs on owned papers in `papers.json`
- Caches results for 30 days (use `--force` to re-fetch)

### Step 2: Score candidates

```
.venv/bin/python3 scripts/link/link_forward.py
```

Scores each citing paper against the DB. Classifies as AUTO_MATCHED (score > 6), NEEDS_JUDGMENT (score 1-6), or NEW (no match). Writes `data/tmp/forward_candidates.txt`.

Follow the output directive:
- `NEXT: forward-citation-linker` → proceed to Step 3
- `STOP: no candidates to review` → skip directly to Step 4

### Step 3: Agent review (if NEXT from Step 2)

Invoke the `forward-citation-linker` agent. It will:
1. Read `data/tmp/forward_candidates.txt`
2. Verify AUTO_MATCHED entries, decide NEEDS_JUDGMENT entries (match or `new`)
3. Write `data/tmp/forward_resolved.txt`
4. Run `apply_forward.py` to apply decisions

### Step 4: Auto-apply (if STOP from Step 2)

```
.venv/bin/python3 scripts/link/apply_forward.py
```

(All citations were NEW — no agent review needed. Creates stubs automatically.)

### Step 5: Author linking

```
.venv/bin/python3 scripts/link/link_authors.py
```

Follow the output directive:
- `NEXT` → invoke the `author-resolver` agent
- `STOP` → done

---

## Legacy mode (skip agent review)

Use `--no-review` to auto-accept all matches without an agent review step:

```
.venv/bin/python3 scripts/enrich/fetch_forward_citations.py [--paper ID ...| --all] --no-review
```

Directly integrates citations into `papers.json` without saving raw results or running link/apply scripts. Useful for batch runs where manual review isn't needed.

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
