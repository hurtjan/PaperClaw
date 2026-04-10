# CLAUDE.md — Query Environment

Read-only query environment for the literature database. The user asks questions about their literature; you answer using the query scripts.

## Rules

- **All data access goes through the query scripts.** Never read files directly — a hook will block it.
- All scripts: `python3 scripts/py.py scripts/query/<script>.py`
- Always use **(Author, Year)** format in user-facing output. Never expose internal paper IDs.
- **Synonym expansion:** Search once first. Only try synonyms or broader terms if the first search returns <3 results.
- **Run compact commands directly** — single-paper lookups, stats, centrality, batch `sql` queries. **Delegate to `query-executor`** when output is verbose: broad searches, `explore`, `owned`, `chain --depth 2+`. Launch multiple agents in parallel for independent queries.
- Run `python3 ../scripts/py.py ../scripts/build/sync_query.py` to refresh the database after upstream changes.

## Extraction levels

Owned papers have a `detail_level` column: `metadata` (Pass 1), `contexts` (1+2), `analysis` (1+2+3), `full` (all 4). `passes_completed` stores comma-separated pass numbers. Stubs have NULL. Use `stats` for distribution; `owned`/`paper` for per-paper detail.

## SQL Schema (for `sql` subcommand)

Syntax: `sql "SELECT ... LIMIT N"` — put `LIMIT` inside the SQL, **no `--limit` flag**.

Key tables and columns:

| Table | Columns |
|---|---|
| `papers` | `paper_id, type, title, authors, year, journal, doi, abstract, s2_id, arxiv_id, pubmed_id, pmc_id, preprint_server, open_access_url, detail_level, passes_completed` |
| `contexts` | `citing_id, cited_id, cited_title, purpose, section, quote, explanation` |
| `citation_edges` | `citing_id, cited_id, cited_title` |
| `authors` | `author_id, canonical_name, type, name_variants, paper_count, owned_paper_count` |
| `paper_authors` | `paper_id, author_id` |
| `citation_counts` | `paper_id, cited_by_count` |

Common mistakes: use `citing_id`/`cited_id` (not `citing_paper_id`/`cited_paper_id`), use `open_access_url` (not `urls`).

## Query tools

**`duckdb_query.py`** — single entry point. DuckDB-backed with BM25 full-text search, compound filters, recursive citation chains. Run `--help` for all subcommands.

No SQL equivalent — must use subcommands: `chain`, `pagerank`, `katz`, `co-cited`, `bib-coupling`, `common-citers`, `search-all`.

**`research.py`** — saved findings: `list` · `show` · `search` · `papers` · `missing` · `for-paper` · `overlap` · `tags`

## Workflow

**Orient:** `stats`, `owned`, `research.py search <topic>`
**Search:** `search-all` (best starting point), `search`, `search-claims`, `search-sections`, `search-keywords`, `purpose <tag>`. Limits: `--limit 20` for `search-all` (multi-field, grows fast), `--limit 50` for single-field, `--limit 10` when running directly.
**Drill:** `paper`, `abstract`, `claims`, `sections`, `keywords`, `methodology`, `questions`, `data-sources`, `explore`. Batch status: `sql "SELECT paper_id, type, detail_level FROM papers WHERE paper_id IN (...)"`.
**Connect:** `cites`, `cited-by`, `chain`, `co-cited`, `bib-coupling`, `shared-refs`, `common-citers`, `pagerank`, `katz`, `top-cited`. Use `pagerank --seed <id>` or `katz --seed <id>` to rank papers by importance relative to specific papers. Tips: `--stubs` for gap discovery, `--reverse` for surveys, `katz` > `pagerank` for foundational works.
**Synthesize:** Answer using **(Author, Year)**. Tables for comparisons/status, prose for literature questions. Suggest follow-up queries.

## Agent prompt format

Give the agent the exact command and what to extract. No open-ended questions.

```
Execute: python3 scripts/py.py scripts/query/duckdb_query.py search-all "topic" --limit 20
Summarize: paper IDs, titles, which fields matched. Note owned vs stub.
```

For two-step queries, add `Step 1`/`Step 2` with a `Pick:` line for selection criteria. Agent summaries preserve `[paper_id]` for chaining.

**If the executor returns an error:** fix the query using the schema above (column names, syntax) and re-invoke the executor once. Do not ask the user — diagnose and correct silently.

## Handoff: forward citation requests

When the user wants to pull forward citations (papers that cite a paper), queue the request:

```
python3 scripts/py.py scripts/query/duckdb_query.py request-pull <paper_id> [paper_id ...]
```

This writes IDs to `data/pull_citing.txt`. Then tell the user:

> Papers queued for citation fetching. To fetch and integrate, switch to the main project:
> `cd .. && claude` then `/fetch-s2 forward`
