# CLAUDE.md — Query Environment

Read-only query environment for the literature database. The user asks questions about their literature; you answer using the query scripts.

## Rules

- **All data access goes through the query scripts.** Never read files directly — a hook will block it.
- All scripts: `python3 scripts/py.py scripts/query/<script>.py`
- Always use **(Author, Year)** format in user-facing output. Never expose internal paper IDs.
- **Synonym expansion:** If a search returns <3 results, try synonyms or broader terms before concluding the topic is absent.
- **Always delegate to the `query-executor` agent** — query output is large and noisy. Let the agent summarize and filter. Launch multiple agents in parallel for independent queries.
- Run `python3 ../scripts/py.py ../scripts/build/sync_query.py` to refresh the database after upstream changes.

## Extraction levels

Owned papers have a `detail_level` column: `metadata` (Pass 1), `contexts` (1+2), `analysis` (1+2+3), `full` (all 4). `passes_completed` stores comma-separated pass numbers. Stubs have NULL. Use `stats` for distribution; `owned`/`paper` for per-paper detail.

## Query tools

**`duckdb_query.py`** — single entry point. DuckDB-backed with BM25 full-text search, compound filters, recursive citation chains. Run `--help` for all subcommands.

No SQL equivalent — must use subcommands: `chain`, `pagerank`, `katz`, `co-cited`, `bib-coupling`, `common-citers`, `search-all`.

**`research.py`** — saved findings: `list` · `show` · `search` · `papers` · `missing` · `for-paper` · `overlap` · `tags`

## Workflow

### 1. Orient
`stats`, `owned`, `research.py search <topic>`

### 2. Search broadly
`search-all "<topic>"` is the best starting point. Also: `search`, `search-claims`, `search-sections`, `search-keywords`, `purpose <tag>`. Always use `--limit`.

### 3. Drill into papers
`paper`, `abstract`, `claims`, `sections`, `keywords`, `methodology`, `questions`, `data-sources`, `explore`.

### 4. Explore connections
`cites`, `cited-by`, `chain --depth N`, `co-cited`, `bib-coupling`, `shared-refs`, `common-citers`, `pagerank`, `katz`, `top-cited`.

**Centrality tips:** `--owned` = owned only, `--stubs` = gap discovery, `--reverse` = surveys, `--undirected` = bridge papers. `katz` > `pagerank` for foundational works.

### 5. Synthesize
Answer using **(Author, Year)**. Suggest follow-up queries. If an agent errors, retry via a new agent.

## Agent prompt format

```
# Single-step:
Execute: python3 scripts/py.py scripts/query/duckdb_query.py <subcommand> [args]
<question about what you want to know>

# Two-step (exploratory):
Step 1: python3 scripts/py.py scripts/query/duckdb_query.py <command> [args]
Step 2: python3 scripts/py.py scripts/query/duckdb_query.py <command template with ___ for IDs>
<selection criteria>
<question>
```

Agent summaries preserve `[paper_id]` for chaining. Purposes: `background` `motivation` `methodology` `data_source` `supporting_evidence` `contrasting_evidence` `comparison` `extension` `tool_software`

## Handoff: forward citation requests

When the user wants to pull forward citations (papers that cite a paper), queue the request:

```
python3 scripts/py.py scripts/query/duckdb_query.py request-pull <paper_id> [paper_id ...]
```

This writes IDs to `data/pull_citing.txt`. Then tell the user:

> Papers queued for citation fetching. To fetch and integrate, switch to the main project:
> `cd .. && claude` then `/pull-citing`
