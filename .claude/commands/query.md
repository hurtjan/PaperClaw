---
description: Query the literature database using the query scripts for lit review research. Usage: /query <topic or question>
---

You are helping the user query their literature database for a literature review.

Their query or topic: $ARGUMENTS

## Rules

- **If /ingest was run in this context**, warn: _"This context was used for ingestion. For best results, run queries in a separate context."_ Ask to confirm before proceeding.
- **NEVER read DB files directly.** All data access goes through `duckdb_query.py` or `research.py`.
- Always use **(Author, Year)** format. Never expose internal paper IDs in user-facing output.
- All scripts: `.venv/bin/python3 scripts/query/<script>.py`
- **Synonym expansion:** If a search returns <3 results, try synonyms or broader terms before concluding the topic is absent.

---

## Query tool

**`duckdb_query.py`** — single entry point for all queries. DuckDB-backed with BM25 full-text search, compound filters, and recursive citation chains. Run `--help` for all subcommands and flags.

Most queries can be answered with raw `sql "<query>"`. Key tables:

```
papers        (paper_id, type, title, authors, year, journal, doi, abstract)
citation_edges(citing_id, cited_id, cited_title)
contexts      (citing_id, cited_id, purpose, section, quote, explanation)
claims        (paper_id, claim, type, confidence, evidence_basis, quantification)
keywords      (paper_id, keyword)
sections      (paper_id, heading, summary, annotated_text)
questions     (paper_id, question)
methodology   (paper_id, type, model_name, approach, temporal_scope, geographic_scope)
authors       (author_id, canonical_name, type, paper_count, owned_paper_count)
paper_authors (paper_id, author_id)
citation_counts(paper_id, cited_by_count)
```

The following have **no SQL equivalent** — must use built-in subcommands: `chain`, `pagerank`, `katz`, `co-cited`, `bib-coupling`, `common-citers`, `search-all`.

**`research.py`** — saved findings: `list` · `show` · `search` · `papers` · `missing` · `for-paper` · `overlap` · `tags`

---

## Workflow

### 1. Orient

Run directly (compact output): `stats`, `owned`, `research.py search <topic>`

### 2. Search broadly

`search-all "<topic>"` is the best starting point. Also: `search`, `search-claims`, `search-sections`, `search-keywords`, `purpose <tag>`. Always use `--limit`.

**Use `query-executor` agent** for broad exploratory searches (`search-all`, multi-term searches) — output is often large and noisy, so let the agent summarize and filter relevance instead of dumping raw results into main context.

**Bash directly** only when output is compact and bounded — single-subcommand searches with low `--limit` and specific terms where you're confident the result fits in a few lines.

Give the agent explicit steps when you want a two-step exploration:

```
Step 1: .venv/bin/python3 scripts/query/duckdb_query.py <command> [args]
Step 2: .venv/bin/python3 scripts/query/duckdb_query.py <command template with ___ for IDs>
<selection criteria — how to pick which results from Step 1 feed into Step 2>
<natural question about what you want to learn>
```

Provide the Step 2 command template — the agent only fills in IDs from Step 1 results. Launch multiple agents in parallel for independent queries.

### 3. Drill into specific papers

Run **directly via Bash** (short output): `paper`, `abstract`, `claims`, `sections`, `keywords`, `methodology`, `questions`, `data-sources`.

### 4. Explore connections

`cites`, `cited-by`, `chain --depth N`, `co-cited`, `bib-coupling`, `shared-refs`, `common-citers`, `pagerank`, `katz`, `top-cited`.

**Use `query-executor` agent** for large or unbounded output — deep chains, centrality rankings with many results, or multi-step exploration (e.g., get co-cited papers then read their abstracts). Let the agent summarize instead of flooding main context.

**Bash directly** only for single bounded commands — `cites`/`cited-by` for one paper, `shared-refs` between two papers, small `top-cited` lists.

**Centrality tips:** `--owned` = owned only, `--stubs` = gap discovery, `--reverse` = surveys, `--undirected` = bridge papers. `katz` > `pagerank` for foundational works.

### 5. Synthesize

Answer using **(Author, Year)** format. Suggest follow-up queries. If an agent returns an error or no DONE line, retry the command directly via Bash.

Agent prompt formats:
```
# Single-step (one command):
Execute: .venv/bin/python3 scripts/query/duckdb_query.py <subcommand> [args]
<question about what you want to know>

# Two-step (exploratory):
Step 1: .venv/bin/python3 scripts/query/duckdb_query.py <command> [args]
Step 2: .venv/bin/python3 scripts/query/duckdb_query.py <command template with ___ for IDs>
<selection criteria — how to pick which results feed into Step 2>
<question about what you want to learn>
```
Agent summaries preserve `[paper_id]` for chaining into follow-ups.

Purposes: `background` `motivation` `methodology` `data_source` `supporting_evidence` `contrasting_evidence` `comparison` `extension` `tool_software`
