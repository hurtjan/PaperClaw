# Query Scripts — Overview

All scripts run from project root with `.venv/bin/python3 scripts/query/<script>.py`.

## Decision guide

| Question type | Script | Command |
|---|---|---|
| **Best starting point for any topic** | `duckdb_query.py` | `search-all "<phrase>"` |
| Full-text BM25 search with filters | `duckdb_query.py` | `search "<phrase>" [--filter-purpose TAG]` |
| Who cites paper X, and why? | `duckdb_query.py` | `cites <id>` |
| What does paper X cite? | `cite_explorer.py` | `<id> --detail summary` |
| Recursive citation chain | `duckdb_query.py` | `chain <id> [--depth N]` |
| Top-cited papers | `duckdb_query.py` | `top-cited [N]` |
| Paper methodology/claims | `duckdb_query.py` | `methodology <id>` / `claims <id>` |
| Citation network importance | `duckdb_query.py` | `pagerank` / `katz` (supports `--reverse`, `--undirected`) |
| Research findings | `research.py` | `list / show <id> / search <term>` |
| All owned papers | `duckdb_query.py` | `owned` |
| Author's work | `duckdb_query.py` | `author <name>` |
| Author entity details | `duckdb_query.py` | `author-info <id>` |
| Find author by name (fuzzy) | `duckdb_query.py` | `search-authors "<name>"` |
| Coauthor network | `duckdb_query.py` | `coauthors <id>` |
| Most prolific authors | `duckdb_query.py` | `top-authors [N]` |
| Corpus statistics | `duckdb_query.py` | `stats` |

## Scripts

- **`duckdb_query.py`** — Primary query engine. DuckDB-backed with BM25 full-text search, compound filters, recursive citation chains, and in-database PageRank/Katz. Auto-builds `data/db/lit.duckdb` on first run; use `rebuild` after ingesting new papers.
- **`cite_explorer.py`** — How one paper cites others (reads `data/extractions/`). Supports `--detail minimal|summary|normal|full` for varying output levels.
- **`research.py`** — Saved research findings

## DuckDB quick reference

```
# Build (or rebuild) the DB after adding papers:
.venv/bin/python3 scripts/build/build_duckdb.py [--fts] [--force]

# Most useful commands:
duckdb_query.py search-all "<topic>"          # broad overview
duckdb_query.py search "<phrase>" --limit 15  # BM25 ranked
duckdb_query.py cites <id> --limit 10         # who cites this
duckdb_query.py chain <id> --depth 2          # citation chain
duckdb_query.py pagerank --top 15 --owned     # most central owned papers
duckdb_query.py stats                         # corpus summary
```
