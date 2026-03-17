---
description: Query the literature database using the query scripts for lit review research. Usage: /query <topic or question>
---

You are helping the user query their literature database for a literature review.

Their query or topic: $ARGUMENTS

## Rules

- **If /ingest was run in this context**, warn: _"This context was used for ingestion. For best results, it's recommended to run queries in a separate context."_ Ask to confirm before proceeding.
- Run all queries via the Bash tool using the scripts below. Do NOT launch any subagents.
- **NEVER write ad-hoc Python or shell scripts to read `data/db/papers.json`, `data/db/contexts.json`, or any extraction file directly.** All data access must go through the query scripts.
- Always use **(Author, Year)** format. Never expose internal paper IDs.
- All scripts: `.venv/bin/python3 scripts/query/<script>.py`
- **Always use `--limit` on broad searches** to avoid flooding context. Start with `--limit 15` for `--search` and `--purpose`; drill down only if needed.
- **Synonym expansion:** If a search returns <3 results, try synonyms or broader/narrower terms before concluding the topic is absent (e.g., regulation → policy / legislation; engagement → involvement / participation).
- **Aim for 8–12 tool calls per query.** Beyond 15 you are likely over-exploring; under 5 you may be under-exploring. Parallelize independent calls in the same message to stay within budget.

---

## Scripts

**`duckdb_query.py`** — primary query tool. DuckDB-backed with BM25 full-text search, compound filters, and recursive citation chains.

**DuckDB tables** (for `sql` command):
- `papers` (paper_id PK, type, title, authors, year INT, journal, doi, abstract, pdf_file, text_file) — type: 'owned'|'cited'; authors: semicolon-separated
- `contexts` (citing_id, cited_id, cited_title, purpose, section, quote, explanation)
- `citation_counts` (paper_id PK, cited_by_count INT)
- `claims` (paper_id, claim, type, confidence, evidence_basis, quantification, supporting_citations)
- `keywords` (paper_id, keyword) · `topics` (paper_id, field, value) — field: themes/geographic_focus/sectors/policy_context
- `sections` (paper_id, heading, summary, annotated_text)
- `methodology` (paper_id PK, type, model_name, approach, temporal_scope, geographic_scope, unit_of_analysis, scenarios)
- `data_sources` (paper_id, name, type, description) · `questions` (paper_id, question)
- `citation_edges` (citing_id, cited_id, cited_title) — complete citation graph from cites arrays; covers all edges, not just those with extracted contexts
- `authors` (author_id PK, canonical_name, type, name_variants, paper_count INT, owned_paper_count INT) · `paper_authors` (paper_id, author_id)

Key commands:
- `sql "<query>"` · `sql --schema` — arbitrary SQL; run `sql --schema` to see all table schemas
- `search-all "<topic>" [--limit N]` — best starting point: summary counts + details across all fields in one pass
- `search "<phrase>" [--limit N] [--filter-purpose TAG] [--filter-year-min Y]` — BM25-ranked search across titles, abstracts, quotes, claims, keywords, topics
- `search-claims "<phrase>" [--limit N] [--type TYPE]` · `search-sections "<phrase>"` · `search-topics "<phrase>"` · `search-keywords "<phrase>"`
- `paper <id>` — paper summary (metadata, cites, cited_by)
- `owned` — list all owned papers
- `author <name>` — search by author (uses entity join when available)
- `author-info <author_id>` — author entity details + paper list
- `search-authors <phrase>` — BM25 search over author names/variants
- `coauthors <author_id>` — coauthor network for an author
- `top-authors [N]` — most prolific authors
- `cites <id> [--limit N]` · `cited-by <id> [--limit N]` — citation relationships with purpose/quote
- `chain <id> [--depth N]` — recursive citation chain traversal (default depth 2)
- `common-citers <id1> <id2>` — papers that cite both
- `co-cited <id> [id2 ...] [--min N] [--limit N]` — co-citation: references frequently appearing alongside the given paper(s) in bibliographies
- `bib-coupling <id> [id2 ...] [--min N] [--limit N]` — bibliographic coupling: papers with most overlapping bibliographies
- `shared-refs <id1> <id2> [id3 ...]` — list cited references shared between papers
- `shared-papers <author1> <author2> [author3 ...]` — papers co-authored by all given authors
- `top-cited [N]` · `purpose <tag> [--limit N]`
- `abstract <id>` · `claims <id> [--type TYPE]` · `keywords <id>` · `methodology <id>` · `sections <id>` · `questions <id>` · `data-sources <id>` — lookup by paper ID; `claims` does **not** support `--limit` (use `search-claims` for text search)
- `pagerank [--seed ID ...] [--top N] [--owned] [--stubs] [--reverse] [--undirected] [--alpha FLOAT]` — in-database PageRank centrality
- `katz [--seed ID ...] [--top N] [--owned] [--stubs] [--reverse] [--undirected] [--alpha FLOAT] [--beta FLOAT]` — in-database Katz centrality
- `stats` · `methods` · `purposes-list`
Purposes: `background` `motivation` `methodology` `data_source` `supporting_evidence` `contrasting_evidence` `comparison` `extension` `tool_software`

Seed from 1–3 topic-relevant owned papers. `--owned` = only owned results, `--stubs` = only cited-only results (gap discovery). `--reverse` = importance flows to citers (finds surveys). `--undirected` = bidirectional (finds bridge papers). `katz` over `pagerank` for foundational/seminal works (counts indirect paths).

Examples: `pagerank --seed ID1 ID2 --top 15 --stubs` (find missing papers), `pagerank --seed ID1 --reverse --top 10` (find reviews).

→ `.venv/bin/python3 scripts/query/duckdb_query.py --help`

**`research.py`** — saved findings from prior queries.
`list` · `show ID` · `search TERM` · `papers ID` · `missing ID` · `for-paper ID` · `overlap ID1 ID2` · `tags`

**Fallback scripts** (if duckdb is unavailable): `query_db.py`, `cite_explorer.py`, `corpus.py` — run with `--help` for usage.

---

## Workflow

1. Check saved findings first: `research.py search <topic>`
2. **Start broad, then narrow:**
   - Overview first → `duckdb_query.py search-all "<topic>"` — summary counts + details across all fields in one pass
   - Narrow by purpose → `duckdb_query.py search "<topic>" --filter-purpose contrasting_evidence --limit 15`
   - Who cites what → `duckdb_query.py cites ID --limit 10` / `purpose TAG --limit 15`
   - Citation chains → `duckdb_query.py chain ID --depth 2`
   - What a paper argues → `duckdb_query.py abstract ID` / `claims ID` / `methodology ID`
   - Centrality analysis → `duckdb_query.py pagerank --seed ID --top 10` (add `--owned`/`--stubs`/`--reverse`/`--undirected` per strategy above)
   - Papers citing an owned paper → `duckdb_query.py cited-by ID --limit 10`
   - Inverse network → `duckdb_query.py co-cited ID1 ID2 --limit 15` / `bib-coupling ID --limit 15` / `shared-refs ID1 ID2`
   - Author overlap → `duckdb_query.py shared-papers AUTHOR1 AUTHOR2`
3. Synthesize using (Author, Year) format and suggest follow-ups.

**Avoid dumping 100+ results.** Use `--limit` to cap broad queries, then selectively expand.

