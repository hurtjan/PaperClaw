# PaperClaw

> Careful, this is just a weekend project.

Transforming your PDF paper library into a cross-referenced, queryable database.

**Did this happen to you?**
You start reading one paper. It cites five interesting ones. Each of those cites five more. Before you know it, you have 50 browser tabs and no idea which papers are actually relevant, which ones you've already read, or which key sources are still missing. Maybe you try pasting them into a chatbot, but you hit the context window after paper four.

**PaperClaw** ingests your PDFs and uses AI agents to extract structured metadata, citations, and context — then fuzzy-matches everything across papers to build a unified citation graph, which you can then query using Claude Code. Think of it as a personal literature database that actually understands how your papers relate to each other. It runs inside Claude Code — no additional subscriptions are needed beyond Claude Code itself.

## Getting started

```bash
git clone <repo-url> PaperClaw
cd PaperClaw
claude
```

On first launch, `/onboarding` runs automatically to set up your environment. After that, drop your PDFs into `pdf-staging/` and run `/ingest`.

## Project structure

PaperClaw is split into two Claude Code environments — one for building the database, one for querying it.

| Environment | How to open | Purpose |
|---|---|---|
| **Main** (`PaperClaw/`) | `cd PaperClaw && claude` | Ingest PDFs, manage the database, merge corpora, fetch citations |
| **Query** (`PaperClaw/query/`) | `cd PaperClaw/query && claude` | Ask questions about your literature — read-only, cheap (Haiku-powered) |

The separation keeps query sessions fast and inexpensive. The query environment is sandboxed and read-only — it can't modify your database, and a guard hook blocks direct file reads so all data flows through optimized query scripts.

**Token usage:** Extraction mostly relies on cheap Haiku agents, only falling back to Sonnet when necessary. A typical Nature paper costs around 5-15% of your session budget on the $20/month Pro plan. Querying in the `query/` subdirectory is significantly cheaper since it uses a Haiku-based agent.

**Cross-corpus analysis**
- "Which papers discuss [topic], and what methodologies do they use?"
- "What are the main claims about [topic] across my literature? Do any contradict each other?"
- "Compare the data sources used by papers on [topic] — which datasets come up most often?"

**Citation graph intelligence**
- "Which references appear in the bibliographies of at least 3 different papers? Those are the foundational works."
- "What do (Author A, Year) and (Author B, Year) both cite? What do those shared references tell us about their common theoretical basis?"
- "Which papers cite (Author, Year) as contrasting evidence vs. supporting evidence?"

**Gap discovery**
- "Which papers are most heavily referenced but don't have PDFs in the corpus yet? Those are the ones I should read next."
- "Run PageRank on the citation graph — which highly-connected papers am I missing?"
- "Find papers from before 2010 that are cited as foundational by multiple papers but that I haven't ingested."

**Network & authorship**
- "Which authors appear across the most papers? Who are the key figures in this literature?"
- "Do any authors cite each other? Show me the mutual citation patterns."
- "Are there clusters of papers that cite each other heavily but don't connect to the rest?"


## Ingestion

`/ingest` runs a four-phase pipeline that takes a PDF from a raw file to a fully integrated database entry.

### Phase 1: PDF intake

Each PDF in `pdf-staging/` is processed by `ingest.py`:

- **Text extraction** via PyMuPDF — produces plain text with page markers
- **Duplicate detection** — fuzzy-matches the title/authors against existing database entries using multi-signal scoring (DOI, author+year, title similarity)
- Clean papers are moved to `data/pdfs/` with their text saved to `data/text/`

### Phase 2: Multi-pass extraction

AI agents read the extracted text and produce structured JSON. Extraction runs up to four passes, configured in `project.yaml` (defaults: 1, 2, 4):

| Pass | Agent | Extracts |
|------|-------|----------|
| **1** | `paper-extractor` | Paper ID, title, authors, abstract, year, DOI, and a complete reference list |
| **2** | `paper-extractor-contexts` | For each citation: the section, purpose (e.g. `background`, `methodology`, `supporting_evidence`), the exact quote, and an explanation of why it's cited |
| **3** | `paper-extractor-analysis` | Research questions, methodology, claims (with confidence levels), keywords, topics |
| **4** | `paper-extractor-sections` | Section headings, summaries, and condensed annotated text |

Each pass writes a sidecar file (e.g. `.contexts.json`, `.sections.json`). After all passes complete, `merge_extraction.py` combines them into a single extraction JSON with full provenance metadata (which models ran, agent versions, date). Large papers are automatically split into page-range chunks for parallel extraction.

### Phase 3: Linking

This is where extracted citations get wired into the database. For each citation in the extraction:

1. **`link_paper.py`** scores it against every existing database entry using a multi-signal system:
   - Exact ID or DOI match: +4 points
   - First-author + year match: +2
   - High title similarity (≥90%): +2, moderate (70-90%): +1

2. Citations are bucketed: **auto-matched** (≥4 points), **needs judgment** (1-3), or **new** (no candidates).

3. The **cross-reference-linker** agent reviews ambiguous matches and makes decisions. Its resolved judgments are applied by `apply_link.py`, which creates or updates database entries and wires bidirectional `cites`/`cited_by` links.

4. **Author entity resolution** follows the same pattern — `link_authors.py` generates candidates, the `author-resolver` agent handles ambiguities, and `apply_authors.py` writes the results.

### Phase 4: Database rebuild

After linking, the database indexes are rebuilt:
- **Context index** (`contexts.json`) — citation contexts indexed by cited paper and by purpose
- **Network graph** — sparse adjacency matrix for PageRank and centrality analysis
- **Consistency check** — validates bidirectional links, required fields, no orphans or dangling references

## The database

The database lives in `data/db/` as three JSON files plus a derived query layer.

### papers.json

Every paper is one of three types:

| Type | Meaning |
|------|---------|
| **`owned`** | You have the PDF. Full extraction, full metadata, complete reference list. |
| **`external_owned`** | Imported from another corpus (see [Merging](#merging-databases)). Full metadata, no local PDF. |
| **`stub`** | Referenced by an owned paper but you don't have it. Minimal metadata from the citing paper's reference list. |

All papers carry bidirectional citation links: `cites` (what this paper references) and `cited_by` (which owned papers reference it). These are enforced to be consistent by `check_db.py`.

### contexts.json

Records *how* each citation is used — not just that paper A cites paper B, but the section, the exact quote, the purpose (`background`, `methodology`, `supporting_evidence`, `contrasting_evidence`, etc.), and an agent-generated explanation. Indexed two ways: by cited paper (look up everything said about a paper) and by purpose (find all methodological citations across the corpus).

### authors.json

Entity-resolved author index. Handles name variants (e.g. "J. Smith" and "John Smith" → same entity), tracks which papers each author appears in, and maintains a coauthor graph. Includes both persons and institutional authors.

### Network graph

A sparse adjacency matrix (scipy CSR format) built from the citation links in `papers.json`. Enables PageRank and Katz centrality analysis to find the most structurally important papers in your corpus.

## Querying

Open the query environment and ask questions in natural language:

```bash
cd query && claude
```

The query environment is a standalone Claude Code project — sandboxed, read-only, powered by a Haiku agent to keep costs low. It has access to:

- **Full-text search** — BM25-ranked search across titles, abstracts, claims, section summaries, and more
- **Citation chains** — trace references forward or backward with configurable depth (recursive SQL)
- **Purpose filtering** — find all papers cited as methodology, contrasting evidence, etc.
- **Author lookup** — search by author, list coauthors, find an author's full publication list
- **Network analysis** — PageRank and Katz centrality for structurally central papers; personalized PageRank; reverse mode to find surveys
- **Co-citation & bibliographic coupling** — find papers that appear together in bibliographies or share references
- **Raw SQL** — escape hatch for arbitrary DuckDB queries when built-in commands aren't enough

The query database syncs automatically after `/ingest`, `/clean-db`, and `/merge`. To sync manually: `python3 query/sync.py` from the project root.

To return to the main environment for ingestion or database management: `cd .. && claude`.


## Merging databases

Different researchers using PaperClaw independently build separate citation graphs — one person might have 50 papers on climate finance, another 30 on network economics, with partial overlap. `/merge` combines them into a single unified database.

```
/merge <source_dir> [--name <label>] [--enrich] [--force]
```

### How it works

`merge_db.py` applies type-aware merge logic:

- **Your owned papers are sacrosanct** — the merge never overwrites a locally owned paper's metadata or links
- **External owned papers** become `external_owned` entries in your DB — full metadata (title, authors, cites, cited_by) without local PDF files. A `source_db` field tracks provenance
- **Stubs get upgraded** — if the other corpus has full metadata for a paper you only have as a stub, it's promoted to `external_owned`
- **Enrichment mode** (`--enrich`) fills in missing fields (DOIs, abstracts, S2 IDs) on overlapping entries without overwriting existing data

After merging papers, the script repairs all bidirectional citation links (removing dangling references, enforcing symmetry), merges citation contexts with deduplication, rebuilds the author index, and runs a full consistency check.

### Adopting imported papers

If you later obtain the PDF for an `external_owned` paper, `/ingest` detects the match and promotes it to `owned` — then you can run full extraction and linking on it.

## Semantic Scholar enrichment

`/pull-citing` connects your local database to Semantic Scholar's broader graph:

- **Forward citations** — discover papers published *after* yours that cite them (something PDF extraction alone can't do)
- **S2 ID backfill** — resolve Semantic Scholar paper IDs for your stubs, enabling richer cross-referencing

## Commands

All commands run in the **main** environment (`PaperClaw/`).

| Command | Purpose |
|---------|---------|
| `/ingest` | Full pipeline: PDF intake → extraction → linking → DB rebuild |
| `/merge <source>` | Import an external PaperClaw database into the local DB |
| `/export` | Bundle the local DB into a shareable `.paperclaw` file |
| `/pull-citing` | Fetch forward citations and backfill S2 IDs from Semantic Scholar |
| `/fetch-preprints` | Download PDFs from arXiv, bioRxiv, medRxiv, SSRN |
| `/clean-db` | Find and merge duplicate papers, then link authors |
| `/test` | End-to-end pipeline test using fixture PDFs |

## Customization

Add custom agents or commands with the `local-` prefix — these are gitignored and won't conflict with upstream updates:

- `.claude/agents/local-my-agent.md`
- `.claude/commands/local-my-command.md`

Extraction passes and models are configured in `project.yaml`.

## Updating

```bash
git pull
```

Your database (`data/db/`), extractions (`data/extractions/`), PDFs (`data/pdfs/`), and `local-*` customizations are never touched by upstream changes.
