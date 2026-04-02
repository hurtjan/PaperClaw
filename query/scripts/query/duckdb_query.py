#!/usr/bin/env python3
"""
duckdb_query.py — DuckDB-backed query engine for the literature database.

Loads papers.json, contexts.json, and extraction JSONs into a persistent DuckDB
database with FTS indexes. Supports compound filters, full-text search with
BM25 ranking, and recursive citation chain traversal.

The DuckDB file (data/db/lit.duckdb) must be built before querying:
  python3 scripts/py.py scripts/build/build_duckdb.py [--fts] [--force]

SETUP
-----
  pip install duckdb
  python3 scripts/py.py scripts/build/build_duckdb.py --fts

USAGE
-----
  python3 scripts/query/duckdb_query.py <command> [options]

PAPER LOOKUP
  paper <id>                   Paper summary (metadata, cites, cited_by)
  owned                        List all owned papers
  author <name>                Search papers by author name
  author-info <author_id>      Author entity details + paper list
  search-authors <phrase>      BM25 search over author names/variants
  coauthors <author_id>        Coauthor network for an author
  top-authors [N]              Most prolific authors (default 15)

CITATION QUERIES
  explore <id> [options]       Explore how a paper cites others (detail levels, filters, sorting)
  cites <id> [--limit N]       Papers that cite this paper, with purpose/quote
  cited-by <id> [--limit N]    Papers this paper cites (outbound)
  chain <id> [--depth N]       Recursive citation chain (default depth 2)
  top-cited [N]                Top N most-cited papers
  common-citers <id1> <id2>    Papers that cite both id1 and id2
  purpose <tag> [--limit N]    All contexts with this purpose tag
  co-cited <id> [id2 ...] [--min N] [--limit N]
                               Co-citation: references frequently co-cited with given paper(s)
  bib-coupling <id> [id2 ...] [--min N] [--limit N]
                               Bibliographic coupling: papers with most overlapping bibliographies
  shared-refs <id1> <id2> [id3 ...]
                               List cited references shared between papers
  shared-papers <author1> <author2> [author3 ...]
                               Papers co-authored by all given authors

FULL-TEXT SEARCH (BM25-ranked)
  search <phrase> [--limit N] [--filter-purpose TAG] [--filter-year-min Y]
      Ranked search across titles, abstracts, quotes, claims, keywords, topics.
      Compound filters narrow results by purpose and/or year.
  search-claims <phrase> [--limit N] [--type TYPE]
  search-sections <phrase> [--limit N]

CORPUS METADATA
  abstract <id>                Paper abstract
  claims <id> [--type TYPE]    Claims with evidence
  keywords <id>                Keywords and topics
  methodology <id>             Methodology details
  sections <id>                Section headings with summaries
  questions <id>               Research questions
  data-sources <id>            Data sources used

CROSS-CORPUS
  search-all <phrase> [--limit N]   Summary counts + details across all fields
  search-topics <phrase> [--limit N]
  search-keywords <phrase> [--limit N]
  search-methods <type>
  stats                        Corpus summary statistics
  methods                      List methodology types with counts
  purposes-list                List purpose tags with counts

HANDOFF
  request-pull <id> [id ...]   Queue papers for forward citation fetching (writes to data/pull_citing.txt)

RAW SQL
  sql <query>                  Execute arbitrary SQL against the database
  sql --schema                 Print all table schemas

  Examples:
    sql "SELECT * FROM papers WHERE year >= 2020 ORDER BY year"
    sql "SELECT cited_id, COUNT(*) n FROM contexts GROUP BY cited_id ORDER BY n DESC LIMIT 10"
    sql "SELECT p.title, c.purpose, c.quote FROM contexts c JOIN papers p ON c.cited_id = p.paper_id WHERE c.citing_id ILIKE '%divestment%'"

TABLES
  papers (paper_id PK, type, title, authors, year INT, journal, doi, abstract, pdf_file, text_file)
      type is 'owned' (fully extracted) or 'cited' (referenced only).
      authors is a semicolon-separated string.

  contexts (citing_id, cited_id, cited_title, purpose, section, quote, explanation)
      One row per citation context. purpose is one of: background, motivation,
      methodology, data_source, supporting_evidence, contrasting_evidence,
      comparison, extension, tool_software.

  citation_edges (citing_id, cited_id)
      Complete citation graph from cites arrays. Covers all edges, not just those
      with extracted contexts (~2x coverage). Used by chain, common-citers,
      pagerank, katz, co-cited, bib-coupling, shared-refs.

  citation_counts (paper_id PK, cited_by_count INT)

  claims (paper_id, claim, type, confidence, evidence_basis, quantification, supporting_citations)
      type: empirical, methodological, theoretical, etc.
      supporting_citations is a JSON array of paper_ids.

  keywords (paper_id, keyword)
  topics (paper_id, field, value)
      field: themes, geographic_focus, sectors, policy_context.

  sections (paper_id, heading, summary, annotated_text)
  methodology (paper_id PK, type, model_name, approach, temporal_scope, geographic_scope, unit_of_analysis, scenarios)
  data_sources (paper_id, name, type, description)
  questions (paper_id, question)

  authors (author_id PK, canonical_name, type, name_variants, paper_count INT, owned_paper_count INT)
      type is 'person' or 'institution'. name_variants is pipe-separated.

  paper_authors (paper_id, author_id)
      Join table linking papers to authors.

All commands support partial paper_id matching.
"""

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: python3 scripts/py.py -m pip install duckdb", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
try:
    from litdb import fast_loads as _fast_loads
except ImportError:
    _fast_loads = json.loads

DB_FILE = ROOT / "data" / "db" / "lit.duckdb"


def _load_fts(con):
    """Try to load the FTS extension. Returns True if successful."""
    try:
        con.execute("LOAD fts;")
        return True
    except Exception:
        pass
    try:
        con.execute("INSTALL fts;")
        con.execute("LOAD fts;")
        return True
    except Exception:
        return False






HAS_FTS = False


def get_connection():
    """Connect to the DuckDB database. Raises if not yet built."""
    global HAS_FTS
    if not DB_FILE.exists():
        raise SystemExit(
            "ERROR: data/db/lit.duckdb not found.\n"
            "Run: python3 scripts/py.py scripts/build/build_duckdb.py [--fts]"
        )
    con = duckdb.connect(str(DB_FILE), read_only=True)
    ext_dir = ROOT / ".duckdb_extensions"
    ext_dir.mkdir(exist_ok=True)
    con.execute(f"SET extension_directory = '{ext_dir}'")
    # Check for old DB missing _build_meta (pre-incremental schema)
    has_meta = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_build_meta'"
    ).fetchone()[0]
    if not has_meta:
        con.close()
        raise SystemExit(
            "ERROR: data/db/lit.duckdb is outdated (missing _build_meta).\n"
            "Run: python3 scripts/py.py scripts/build/build_duckdb.py --force [--fts]"
        )
    HAS_FTS = _load_fts(con)
    return con


def resolve_paper_id(con, paper_id):
    """Resolve partial paper_id to full ID."""
    exact = con.execute("SELECT paper_id FROM papers WHERE paper_id = ?", [paper_id]).fetchone()
    if exact:
        return exact[0]
    matches = con.execute(
        "SELECT paper_id FROM papers WHERE paper_id ILIKE ?",
        [f"%{paper_id}%"]
    ).fetchall()
    if not matches:
        print(f"ERROR: No paper found for '{paper_id}'", file=sys.stderr)
        sys.exit(1)
    if len(matches) > 1:
        print(f"ERROR: Ambiguous ID '{paper_id}', matches:", file=sys.stderr)
        for m in sorted(matches):
            print(f"  {m[0]}", file=sys.stderr)
        sys.exit(1)
    return matches[0][0]


def fmt_authors(authors_val, max_n=3):
    """Format author string or list for display."""
    if not authors_val:
        return ""
    # DuckDB may store lists as Python lists or as stringified lists
    if isinstance(authors_val, list):
        parts = [str(a).split(",")[0].strip() for a in authors_val if a]
    else:
        s = str(authors_val).strip()
        # Try to parse stringified list like "['Smith, J.', 'Jones, A.']"
        if s.startswith("["):
            try:
                parsed = json.loads(s.replace("'", '"'))
                parts = [str(a).split(",")[0].strip() for a in parsed if a]
            except Exception:
                parts = [a.strip(" []'\"").split(",")[0].strip() for a in s.split("'") if a.strip(" [],'\"")]
        else:
            parts = [a.strip().split(",")[0].strip() for a in s.split(";") if a.strip()]
            if len(parts) <= 1:
                parts = [a.strip().split(",")[0].strip() for a in s.split(",") if a.strip()]
    parts = [p for p in parts if p]
    if not parts:
        return str(authors_val)[:40]
    if len(parts) <= max_n:
        return ", ".join(parts)
    return ", ".join(parts[:max_n]) + " et al."


def wrap(text, prefix="    ", width=100):
    """Word-wrap text with prefix."""
    if not text:
        return ""
    words = text.split()
    lines, line = [], prefix
    for w in words:
        if len(line) + len(w) + 1 > width and line != prefix:
            lines.append(line)
            line = prefix + w
        else:
            line += (" " if line != prefix else "") + w
    if line != prefix:
        lines.append(line)
    return "\n".join(lines)


# ── Commands ────────────────────────────────────────────────────────────────



def cmd_paper(args, con):
    pid = resolve_paper_id(con, args.id)
    row = con.execute(
        "SELECT title, authors, year, journal, doi, type, abstract, detail_level, passes_completed FROM papers WHERE paper_id = ?",
        [pid]
    ).fetchone()
    if not row:
        print(f"Paper not found: {pid}")
        return
    title, authors, year, journal, doi, ptype, abstract, detail, passes = row

    print(f"  {pid}")
    print(f"  {row[0]}")
    if authors:
        print(f"  By: {fmt_authors(authors)} ({year})")
    if journal:
        print(f"  Journal: {journal}")
    if doi:
        print(f"  DOI: {doi}")
    print(f"  Type: {ptype}")
    if ptype in ("owned", "external_owned"):
        detail_str = detail or "none"
        passes_str = f" (passes: {passes})" if passes else ""
        print(f"  Extraction: {detail_str}{passes_str}")

    # Citation stats
    cc = con.execute("SELECT cited_by_count FROM citation_counts WHERE paper_id = ?", [pid]).fetchone()
    cited_by = cc[0] if cc else 0
    cites_count = con.execute("SELECT COUNT(DISTINCT cited_id) FROM contexts WHERE citing_id = ?", [pid]).fetchone()[0]
    print(f"  Cites: {cites_count} | Cited by: {cited_by}")

    if abstract:
        print(f"\n  Abstract:")
        print(wrap(abstract, "    "))


def cmd_owned(args, con):
    rows = con.execute("""
        SELECT p.paper_id, p.title, p.authors, p.year,
               (SELECT COUNT(DISTINCT cited_id) FROM contexts WHERE citing_id = p.paper_id) as cites,
               COALESCE(cc.cited_by_count, 0) as cited_by,
               p.detail_level
        FROM papers p
        LEFT JOIN citation_counts cc ON p.paper_id = cc.paper_id
        WHERE p.type IN ('owned', 'external_owned')
        ORDER BY p.paper_id
    """).fetchall()
    print(f"Owned papers ({len(rows)}):\n")
    for pid, title, authors, year, cites, cited_by, detail in rows:
        t = (title or "")[:75]
        tag = f"[{detail}]" if detail else "[no extraction]"
        print(f"  {pid}")
        print(f"    {fmt_authors(authors)} ({year}) — {t}")
        print(f"    cites {cites} | cited_by {cited_by} | {tag}")
        print()


def cmd_author(args, con):
    name = args.name
    # Use author entity table if available, fall back to ILIKE on flat authors string
    try:
        rows = con.execute("""
            SELECT DISTINCT p.paper_id, p.title, p.authors, p.year, p.type
            FROM authors a
            JOIN paper_authors pa ON a.author_id = pa.author_id
            JOIN papers p ON pa.paper_id = p.paper_id
            WHERE a.canonical_name ILIKE ? OR a.name_variants ILIKE ?
            ORDER BY p.year DESC
        """, [f"%{name}%", f"%{name}%"]).fetchall()
    except Exception:
        rows = con.execute(
            "SELECT paper_id, title, authors, year, type FROM papers WHERE authors ILIKE ? ORDER BY year DESC",
            [f"%{name}%"]
        ).fetchall()
    print(f"Author search: '{name}' — {len(rows)} paper(s)\n")
    for pid, title, authors, year, ptype in rows:
        tag = " (owned)" if ptype in ("owned", "external_owned") else ""
        print(f"  [{pid}] {fmt_authors(authors)} ({year}) — {(title or '')[:70]}{tag}")


def cmd_author_info(args, con):
    aid = args.author_id
    row = con.execute(
        "SELECT author_id, canonical_name, type, name_variants, paper_count, owned_paper_count FROM authors WHERE author_id = ?",
        [aid]
    ).fetchone()
    if not row:
        # Try partial match
        matches = con.execute(
            "SELECT author_id, canonical_name FROM authors WHERE author_id ILIKE ?",
            [f"%{aid}%"]
        ).fetchall()
        if not matches:
            print(f"ERROR: No author found for '{aid}'", file=sys.stderr)
            return
        if len(matches) > 1:
            print(f"Ambiguous author '{aid}', matches:")
            for mid, mname in sorted(matches):
                print(f"  {mid}  ({mname})")
            return
        row = con.execute(
            "SELECT author_id, canonical_name, type, name_variants, paper_count, owned_paper_count FROM authors WHERE author_id = ?",
            [matches[0][0]]
        ).fetchone()

    author_id, canonical_name, atype, name_variants, paper_count, owned_paper_count = row
    print(f"Author: {canonical_name}")
    print(f"  ID:         {author_id}")
    print(f"  Type:       {atype}")
    if name_variants and name_variants != canonical_name:
        variants = [v for v in name_variants.split("|") if v and v != canonical_name]
        if variants:
            print(f"  Variants:   {', '.join(variants)}")
    print(f"  Papers:     {paper_count} total ({owned_paper_count} owned)")

    papers = con.execute("""
        SELECT p.paper_id, p.title, p.authors, p.year, p.type
        FROM paper_authors pa
        JOIN papers p ON pa.paper_id = p.paper_id
        WHERE pa.author_id = ?
        ORDER BY p.year DESC
    """, [author_id]).fetchall()
    if papers:
        print(f"\n  Papers ({len(papers)}):")
        for pid, title, authors, year, ptype in papers:
            tag = " (owned)" if ptype in ("owned", "external_owned") else ""
            print(f"    [{pid}] {fmt_authors(authors)} ({year}) — {(title or '')[:65]}{tag}")


def cmd_search_authors(args, con):
    phrase = args.phrase
    results = []
    if HAS_FTS:
        try:
            fts_rows = con.execute("""
                SELECT author_id, canonical_name, type, paper_count, owned_paper_count,
                       fts_main_authors.match_bm25(author_id, ?) AS score
                FROM authors
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT 20
            """, [phrase]).fetchall()
            results = fts_rows
        except Exception:
            pass
    if not results:
        # Fallback to ILIKE
        results = con.execute("""
            SELECT author_id, canonical_name, type, paper_count, owned_paper_count, NULL
            FROM authors
            WHERE canonical_name ILIKE ? OR name_variants ILIKE ?
            ORDER BY paper_count DESC
            LIMIT 20
        """, [f"%{phrase}%", f"%{phrase}%"]).fetchall()
    print(f"Author search: '{phrase}' — {len(results)} result(s)\n")
    for author_id, canonical_name, atype, paper_count, owned_paper_count, score in results:
        score_str = f"  score={score:.3f}" if score is not None else ""
        owned_str = f", {owned_paper_count} owned" if owned_paper_count else ""
        print(f"  {author_id}")
        print(f"    {canonical_name} ({atype}) — {paper_count} paper(s){owned_str}{score_str}")


def cmd_coauthors(args, con):
    aid = args.author_id
    # Resolve partial match
    exact = con.execute("SELECT author_id, canonical_name FROM authors WHERE author_id = ?", [aid]).fetchone()
    if not exact:
        matches = con.execute(
            "SELECT author_id, canonical_name FROM authors WHERE author_id ILIKE ?",
            [f"%{aid}%"]
        ).fetchall()
        if not matches:
            print(f"ERROR: No author found for '{aid}'", file=sys.stderr)
            return
        if len(matches) > 1:
            print(f"Ambiguous author '{aid}', matches:")
            for mid, mname in sorted(matches):
                print(f"  {mid}  ({mname})")
            return
        exact = matches[0]
    author_id, canonical_name = exact
    rows = con.execute("""
        SELECT a2.author_id, a2.canonical_name, COUNT(*) as shared_papers
        FROM paper_authors pa1
        JOIN paper_authors pa2 ON pa1.paper_id = pa2.paper_id AND pa1.author_id != pa2.author_id
        JOIN authors a2 ON pa2.author_id = a2.author_id
        WHERE pa1.author_id = ?
        GROUP BY a2.author_id, a2.canonical_name
        ORDER BY shared_papers DESC
    """, [author_id]).fetchall()
    print(f"Coauthors of {canonical_name} ({author_id}) — {len(rows)} coauthor(s)\n")
    for cid, cname, shared in rows:
        print(f"  {cid}  {cname}  ({shared} shared paper{'s' if shared != 1 else ''})")


def cmd_top_authors(args, con):
    n = args.n or 15
    rows = con.execute("""
        SELECT author_id, canonical_name, paper_count, owned_paper_count
        FROM authors WHERE type = 'person'
        ORDER BY paper_count DESC
        LIMIT ?
    """, [n]).fetchall()
    print(f"Top {n} most prolific authors:\n")
    for i, (aid, name, total, owned) in enumerate(rows, 1):
        owned_str = f"  ({owned} owned)" if owned else ""
        print(f"  {i:3}. {name:<40} {total} paper(s){owned_str}")


def cmd_cites(args, con):
    pid = resolve_paper_id(con, args.id)
    limit = f"LIMIT {args.limit}" if args.limit else ""
    rows = con.execute(f"""
        SELECT citing_id, cited_title, purpose, section, quote, explanation
        FROM contexts WHERE cited_id = ?
        ORDER BY citing_id
        {limit}
    """, [pid]).fetchall()

    paper = con.execute("SELECT title, authors, year FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Citations to: {pid}")
    if paper:
        print(f"  Title:   {paper[0]}")
        print(f"  Authors: {fmt_authors(paper[1])}")
        print(f"  Year:    {paper[2]}")
    print(f"  Cited {len(rows)} time(s) across the corpus\n")

    for citing, ctitle, purpose, section, quote, explanation in rows:
        print(f"  [{citing}] {ctitle or ''}")
        print(f"    purpose     : {purpose}")
        if section:
            print(f"    section     : {section}")
        if quote:
            print(f"    quote       : \"{quote[:100]}\"")
        if explanation:
            print(f"    explanation : {explanation}")
        print()


def cmd_cited_by(args, con):
    pid = resolve_paper_id(con, args.id)
    limit = f"LIMIT {args.limit}" if args.limit else ""
    rows = con.execute(f"""
        SELECT DISTINCT cited_id, cited_title, purpose
        FROM contexts WHERE citing_id = ?
        ORDER BY cited_id
        {limit}
    """, [pid]).fetchall()
    print(f"Papers cited by: {pid} ({len(rows)} references)\n")
    for cited, title, purpose in rows:
        print(f"  [{cited}] {title or ''}")
        print(f"    purpose: {purpose}")
        print()


def cmd_chain(args, con):
    pid = resolve_paper_id(con, args.id)
    depth = args.depth or 2
    rows = con.execute(f"""
        WITH RECURSIVE chain AS (
            SELECT cited_id, citing_id, 1 as depth
            FROM citation_edges WHERE cited_id = ?
            UNION
            SELECT c.cited_id, c.citing_id, ch.depth + 1
            FROM citation_edges c
            JOIN chain ch ON c.cited_id = ch.citing_id
            WHERE ch.depth < ?
        )
        SELECT DISTINCT citing_id, depth,
               p.title, p.authors, p.year
        FROM chain
        JOIN papers p ON chain.citing_id = p.paper_id
        ORDER BY depth, citing_id
    """, [pid, depth]).fetchall()

    print(f"Citation chain from: {pid} (depth {depth})\n")
    current_depth = 0
    for citing, d, title, authors, year in rows:
        if d != current_depth:
            current_depth = d
            print(f"  ── Depth {d} ({sum(1 for r in rows if r[1] == d)} papers) ──")
        print(f"    {fmt_authors(authors)} ({year}) — {(title or '')[:70]}")
        print(f"      [{citing}]")


def cmd_common_citers(args, con):
    pid1 = resolve_paper_id(con, args.id1)
    pid2 = resolve_paper_id(con, args.id2)
    rows = con.execute("""
        SELECT DISTINCT c1.citing_id, p.title, p.authors, p.year
        FROM citation_edges c1
        JOIN citation_edges c2 ON c1.citing_id = c2.citing_id
        JOIN papers p ON c1.citing_id = p.paper_id
        WHERE c1.cited_id = ? AND c2.cited_id = ?
        ORDER BY p.year DESC
    """, [pid1, pid2]).fetchall()
    print(f"Papers citing both {pid1} and {pid2}: {len(rows)}\n")
    for citing, title, authors, year in rows:
        print(f"  {fmt_authors(authors)} ({year}) — {(title or '')[:70]}")
        print(f"    [{citing}]")


def cmd_co_cited(args, con):
    ids = [resolve_paper_id(con, i) for i in args.ids]
    min_count = args.min
    limit = args.limit
    placeholders = ",".join("?" * len(ids))
    if len(ids) == 1:
        rows = con.execute(f"""
            SELECT e2.cited_id, COUNT(DISTINCT e1.citing_id) AS co_cite_count,
                   p.title, p.authors, p.year
            FROM citation_edges e1
            JOIN citation_edges e2 ON e1.citing_id = e2.citing_id AND e1.cited_id != e2.cited_id
            JOIN papers p ON e2.cited_id = p.paper_id
            WHERE e1.cited_id IN ({placeholders})
              AND e2.cited_id NOT IN ({placeholders})
            GROUP BY e2.cited_id, p.title, p.authors, p.year
            HAVING co_cite_count >= ?
            ORDER BY co_cite_count DESC
            LIMIT ?
        """, ids + ids + [min_count, limit]).fetchall()
    else:
        rows = con.execute(f"""
            SELECT e2.cited_id, COUNT(DISTINCT e1.citing_id) AS co_cite_count,
                   p.title, p.authors, p.year
            FROM citation_edges e1
            JOIN citation_edges e2 ON e1.citing_id = e2.citing_id AND e1.cited_id != e2.cited_id
            JOIN papers p ON e2.cited_id = p.paper_id
            WHERE e1.cited_id IN ({placeholders})
              AND e2.cited_id NOT IN ({placeholders})
            GROUP BY e2.cited_id, p.title, p.authors, p.year
            HAVING COUNT(DISTINCT e1.cited_id) = ? AND co_cite_count >= ?
            ORDER BY co_cite_count DESC
            LIMIT ?
        """, ids + ids + [len(ids), min_count, limit]).fetchall()
    seed_str = ", ".join(ids)
    print(f"Co-citation for [{seed_str}] (min={min_count}): {len(rows)} result(s)\n")
    for cited, count, title, authors, year in rows:
        print(f"  [{count}x] {fmt_authors(authors)} ({year}) — {(title or '')[:65]}")
        print(f"    [{cited}]")


def cmd_bib_coupling(args, con):
    ids = [resolve_paper_id(con, i) for i in args.ids]
    min_count = args.min
    limit = args.limit
    placeholders = ",".join("?" * len(ids))
    if len(ids) == 1:
        rows = con.execute(f"""
            SELECT e2.citing_id, COUNT(DISTINCT e1.cited_id) AS shared_refs,
                   p.title, p.authors, p.year, p.type
            FROM citation_edges e1
            JOIN citation_edges e2 ON e1.cited_id = e2.cited_id AND e1.citing_id != e2.citing_id
            JOIN papers p ON e2.citing_id = p.paper_id
            WHERE e1.citing_id IN ({placeholders})
              AND e2.citing_id NOT IN ({placeholders})
            GROUP BY e2.citing_id, p.title, p.authors, p.year, p.type
            HAVING shared_refs >= ?
            ORDER BY shared_refs DESC
            LIMIT ?
        """, ids + ids + [min_count, limit]).fetchall()
    else:
        rows = con.execute(f"""
            SELECT e2.citing_id, COUNT(DISTINCT e1.cited_id) AS shared_refs,
                   p.title, p.authors, p.year, p.type
            FROM citation_edges e1
            JOIN citation_edges e2 ON e1.cited_id = e2.cited_id AND e1.citing_id != e2.citing_id
            JOIN papers p ON e2.citing_id = p.paper_id
            WHERE e1.citing_id IN ({placeholders})
              AND e2.citing_id NOT IN ({placeholders})
            GROUP BY e2.citing_id, p.title, p.authors, p.year, p.type
            HAVING COUNT(DISTINCT e1.citing_id) = ? AND shared_refs >= ?
            ORDER BY shared_refs DESC
            LIMIT ?
        """, ids + ids + [len(ids), min_count, limit]).fetchall()
    seed_str = ", ".join(ids)
    print(f"Bibliographic coupling for [{seed_str}] (min={min_count}): {len(rows)} result(s)\n")
    for coupled, shared, title, authors, year, ptype in rows:
        tag = " (owned)" if ptype in ("owned", "external_owned") else ""
        print(f"  [{shared} shared] {fmt_authors(authors)} ({year}) — {(title or '')[:60]}{tag}")
        print(f"    [{coupled}]")


def cmd_shared_refs(args, con):
    if len(args.ids) < 2:
        print("ERROR: shared-refs requires at least 2 paper IDs", file=sys.stderr)
        sys.exit(1)
    ids = [resolve_paper_id(con, i) for i in args.ids]
    n = len(ids)
    placeholders = ",".join("?" * n)
    rows = con.execute(f"""
        SELECT ce.cited_id, p.title, p.authors, p.year
        FROM citation_edges ce
        JOIN papers p ON ce.cited_id = p.paper_id
        WHERE ce.citing_id IN ({placeholders})
        GROUP BY ce.cited_id, p.title, p.authors, p.year
        HAVING COUNT(DISTINCT ce.citing_id) = ?
        ORDER BY p.year DESC
    """, ids + [n]).fetchall()
    id_str = " \u2229 ".join(ids)
    print(f"Shared references ({id_str}): {len(rows)}\n")
    for cited, title, authors, year in rows:
        print(f"  {fmt_authors(authors)} ({year}) — {(title or '')[:65]}")
        print(f"    [{cited}]")


def cmd_shared_papers(args, con):
    if len(args.ids) < 2:
        print("ERROR: shared-papers requires at least 2 author IDs", file=sys.stderr)
        sys.exit(1)
    n = len(args.ids)
    placeholders = ",".join("?" * n)
    rows = con.execute(f"""
        SELECT pa.paper_id, p.title, p.authors, p.year, p.type
        FROM paper_authors pa
        JOIN papers p ON pa.paper_id = p.paper_id
        WHERE pa.author_id IN ({placeholders})
        GROUP BY pa.paper_id, p.title, p.authors, p.year, p.type
        HAVING COUNT(DISTINCT pa.author_id) = ?
        ORDER BY p.year DESC
    """, args.ids + [n]).fetchall()
    id_str = " & ".join(args.ids)
    print(f"Papers co-authored by [{id_str}]: {len(rows)}\n")
    for pid, title, authors, year, ptype in rows:
        tag = " (owned)" if ptype in ("owned", "external_owned") else ""
        print(f"  {fmt_authors(authors)} ({year}) — {(title or '')[:65]}{tag}")
        print(f"    [{pid}]")


def cmd_top_cited(args, con):
    n = args.n or 15
    rows = con.execute(f"""
        SELECT cc.paper_id, cc.cited_by_count, p.title, p.authors, p.year, p.type
        FROM citation_counts cc
        JOIN papers p ON cc.paper_id = p.paper_id
        ORDER BY cc.cited_by_count DESC
        LIMIT ?
    """, [n]).fetchall()
    print(f"Top {n} most cited papers:\n")
    for i, (pid, count, title, authors, year, ptype) in enumerate(rows, 1):
        tag = " (owned)" if ptype in ("owned", "external_owned") else ""
        print(f"  {i:3}. [{count}x] {fmt_authors(authors)} ({year}) — {(title or '')[:65]}{tag}")


def cmd_purpose(args, con):
    tag = args.tag
    limit = f"LIMIT {args.limit}" if args.limit else "LIMIT 30"
    rows = con.execute(f"""
        SELECT citing_id, cited_id, cited_title, section, quote, explanation
        FROM contexts WHERE purpose = ?
        ORDER BY citing_id
        {limit}
    """, [tag]).fetchall()
    total = con.execute("SELECT COUNT(*) FROM contexts WHERE purpose = ?", [tag]).fetchone()[0]
    shown = len(rows)
    print(f"Purpose: {tag} — {total} context(s){f' (showing {shown})' if shown < total else ''}\n")
    for citing, cited, ctitle, section, quote, explanation in rows:
        print(f"  {citing} → {cited}")
        if ctitle:
            print(f"    cited title : {ctitle[:80]}")
        if section:
            print(f"    section     : {section}")
        if quote:
            print(f"    quote       : \"{quote[:100]}\"")
        if explanation:
            print(f"    explanation : {explanation}")
        print()


def _fts_or_ilike(con, table, fts_name, id_col, search_cols, phrase, extra_where="", extra_params=None, limit=15):
    """Try FTS BM25 search; fall back to ILIKE if FTS unavailable."""
    params = extra_params or []
    if HAS_FTS:
        try:
            query = f"""
                SELECT t.*, score FROM (
                    SELECT *, {fts_name}.match_bm25({id_col}, ?) AS score FROM {table}
                ) t WHERE score IS NOT NULL {extra_where}
                ORDER BY score DESC LIMIT ?
            """
            return con.execute(query, [phrase] + params + [limit]).fetchall(), True
        except Exception:
            pass
    # Fallback: ILIKE across search columns
    conditions = " OR ".join(f"{c} ILIKE ?" for c in search_cols)
    like_params = [f"%{phrase}%" for _ in search_cols]
    query = f"SELECT *, NULL as score FROM {table} WHERE ({conditions}) {extra_where} LIMIT ?"
    return con.execute(query, like_params + params + [limit]).fetchall(), False


def cmd_search(args, con):
    phrase = args.phrase
    limit = args.limit or 15
    purpose_filter = args.filter_purpose
    year_filter = args.filter_year_min

    # Search papers
    paper_hits, _ = _fts_or_ilike(
        con, "papers", "fts_main_papers", "paper_id",
        ["title", "abstract", "authors"], phrase, limit=limit
    )

    # Search contexts
    ctx_extra = ""
    ctx_params = []
    if purpose_filter:
        ctx_extra = " AND t.purpose = ?"
        ctx_params = [purpose_filter]
    ctx_hits, _ = _fts_or_ilike(
        con, "contexts", "fts_main_contexts", "rowid",
        ["quote", "explanation", "cited_title"], phrase,
        extra_where=ctx_extra, extra_params=ctx_params, limit=limit
    )

    # Search claims
    claim_hits, _ = _fts_or_ilike(
        con, "claims", "fts_main_claims", "rowid",
        ["claim", "quantification"], phrase, limit=limit
    )

    print(f"Search: '{phrase}' — {len(paper_hits)} papers, {len(ctx_hits)} contexts, {len(claim_hits)} claims\n")

    if paper_hits:
        print("── Papers (by title/abstract/author relevance) ──")
        for row in paper_hits:
            pid, ptype, title, authors, year = row[0], row[1], row[2], row[3], row[4]
            tag = " (owned)" if ptype in ("owned", "external_owned") else ""
            print(f"  {fmt_authors(authors)} ({year}) — {(title or '')[:70]}{tag}")
            print(f"    [{pid}]")
        print()

    if ctx_hits:
        pfx = f" [purpose={purpose_filter}]" if purpose_filter else ""
        print(f"── Citation contexts{pfx} ──")
        for row in ctx_hits:
            citing, cited, ctitle, purpose, section, quote, explanation = row[0], row[1], row[2], row[3], row[4], row[5], row[6]
            print(f"  {citing} → {cited}")
            if ctitle:
                print(f"    title   : {ctitle[:80]}")
            print(f"    purpose : {purpose}")
            if quote:
                print(f"    quote   : \"{quote[:100]}\"")
            if explanation:
                print(f"    explain : {explanation}")
            print()

    if claim_hits:
        print("── Claims ──")
        for row in claim_hits:
            pid, claim, ctype, confidence = row[0], row[1], row[2], row[3]
            print(f"  [{pid}] ({ctype}, {confidence})")
            print(f"    {claim[:120]}")
            print()


def _count_matches(con, table, col, phrase):
    """Count matches using FTS or ILIKE fallback."""
    if HAS_FTS:
        try:
            r = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT *, fts_main_{table}.match_bm25(rowid, ?) AS score FROM {table}
                ) t WHERE score IS NOT NULL
            """, [phrase]).fetchone()
            return r[0] if r else 0
        except Exception:
            pass
    r = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} ILIKE ?", [f"%{phrase}%"]).fetchone()
    return r[0] if r else 0


def _search_table_join(con, table, fts_name, id_col, search_col, phrase, join_clause, select_cols, limit):
    """FTS or ILIKE search with JOIN to papers."""
    if HAS_FTS:
        try:
            return con.execute(f"""
                SELECT {select_cols}, score
                FROM (SELECT *, {fts_name}.match_bm25({id_col}, ?) AS score FROM {table}) t
                {join_clause}
                WHERE score IS NOT NULL
                ORDER BY score DESC LIMIT ?
            """, [phrase, limit]).fetchall()
        except Exception:
            pass
    return con.execute(f"""
        SELECT {select_cols}, NULL as score
        FROM {table} t {join_clause}
        WHERE t.{search_col} ILIKE ?
        LIMIT ?
    """, [f"%{phrase}%", limit]).fetchall()


def cmd_search_all(args, con):
    phrase = args.phrase
    limit = args.limit or 10

    counts = {}
    for table, col in [("topics", "value"), ("keywords", "keyword"),
                       ("sections", "summary"), ("claims", "claim")]:
        counts[table] = _count_matches(con, table, col, phrase)

    # Abstract count
    if HAS_FTS:
        try:
            abs_count = con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT *, fts_main_papers.match_bm25(paper_id, ?) AS score FROM papers
                ) p WHERE score IS NOT NULL AND abstract != ''
            """, [phrase]).fetchone()[0]
        except Exception:
            abs_count = con.execute(
                "SELECT COUNT(*) FROM papers WHERE abstract ILIKE ? AND abstract != ''",
                [f"%{phrase}%"]
            ).fetchone()[0]
    else:
        abs_count = con.execute(
            "SELECT COUNT(*) FROM papers WHERE abstract ILIKE ? AND abstract != ''",
            [f"%{phrase}%"]
        ).fetchone()[0]
    counts["abstracts"] = abs_count

    total_papers = set()
    print(f"=== Search-all: '{phrase}' ===\n")
    print(f"  Topics    : {counts['topics']} match(es)")
    print(f"  Claims    : {counts['claims']} match(es)")
    print(f"  Keywords  : {counts['keywords']} match(es)")
    print(f"  Abstracts : {counts['abstracts']} match(es)")
    print(f"  Sections  : {counts['sections']} match(es)")

    if counts["topics"]:
        print(f"\n── Topics ──")
        rows = _search_table_join(con, "topics", "fts_main_topics", "rowid", "value", phrase,
                                  "JOIN papers p ON t.paper_id = p.paper_id",
                                  "t.paper_id, t.field, t.value, p.title", limit)
        for row in rows:
            total_papers.add(row[0])
            print(f"  [{row[0]}] {(row[3] or '')[:70]}")
            print(f"    {row[1]}: {row[2]}")

    if counts["claims"]:
        print(f"\n── Claims ──")
        rows = _search_table_join(con, "claims", "fts_main_claims", "rowid", "claim", phrase,
                                  "JOIN papers p ON t.paper_id = p.paper_id",
                                  "t.paper_id, t.claim, t.type, p.title", limit)
        for row in rows:
            total_papers.add(row[0])
            print(f"  [{row[0]}] {(row[3] or '')[:70]}")
            print(f"    [{row[2]}] {row[1][:120]}")

    if counts["keywords"]:
        print(f"\n── Keywords ──")
        rows = _search_table_join(con, "keywords", "fts_main_keywords", "rowid", "keyword", phrase,
                                  "JOIN papers p ON t.paper_id = p.paper_id",
                                  "t.paper_id, t.keyword, p.title", limit)
        for row in rows:
            total_papers.add(row[0])
            print(f"  [{row[0]}] {(row[2] or '')[:70]}  →  {row[1]}")

    if counts["abstracts"]:
        print(f"\n── Abstracts ──")
        if HAS_FTS:
            try:
                rows = con.execute("""
                    SELECT paper_id, title, abstract, score
                    FROM (SELECT *, fts_main_papers.match_bm25(paper_id, ?) AS score FROM papers) p
                    WHERE score IS NOT NULL AND abstract != ''
                    ORDER BY score DESC LIMIT ?
                """, [phrase, limit]).fetchall()
            except Exception:
                rows = con.execute(
                    "SELECT paper_id, title, abstract, NULL FROM papers WHERE abstract ILIKE ? AND abstract != '' LIMIT ?",
                    [f"%{phrase}%", limit]
                ).fetchall()
        else:
            rows = con.execute(
                "SELECT paper_id, title, abstract, NULL FROM papers WHERE abstract ILIKE ? AND abstract != '' LIMIT ?",
                [f"%{phrase}%", limit]
            ).fetchall()
        for pid, title, abstract, score in rows:
            total_papers.add(pid)
            abs_lower = (abstract or "").lower()
            phrase_lower = phrase.lower()
            idx = abs_lower.find(phrase_lower)
            if idx >= 0:
                start = max(0, idx - 40)
                end = min(len(abstract), idx + len(phrase) + 40)
                snippet = ("..." if start > 0 else "") + abstract[start:end] + ("..." if end < len(abstract) else "")
            else:
                snippet = abstract[:100] + "..."
            print(f"  [{pid}] {(title or '')[:70]}")
            print(f"    {snippet}")

    if counts["sections"]:
        print(f"\n── Sections ──")
        rows = _search_table_join(con, "sections", "fts_main_sections", "rowid", "summary", phrase,
                                  "JOIN papers p ON t.paper_id = p.paper_id",
                                  "t.paper_id, t.heading, t.summary, p.title", limit)
        for row in rows:
            total_papers.add(row[0])
            print(f"  [{row[0]}] {(row[3] or '')[:70]}")
            print(f"    {row[1]}: {(row[2] or '')[:100]}")

    print(f"\n  Total unique papers: {len(total_papers)}")


def cmd_abstract(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'abstract' does not support --limit. It takes a paper ID and returns the full abstract.")
        print("For text search try: search \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    row = con.execute("SELECT title, authors, year, abstract FROM papers WHERE paper_id = ?", [pid]).fetchone()
    if not row or not row[3]:
        print(f"Paper: {pid}\n  (no abstract available)")
        return
    print(f"Paper: {pid}")
    print(f"Title: {row[0]}")
    print(f"By:    {fmt_authors(row[1])} ({row[2]})\n")
    print(wrap(row[3], "  "))


def cmd_claims(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'claims' does not support --limit. It takes a paper ID and returns all claims for that paper.")
        print("Did you mean: search-claims \"<phrase>\" [--limit N]  (full-text search across claims)")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    query = "SELECT claim, type, confidence, evidence_basis, quantification, supporting_citations FROM claims WHERE paper_id = ?"
    params = [pid]
    if args.type:
        query += " AND type = ?"
        params.append(args.type)
    rows = con.execute(query, params).fetchall()
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")
    if not rows:
        print(f"\n  (no claims data — run Pass 3 extraction for this paper)")
        return
    print(f"\n  Claims ({len(rows)}):\n")
    for i, (claim, ctype, conf, evidence, quant, supporting) in enumerate(rows, 1):
        print(f"  {i}. [{ctype}] (confidence: {conf})")
        print(wrap(claim, "     "))
        if evidence:
            print(f"     Evidence: {evidence}")
        if quant:
            print(f"     Quantification: {quant}")
        if supporting:
            cites = json.loads(supporting) if isinstance(supporting, str) else supporting
            if cites:
                print(f"     Supporting: {', '.join(cites)}")
        print()


def cmd_keywords(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'keywords' does not support --limit. It takes a paper ID and returns all keywords for that paper.")
        print("For text search try: search-keywords \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")

    kws = con.execute("SELECT keyword FROM keywords WHERE paper_id = ?", [pid]).fetchall()
    topics = con.execute("SELECT field, value FROM topics WHERE paper_id = ? ORDER BY field", [pid]).fetchall()

    if not kws and not topics:
        print(f"\n  (no keywords/topics data — run Pass 3 extraction for this paper)")
        return

    if kws:
        print(f"\n  Keywords: {', '.join(k[0] for k in kws)}")

    if topics:
        current_field = None
        for field, val in topics:
            if field != current_field:
                current_field = field
                print(f"\n  {field}:")
            print(f"    - {val}")


def cmd_methodology(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'methodology' does not support --limit. It takes a paper ID and returns methodology details for that paper.")
        print("For text search try: search-methods <type>  or  search \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    row = con.execute("SELECT * FROM methodology WHERE paper_id = ?", [pid]).fetchone()
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")
    if not row:
        print(f"\n  (no methodology data — run Pass 3 extraction for this paper)")
        return
    _, mtype, model, approach, temporal, geo, unit, scenarios = row
    print(f"\n  Type: {mtype}")
    if model:
        print(f"  Model: {model}")
    if approach:
        print(f"  Approach:")
        print(wrap(approach, "    "))
    if temporal:
        print(f"  Temporal scope: {temporal}")
    if geo:
        print(f"  Geographic scope: {geo}")
    if unit:
        print(f"  Unit of analysis: {unit}")

    ds = con.execute("SELECT name, type, description FROM data_sources WHERE paper_id = ?", [pid]).fetchall()
    if ds:
        print(f"\n  Data sources:")
        for name, dtype, desc in ds:
            print(f"    - {name}" + (f" ({dtype})" if dtype else ""))
            if desc:
                print(f"      {desc[:100]}")


def cmd_sections(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'sections' does not support --limit. It takes a paper ID and returns all sections for that paper.")
        print("For text search try: search-sections \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    rows = con.execute("SELECT heading, summary FROM sections WHERE paper_id = ?", [pid]).fetchall()
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")
    if not rows:
        print(f"\n  (no sections data — run Pass 4 extraction for this paper)")
        return
    print()
    for heading, summary in rows:
        print(f"  {heading}")
        if summary:
            print(wrap(summary, "    "))
        print()


def cmd_questions(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'questions' does not support --limit. It takes a paper ID and returns all research questions for that paper.")
        print("For text search try: search \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    rows = con.execute("SELECT question FROM questions WHERE paper_id = ?", [pid]).fetchall()
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")
    if not rows:
        print(f"\n  (no research questions — run Pass 3 extraction)")
        return
    print(f"\n  Research questions:\n")
    for i, (q,) in enumerate(rows, 1):
        print(f"  {i}. {q}")


def cmd_data_sources(args, con):
    if getattr(args, "limit", None) is not None:
        print("Error: 'data-sources' does not support --limit. It takes a paper ID and returns all data sources for that paper.")
        print("For text search try: search \"<phrase>\" [--limit N]")
        sys.exit(1)
    pid = resolve_paper_id(con, args.id)
    rows = con.execute("SELECT name, type, description FROM data_sources WHERE paper_id = ?", [pid]).fetchall()
    paper = con.execute("SELECT title FROM papers WHERE paper_id = ?", [pid]).fetchone()
    print(f"Paper: {pid}")
    if paper:
        print(f"Title: {paper[0]}")
    if not rows:
        print(f"\n  (no data source info — run Pass 3 extraction)")
        return
    print(f"\n  Data sources ({len(rows)}):\n")
    for name, dtype, desc in rows:
        print(f"  - {name}" + (f" ({dtype})" if dtype else ""))
        if desc:
            print(f"    {desc[:120]}")


def cmd_search_claims(args, con):
    phrase = args.phrase
    limit = args.limit or 15
    rows = _search_table_join(con, "claims", "fts_main_claims", "rowid", "claim", phrase,
                              "JOIN papers p ON t.paper_id = p.paper_id",
                              "t.paper_id, t.claim, t.type, t.confidence, p.title", limit)
    if args.type:
        rows = [r for r in rows if r[2] == args.type]
    print(f"Claims matching '{phrase}' ({len(rows)}):\n")
    for row in rows:
        print(f"  [{row[0]}] {(row[4] or '')[:70]}")
        print(f"    Type: {row[2]} | Confidence: {row[3]}")
        print(wrap(row[1], "    "))
        print()


def cmd_search_sections(args, con):
    phrase = args.phrase
    limit = args.limit or 15
    rows = _search_table_join(con, "sections", "fts_main_sections", "rowid", "summary", phrase,
                              "JOIN papers p ON t.paper_id = p.paper_id",
                              "t.paper_id, t.heading, t.summary, p.title", limit)
    print(f"Sections matching '{phrase}' ({len(rows)}):\n")
    for row in rows:
        print(f"  [{row[0]}] {(row[3] or '')[:70]}")
        print(f"    {row[1]}")
        if row[2]:
            print(f"    {(row[2] or '')[:120]}")
        print()


def cmd_search_topics(args, con):
    phrase = args.phrase
    limit = args.limit or 15
    rows = _search_table_join(con, "topics", "fts_main_topics", "rowid", "value", phrase,
                              "JOIN papers p ON t.paper_id = p.paper_id",
                              "t.paper_id, t.field, t.value, p.title", limit)
    print(f"Topics matching '{phrase}' ({len(rows)}):\n")
    for row in rows:
        print(f"  [{row[0]}] {(row[3] or '')[:70]}")
        print(f"    {row[1]}: {row[2]}")


def cmd_search_keywords(args, con):
    phrase = args.phrase
    limit = args.limit or 15
    rows = _search_table_join(con, "keywords", "fts_main_keywords", "rowid", "keyword", phrase,
                              "JOIN papers p ON t.paper_id = p.paper_id",
                              "t.paper_id, t.keyword, p.title", limit)
    print(f"Keywords matching '{phrase}' ({len(rows)}):\n")
    for row in rows:
        print(f"  [{row[0]}] {(row[2] or '')[:70]}  →  {row[1]}")


def cmd_search_methods(args, con):
    mtype = args.type
    rows = con.execute("""
        SELECT m.paper_id, m.type, m.approach, p.title, p.authors, p.year
        FROM methodology m
        JOIN papers p ON m.paper_id = p.paper_id
        WHERE m.type ILIKE ?
        ORDER BY p.year DESC
    """, [f"%{mtype}%"]).fetchall()
    print(f"Methodology type: '{mtype}' — {len(rows)} paper(s)\n")
    for pid, mt, approach, title, authors, year in rows:
        print(f"  {fmt_authors(authors)} ({year}) — {(title or '')[:65]}")
        print(f"    [{pid}] type={mt}")
        if approach:
            print(f"    {approach[:100]}")
        print()


def cmd_stats(args, con):
    total = con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    owned = con.execute("SELECT COUNT(*) FROM papers WHERE type IN ('owned', 'external_owned')").fetchone()[0]
    cited = total - owned
    contexts = con.execute("SELECT COUNT(*) FROM contexts").fetchone()[0]
    cite_edges = con.execute("SELECT COUNT(*) FROM citation_edges").fetchone()[0]
    claims_n = con.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    kw_papers = con.execute("SELECT COUNT(DISTINCT paper_id) FROM keywords").fetchone()[0]
    topic_papers = con.execute("SELECT COUNT(DISTINCT paper_id) FROM topics").fetchone()[0]
    section_papers = con.execute("SELECT COUNT(DISTINCT paper_id) FROM sections").fetchone()[0]
    meth_papers = con.execute("SELECT COUNT(DISTINCT paper_id) FROM methodology").fetchone()[0]
    try:
        author_count = con.execute("SELECT COUNT(*) FROM authors WHERE type = 'person'").fetchone()[0]
        inst_count = con.execute("SELECT COUNT(*) FROM authors WHERE type = 'institution'").fetchone()[0]
        has_authors = True
    except Exception:
        has_authors = False

    print(f"Corpus statistics:\n")
    print(f"  Papers:       {total} total ({owned} owned, {cited} cited-only)")
    print(f"  Contexts:     {contexts} (annotated)")
    print(f"  Cite edges:   {cite_edges} (full graph)")
    print(f"  Claims:       {claims_n}")
    print(f"  With keywords:    {kw_papers} papers")
    print(f"  With topics:      {topic_papers} papers")
    print(f"  With sections:    {section_papers} papers")
    print(f"  With methodology: {meth_papers} papers")
    if has_authors:
        print(f"  Authors:      {author_count} persons, {inst_count} institutions")

    # Extraction level distribution
    detail_rows = con.execute("""
        SELECT COALESCE(detail_level, 'none') as level, COUNT(*) as n
        FROM papers WHERE type IN ('owned', 'external_owned')
        GROUP BY level ORDER BY n DESC
    """).fetchall()
    if detail_rows:
        print(f"\n  Extraction levels (owned):")
        for level, n in detail_rows:
            print(f"    {level}: {n}")

    # Year distribution for owned
    years = con.execute("""
        SELECT year, COUNT(*) FROM papers WHERE type IN ('owned', 'external_owned') AND year IS NOT NULL
        GROUP BY year ORDER BY year
    """).fetchall()
    if years:
        print(f"\n  Year range: {years[0][0]}–{years[-1][0]}")

    # Top methods
    methods = con.execute("""
        SELECT type, COUNT(*) as n FROM methodology GROUP BY type ORDER BY n DESC LIMIT 5
    """).fetchall()
    if methods:
        print(f"\n  Methodology types:")
        for mt, n in methods:
            print(f"    {mt}: {n}")

    # Top purposes
    purposes = con.execute("""
        SELECT purpose, COUNT(*) as n FROM contexts WHERE purpose != ''
        GROUP BY purpose ORDER BY n DESC
    """).fetchall()
    if purposes:
        print(f"\n  Citation purposes:")
        for p, n in purposes:
            print(f"    {p}: {n}")


def cmd_methods(args, con):
    rows = con.execute("""
        SELECT m.type, COUNT(*) as n,
               GROUP_CONCAT(m.paper_id, ', ') as papers
        FROM methodology m
        GROUP BY m.type ORDER BY n DESC
    """).fetchall()
    print(f"Methodology types:\n")
    for mt, n, papers in rows:
        print(f"  {mt}: {n} paper(s)")
        for pid in papers.split(", ")[:5]:
            print(f"    - {pid}")
        if n > 5:
            print(f"    ... and {n-5} more")
        print()


def cmd_purposes_list(args, con):
    rows = con.execute("""
        SELECT purpose, COUNT(*) as n FROM contexts WHERE purpose != ''
        GROUP BY purpose ORDER BY n DESC
    """).fetchall()
    print(f"Citation purposes:\n")
    for p, n in rows:
        print(f"  {p}: {n}")


def cmd_request_pull(args, con):
    """Write paper IDs to data/pull_citing.txt for the main project to fetch."""
    pull_file = ROOT / "data" / "pull_citing.txt"

    # Validate all IDs exist
    ids = args.ids
    valid = []
    for pid in ids:
        row = con.execute(
            "SELECT paper_id, title, year FROM papers WHERE paper_id = ?", [pid]
        ).fetchone()
        if not row:
            print(f"  ERROR: '{pid}' not found in database", file=sys.stderr)
            sys.exit(1)
        valid.append(row)

    # Append to file (don't overwrite previous requests)
    existing = set()
    if pull_file.exists():
        existing = {l.strip() for l in pull_file.read_text().splitlines() if l.strip()}

    pull_file.parent.mkdir(parents=True, exist_ok=True)
    added = 0
    with open(pull_file, "a") as f:
        for paper_id, title, year in valid:
            if paper_id not in existing:
                f.write(f"{paper_id}\n")
                added += 1
                print(f"  + {paper_id}  {title} ({year})")
            else:
                print(f"  = {paper_id}  (already queued)")

    total = len(existing) + added
    print(f"\n{added} paper(s) added to pull queue ({total} total)")
    print(f"File: {pull_file}")
    print(f"\nTo fetch citations, switch to the main PaperClaw project:")
    print(f"  cd .. && claude")
    print(f"  /pull-citing")


def cmd_sql(args, con):
    """Execute arbitrary SQL against the database."""
    if args.schema:
        # Print all table schemas
        tables = con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        for (tname,) in tables:
            print(f"\n{tname}")
            cols = con.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{tname}' AND table_schema = 'main'
                ORDER BY ordinal_position
            """).fetchall()
            for cname, dtype, nullable in cols:
                null_tag = "" if nullable == "YES" else " NOT NULL"
                print(f"  {cname:<30s} {dtype}{null_tag}")
        return

    query = args.query
    if not query:
        print("ERROR: provide a SQL query or use --schema", file=sys.stderr)
        sys.exit(1)

    try:
        result = con.execute(query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    except Exception as e:
        print(f"SQL error: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("(no results)")
        return

    # Compute column widths
    col_widths = [len(c) for c in columns]
    str_rows = []
    for row in rows:
        str_row = [str(v) if v is not None else "NULL" for v in row]
        str_rows.append(str_row)
        for i, val in enumerate(str_row):
            col_widths[i] = min(max(col_widths[i], len(val)), 80)

    # Print header
    header = "  ".join(c.ljust(col_widths[i]) for i, c in enumerate(columns))
    print(header)
    print("  ".join("-" * col_widths[i] for i in range(len(columns))))

    # Print rows
    for str_row in str_rows:
        line = "  ".join(str_row[i][:80].ljust(col_widths[i]) for i in range(len(columns)))
        print(line)

    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ── Centrality: PageRank and Katz (in-database power iteration) ──────────────


def _build_graph_tables(con, reverse=False, undirected=False):
    """Build _pr_adj (distinct edges) and _pr_out_deg from citation_edges. Returns N."""
    con.execute("DROP TABLE IF EXISTS _pr_adj")
    con.execute("DROP TABLE IF EXISTS _pr_out_deg")
    if undirected:
        con.execute("""
            CREATE TEMP TABLE _pr_adj AS
            SELECT DISTINCT citing_id, cited_id FROM citation_edges
            UNION
            SELECT DISTINCT cited_id AS citing_id, citing_id AS cited_id FROM citation_edges
        """)
    elif reverse:
        con.execute("""
            CREATE TEMP TABLE _pr_adj AS
            SELECT DISTINCT cited_id AS citing_id, citing_id AS cited_id FROM citation_edges
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE _pr_adj AS
            SELECT DISTINCT citing_id, cited_id FROM citation_edges
        """)
    con.execute("""
        CREATE TEMP TABLE _pr_out_deg AS
        SELECT citing_id, COUNT(*) AS out_deg FROM _pr_adj GROUP BY citing_id
    """)
    return con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]


def _build_personalization(con, seeds):
    """Create _pr_v table: 1/|seeds| for seed papers, 0 elsewhere."""
    con.execute("DROP TABLE IF EXISTS _pr_v")
    v = 1.0 / len(seeds)
    seed_set = set(seeds)
    rows = con.execute("SELECT paper_id FROM papers").fetchall()
    con.execute("CREATE TEMP TABLE _pr_v (paper_id VARCHAR, v DOUBLE)")
    con.executemany(
        "INSERT INTO _pr_v VALUES (?, ?)",
        [(r[0], v if r[0] in seed_set else 0.0) for r in rows],
    )


def _compute_pagerank_db(con, n, damping=0.85, max_iter=100, tol=1e-6, top_k=15, seeds=None):
    """
    Power-iteration PageRank over _pr_adj / _pr_out_deg.

    Formula (per node v):
        r[v] = d * sum_{u->v} r[u]/out_deg[u]  +  (dangling_mass + 1-d) * v[v]
    where v[v] = 1/N (global) or 1/|seeds| for seed nodes (personalized).

    Stops when top-K set is unchanged for 2 consecutive iterations OR L1 < tol.
    Returns ([(paper_id, score), ...] sorted desc, iters).
    """
    personalized = bool(seeds)
    if personalized:
        _build_personalization(con, seeds)
        con.execute("DROP TABLE IF EXISTS _pr_scores")
        con.execute("""
            CREATE TEMP TABLE _pr_scores AS
            SELECT paper_id, v AS score FROM _pr_v
        """)
    else:
        init = 1.0 / n
        con.execute("DROP TABLE IF EXISTS _pr_scores")
        con.execute(f"CREATE TEMP TABLE _pr_scores AS SELECT paper_id, {init} AS score FROM papers")

    prev_top_k = None
    stable = 0

    for it in range(max_iter):
        # Dangling mass: scores of papers with no outgoing edges
        dangling_mass = float(con.execute("""
            SELECT COALESCE(SUM(s.score), 0.0) FROM _pr_scores s
            WHERE s.paper_id NOT IN (SELECT citing_id FROM _pr_out_deg)
        """).fetchone()[0])

        # Teleportation: (dangling_mass + 1 - d) * v[v]
        # For global PR, v[v] = 1/N for all — fold into a constant
        teleport_const = (dangling_mass + 1.0 - damping) / n  # global only

        con.execute("CREATE OR REPLACE TEMP TABLE _pr_old AS SELECT * FROM _pr_scores")

        if personalized:
            # teleport = (dangling_mass + 1-d) * v[v], varies per paper
            teleport_factor = dangling_mass + 1.0 - damping
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _pr_scores AS
                SELECT p.paper_id,
                       {teleport_factor} * pv.v
                       + {damping} * COALESCE(SUM(s.score / od.out_deg), 0.0) AS score
                FROM papers p
                JOIN _pr_v pv ON pv.paper_id = p.paper_id
                LEFT JOIN _pr_adj a  ON a.cited_id  = p.paper_id
                LEFT JOIN _pr_old s  ON s.paper_id  = a.citing_id
                LEFT JOIN _pr_out_deg od ON od.citing_id = a.citing_id
                GROUP BY p.paper_id, pv.v
            """)
        else:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _pr_scores AS
                SELECT p.paper_id,
                       {teleport_const}
                       + {damping} * COALESCE(SUM(s.score / od.out_deg), 0.0) AS score
                FROM papers p
                LEFT JOIN _pr_adj a  ON a.cited_id  = p.paper_id
                LEFT JOIN _pr_old s  ON s.paper_id  = a.citing_id
                LEFT JOIN _pr_out_deg od ON od.citing_id = a.citing_id
                GROUP BY p.paper_id
            """)

        l1 = float(con.execute("""
            SELECT SUM(ABS(n.score - o.score))
            FROM _pr_scores n JOIN _pr_old o ON n.paper_id = o.paper_id
        """).fetchone()[0] or 0.0)

        cur_top = frozenset(
            r[0] for r in con.execute(
                f"SELECT paper_id FROM _pr_scores ORDER BY score DESC LIMIT {top_k}"
            ).fetchall()
        )
        stable = (stable + 1) if cur_top == prev_top_k else 0
        prev_top_k = cur_top

        if stable >= 2 or l1 / n < tol:
            break

    result = con.execute("SELECT paper_id, score FROM _pr_scores ORDER BY score DESC").fetchall()
    con.execute("DROP TABLE IF EXISTS _pr_scores")
    con.execute("DROP TABLE IF EXISTS _pr_v")
    return result, it + 1


def _compute_katz_db(con, n, alpha=None, beta=1.0, max_iter=100, tol=1e-6, top_k=15, seeds=None):
    """
    Power-iteration Katz centrality over _pr_adj.

    Formula (per node v):
        x[v] = beta_v + alpha * sum_{u->v} x[u]
    where beta_v = beta (global) or 1 for seed nodes (personalized).
    alpha must be < 1/lambda_max; auto-chosen as 0.5/max_in_degree if None.

    Stops when top-K set is unchanged for 2 consecutive iterations OR L1 < tol.
    Returns ([(paper_id, score), ...] sorted desc, iters, alpha_used).
    """
    max_in_deg = con.execute("""
        SELECT MAX(cnt) FROM (
            SELECT cited_id, COUNT(*) AS cnt FROM _pr_adj GROUP BY cited_id
        )
    """).fetchone()[0] or 1
    lambda_max = max(float(max_in_deg), 1.0)
    safe_alpha = 0.5 / lambda_max

    if alpha is None:
        alpha = safe_alpha
    elif alpha >= 1.0 / lambda_max:
        print(f"  Warning: alpha {alpha:.4f} >= 1/λmax ({1/lambda_max:.4f}), "
              f"clamping to {safe_alpha:.4f}", file=sys.stderr)
        alpha = safe_alpha

    # Initialization and per-node bias
    con.execute("DROP TABLE IF EXISTS _pr_scores")
    if seeds:
        seed_set = set(seeds)
        rows = con.execute("SELECT paper_id FROM papers").fetchall()
        con.execute("CREATE TEMP TABLE _pr_scores (paper_id VARCHAR, score DOUBLE)")
        con.execute("CREATE TEMP TABLE _pr_bias  (paper_id VARCHAR, bias  DOUBLE)")
        score_rows = [(r[0], 1.0 if r[0] in seed_set else 0.0) for r in rows]
        con.executemany("INSERT INTO _pr_scores VALUES (?, ?)", score_rows)
        con.executemany("INSERT INTO _pr_bias   VALUES (?, ?)", score_rows)
    else:
        con.execute(f"CREATE TEMP TABLE _pr_scores AS SELECT paper_id, {beta} AS score FROM papers")
        con.execute(f"CREATE TEMP TABLE _pr_bias  AS SELECT paper_id, {beta} AS bias  FROM papers")

    prev_top_k = None
    stable = 0

    for it in range(max_iter):
        con.execute("CREATE OR REPLACE TEMP TABLE _pr_old AS SELECT * FROM _pr_scores")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _pr_scores AS
            SELECT p.paper_id,
                   CAST(b.bias AS DOUBLE)
                   + {alpha} * CAST(COALESCE(SUM(s.score), 0.0) AS DOUBLE) AS score
            FROM papers p
            JOIN _pr_bias b ON b.paper_id = p.paper_id
            LEFT JOIN _pr_adj a ON a.cited_id = p.paper_id
            LEFT JOIN _pr_old s ON s.paper_id = a.citing_id
            GROUP BY p.paper_id, b.bias
        """)

        l1 = float(con.execute("""
            SELECT SUM(ABS(n.score - o.score))
            FROM _pr_scores n JOIN _pr_old o ON n.paper_id = o.paper_id
        """).fetchone()[0] or 0.0)

        cur_top = frozenset(
            r[0] for r in con.execute(
                f"SELECT paper_id FROM _pr_scores ORDER BY score DESC LIMIT {top_k}"
            ).fetchall()
        )
        stable = (stable + 1) if cur_top == prev_top_k else 0
        prev_top_k = cur_top

        if stable >= 2 or l1 / n < tol:
            break

    result = con.execute("SELECT paper_id, score FROM _pr_scores ORDER BY score DESC").fetchall()
    con.execute("DROP TABLE IF EXISTS _pr_scores")
    con.execute("DROP TABLE IF EXISTS _pr_bias")
    return result, it + 1, alpha


def _print_centrality(scores, con, top_n, owned_only, stubs_only, label, iters):
    meta = {
        r[0]: (r[1], r[2], r[3])
        for r in con.execute("SELECT paper_id, title, authors, year FROM papers").fetchall()
    }
    owned_ids = {r[0] for r in con.execute(
        "SELECT paper_id FROM papers WHERE type IN ('owned', 'external_owned')"
    ).fetchall()}

    print(f"\n{label}  [converged in {iters} iteration{'s' if iters != 1 else ''}]")
    print(f" {'Rank':>4}  {'Score':>10}  {'Year':>4}  {'Type':<6}  Paper")
    print(" " + "-" * 95)
    shown = 0
    for pid, score in scores:
        if owned_only and pid not in owned_ids:
            continue
        if stubs_only and pid in owned_ids:
            continue
        title, authors, year = meta.get(pid, ("", "", ""))
        ptype = "owned" if pid in owned_ids else "cited"
        print(f" {shown+1:>4}  {score:>10.6f}  {year or '????':>4}  {ptype:<6}  "
              f"{fmt_authors(authors)} — {(title or '')[:55]}")
        shown += 1
        if shown >= top_n:
            break


def _add_centrality_args(p):
    """Shared flags for pagerank / katz subcommands."""
    p.add_argument("--reverse", action="store_true",
                   help="Flip edges: importance flows to citers (surfaces surveys)")
    p.add_argument("--undirected", action="store_true",
                   help="Symmetrise graph (bidirectional co-citation centrality)")
    p.add_argument("--seed", nargs="+", metavar="ID",
                   help="Personalization seeds (one or more paper IDs)")
    p.add_argument("--top", type=int, default=15, metavar="N")
    filt = p.add_mutually_exclusive_group()
    filt.add_argument("--owned", action="store_true",
                      help="Restrict output to owned papers")
    filt.add_argument("--stubs", action="store_true",
                      help="Restrict output to cited-only papers (gap discovery)")
    p.add_argument("--max-iter", type=int, default=100, dest="max_iter")
    p.add_argument("--tol", type=float, default=1e-6,
                   help="L1 score-change convergence threshold (default 1e-6)")


def cmd_pagerank(args, con):
    if args.reverse and args.undirected:
        print("Error: --reverse and --undirected are mutually exclusive.", file=sys.stderr)
        sys.exit(1)
    seeds = [resolve_paper_id(con, s) for s in args.seed] if args.seed else None
    n = _build_graph_tables(con, reverse=args.reverse, undirected=args.undirected)
    scores, iters = _compute_pagerank_db(
        con, n, damping=args.alpha, max_iter=args.max_iter, tol=args.tol,
        top_k=args.top, seeds=seeds,
    )
    dir_tag = " [undirected]" if args.undirected else (" [reverse]" if args.reverse else "")
    seed_tag = f" · seeds: {', '.join(seeds)}" if seeds else f" · d={args.alpha}"
    label = f"PageRank{dir_tag}  ({seed_tag.strip(' ·')})  top {args.top}"
    if args.owned:
        label += "  — owned only"
    elif args.stubs:
        label += "  — cited-only (gaps)"
    _print_centrality(scores, con, args.top, args.owned, args.stubs, label, iters)
    con.execute("DROP TABLE IF EXISTS _pr_adj")
    con.execute("DROP TABLE IF EXISTS _pr_out_deg")


def cmd_katz(args, con):
    if args.reverse and args.undirected:
        print("Error: --reverse and --undirected are mutually exclusive.", file=sys.stderr)
        sys.exit(1)
    seeds = [resolve_paper_id(con, s) for s in args.seed] if args.seed else None
    n = _build_graph_tables(con, reverse=args.reverse, undirected=args.undirected)
    scores, iters, alpha_used = _compute_katz_db(
        con, n, alpha=args.alpha, beta=args.beta, max_iter=args.max_iter,
        tol=args.tol, top_k=args.top, seeds=seeds,
    )
    dir_tag = " [undirected]" if args.undirected else (" [reverse]" if args.reverse else "")
    seed_tag = f" · seeds: {', '.join(seeds)}" if seeds else f" · α={alpha_used:.4f}"
    label = f"Katz centrality{dir_tag}  ({seed_tag.strip(' ·')})  top {args.top}"
    if args.owned:
        label += "  — owned only"
    elif args.stubs:
        label += "  — cited-only (gaps)"
    _print_centrality(scores, con, args.top, args.owned, args.stubs, label, iters)
    con.execute("DROP TABLE IF EXISTS _pr_adj")
    con.execute("DROP TABLE IF EXISTS _pr_out_deg")


# ── Explore (cite_explorer fold-in) ────────────────────────────────────────

ALL_PURPOSES = [
    "background", "motivation", "methodology", "data_source",
    "supporting_evidence", "contrasting_evidence",
    "comparison", "extension", "tool_software",
]


def cmd_explore(args, con):
    """Explore how an owned paper cites others (replaces cite_explorer.py)."""
    pid = resolve_paper_id(con, args.id)

    # Fetch all contexts where this paper is the citer
    rows = con.execute("""
        SELECT c.cited_id, c.cited_title, c.purpose, c.section, c.quote, c.explanation,
               p.title, p.authors, p.year, p.journal, p.doi, p.type
        FROM contexts c
        LEFT JOIN papers p ON c.cited_id = p.paper_id
        WHERE c.citing_id = ?
        ORDER BY c.cited_id
    """, [pid]).fetchall()

    # Group by cited_id
    citations = {}
    for cited_id, ctitle, purpose, section, quote, explanation, \
            title, authors, year, journal, doi, ptype in rows:
        if cited_id not in citations:
            citations[cited_id] = {
                "id": cited_id,
                "title": title or ctitle or "",
                "authors": authors or "",
                "year": year,
                "journal": journal or "",
                "doi": doi or "",
                "type": ptype or "unknown",
                "contexts": [],
            }
        citations[cited_id]["contexts"].append({
            "purpose": purpose or "",
            "section": section or "",
            "quote": quote or "",
            "explanation": explanation or "",
        })

    cit_list = list(citations.values())

    # --owned-only filter
    if args.owned_only:
        cit_list = [c for c in cit_list if c["type"] in ("owned", "external_owned")]

    # --purpose filter
    if args.purpose:
        filtered = []
        for c in cit_list:
            matching = [ctx for ctx in c["contexts"] if ctx["purpose"] in args.purpose]
            if matching:
                filtered.append({**c, "contexts": matching})
        cit_list = filtered

    # --search filter
    if args.search:
        phrase = args.search.lower()
        filtered = []
        for c in cit_list:
            title_match = phrase in c["title"].lower()
            matching = [ctx for ctx in c["contexts"]
                        if phrase in ctx["quote"].lower() or phrase in ctx["section"].lower()]
            if title_match:
                filtered.append(c)
            elif matching:
                filtered.append({**c, "contexts": matching})
        cit_list = filtered

    # Sort
    sort_by = args.sort
    if sort_by == "year":
        cit_list.sort(key=lambda c: str(c.get("year") or "9999"))
    elif sort_by == "purpose":
        cit_list.sort(key=lambda c: c["contexts"][0]["purpose"] if c["contexts"] else "zzz")
    elif sort_by == "appearances":
        cit_list.sort(key=lambda c: -len(c["contexts"]))
    else:
        cit_list.sort(key=lambda c: c["id"])

    total = len(cit_list)

    # Limit
    if args.limit:
        cit_list = cit_list[:args.limit]

    # JSON output
    if args.json:
        out = []
        for c in cit_list:
            out.append({
                "id": c["id"], "title": c["title"],
                "authors": c["authors"], "year": c["year"],
                "journal": c["journal"], "doi": c["doi"],
                "type": c["type"],
                "purposes": sorted({ctx["purpose"] for ctx in c["contexts"]}),
                "contexts": c["contexts"],
            })
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return

    # Header (not in minimal mode)
    if args.detail != "minimal":
        paper = con.execute("SELECT title, authors, year FROM papers WHERE paper_id = ?", [pid]).fetchone()
        print(f"Paper : {pid}")
        if paper:
            print(f"Title : {paper[0]}")
            print(f"By    : {fmt_authors(paper[1])} ({paper[2]})")
        filters = []
        if args.purpose:
            filters.append(f"purpose={','.join(args.purpose)}")
        if args.search:
            filters.append(f"search='{args.search}'")
        if args.owned_only:
            filters.append("owned-only")
        filter_str = f"  [filters: {', '.join(filters)}]" if filters else ""
        shown = len(cit_list)
        print(f"Citing: {total} citation(s){filter_str}" +
              (f", showing first {shown}" if args.limit and shown < total else ""))
        print()

    # Output by detail level
    if args.detail == "minimal":
        for c in cit_list:
            print(c["id"])

    elif args.detail == "summary":
        for c in cit_list:
            if not c["contexts"]:
                continue
            owned_flag = " [OWNED]" if c["type"] in ("owned", "external_owned") else ""
            for ctx in c["contexts"]:
                print(f"  {c['id']}{owned_flag} | {ctx['purpose']} | {ctx['explanation']}")

    elif args.detail == "normal":
        for c in cit_list:
            owned_flag = " [OWNED]" if c["type"] in ("owned", "external_owned") else ""
            purpose_counts = {}
            for ctx in c["contexts"]:
                p = ctx["purpose"]
                purpose_counts[p] = purpose_counts.get(p, 0) + 1

            print(f"  {c['id']}{owned_flag}")
            print(f"    title   : {c['title']}")
            if c["authors"]:
                yr = f" ({c['year']})" if c["year"] else ""
                print(f"    authors : {fmt_authors(c['authors'])}{yr}")
            if c["journal"]:
                print(f"    journal : {c['journal']}")
            if purpose_counts:
                purposes_str = ", ".join(
                    f"{p} x{n}" if n > 1 else p
                    for p, n in sorted(purpose_counts.items())
                )
                print(f"    cited as: {purposes_str}")
            print()

    else:  # full
        for c in cit_list:
            owned_flag = " [OWNED]" if c["type"] in ("owned", "external_owned") else ""
            print(f"  {c['id']}{owned_flag}")
            print(f"    title   : {c['title']}")
            if c["authors"]:
                yr = f" ({c['year']})" if c["year"] else ""
                print(f"    authors : {fmt_authors(c['authors'])}{yr}")
            if c["journal"]:
                print(f"    journal : {c['journal']}")
            if c["doi"]:
                print(f"    doi     : {c['doi']}")
            print(f"    contexts: {len(c['contexts'])}")
            for ctx in c["contexts"]:
                print(f"      [{ctx['purpose']}] {ctx['section']}")
                quote = ctx["quote"]
                if quote:
                    wrapped = wrap(f'"{quote}"', prefix="      ", width=100)
                    print(wrapped)
            print()


# ── CLI ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="DuckDB-backed literature query engine")
    sub = parser.add_subparsers(dest="command")


    # Paper lookup
    p = sub.add_parser("paper", help="Paper summary")
    p.add_argument("id")

    sub.add_parser("owned", help="List owned papers")

    p = sub.add_parser("author", help="Search by author")
    p.add_argument("name")

    p = sub.add_parser("author-info", help="Author entity details + paper list")
    p.add_argument("author_id")

    p = sub.add_parser("search-authors", help="BM25 search over author names/variants")
    p.add_argument("phrase")

    p = sub.add_parser("coauthors", help="Coauthor network for an author")
    p.add_argument("author_id")

    p = sub.add_parser("top-authors", help="Most prolific authors")
    p.add_argument("n", nargs="?", type=int, default=15)

    # Citation queries
    p = sub.add_parser("cites", help="Papers that cite this paper")
    p.add_argument("id")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("cited-by", help="Papers this paper cites")
    p.add_argument("id")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("chain", help="Recursive citation chain")
    p.add_argument("id")
    p.add_argument("--depth", type=int, default=2)

    p = sub.add_parser("common-citers", help="Papers citing both id1 and id2")
    p.add_argument("id1")
    p.add_argument("id2")

    p = sub.add_parser("co-cited", help="Co-citation: references frequently co-cited with given paper(s)")
    p.add_argument("ids", nargs="+")
    p.add_argument("--min", type=int, default=2)
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("bib-coupling", help="Bibliographic coupling: papers sharing citations with given paper(s)")
    p.add_argument("ids", nargs="+")
    p.add_argument("--min", type=int, default=2)
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("shared-refs", help="Shared references between papers")
    p.add_argument("ids", nargs="+")

    p = sub.add_parser("shared-papers", help="Papers co-authored by given authors")
    p.add_argument("ids", nargs="+")

    p = sub.add_parser("top-cited", help="Top N most-cited papers")
    p.add_argument("n", nargs="?", type=int, default=15)

    p = sub.add_parser("pagerank", help="PageRank centrality (in-database power iteration)")
    _add_centrality_args(p)
    p.add_argument("--alpha", type=float, default=0.85, help="Damping factor (default 0.85)")

    p = sub.add_parser("katz", help="Katz centrality (in-database power iteration)")
    _add_centrality_args(p)
    p.add_argument("--alpha", type=float, default=None,
                   help="Attenuation factor (default 0.5/λmax, auto-safe)")
    p.add_argument("--beta", type=float, default=1.0,
                   help="Base prestige added each iteration (default 1.0)")

    # Explore (cite_explorer replacement)
    p = sub.add_parser("explore", help="Explore how a paper cites others")
    p.add_argument("id")
    p.add_argument("--detail", choices=["minimal", "summary", "normal", "full"],
                   default="normal", help="Output detail level (default: normal)")
    p.add_argument("--purpose", nargs="+", metavar="PURPOSE", choices=ALL_PURPOSES,
                   help="Filter by citation purpose(s)")
    p.add_argument("--search", metavar="PHRASE",
                   help="Filter by phrase in title, quote, or section")
    p.add_argument("--limit", type=int, metavar="N",
                   help="Show only first N citations after filtering")
    p.add_argument("--sort", choices=["id", "year", "purpose", "appearances"],
                   default="id", help="Sort order (default: id)")
    p.add_argument("--owned-only", action="store_true",
                   help="Only show citations that are owned papers")
    p.add_argument("--json", action="store_true",
                   help="Output as JSON")

    p = sub.add_parser("purpose", help="Contexts by purpose tag")
    p.add_argument("tag")
    p.add_argument("--limit", type=int)

    # Full-text search
    p = sub.add_parser("search", help="BM25-ranked search across all fields")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)
    p.add_argument("--filter-purpose", type=str)
    p.add_argument("--filter-year-min", type=int)

    p = sub.add_parser("search-all", help="Summary counts + details across all fields")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("search-claims", help="Search claims")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)
    p.add_argument("--type", type=str)

    p = sub.add_parser("search-sections", help="Search sections")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("search-topics", help="Search topics")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("search-keywords", help="Search keywords")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("search-methods", help="Search by methodology type")
    p.add_argument("type")

    # Corpus metadata
    p = sub.add_parser("abstract", help="Paper abstract")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("claims", help="Paper claims")
    p.add_argument("id")
    p.add_argument("--type", type=str)
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("keywords", help="Keywords and topics")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("methodology", help="Methodology details")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("sections", help="Section headings")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("questions", help="Research questions")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    p = sub.add_parser("data-sources", help="Data sources")
    p.add_argument("id")
    p.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    # Overview
    sub.add_parser("stats", help="Corpus statistics")
    sub.add_parser("methods", help="Methodology types with counts")
    sub.add_parser("purposes-list", help="Purpose tags with counts")

    # Pull request handoff
    p = sub.add_parser("request-pull", help="Queue papers for forward citation fetching")
    p.add_argument("ids", nargs="+", metavar="ID", help="Paper IDs to queue")

    # Raw SQL
    p = sub.add_parser("sql", help="Execute arbitrary SQL against the database")
    p.add_argument("query", nargs="?", default=None, help="SQL query to execute")
    p.add_argument("--schema", action="store_true", help="Print all table schemas")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    con = get_connection()

    commands = {
        "paper": cmd_paper,
        "owned": cmd_owned,
        "author": cmd_author,
        "author-info": cmd_author_info,
        "search-authors": cmd_search_authors,
        "coauthors": cmd_coauthors,
        "top-authors": cmd_top_authors,
        "cites": cmd_cites,
        "cited-by": cmd_cited_by,
        "chain": cmd_chain,
        "common-citers": cmd_common_citers,
        "co-cited": cmd_co_cited,
        "bib-coupling": cmd_bib_coupling,
        "shared-refs": cmd_shared_refs,
        "shared-papers": cmd_shared_papers,
        "top-cited": cmd_top_cited,
        "pagerank": cmd_pagerank,
        "katz": cmd_katz,
        "explore": cmd_explore,
        "purpose": cmd_purpose,
        "search": cmd_search,
        "search-all": cmd_search_all,
        "search-claims": cmd_search_claims,
        "search-sections": cmd_search_sections,
        "search-topics": cmd_search_topics,
        "search-keywords": cmd_search_keywords,
        "search-methods": cmd_search_methods,
        "abstract": cmd_abstract,
        "claims": cmd_claims,
        "keywords": cmd_keywords,
        "methodology": cmd_methodology,
        "sections": cmd_sections,
        "questions": cmd_questions,
        "data-sources": cmd_data_sources,
        "stats": cmd_stats,
        "methods": cmd_methods,
        "purposes-list": cmd_purposes_list,
        "request-pull": cmd_request_pull,
        "sql": cmd_sql,
    }

    cmd_fn = commands.get(args.command)
    if cmd_fn:
        cmd_fn(args, con)
    else:
        parser.print_help()

    con.close()


if __name__ == "__main__":
    main()
