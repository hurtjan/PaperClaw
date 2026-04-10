#!/usr/bin/env python3
"""
Build data/db/lit.duckdb from the JSON database files.

Loads papers.json, contexts.json, extractions/*.json, and authors.json into
DuckDB tables used by duckdb_query.py. Supports incremental builds (skips
groups whose source files haven't changed) and optional FTS index creation.

Usage:
  python3 scripts/py.py scripts/build/build_duckdb.py
  python3 scripts/py.py scripts/build/build_duckdb.py --force
  python3 scripts/py.py scripts/build/build_duckdb.py --fts
  python3 scripts/py.py scripts/build/build_duckdb.py --force --fts
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime
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

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
INDEX_FILE = ROOT / "data" / "db" / "contexts.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
DB_FILE = ROOT / "data" / "db" / "lit.duckdb"

_TMP_DIR = ROOT / "data" / "tmp"


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
        print("  FTS extension installed and loaded.")
        return True
    except Exception as e:
        print(f"  WARNING: Could not install/load FTS extension: {e}")
        return False


def _stat_key(path):
    """Return (mtime_ns, size) for a file, or (0, 0) if missing."""
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return (0, 0)


def _extractions_stat():
    """Return (max_mtime_ns, file_count) across all extraction JSON files."""
    if not EXTRACTIONS_DIR.exists():
        return (0, 0)
    files = [
        f for f in EXTRACTIONS_DIR.glob("*.json")
        if not any(p in f.stem for p in (".refs", ".contexts", ".sections", ".analysis"))
    ]
    if not files:
        return (0, 0)
    return (max(os.stat(f).st_mtime_ns for f in files), len(files))


def _read_build_meta(con):
    """Read stored (mtime_ns, size) from _build_meta. Returns dict source->(mtime_ns, size)."""
    try:
        rows = con.execute("SELECT source, mtime_ns, size FROM _build_meta").fetchall()
        return {r[0]: (r[1], r[2]) for r in rows}
    except Exception:
        return {}


def _upsert_build_meta(con, source, mtime_ns, size):
    """Upsert mtime/size into _build_meta."""
    con.execute(
        "INSERT OR REPLACE INTO _build_meta (source, mtime_ns, size) VALUES (?, ?, ?)",
        [source, mtime_ns, size],
    )


def _bulk_load(con, table_name, rows, empty_schema=None):
    """Write rows as JSONL, bulk-load via read_json with explicit schema, clean up temp file."""
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    if not rows:
        if empty_schema:
            con.execute(f"CREATE TABLE {table_name} ({empty_schema})")
        return
    _TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.jsonl', delete=False, dir=str(_TMP_DIR)
    ) as f:
        tmp = Path(f.name)
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    try:
        if empty_schema:
            con.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_json('{tmp}', columns={{{_schema_to_dict(empty_schema)}}})"
            )
        else:
            con.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{tmp}')"
            )
    finally:
        tmp.unlink(missing_ok=True)


def _schema_to_dict(schema: str) -> str:
    """Convert 'col1 TYPE1, col2 TYPE2' into DuckDB columns dict literal string."""
    parts = []
    for col_def in schema.split(","):
        col_def = col_def.strip()
        name, _, dtype = col_def.partition(" ")
        parts.append(f"'{name}': '{dtype.strip()}'")
    return ", ".join(parts)


def build_db(con, force=False, fts=False):
    """Load all JSON data into DuckDB tables, skipping groups whose source hasn't changed."""
    t0 = time.time()

    # Ensure _build_meta tracking table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS _build_meta (
            source VARCHAR PRIMARY KEY,
            mtime_ns BIGINT,
            size BIGINT
        )
    """)

    # Ensure _fts_meta tracking table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS _fts_meta (
            rebuilt_at VARCHAR,
            owned_count INTEGER
        )
    """)

    stored = {} if force else _read_build_meta(con)

    # Track which groups rebuilt (drives FTS rebuild decisions)
    rebuilt = {"papers": False, "contexts": False, "extractions": False, "authors": False}

    # ── Group A: papers.json → papers, citation_edges ────────────────────────
    papers_stat = _stat_key(PAPERS_FILE)
    if not force and stored.get("papers.json") == papers_stat:
        print("  papers.json unchanged, skipping Group A")
        paper_count = con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        owned_count = con.execute(
            "SELECT COUNT(*) FROM papers WHERE type IN ('owned','external_owned')"
        ).fetchone()[0]
    else:
        rebuilt["papers"] = True
        papers_data = _fast_loads(PAPERS_FILE.read_text())["papers"]
        superseded_ids = {pid for pid, p in papers_data.items() if p.get("superseded_by")}
        paper_rows = []
        for pid, p in papers_data.items():
            if pid in superseded_ids:
                continue
            raw_year = p.get("year")
            if isinstance(raw_year, str):
                m = re.search(r'\d{4}', raw_year)
                year = int(m.group()) if m else None
            elif isinstance(raw_year, (int, float)):
                year = int(raw_year)
            else:
                year = None
            raw_authors = p.get("authors", "")
            if isinstance(raw_authors, list):
                authors_str = "; ".join(str(a) for a in raw_authors)
            else:
                authors_str = str(raw_authors) if raw_authors else ""
            em = p.get("extraction_meta")
            if em:
                detail = em.get("detail_level", "")
                passes = ",".join(str(x) for x in sorted(em.get("passes_completed", [])))
            else:
                detail = None
                passes = None
            paper_rows.append({
                "paper_id": pid,
                "type": p.get("type", ""),
                "title": p.get("title", ""),
                "authors": authors_str,
                "year": year,
                "journal": p.get("journal", ""),
                "doi": p.get("doi", ""),
                "abstract": p.get("abstract", ""),
                "pdf_file": p.get("pdf_file", ""),
                "text_file": p.get("text_file", ""),
                "detail_level": detail,
                "passes_completed": passes,
                "s2_id": p.get("s2_paper_id") or None,
                "arxiv_id": p.get("arxiv_id") or None,
                "pubmed_id": p.get("pubmed_id") or None,
                "pmc_id": p.get("pmc_id") or None,
                "preprint_server": p.get("preprint_server") or None,
                "open_access_url": p.get("open_access_url") or None,
            })

        _bulk_load(con, "papers", paper_rows,
            "paper_id VARCHAR, type VARCHAR, title VARCHAR, authors VARCHAR, "
            "year INTEGER, journal VARCHAR, doi VARCHAR, abstract VARCHAR, "
            "pdf_file VARCHAR, text_file VARCHAR, "
            "detail_level VARCHAR, passes_completed VARCHAR, "
            "s2_id VARCHAR, arxiv_id VARCHAR, pubmed_id VARCHAR, pmc_id VARCHAR, "
            "preprint_server VARCHAR, open_access_url VARCHAR")

        edge_rows = []
        for pid, p in papers_data.items():
            if pid in superseded_ids:
                continue
            for cited_id in p.get("cites", []):
                edge_rows.append({"citing_id": pid, "cited_id": cited_id, "cited_title": ""})
        _bulk_load(con, "citation_edges", edge_rows,
            "citing_id VARCHAR, cited_id VARCHAR, cited_title VARCHAR")
        if edge_rows:
            con.execute("UPDATE citation_edges SET cited_title = p.title FROM papers p WHERE citation_edges.cited_id = p.paper_id")
        con.execute("CREATE INDEX idx_ce_citing ON citation_edges(citing_id)")
        con.execute("CREATE INDEX idx_ce_cited ON citation_edges(cited_id)")

        paper_count = len(paper_rows)
        owned_count = sum(1 for r in paper_rows if r["type"] in ("owned", "external_owned"))
        _upsert_build_meta(con, "papers.json", *papers_stat)

    # ── Group B: contexts.json → contexts, citation_counts ───────────────────
    contexts_stat = _stat_key(INDEX_FILE)
    if not force and stored.get("contexts.json") == contexts_stat:
        print("  contexts.json unchanged, skipping Group B")
        context_count = con.execute("SELECT COUNT(*) FROM contexts").fetchone()[0]
    else:
        rebuilt["contexts"] = True
        index_data = _fast_loads(INDEX_FILE.read_text())
        context_rows = []
        for cited_id, entries in index_data.get("by_cited", {}).items():
            for e in entries:
                context_rows.append({
                    "citing_id": e.get("citing", ""),
                    "cited_id": e.get("cited", cited_id),
                    "cited_title": e.get("cited_title", ""),
                    "purpose": e.get("purpose", ""),
                    "section": e.get("section", ""),
                    "quote": e.get("quote", ""),
                    "explanation": e.get("explanation", ""),
                })
        _bulk_load(con, "contexts", context_rows,
            "citing_id VARCHAR, cited_id VARCHAR, cited_title VARCHAR, "
            "purpose VARCHAR, section VARCHAR, quote VARCHAR, explanation VARCHAR")

        cc = index_data.get("citation_counts", {})
        cc_rows = [{"paper_id": k, "cited_by_count": v} for k, v in cc.items()]
        _bulk_load(con, "citation_counts", cc_rows,
            "paper_id VARCHAR, cited_by_count INTEGER")

        con.execute("CREATE INDEX IF NOT EXISTS idx_contexts_cited ON contexts(cited_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_contexts_citing ON contexts(citing_id)")

        context_count = len(context_rows)
        _upsert_build_meta(con, "contexts.json", *contexts_stat)

    # ── Group C: extractions/*.json → claims, keywords, topics, etc. ─────────
    ext_stat = _extractions_stat()
    if not force and stored.get("extractions") == ext_stat:
        print("  extractions unchanged, skipping Group C")
        claim_count = con.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        keyword_count = con.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
        topic_count = con.execute("SELECT COUNT(*) FROM topics").fetchone()[0]
        section_count = con.execute("SELECT COUNT(*) FROM sections").fetchone()[0]
    else:
        rebuilt["extractions"] = True
        claim_rows, keyword_rows, topic_rows, section_rows = [], [], [], []
        methodology_rows, question_rows, datasource_rows = [], [], []

        # Skip extractions for superseded papers
        _pdb = _fast_loads(PAPERS_FILE.read_text())["papers"]
        superseded_ids = {pid for pid, p in _pdb.items() if p.get("superseded_by")}
        del _pdb

        for f in sorted(EXTRACTIONS_DIR.glob("*.json")):
            if any(part in f.stem for part in (".refs", ".contexts", ".sections", ".analysis")):
                continue
            try:
                data = _fast_loads(f.read_text())
                if not isinstance(data, dict):
                    continue
            except Exception:
                continue

            pid = f.stem
            if pid in superseded_ids:
                continue

            for c in data.get("claims", []) or []:
                claim_rows.append({
                    "paper_id": pid, "claim": c.get("claim", ""),
                    "type": c.get("type", ""), "confidence": c.get("confidence", ""),
                    "evidence_basis": c.get("evidence_basis", ""),
                    "quantification": c.get("quantification", ""),
                    "supporting_citations": json.dumps(c.get("supporting_citations", [])),
                })
            for kw in data.get("keywords", []) or []:
                keyword_rows.append({"paper_id": pid, "keyword": str(kw)})
            topics = data.get("topics") or {}
            if isinstance(topics, dict):
                for field in ("themes", "geographic_focus", "sectors", "policy_context"):
                    for val in topics.get(field, []) or []:
                        topic_rows.append({"paper_id": pid, "field": field, "value": str(val)})
            for s in data.get("sections", []) or []:
                section_rows.append({
                    "paper_id": pid, "heading": s.get("heading", ""),
                    "summary": s.get("summary", ""), "annotated_text": s.get("annotated_text", ""),
                })
            meth = data.get("methodology") or {}
            if isinstance(meth, dict) and meth:
                methodology_rows.append({
                    "paper_id": pid, "type": meth.get("type", ""),
                    "model_name": meth.get("model_name", ""),
                    "approach": meth.get("approach", ""),
                    "temporal_scope": meth.get("temporal_scope", ""),
                    "geographic_scope": json.dumps(meth.get("geographic_scope", "")),
                    "unit_of_analysis": meth.get("unit_of_analysis", ""),
                    "scenarios": json.dumps(meth.get("scenarios", "")),
                })
                for ds in meth.get("data_sources", []) or []:
                    if isinstance(ds, dict):
                        datasource_rows.append({"paper_id": pid, "name": ds.get("name", ""),
                            "type": ds.get("type", ""), "description": ds.get("description", "")})
                    else:
                        datasource_rows.append({"paper_id": pid, "name": str(ds), "type": "", "description": ""})
            for q in data.get("research_questions", []) or []:
                question_rows.append({"paper_id": pid, "question": str(q)})

        _bulk_load(con, "claims", claim_rows,
            "paper_id VARCHAR, claim VARCHAR, type VARCHAR, confidence VARCHAR, "
            "evidence_basis VARCHAR, quantification VARCHAR, supporting_citations VARCHAR")
        _bulk_load(con, "keywords", keyword_rows,
            "paper_id VARCHAR, keyword VARCHAR")
        _bulk_load(con, "topics", topic_rows,
            "paper_id VARCHAR, field VARCHAR, value VARCHAR")
        _bulk_load(con, "sections", section_rows,
            "paper_id VARCHAR, heading VARCHAR, summary VARCHAR, annotated_text VARCHAR")
        _bulk_load(con, "methodology", methodology_rows,
            "paper_id VARCHAR, type VARCHAR, model_name VARCHAR, approach VARCHAR, "
            "temporal_scope VARCHAR, geographic_scope VARCHAR, unit_of_analysis VARCHAR, "
            "scenarios VARCHAR")
        _bulk_load(con, "data_sources", datasource_rows,
            "paper_id VARCHAR, name VARCHAR, type VARCHAR, description VARCHAR")
        _bulk_load(con, "questions", question_rows,
            "paper_id VARCHAR, question VARCHAR")

        claim_count = len(claim_rows)
        keyword_count = len(keyword_rows)
        topic_count = len(topic_rows)
        section_count = len(section_rows)
        _upsert_build_meta(con, "extractions", *ext_stat)

    # ── Group D: authors.json → authors, paper_authors ───────────────────────
    authors_stat = _stat_key(AUTHORS_FILE)
    if not force and stored.get("authors.json") == authors_stat:
        print("  authors.json unchanged, skipping Group D")
        author_count = con.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
        paper_author_count = con.execute("SELECT COUNT(*) FROM paper_authors").fetchone()[0]
    else:
        rebuilt["authors"] = True
        author_rows, paper_author_rows = [], []

        if AUTHORS_FILE.exists():
            authors_data = _fast_loads(AUTHORS_FILE.read_text())
            for aid, a in authors_data.get("persons", {}).items():
                variants = "|".join(a.get("name_variants", []))
                author_rows.append({
                    "author_id": a.get("id", aid),
                    "canonical_name": a.get("canonical_name", ""),
                    "type": "person", "name_variants": variants,
                    "paper_count": a.get("paper_count", 0),
                    "owned_paper_count": a.get("owned_paper_count", 0),
                })
                for pid in a.get("papers", []):
                    paper_author_rows.append({"paper_id": pid, "author_id": a.get("id", aid)})
            for iid, inst in authors_data.get("institutions", {}).items():
                author_rows.append({
                    "author_id": inst.get("id", iid),
                    "canonical_name": inst.get("name", ""),
                    "type": "institution", "name_variants": inst.get("name", ""),
                    "paper_count": inst.get("paper_count", 0),
                    "owned_paper_count": 0,
                })
                for pid in inst.get("papers", []):
                    paper_author_rows.append({"paper_id": pid, "author_id": inst.get("id", iid)})

        _bulk_load(con, "authors", author_rows,
            "author_id VARCHAR, canonical_name VARCHAR, type VARCHAR, "
            "name_variants VARCHAR, paper_count INTEGER, owned_paper_count INTEGER")
        _bulk_load(con, "paper_authors", paper_author_rows,
            "paper_id VARCHAR, author_id VARCHAR")
        con.execute("CREATE INDEX IF NOT EXISTS idx_paper_authors_pid ON paper_authors(paper_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_paper_authors_aid ON paper_authors(author_id)")

        author_count = len(author_rows)
        paper_author_count = len(paper_author_rows)
        _upsert_build_meta(con, "authors.json", *authors_stat)

    # ── FTS indexes (only rebuild when fts=True and groups changed) ──────────
    fts_ok = _load_fts(con)
    if fts and fts_ok:
        if rebuilt["papers"]:
            con.execute("PRAGMA create_fts_index('papers', 'paper_id', 'title', 'abstract', 'authors', overwrite=1)")
        if rebuilt["contexts"]:
            con.execute("PRAGMA create_fts_index('contexts', 'rowid', 'quote', 'explanation', 'cited_title', overwrite=1)")
        if rebuilt["extractions"]:
            if claim_rows:
                con.execute("PRAGMA create_fts_index('claims', 'rowid', 'claim', 'quantification', overwrite=1)")
            if section_rows:
                con.execute("PRAGMA create_fts_index('sections', 'rowid', 'heading', 'summary', overwrite=1)")
            if keyword_rows:
                con.execute("PRAGMA create_fts_index('keywords', 'rowid', 'keyword', overwrite=1)")
            if topic_rows:
                con.execute("PRAGMA create_fts_index('topics', 'rowid', 'value', overwrite=1)")
        if rebuilt["authors"] and author_rows:
            con.execute("PRAGMA create_fts_index('authors', 'author_id', 'canonical_name', 'name_variants', overwrite=1)")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        con.execute("DELETE FROM _fts_meta")
        con.execute("INSERT INTO _fts_meta VALUES (?, ?)", [now_str, owned_count])
    elif not fts and not fts_ok and any(rebuilt.values()):
        print("  WARNING: FTS extension not available. BM25 search disabled.")
        print("  To enable: run `python3 scripts/py.py scripts/build/install_fts.py` once, then rebuild with --fts.")

    elapsed = time.time() - t0
    changed_groups = [k for k, v in rebuilt.items() if v]
    if not changed_groups:
        print(f"DuckDB up-to-date (all groups unchanged) in {elapsed:.2f}s")
    else:
        print(
            f"Built DuckDB: {paper_count} papers ({owned_count} owned), "
            f"{context_count} contexts, {claim_count} claims, "
            f"{keyword_count} keywords, {topic_count} topics, "
            f"{section_count} sections, "
            f"{author_count} authors, {paper_author_count} paper_author links "
            f"in {elapsed:.2f}s"
        )
        print(f"Saved to: {DB_FILE}")

    # ── FTS staleness report ──────────────────────────────────────────────────
    fts_row = con.execute("SELECT rebuilt_at, owned_count FROM _fts_meta").fetchone()
    if fts:
        print(f"  FTS indexes rebuilt.")
    elif fts_row:
        rebuilt_at_str, fts_owned_count = fts_row
        rebuilt_dt = datetime.strptime(rebuilt_at_str, "%Y-%m-%d %H:%M:%S")
        days_ago = (datetime.now() - rebuilt_dt).days
        added = owned_count - fts_owned_count
        age_str = f"{days_ago}d ago" if days_ago > 0 else "today"
        if added > 0:
            print(f"  FTS index: last rebuilt {rebuilt_at_str} ({age_str}), "
                  f"{added} paper(s) added since — run with --fts to update")
        else:
            print(f"  FTS index: last rebuilt {rebuilt_at_str} ({age_str}), up-to-date")
    else:
        print(f"  FTS index: never built — run with --fts to enable BM25 search")


def main():
    parser = argparse.ArgumentParser(description="Build data/db/lit.duckdb from JSON database files")
    parser.add_argument("--force", action="store_true", help="Full rebuild, ignore cached state")
    parser.add_argument("--fts", action="store_true", help="Also rebuild FTS indexes (slower, needed for BM25 search)")
    args = parser.parse_args()

    ext_dir = ROOT / ".duckdb_extensions"
    ext_dir.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_FILE))
    con.execute(f"SET extension_directory = '{ext_dir}'")
    try:
        build_db(con, force=args.force, fts=args.fts)
    finally:
        con.close()


if __name__ == "__main__":
    main()
