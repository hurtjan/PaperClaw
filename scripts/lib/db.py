#!/usr/bin/env python3
"""
DuckDB acceleration layer for heavy DB operations.

Provides SQL-backed versions of operations that are O(n²) or worse in Python:
- Candidate pair generation (bucketing with size caps)
- Batch reference rewriting (alias→canonical in one pass)
- Bidirectional edge repair (set-based via SQL)

The JSON files remain the canonical store. DuckDB is used as an acceleration
engine — data is loaded from JSON, operations run in SQL, results written back.
"""

import json
import os
import re
import tempfile
from pathlib import Path

try:
    import duckdb
except ImportError:
    duckdb = None

ROOT = Path(__file__).resolve().parent.parent.parent
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
DB_FILE = ROOT / "data" / "db" / "lit.duckdb"
TMP_DIR = ROOT / "data" / "tmp"

# Maximum bucket size for candidate pair generation
MAX_BUCKET_SIZE = 200


def _get_connection():
    """Get a connection to the persistent DuckDB file."""
    if duckdb is None:
        raise ImportError("duckdb not installed. Run: .venv/bin/pip install duckdb")
    con = duckdb.connect(str(DB_FILE))
    _ensure_changes_table(con)
    return con


def _ensure_changes_table(con):
    """Create the _changes history table if it doesn't exist."""
    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS _changes_seq START 1")
    except Exception:
        pass
    con.execute("""
        CREATE TABLE IF NOT EXISTS _changes (
            id INTEGER DEFAULT (nextval('_changes_seq')),
            timestamp TIMESTAMP DEFAULT current_timestamp,
            source VARCHAR,
            description VARCHAR,
            table_name VARCHAR,
            operation VARCHAR,
            paper_id VARCHAR,
            details VARCHAR
        )
    """)


def record_change(source: str, description: str, table_name: str = "papers",
                  operation: str = "batch", paper_id: str = "",
                  details: str = ""):
    """Record a change in the DuckDB WAL-based history."""
    try:
        con = _get_connection()
        con.execute("""
            INSERT INTO _changes (source, description, table_name, operation, paper_id, details)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [source, description, table_name, operation, paper_id, details])
        con.close()
    except Exception:
        pass  # non-critical — don't block operations if history fails


def get_change_history(limit: int = 20) -> list[dict]:
    """Retrieve recent changes from the DuckDB history."""
    try:
        con = _get_connection()
        rows = con.execute("""
            SELECT id, timestamp, source, description, table_name, operation, paper_id
            FROM _changes ORDER BY id DESC LIMIT ?
        """, [limit]).fetchall()
        con.close()
        return [
            {"id": r[0], "timestamp": str(r[1]), "source": r[2],
             "description": r[3], "table_name": r[4], "operation": r[5],
             "paper_id": r[6]}
            for r in rows
        ]
    except Exception:
        return []


def prune_change_history(keep_last: int = 500):
    """Remove old entries from the _changes table."""
    try:
        con = _get_connection()
        con.execute(f"""
            DELETE FROM _changes WHERE id NOT IN (
                SELECT id FROM _changes ORDER BY id DESC LIMIT {keep_last}
            )
        """)
        con.close()
    except Exception:
        pass


def _ensure_papers_table(con):
    """Ensure the papers table exists and is reasonably up-to-date."""
    try:
        count = con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        if count > 0:
            return True
    except Exception:
        pass
    return False


def _load_papers_into_duckdb(con, papers_data: dict):
    """Bulk-load papers dict into a DuckDB table with citation arrays."""
    con.execute("DROP TABLE IF EXISTS _accel_papers")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, dir=str(TMP_DIR)) as f:
        tmp = Path(f.name)
        for pid, p in papers_data.items():
            first_author_key = _extract_first_author_key(p)
            doi = _normalize_doi(p.get("doi"))
            s2_id = p.get("s2_paper_id") or ""
            title_prefix = _title_prefix(p)
            superseded = bool(p.get("superseded_by"))
            dedup_pending = bool(p.get("dedup_pending"))

            row = {
                "paper_id": pid,
                "type": p.get("type", ""),
                "title": p.get("title", ""),
                "year": _safe_year(p.get("year")),
                "doi": doi or "",
                "s2_paper_id": s2_id,
                "first_author_key": first_author_key or "",
                "title_prefix": title_prefix,
                "superseded": superseded,
                "dedup_pending": dedup_pending,
                "cites": json.dumps(p.get("cites", [])),
                "cited_by": json.dumps(p.get("cited_by", [])),
            }
            f.write(json.dumps(row, ensure_ascii=False) + '\n')

    try:
        con.execute(f"""
            CREATE TABLE _accel_papers AS
            SELECT
                paper_id, type, title, year, doi, s2_paper_id,
                first_author_key, title_prefix, superseded, dedup_pending,
                cites, cited_by
            FROM read_json_auto('{tmp}')
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ap_pid ON _accel_papers(paper_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ap_doi ON _accel_papers(doi)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ap_s2 ON _accel_papers(s2_paper_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ap_fak ON _accel_papers(first_author_key)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ap_tp ON _accel_papers(title_prefix)")
    finally:
        tmp.unlink(missing_ok=True)


def _extract_first_author_key(paper: dict) -> str | None:
    """Extract normalized first-author lastname for bucketing."""
    authors = paper.get("authors", [])
    if not authors:
        return None
    first = str(authors[0])
    if "," in first:
        lastname = first.split(",")[0].strip().lower()
    else:
        parts = first.split()
        lastname = parts[-1].lower() if parts else None
    if not lastname:
        return None
    lastname = re.sub(r"[^a-z]", "", lastname)
    return lastname or None


def _normalize_doi(doi) -> str | None:
    if not doi:
        return None
    doi = str(doi).strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    return doi.lower() if doi else None


def _title_prefix(paper: dict) -> str:
    """First 5 significant words of title, joined by underscore."""
    title = paper.get("title") or ""
    title = re.sub(r"[^\w\s]", " ", title.lower())
    title = re.sub(r"\s+", " ", title).strip()
    words = title.split()[:5]
    result = "_".join(words)
    return result if len(result) > 8 else ""


def _safe_year(yr) -> int | None:
    if yr is None:
        return None
    if isinstance(yr, str):
        m = re.search(r'\d{4}', yr)
        return int(m.group()) if m else None
    try:
        return int(yr)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_candidate_pairs_sql(papers_data: dict, full: bool = False,
                             skip_pairs: set | None = None) -> set[frozenset]:
    """Generate candidate pairs using DuckDB bucketing with size caps.

    Same semantics as find_matches.generate_candidate_pairs() but uses SQL
    for O(n log n) bucketing instead of O(n²) Python loops.
    """
    if duckdb is None:
        return set()

    con = duckdb.connect(":memory:")
    _load_papers_into_duckdb(con, papers_data)

    # Build pairs from multiple bucket types via SQL
    bucket_queries = []

    # 1. First-author lastname buckets (with size cap)
    bucket_queries.append(f"""
        SELECT a.paper_id AS a_id, b.paper_id AS b_id
        FROM _accel_papers a
        JOIN _accel_papers b ON a.first_author_key = b.first_author_key
            AND a.paper_id < b.paper_id
        WHERE a.first_author_key != ''
            AND NOT a.superseded AND NOT b.superseded
            AND a.first_author_key IN (
                SELECT first_author_key FROM _accel_papers
                WHERE first_author_key != '' AND NOT superseded
                GROUP BY first_author_key
                HAVING COUNT(*) BETWEEN 2 AND {MAX_BUCKET_SIZE}
            )
    """)

    # 2. DOI buckets (naturally small)
    bucket_queries.append("""
        SELECT a.paper_id AS a_id, b.paper_id AS b_id
        FROM _accel_papers a
        JOIN _accel_papers b ON a.doi = b.doi
            AND a.paper_id < b.paper_id
        WHERE a.doi != '' AND NOT a.superseded AND NOT b.superseded
    """)

    # 3. S2 paper ID buckets
    bucket_queries.append("""
        SELECT a.paper_id AS a_id, b.paper_id AS b_id
        FROM _accel_papers a
        JOIN _accel_papers b ON a.s2_paper_id = b.s2_paper_id
            AND a.paper_id < b.paper_id
        WHERE a.s2_paper_id != '' AND NOT a.superseded AND NOT b.superseded
    """)

    # 4. Title prefix buckets (with size cap)
    bucket_queries.append(f"""
        SELECT a.paper_id AS a_id, b.paper_id AS b_id
        FROM _accel_papers a
        JOIN _accel_papers b ON a.title_prefix = b.title_prefix
            AND a.paper_id < b.paper_id
        WHERE a.title_prefix != ''
            AND NOT a.superseded AND NOT b.superseded
            AND a.title_prefix IN (
                SELECT title_prefix FROM _accel_papers
                WHERE title_prefix != '' AND NOT superseded
                GROUP BY title_prefix
                HAVING COUNT(*) BETWEEN 2 AND {MAX_BUCKET_SIZE}
            )
    """)

    union_sql = " UNION ".join(f"({q})" for q in bucket_queries)

    rows = con.execute(union_sql).fetchall()
    con.close()

    _skip = skip_pairs or set()
    candidate_pairs: set[frozenset] = set()

    for a_id, b_id in rows:
        a = papers_data.get(a_id)
        b = papers_data.get(b_id)
        if not a or not b:
            continue
        if a.get("superseded_by") == b_id or b.get("superseded_by") == a_id:
            continue
        if a_id in b.get("aliases", []) or b_id in a.get("aliases", []):
            continue
        if not full and not (a.get("dedup_pending") or b.get("dedup_pending")):
            continue
        pair = frozenset([a_id, b_id])
        if pair in _skip:
            continue
        candidate_pairs.add(pair)

    return candidate_pairs


def batch_rewrite_references_sql(papers_data: dict,
                                  remap: dict[str, str]) -> int:
    """Rewrite alias→canonical references across all papers using DuckDB.

    Modifies papers_data in-place. Returns number of papers rewritten.

    Args:
        papers_data: The papers dict (modified in-place)
        remap: Mapping of old_id → new_id
    """
    if not remap or duckdb is None:
        return 0

    all_alias_ids = set(remap.keys())
    all_canonical_ids = set(remap.values())
    skip_ids = all_alias_ids | all_canonical_ids

    rewritten = 0
    for pid, paper in papers_data.items():
        if pid in skip_ids:
            continue

        needs_rewrite = False
        for c in paper.get("cites", []):
            if c in remap:
                needs_rewrite = True
                break
        if not needs_rewrite:
            for c in paper.get("cited_by", []):
                if c in remap:
                    needs_rewrite = True
                    break

        if needs_rewrite:
            rewritten += 1
            new_cites = []
            seen = set()
            for c in paper.get("cites", []):
                target = remap.get(c, c)
                if target not in seen:
                    new_cites.append(target)
                    seen.add(target)
            paper["cites"] = new_cites

            new_cited_by = []
            seen = set()
            for c in paper.get("cited_by", []):
                target = remap.get(c, c)
                if target not in seen:
                    new_cited_by.append(target)
                    seen.add(target)
            paper["cited_by"] = new_cited_by

    return rewritten


def _build_alias_remap(papers_data: dict) -> dict[str, str]:
    """Build alias→canonical remap from superseded_by links (transitive).

    Handles cycles (self-references and mutual supersession) by dropping
    cyclic entries from the remap.
    """
    remap: dict[str, str] = {}
    for pid, p in papers_data.items():
        target = p.get("superseded_by")
        if target and target in papers_data and target != pid:
            remap[pid] = target
    # Resolve transitive chains: A→B→C becomes A→C, B→C
    # Detect and break cycles by tracking visited nodes per chain
    for alias in list(remap.keys()):
        visited = {alias}
        current = remap.get(alias)
        while current and current in remap:
            if current in visited:
                # Cycle detected — remove all participants
                for node in visited:
                    remap.pop(node, None)
                break
            visited.add(current)
            current = remap.get(current)
        else:
            # No cycle — flatten chain to point directly at terminal
            if current:
                for node in visited:
                    if node in remap:
                        remap[node] = current
    return remap


def resolve_aliases_in_edges(papers_data: dict,
                             remap: dict[str, str] | None = None) -> int:
    """Rewrite alias IDs in all papers' cites/cited_by to their canonical.

    Modifies papers_data in-place. Returns number of papers whose edges changed.
    """
    if remap is None:
        remap = _build_alias_remap(papers_data)
    if not remap:
        return 0

    rewritten = 0
    for pid, p in papers_data.items():
        changed = False
        for field in ("cites", "cited_by"):
            old = p.get(field, [])
            new = []
            seen = set()
            for ref in old:
                target = remap.get(ref, ref)
                if target == pid:
                    continue  # skip self-citation
                if target not in seen:
                    new.append(target)
                    seen.add(target)
            if new != old:
                p[field] = new
                changed = True
        if changed:
            rewritten += 1
    return rewritten


def repair_bidi_sql(papers_data: dict) -> int:
    """Bidirectional edge repair using set-based operations.

    Resolves alias references first, then ensures A.cites B ↔ B.cited_by A.
    Superseded papers are excluded from bidi propagation (their edges are
    stale — the canonical holds the real edges).
    Modifies papers_data in-place. Returns number of edges added.
    """
    all_ids = set(papers_data.keys())

    # Resolve alias→canonical in all edges before repairing
    remap = _build_alias_remap(papers_data)
    if remap:
        resolve_aliases_in_edges(papers_data, remap)

    # Active papers: exclude superseded (their edges are redirected to canonical)
    superseded_ids = set(remap.keys()) if remap else set()
    active_ids = all_ids - superseded_ids

    edges_added = 0

    # Build set-based lookups (only for active papers)
    cites_sets: dict[str, set] = {}
    cited_by_sets: dict[str, set] = {}
    for pid in active_ids:
        p = papers_data[pid]
        pid_self = p.get("id", pid)
        cites_sets[pid] = {c for c in p.get("cites", []) if c in active_ids and c != pid_self}
        cited_by_sets[pid] = {c for c in p.get("cited_by", []) if c in active_ids and c != pid_self}

    # Forward: A.cites B → B.cited_by must include A
    for pid in active_ids:
        for cited_id in cites_sets[pid]:
            if pid not in cited_by_sets.get(cited_id, set()):
                cited_by_sets.setdefault(cited_id, set()).add(pid)
                edges_added += 1

    # Reverse: A in B.cited_by → A.cites must include B
    for pid in active_ids:
        for citing_id in cited_by_sets[pid]:
            if pid not in cites_sets.get(citing_id, set()):
                cites_sets.setdefault(citing_id, set()).add(pid)
                edges_added += 1

    # Write back deduplicated lists (only active papers)
    for pid in active_ids:
        p = papers_data[pid]
        p["cites"] = sorted(cites_sets.get(pid, set()))
        p["cited_by"] = sorted(cited_by_sets.get(pid, set()))

    return edges_added
