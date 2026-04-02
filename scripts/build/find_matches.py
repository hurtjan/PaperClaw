#!/usr/bin/env python3
"""
Unified paper matching: detect duplicates across the entire DB.

Contains all matching logic. Replaces find_duplicates.py,
find_merge_candidates.py, and the matching parts of link_paper.py.

Scoring function score_paper_pair() and candidate helpers
find_candidates_indexed() / find_candidates() are importable by other scripts.

Usage:
  python3 scripts/py.py scripts/build/find_matches.py [--threshold N] [--limit N] [--max-group-size N]
  Default threshold: 3.0

Exit codes:
  0 - no matches found above threshold
  2 - groups found (auto-merges applied, judgment groups written)
"""

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

from rapidfuzz.fuzz import ratio as rapidfuzz_ratio, token_sort_ratio as rapidfuzz_token_sort

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import (
    PaperIndex, normalize_doi, normalize_author_lastname,
    export_json, is_owned, fast_loads,
    _get_first_lastname, _get_title_text, _title_prefix_fp, _raw_title_prefix,
)
try:
    from db import find_candidate_pairs_sql
    _HAS_DUCKDB_ACCEL = True
except ImportError:
    _HAS_DUCKDB_ACCEL = False

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.json"
TXT_OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.txt"
AUTO_MERGE_PLAN_FILE = ROOT / "data" / "tmp" / "auto_merge_plan.json"

MAX_TXT_SIZE = 30 * 1024  # 30KB per TXT file (must stay under Read tool's 10K token limit)
MAX_BUCKET_SIZE = 200  # skip author-lastname buckets larger than this (O(n²) prevention)

# Type priority for canonical selection: higher = preferred
TYPE_PRIORITY = {"owned": 3, "external_owned": 2, "stub": 1}


# ---------------------------------------------------------------------------
# Author helpers (from find_duplicates.py)
# ---------------------------------------------------------------------------

def get_author_lastname_set(paper: dict) -> set:
    """Get set of primary normalized lastnames for all authors of a paper."""
    result = set()
    for author in paper.get("authors", []):
        variants = normalize_author_lastname(str(author))
        if variants:
            result.add(variants[0])
    return result


def _get_first_author_key(paper: dict) -> str | None:
    """Get first author's primary normalized lastname for bucketing."""
    authors = paper.get("authors", [])
    if not authors:
        return None
    variants = normalize_author_lastname(str(authors[0]))
    return variants[0] if variants else None


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_paper_pair(a: dict, b: dict) -> tuple[float, list[str], dict]:
    """
    Score how likely two papers refer to the same work.

    Returns (score, signals, details) where details contains:
    - title_similarity, author_jaccard, cited_by_jaccard, cites_jaccard,
      shared_citers, shared_cites, shared_authors
    """
    score = 0.0
    signals = []

    # S2 ID match (+4.0)
    a_s2 = a.get("s2_paper_id")
    b_s2 = b.get("s2_paper_id")
    if a_s2 and b_s2 and a_s2 == b_s2:
        score += 4.0
        signals.append("s2_id_match")

    # DOI match (+4.0)
    a_doi = normalize_doi(a.get("doi"))
    b_doi = normalize_doi(b.get("doi"))
    if a_doi and b_doi and a_doi == b_doi:
        score += 4.0
        signals.append("doi_match")

    # Title similarity — dominant signal; academic titles are long and specific
    a_title = _get_title_text(a)
    b_title = _get_title_text(b)
    title_sim = 0.0
    title_token_sort = 0.0
    if a_title and b_title:
        raw_ratio = rapidfuzz_ratio(a_title, b_title) / 100.0
        token_sort = rapidfuzz_token_sort(a_title, b_title) / 100.0
        title_sim = max(raw_ratio, token_sort)
        title_token_sort = token_sort
        if title_sim >= 0.97:
            score += 5.0
            signals.append("title_exact")
        elif title_sim >= 0.90:
            score += 3.5
            signals.append("title_high")
        elif title_sim >= 0.80:
            score += 2.0
            signals.append("title_mid_high")
        elif title_sim >= 0.70:
            score += 1.0
            signals.append("title_mid")

        # Title prefix (+0.75, only if below title_mid_high)
        if "title_exact" not in signals and "title_high" not in signals and "title_mid_high" not in signals:
            a_fp = _title_prefix_fp(a_title)
            b_fp = _title_prefix_fp(b_title)
            if len(a_fp) > 8 and a_fp == b_fp:
                score += 0.75
                signals.append("title_prefix")

    # First author match (+0.5)
    a_first = _get_first_author_key(a)
    b_first = _get_first_author_key(b)
    if a_first and b_first and a_first == b_first:
        score += 0.5
        signals.append("first_author")

    # Author overlap — tiered by Jaccard + containment for partial lists
    a_authors = get_author_lastname_set(a)
    b_authors = get_author_lastname_set(b)
    shared_authors = set()
    author_jaccard = 0.0
    author_containment = 0.0
    author_pts = 0.0
    if a_authors and b_authors:
        shared_authors = a_authors & b_authors
        union_size = len(a_authors | b_authors)
        author_jaccard = len(shared_authors) / union_size if union_size else 0.0
        min_size = min(len(a_authors), len(b_authors))
        author_containment = len(shared_authors) / min_size if min_size else 0.0
        if author_jaccard >= 0.80:
            author_pts = 1.0
            signals.append("author_overlap_high")
        elif author_jaccard >= 0.50:
            author_pts = 0.75
            signals.append("author_overlap")
        elif author_jaccard >= 0.30:
            author_pts = 0.25
            signals.append("author_overlap_low")
        # Containment: stub has 1-2 authors fully contained in larger list
        if author_pts < 0.5 and min_size <= 2 and max(len(a_authors), len(b_authors)) >= 3 and author_containment >= 1.0:
            author_pts = 0.5
            signals.append("author_contained")
        score += author_pts

    # Cap total author signals at +1.5
    author_total = author_pts + (0.5 if "first_author" in signals else 0.0)
    if author_total > 1.5:
        score -= (author_total - 1.5)

    # Year proximity
    a_yr = str(a.get("year", "")).strip()
    b_yr = str(b.get("year", "")).strip()
    year_diff = None
    if a_yr and b_yr and a_yr.isdigit() and b_yr.isdigit():
        year_diff = abs(int(a_yr) - int(b_yr))
        if year_diff == 0:
            score += 0.5
            signals.append("year_match")
        elif year_diff == 1:
            score += 0.3
            signals.append("year_close")
        elif year_diff == 2:
            score += 0.1
            signals.append("year_near")

    # Cited_by overlap (+0.75)
    cited_by_a = set(a.get("cited_by", []))
    cited_by_b = set(b.get("cited_by", []))
    shared_citers = cited_by_a & cited_by_b
    cited_by_jaccard = 0.0
    if cited_by_a and cited_by_b:
        cited_by_jaccard = len(shared_citers) / len(cited_by_a | cited_by_b)
        if cited_by_jaccard >= 0.30 and len(shared_citers) >= 3:
            score += 0.75
            signals.append("cited_by_overlap")

    # Cites overlap (up to +7.0 at >= 80% Jaccard)
    cites_a = set(a.get("cites", []))
    cites_b = set(b.get("cites", []))
    shared_cites = cites_a & cites_b
    cites_jaccard = 0.0
    if cites_a and cites_b:
        cites_jaccard = len(shared_cites) / len(cites_a | cites_b)
        if cites_jaccard >= 0.80:
            score += 7.0
            signals.append("cites_overlap_high")
        elif cites_jaccard >= 0.60:
            score += 5.0
            signals.append("cites_overlap_strong")
        elif cites_jaccard >= 0.50:
            score += 4.0
            signals.append("cites_overlap_mid")
        elif cites_jaccard >= 0.25:
            score += 2.0
            signals.append("cites_overlap")

    details = {
        "title_similarity": round(title_sim, 3),
        "title_token_sort": round(title_token_sort, 3),
        "author_jaccard": round(author_jaccard, 3),
        "author_containment": round(author_containment, 3),
        "year_diff": year_diff,
        "cited_by_jaccard": round(cited_by_jaccard, 3),
        "cites_jaccard": round(cites_jaccard, 3),
        "shared_citers": sorted(shared_citers),
        "shared_cites": sorted(shared_cites),
        "shared_authors": sorted(shared_authors),
        "_title_len": max(len(a_title), len(b_title)) if a_title and b_title else 0,
    }

    return round(score, 1), signals, details


def is_s2_mismatch(a: dict, b: dict) -> bool:
    """Return True if both papers have S2 IDs but they differ — never match."""
    a_s2 = a.get("s2_paper_id")
    b_s2 = b.get("s2_paper_id")
    return bool(a_s2 and b_s2 and a_s2 != b_s2)


def is_auto_match(signals: list[str], details: dict, score: float) -> bool:
    """Check if a pair qualifies for auto-merge (skips agent review).

    Path 1: DOI or S2 ID match — auto merge unconditionally
    Path 2: Near-exact long title (>=0.97 sim, >=40 chars) + corroborating score
    Path 3: High cites overlap (>=0.80 Jaccard) + first author match
    Path 4: High cites overlap (>=0.80 Jaccard) + reasonable title match (>=0.70)
    """
    # Path 1: identifier match
    if "s2_id_match" in signals or "doi_match" in signals:
        return True
    title_sim = details.get("title_similarity", 0.0)
    title_len = details.get("_title_len", 0)
    cites_j = details.get("cites_jaccard", 0.0)
    # Path 2: near-exact long title + at least one corroborating signal
    if title_sim >= 0.97 and title_len >= 40 and score > 6.0:
        return True
    # Path 3: shared reference lists + first author
    if cites_j >= 0.80 and "first_author" in signals and score > 5.0:
        return True
    # Path 4: shared reference lists + reasonable title match
    if cites_j >= 0.80 and title_sim >= 0.70:
        return True
    return False


def is_auto_skip(signals: list[str], details: dict, score: float) -> bool:
    """Check if a pair can safely be auto-skipped (no agent review needed).

    These are pairs that score above threshold due to author overlap alone,
    with no corroborating title or citation evidence.
    """
    title_sim = details.get("title_similarity", 0.0)
    cites_j = details.get("cites_jaccard", 0.0)
    # Rule 1: author-only pairs — low title, no cites, low score
    if title_sim < 0.50 and cites_j == 0.0 and score < 4.0:
        return True
    # Rule 2: very different titles with no identifier/cites evidence
    if (title_sim < 0.30
            and "doi_match" not in signals
            and "s2_id_match" not in signals
            and cites_j < 0.50):
        return True
    return False


# ---------------------------------------------------------------------------
# Candidate gathering (moved from litdb.py)
# ---------------------------------------------------------------------------

def find_candidates_indexed(citation: dict, index: PaperIndex,
                            min_score: float = 1.0) -> list[dict]:
    """Score one citation against papers using pre-computed indexes."""
    candidate_ids: set[int] = set()
    candidate_papers: list[dict] = []

    def _add(p):
        pid = id(p)
        if pid not in candidate_ids:
            candidate_ids.add(pid)
            candidate_papers.append(p)

    cit_id = citation.get("id", "")
    if cit_id and cit_id in index.by_id:
        _add(index.by_id[cit_id])

    cit_doi = normalize_doi(citation.get("doi"))
    if cit_doi and cit_doi in index.by_doi:
        for p in index.by_doi[cit_doi]:
            _add(p)

    cit_s2 = citation.get("s2_paper_id")
    if cit_s2 and hasattr(index, "by_s2_id") and cit_s2 in index.by_s2_id:
        for p in index.by_s2_id[cit_s2]:
            _add(p)

    cit_ln = _get_first_lastname(citation)
    cit_yr = str(citation.get("year", "")).strip()
    if cit_ln and cit_yr and (cit_ln, cit_yr) in index.by_author_year:
        for p in index.by_author_year[(cit_ln, cit_yr)]:
            _add(p)

    cit_fp = _raw_title_prefix(citation)
    if len(cit_fp) > 8 and cit_fp in index.by_title_prefix:
        for p in index.by_title_prefix[cit_fp]:
            _add(p)

    results = []
    for p in candidate_papers:
        s, sigs, details = score_paper_pair(citation, p)
        if s >= min_score:
            results.append({
                "id": p.get("id", ""),
                "title": p.get("title", ""),
                "authors": p.get("authors", []),
                "year": p.get("year"),
                "journal": p.get("journal"),
                "doi": p.get("doi"),
                "signals": sigs,
                "title_similarity": details.get("title_similarity", 0),
                "score": s,
            })

    if not results:
        results = find_candidates(citation, index.papers, min_score)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def find_candidates(citation: dict, papers: list[dict],
                    min_score: float = 1.0) -> list[dict]:
    """Score one citation against all papers (brute-force fallback)."""
    results = []
    for paper in papers:
        s, sigs, details = score_paper_pair(citation, paper)
        if s >= min_score:
            results.append({
                "id": paper.get("id", ""),
                "title": paper.get("title", ""),
                "authors": paper.get("authors", []),
                "year": paper.get("year"),
                "journal": paper.get("journal"),
                "doi": paper.get("doi"),
                "signals": sigs,
                "title_similarity": details.get("title_similarity", 0),
                "score": s,
            })
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Union-Find (from find_duplicates.py)
# ---------------------------------------------------------------------------

class UnionFind:
    """Union-Find for transitive duplicate grouping."""

    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx


def split_oversized_group(pids_set: set, pair_score_map: dict,
                          threshold: float, max_size: int) -> list[set]:
    """Split an oversized group into smaller dense subgroups."""
    pids = sorted(pids_set)

    adj: dict[str, dict[str, float]] = {pid: {} for pid in pids}
    for i in range(len(pids)):
        for j in range(i + 1, len(pids)):
            pair = frozenset([pids[i], pids[j]])
            if pair in pair_score_map:
                score = pair_score_map[pair]["score"]
                adj[pids[i]][pids[j]] = score
                adj[pids[j]][pids[i]] = score

    subgroups = []
    remaining = set(pids)

    while remaining:
        best_pair = None
        best_score = -1
        for pid in remaining:
            for neighbor, score in adj.get(pid, {}).items():
                if neighbor in remaining and score > best_score:
                    best_score = score
                    best_pair = (pid, neighbor)

        if best_pair is None or best_score < threshold:
            break

        clique = set(best_pair)
        for candidate in sorted(remaining - clique):
            if len(clique) >= max_size:
                break
            if all(
                adj.get(member, {}).get(candidate, 0) >= threshold
                for member in clique
            ):
                clique.add(candidate)

        if len(clique) >= 2:
            subgroups.append(clique)
        remaining -= clique

    return subgroups


def select_canonical(papers: dict, paper_ids: list) -> str:
    """Choose the canonical paper from a group."""
    def rank(pid):
        p = papers[pid]
        return (
            TYPE_PRIORITY.get(p.get("type", "stub"), 0),
            1 if p.get("doi") else 0,
            1 if p.get("abstract") else 0,
            len(p.get("cited_by", [])),
            len(p.get("cites", [])),
            len(p.get("title") or ""),
        )

    return max(paper_ids, key=rank)


# ---------------------------------------------------------------------------
# Output formatting (adapted from find_duplicates.py)
# ---------------------------------------------------------------------------

def build_paper_summary(paper: dict) -> dict:
    """Build a compact summary dict for output."""
    abstract = paper.get("abstract") or ""
    snippet = abstract[:200] if abstract else None
    return {
        "id": paper.get("id", ""),
        "title": paper.get("title", ""),
        "authors": paper.get("authors", []),
        "year": paper.get("year"),
        "type": paper.get("type", ""),
        "doi": paper.get("doi"),
        "s2_paper_id": paper.get("s2_paper_id"),
        "abstract_snippet": snippet,
        "cites_count": len(paper.get("cites", [])),
        "cited_by_count": len(paper.get("cited_by", [])),
    }


def _short_authors(authors: list) -> str:
    """First author et al., or (unknown)."""
    if not authors:
        return "(unknown)"
    first = str(authors[0]).split(",")[0].strip()
    if len(authors) > 1:
        return f"{first} et al."
    return first


def format_group_txt(group: dict) -> str:
    """Render a single duplicate candidate set as a one-line entry."""
    canonical_id = group["recommended_canonical"]
    papers_in_group = group["papers"]

    parts = []
    for paper in papers_in_group:
        pid = paper["id"]
        prefix = "*" if pid == canonical_id else ""
        title = paper.get("title") or "(no title)"
        authors_str = _short_authors(paper.get("authors", []))
        parts.append(f"{prefix}{pid} | {title} | {authors_str}")

    return "  <?>  ".join(parts)


def write_txt_output(result: dict) -> list[Path]:
    """Write TXT output files, splitting if total exceeds MAX_TXT_SIZE."""
    groups = result["groups"]
    generated = result["generated"]
    threshold = result["threshold"]
    total_groups = len(groups)
    txt_dir = TXT_OUTPUT_FILE.parent

    # Clean up old numbered files from previous runs
    for old in txt_dir.glob("duplicate_candidates_*.txt"):
        old.unlink()

    txt_dir.mkdir(parents=True, exist_ok=True)

    if not groups:
        content = f"# Duplicate candidates | {generated}\n# No candidates found.\n"
        TXT_OUTPUT_FILE.write_text(content)
        return [TXT_OUTPUT_FILE]

    group_texts = [format_group_txt(g) for g in groups]

    header = f"# Duplicate candidates | {generated}"
    full_content = header + "\n" + "\n".join(group_texts)

    if len(full_content.encode("utf-8")) <= MAX_TXT_SIZE:
        TXT_OUTPUT_FILE.write_text(full_content)
        return [TXT_OUTPUT_FILE]

    # Split into multiple files
    if TXT_OUTPUT_FILE.exists():
        TXT_OUTPUT_FILE.unlink()

    bins: list[list[str]] = []
    current_bin: list[str] = []
    current_size = 0
    header_estimate = 200

    for group_text in group_texts:
        text_size = len(group_text.encode("utf-8"))
        if current_bin and current_size + text_size + header_estimate > MAX_TXT_SIZE:
            bins.append(current_bin)
            current_bin = []
            current_size = 0
        current_bin.append(group_text)
        current_size += text_size

    if current_bin:
        bins.append(current_bin)

    written: list[Path] = []
    total_parts = len(bins)
    for part_num, part_texts in enumerate(bins, 1):
        n_in_part = len(part_texts)
        file_header = f"# Duplicate candidates | {generated}"
        content = file_header + "\n" + "\n".join(part_texts)
        path = txt_dir / f"duplicate_candidates_{part_num}.txt"
        path.write_text(content)
        written.append(path)

    return written


# ---------------------------------------------------------------------------
# Candidate pair generation
# ---------------------------------------------------------------------------

def generate_candidate_pairs(papers: dict, index: PaperIndex,
                             full: bool = False,
                             skip_pairs: set | None = None) -> set[frozenset]:
    """Generate candidate pairs from multiple buckets (union of all)."""
    _skip = skip_pairs or set()
    candidate_pairs: set[frozenset] = set()

    def _add_pair(a_id: str, b_id: str):
        if a_id == b_id:
            return
        a, b = papers.get(a_id), papers.get(b_id)
        if not a or not b:
            return
        if a.get("superseded_by") == b_id or b.get("superseded_by") == a_id:
            return
        if a_id in b.get("aliases", []) or b_id in a.get("aliases", []):
            return
        if not full and not (a.get("dedup_pending") or b.get("dedup_pending")):
            return
        pair = frozenset([a_id, b_id])
        if pair in _skip:
            return
        candidate_pairs.add(pair)

    # First-author lastname buckets (capped to avoid O(n²) on common names)
    author_index: dict[str, list[str]] = {}
    for pid, paper in papers.items():
        if paper.get("superseded_by"):
            continue
        first_key = _get_first_author_key(paper)
        if first_key:
            author_index.setdefault(first_key, []).append(pid)

    for pids in author_index.values():
        if len(pids) < 2 or len(pids) > MAX_BUCKET_SIZE:
            continue
        for i in range(len(pids)):
            for j in range(i + 1, len(pids)):
                _add_pair(pids[i], pids[j])

    # DOI buckets
    for doi, doi_papers in index.by_doi.items():
        ids = [p.get("id") for p in doi_papers if p.get("id") and not papers.get(p["id"], {}).get("superseded_by")]
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                _add_pair(ids[i], ids[j])

    # S2 paper ID buckets
    if hasattr(index, "by_s2_id"):
        for s2_id, s2_papers in index.by_s2_id.items():
            ids = [p.get("id") for p in s2_papers if p.get("id") and not papers.get(p["id"], {}).get("superseded_by")]
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    _add_pair(ids[i], ids[j])

    # Title prefix buckets
    for fp, fp_papers in index.by_title_prefix.items():
        ids = [p.get("id") for p in fp_papers if p.get("id") and not papers.get(p["id"], {}).get("superseded_by")]
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                _add_pair(ids[i], ids[j])

    return candidate_pairs


# ---------------------------------------------------------------------------
# Auto-merge
# ---------------------------------------------------------------------------

def apply_auto_merges(auto_groups: list[dict]) -> int:
    """Write auto merge plan and run merge_duplicates.py. Returns merge count."""
    if not auto_groups:
        return 0

    merges = []
    for g in auto_groups:
        canonical_id = g["canonical_id"]
        alias_ids = g["alias_ids"]
        merges.append({"canonical_id": canonical_id, "alias_ids": alias_ids})

    plan = {"merges": merges}
    AUTO_MERGE_PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_MERGE_PLAN_FILE.write_text(json.dumps(plan, indent=2))

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "merge_duplicates.py"),
         "--plan", str(AUTO_MERGE_PLAN_FILE)],
        cwd=ROOT, capture_output=True, text=True,
    )
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            print(f"  [auto-merge] {line}")
    if result.returncode != 0:
        if result.stderr:
            print(f"  ERROR: {result.stderr.strip()}", file=sys.stderr)
        print("ERROR: auto-merge failed", file=sys.stderr)
        sys.exit(1)

    return len(merges)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Unified paper matching")
    parser.add_argument("--threshold", type=float, default=3.5,
                        help="Minimum score to report (default: 3.5)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max groups to output (default: 50, or 200 in --full mode)")
    parser.add_argument("--max-group-size", type=int, default=10,
                        help="Max papers per group (default: 10)")
    parser.add_argument("--json", action="store_true",
                        help="JSON-only output (no human-readable summary)")
    parser.add_argument("--full", action="store_true",
                        help="Scan all pairs, ignoring dedup_pending flag")
    parser.add_argument("--skip-file", metavar="FILE",
                        help="File of already-decided pairs to exclude (one 'id1|||id2' per line)")
    args = parser.parse_args()
    if args.limit is None:
        args.limit = 200 if args.full else 50

    if not PAPERS_FILE.exists():
        print("ERROR: data/db/papers.json not found.", file=sys.stderr)
        sys.exit(1)

    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    if not args.json:
        print(f"Loaded {len(papers)} papers")

    # Build index (with S2 ID support)
    active_papers = [p for p in papers.values() if not p.get("superseded_by")]
    index = PaperIndex(active_papers)

    # Load already-decided pairs to skip (for iterative --full scanning)
    skip_pairs: set = set()
    if args.skip_file:
        skip_path = Path(args.skip_file)
        if skip_path.exists():
            for line in skip_path.read_text().splitlines():
                line = line.strip()
                if "|||" in line:
                    a, b = line.split("|||", 1)
                    skip_pairs.add(frozenset([a.strip(), b.strip()]))
        if not args.json and skip_pairs:
            print(f"Skipping {len(skip_pairs)} already-decided pair(s)")

    # Generate candidate pairs from all buckets
    # Use DuckDB acceleration when available (SQL bucketing with size caps)
    if _HAS_DUCKDB_ACCEL:
        if not args.json:
            print("Using DuckDB-accelerated candidate generation")
        candidate_pairs = find_candidate_pairs_sql(papers, full=args.full,
                                                   skip_pairs=skip_pairs)
    else:
        candidate_pairs = generate_candidate_pairs(papers, index, full=args.full,
                                                   skip_pairs=skip_pairs)
    if not args.json:
        print(f"Candidate pairs: {len(candidate_pairs)}")

    # Score each candidate pair
    auto_match_pairs: list[dict] = []
    auto_skip_pairs: list[dict] = []
    judgment_pairs: list[dict] = []
    pair_score_map: dict[frozenset, dict] = {}

    for pair in candidate_pairs:
        a_id, b_id = tuple(pair)
        a, b = papers[a_id], papers[b_id]

        # Skip pairs where both have S2 IDs but they differ
        if is_s2_mismatch(a, b):
            continue

        score, signals, details = score_paper_pair(a, b)

        if score < args.threshold and not is_auto_match(signals, details, score):
            continue

        pair_data = {
            "a": a_id, "b": b_id,
            "score": score,
            "signals": signals,
            **details,
        }
        pair_score_map[pair] = pair_data

        if is_auto_match(signals, details, score):
            auto_match_pairs.append(pair_data)
        elif is_auto_skip(signals, details, score):
            auto_skip_pairs.append(pair_data)
        elif score >= args.threshold:
            judgment_pairs.append(pair_data)

    # Record auto-skipped pairs to skip file (for iterative --full scanning)
    if args.skip_file and auto_skip_pairs:
        with open(args.skip_file, "a") as f:
            for pr in auto_skip_pairs:
                a, b = sorted([pr["a"], pr["b"]])
                f.write(f"{a}|||{b}\n")

    if not args.json:
        print(f"Auto-match pairs: {len(auto_match_pairs)}")
        print(f"Auto-skip pairs: {len(auto_skip_pairs)}")
        print(f"Judgment pairs (>= {args.threshold}): {len(judgment_pairs)}")

    # --- Phase 1: Auto-merge ---
    auto_merged_aliases = set()
    n_auto_merged = 0

    if auto_match_pairs:
        # Group auto-match pairs via union-find
        uf = UnionFind()
        for pr in auto_match_pairs:
            uf.union(pr["a"], pr["b"])

        groups_by_root: dict[str, set] = {}
        for pr in auto_match_pairs:
            root = uf.find(pr["a"])
            groups_by_root.setdefault(root, set())
            groups_by_root[root].add(pr["a"])
            groups_by_root[root].add(pr["b"])

        auto_groups = []
        for pids_set in groups_by_root.values():
            pids = sorted(pids_set)
            canonical_id = select_canonical(papers, pids)
            alias_ids = [pid for pid in pids if pid != canonical_id]
            auto_groups.append({
                "canonical_id": canonical_id,
                "alias_ids": alias_ids,
            })
            auto_merged_aliases.update(alias_ids)

        if not args.json:
            print(f"\nAuto-merging {len(auto_groups)} group(s)...")
        n_auto_merged = apply_auto_merges(auto_groups)
        if not args.json:
            print(f"Auto-merged: {n_auto_merged} group(s)\n")

    # --- Phase 2: Judgment groups ---
    # Filter out pairs involving auto-merged aliases
    if auto_merged_aliases:
        judgment_pairs = [
            p for p in judgment_pairs
            if p["a"] not in auto_merged_aliases and p["b"] not in auto_merged_aliases
        ]

    if not judgment_pairs:
        if args.json:
            print(json.dumps({"auto_merged": n_auto_merged, "groups_found": 0, "files": 0}, indent=2))
        elif n_auto_merged:
            print(f"Auto-merged {n_auto_merged} group(s). No judgment groups remaining.")
        else:
            print("No matches found above threshold.")
        sys.exit(0)

    # Group judgment pairs via union-find
    uf = UnionFind()
    for pr in judgment_pairs:
        uf.union(pr["a"], pr["b"])

    groups_by_root: dict[str, set] = {}
    for pr in judgment_pairs:
        root = uf.find(pr["a"])
        groups_by_root.setdefault(root, set())
        groups_by_root[root].add(pr["a"])
        groups_by_root[root].add(pr["b"])

    # Split oversized groups
    final_groups: list[set] = []
    for root, pids_set in groups_by_root.items():
        if len(pids_set) > args.max_group_size:
            subgroups = split_oversized_group(
                pids_set, pair_score_map, args.threshold, args.max_group_size
            )
            final_groups.extend(subgroups)
        else:
            final_groups.append(pids_set)

    # Build output groups
    output_groups: list[dict] = []

    for pids_set in final_groups:
        pids = sorted(pids_set)
        canonical_id = select_canonical(papers, pids)

        pairwise = []
        for i in range(len(pids)):
            for j in range(i + 1, len(pids)):
                a_id, b_id = pids[i], pids[j]
                pr = pair_score_map.get(frozenset([a_id, b_id]))
                if pr:
                    pairwise.append({
                        "a": a_id, "b": b_id,
                        "score": pr["score"],
                        "signals": pr["signals"],
                        "title_similarity": pr.get("title_similarity", 0),
                        "shared_citers": pr["shared_citers"],
                        "shared_cites": pr["shared_cites"],
                        "shared_authors": pr["shared_authors"],
                        "author_jaccard": pr["author_jaccard"],
                        "cited_by_jaccard": pr["cited_by_jaccard"],
                        "cites_jaccard": pr.get("cites_jaccard", 0),
                    })

        max_score = max((p["score"] for p in pairwise), default=0.0)
        if max_score >= 7.0:
            confidence = "high"
        elif max_score >= 4.5:
            confidence = "medium"
        else:
            confidence = "low"

        output_groups.append({
            "group_id": 0,
            "confidence": confidence,
            "_max_score": max_score,
            "recommended_canonical": canonical_id,
            "papers": [build_paper_summary(papers[pid]) for pid in pids],
            "pairwise_scores": pairwise,
        })

    # Sort: high confidence first, then medium, then low; within tier by max_score descending
    _CONF_ORDER = {"high": 0, "medium": 1, "low": 2}
    output_groups.sort(key=lambda g: (_CONF_ORDER.get(g["confidence"], 9), -g["_max_score"]))

    # Limit and set group_id to canonical paper ID
    output_groups = output_groups[:args.limit]
    for g in output_groups:
        g["group_id"] = g["recommended_canonical"]
        del g["_max_score"]

    result = {
        "generated": str(date.today()),
        "threshold": args.threshold,
        "auto_merged": n_auto_merged,
        "groups_found": len(output_groups),
        "groups": output_groups,
    }

    # Write JSON
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    export_json(result, OUTPUT_FILE, track=False)

    # Write TXT
    txt_files = write_txt_output(result)
    n_files = len(txt_files)

    if not args.json:
        high = sum(1 for g in output_groups if g["confidence"] == "high")
        medium = sum(1 for g in output_groups if g["confidence"] == "medium")
        low = sum(1 for g in output_groups if g["confidence"] == "low")
        print(f"\nFound {len(output_groups)} judgment group(s) (threshold={args.threshold})")
        print(f"  High confidence:   {high}")
        print(f"  Medium confidence: {medium}")
        print(f"  Low confidence:    {low}")
        if n_auto_merged:
            print(f"  Auto-merged:       {n_auto_merged}")
        if auto_skip_pairs:
            print(f"  Auto-skipped:      {len(auto_skip_pairs)}")
        print(f"Output: {OUTPUT_FILE}")
        if output_groups:
            print(f"FILES: {n_files}")
    else:
        print(json.dumps({"auto_merged": n_auto_merged, "auto_skipped": len(auto_skip_pairs),
                          "groups_found": len(output_groups), "files": n_files}, indent=2))

    sys.exit(2 if output_groups else 0)


if __name__ == "__main__":
    main()
