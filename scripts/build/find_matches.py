#!/usr/bin/env python3
"""
Unified paper matching: detect duplicates across the entire DB.

Contains all matching logic. Replaces find_duplicates.py,
find_merge_candidates.py, and the matching parts of link_paper.py.

Scoring function score_paper_pair() and candidate helpers
find_candidates_indexed() / find_candidates() are importable by other scripts.

Usage:
  .venv/bin/python3 scripts/build/find_matches.py [--threshold N] [--limit N] [--max-group-size N]
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

from rapidfuzz.fuzz import ratio as rapidfuzz_ratio

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import (
    PaperIndex, normalize_doi, normalize_author_lastname,
    export_json, is_owned,
    _get_first_lastname, _get_title_text, _title_prefix_fp, _raw_title_prefix,
)

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.json"
TXT_OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.txt"
AUTO_MERGE_PLAN_FILE = ROOT / "data" / "tmp" / "auto_merge_plan.json"

MAX_TXT_SIZE = 60 * 1024  # 60KB per TXT file
MAX_PAIRWISE_TXT = 20  # max pairwise entries per group in TXT output

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

    # Title similarity
    a_title = _get_title_text(a)
    b_title = _get_title_text(b)
    title_sim = 0.0
    if a_title and b_title:
        ratio = rapidfuzz_ratio(a_title, b_title) / 100.0
        title_sim = ratio
        if ratio >= 0.90:
            score += 2.0
            signals.append("title_high")
        elif ratio >= 0.70:
            score += 1.0
            signals.append("title_mid")

        # Title prefix (+0.5, only if not title_high)
        if "title_high" not in signals:
            a_fp = _title_prefix_fp(a_title)
            b_fp = _title_prefix_fp(b_title)
            if len(a_fp) > 8 and a_fp == b_fp:
                score += 0.5
                signals.append("title_prefix")

    # First author match (+1.0)
    a_first = _get_first_author_key(a)
    b_first = _get_first_author_key(b)
    if a_first and b_first and a_first == b_first:
        score += 1.0
        signals.append("first_author")

    # Author overlap Jaccard (+1.5)
    a_authors = get_author_lastname_set(a)
    b_authors = get_author_lastname_set(b)
    shared_authors = set()
    author_jaccard = 0.0
    if a_authors and b_authors:
        shared_authors = a_authors & b_authors
        author_jaccard = len(shared_authors) / len(a_authors | b_authors)
        if author_jaccard >= 0.5:
            score += 1.5
            signals.append("author_overlap")

    # Year match (+0.5)
    a_yr = str(a.get("year", "")).strip()
    b_yr = str(b.get("year", "")).strip()
    if a_yr and b_yr and a_yr == b_yr:
        score += 0.5
        signals.append("year_match")

    # Cited_by overlap (+1.5)
    cited_by_a = set(a.get("cited_by", []))
    cited_by_b = set(b.get("cited_by", []))
    shared_citers = cited_by_a & cited_by_b
    cited_by_jaccard = 0.0
    if cited_by_a and cited_by_b:
        cited_by_jaccard = len(shared_citers) / len(cited_by_a | cited_by_b)
        if cited_by_jaccard >= 0.25 or len(shared_citers) >= 2:
            score += 1.5
            signals.append("cited_by_overlap")

    # Cites overlap (+1.0)
    cites_a = set(a.get("cites", []))
    cites_b = set(b.get("cites", []))
    shared_cites = cites_a & cites_b
    cites_jaccard = 0.0
    if cites_a and cites_b:
        cites_jaccard = len(shared_cites) / len(cites_a | cites_b)
        if cites_jaccard >= 0.25:
            score += 1.0
            signals.append("cites_overlap")

    details = {
        "title_similarity": round(title_sim, 3),
        "author_jaccard": round(author_jaccard, 3),
        "cited_by_jaccard": round(cited_by_jaccard, 3),
        "cites_jaccard": round(cites_jaccard, 3),
        "shared_citers": sorted(shared_citers),
        "shared_cites": sorted(shared_cites),
        "shared_authors": sorted(shared_authors),
    }

    return round(score, 1), signals, details


def is_auto_match(signals: list[str], title_similarity: float) -> bool:
    """Check if a pair qualifies for auto-merge."""
    return (
        ("s2_id_match" in signals or "doi_match" in signals)
        and title_similarity > 0.90
    )


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


def format_group_txt(group: dict) -> str:
    """Render a single duplicate group as human-readable text."""
    lines = []
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    group_id = group["group_id"]
    confidence = group["confidence"].upper()
    papers_in_group = group["papers"]
    pairwise = group["pairwise_scores"]
    canonical_id = group["recommended_canonical"]
    max_score = max((p["score"] for p in pairwise), default=0.0)

    lines.append(
        f"--- GROUP {group_id} ({confidence} confidence, max score: {max_score}) ---"
    )
    lines.append(f"Recommended canonical: {canonical_id}")
    lines.append("")

    paper_info: dict[str, dict] = {}
    paper_to_letter: dict[str, str] = {}
    for idx, paper in enumerate(papers_in_group):
        letter = letters[idx] if idx < len(letters) else str(idx + 1)
        paper_to_letter[paper["id"]] = letter
        paper_info[paper["id"]] = paper

        authors_str = (
            ", ".join(str(a) for a in paper.get("authors", [])) or "(unknown)"
        )
        doi_str = paper.get("doi") or "(none)"
        s2_str = paper.get("s2_paper_id") or "(none)"
        year_str = str(paper.get("year", "")) if paper.get("year") else "(unknown)"
        title = paper.get("title") or "(no title)"

        lines.append(f"  Paper {letter}: {paper['id']}")
        lines.append(f'    Title: "{title}"')
        lines.append(f"    Authors: {authors_str}")
        lines.append(
            f"    Year: {year_str} | Type: {paper.get('type', 'unknown')} "
            f"| DOI: {doi_str}"
        )
        lines.append(
            f"    Cites: {paper.get('cites_count', 0)} "
            f"| Cited by: {paper.get('cited_by_count', 0)}"
        )
        if s2_str != "(none)":
            lines.append(f"    S2 ID: {s2_str}")
        lines.append("")

    sorted_pairwise = sorted(pairwise, key=lambda p: -p["score"])
    shown = sorted_pairwise[:MAX_PAIRWISE_TXT]
    truncated = len(sorted_pairwise) - len(shown)

    for pw in shown:
        a_letter = paper_to_letter.get(pw["a"], pw["a"])
        b_letter = paper_to_letter.get(pw["b"], pw["b"])
        signals = ", ".join(pw["signals"]) if pw["signals"] else "(none)"
        lines.append(
            f"  Pairwise: {a_letter}-{b_letter} score={pw['score']} "
            f"signals=[{signals}]"
        )

        authors_part = (
            ", ".join(pw["shared_authors"]) if pw["shared_authors"] else "(none)"
        )

        a_info = paper_info.get(pw["a"], {})
        b_info = paper_info.get(pw["b"], {})
        a_cited_by = a_info.get("cited_by_count", 0)
        b_cited_by = b_info.get("cited_by_count", 0)

        if a_cited_by == 0:
            citers_part = f"n/a ({a_letter} has no cited_by)"
        elif b_cited_by == 0:
            citers_part = f"n/a ({b_letter} has no cited_by)"
        else:
            citers_part = str(len(pw["shared_citers"]))

        a_cites = a_info.get("cites_count", 0)
        b_cites = b_info.get("cites_count", 0)

        if a_cites == 0:
            cites_part = f"n/a ({a_letter} has no cites)"
        elif b_cites == 0:
            cites_part = f"n/a ({b_letter} has no cites)"
        else:
            cites_part = str(len(pw["shared_cites"]))

        lines.append(
            f"    Title sim: {pw.get('title_similarity', 0):.3f} "
            f"| Shared authors: {authors_part}"
        )
        lines.append(
            f"    Shared citers: {citers_part} "
            f"| Shared cites: {cites_part}"
        )
        lines.append(
            f"    Author Jaccard: {pw['author_jaccard']:.3f} "
            f"| Cited-by Jaccard: {pw['cited_by_jaccard']:.3f}"
        )
        lines.append("")

    if truncated > 0:
        lines.append(f"  ... and {truncated} more pairs (see JSON for full list)")
        lines.append("")

    return "\n".join(lines)


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
        content = (
            f"=== DUPLICATE GROUPS ===\n"
            f"# Generated: {generated} | Threshold: {threshold} | Groups: 0\n\n"
            f"No duplicate groups found.\n"
        )
        TXT_OUTPUT_FILE.write_text(content)
        return [TXT_OUTPUT_FILE]

    group_texts = [format_group_txt(g) for g in groups]

    header = (
        f"=== DUPLICATE GROUPS ===\n"
        f"# Generated: {generated} | Threshold: {threshold} | Groups: {total_groups}"
    )
    full_content = header + "\n\n" + "\n".join(group_texts)

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
        file_header = (
            f"=== DUPLICATE GROUPS (Part {part_num} of {total_parts}) ===\n"
            f"# Generated: {generated} | Threshold: {threshold} "
            f"| Groups in this file: {n_in_part} (of {total_groups} total)"
        )
        content = file_header + "\n\n" + "\n".join(part_texts)
        path = txt_dir / f"duplicate_candidates_{part_num}.txt"
        path.write_text(content)
        written.append(path)

    return written


# ---------------------------------------------------------------------------
# Candidate pair generation
# ---------------------------------------------------------------------------

def generate_candidate_pairs(papers: dict, index: PaperIndex) -> set[frozenset]:
    """Generate candidate pairs from multiple buckets (union of all)."""
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
        candidate_pairs.add(frozenset([a_id, b_id]))

    # First-author lastname buckets
    author_index: dict[str, list[str]] = {}
    for pid, paper in papers.items():
        if paper.get("superseded_by"):
            continue
        first_key = _get_first_author_key(paper)
        if first_key:
            author_index.setdefault(first_key, []).append(pid)

    for pids in author_index.values():
        if len(pids) < 2:
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
    parser.add_argument("--threshold", type=float, default=3.0,
                        help="Minimum score to report (default: 3.0)")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max groups to output (default: 50)")
    parser.add_argument("--max-group-size", type=int, default=10,
                        help="Max papers per group (default: 10)")
    parser.add_argument("--json", action="store_true",
                        help="JSON-only output (no human-readable summary)")
    args = parser.parse_args()

    if not PAPERS_FILE.exists():
        print("ERROR: data/db/papers.json not found.", file=sys.stderr)
        sys.exit(1)

    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]
    if not args.json:
        print(f"Loaded {len(papers)} papers")

    # Build index (with S2 ID support)
    active_papers = [p for p in papers.values() if not p.get("superseded_by")]
    index = PaperIndex(active_papers)

    # Generate candidate pairs from all buckets
    candidate_pairs = generate_candidate_pairs(papers, index)
    if not args.json:
        print(f"Candidate pairs: {len(candidate_pairs)}")

    # Score each candidate pair
    auto_match_pairs: list[dict] = []
    judgment_pairs: list[dict] = []
    pair_score_map: dict[frozenset, dict] = {}

    for pair in candidate_pairs:
        a_id, b_id = tuple(pair)
        a, b = papers[a_id], papers[b_id]

        score, signals, details = score_paper_pair(a, b)

        if score < args.threshold and not is_auto_match(signals, details["title_similarity"]):
            continue

        pair_data = {
            "a": a_id, "b": b_id,
            "score": score,
            "signals": signals,
            **details,
        }
        pair_score_map[pair] = pair_data

        if is_auto_match(signals, details["title_similarity"]):
            auto_match_pairs.append(pair_data)
        elif score >= args.threshold:
            judgment_pairs.append(pair_data)

    if not args.json:
        print(f"Auto-match pairs: {len(auto_match_pairs)}")
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
        confidence = "high" if max_score >= 6.0 else "medium"

        output_groups.append({
            "group_id": 0,
            "confidence": confidence,
            "_max_score": max_score,
            "recommended_canonical": canonical_id,
            "papers": [build_paper_summary(papers[pid]) for pid in pids],
            "pairwise_scores": pairwise,
        })

    # Sort: high confidence first, then by max_score descending
    output_groups.sort(key=lambda g: (g["confidence"] != "high", -g["_max_score"]))

    # Limit and renumber
    output_groups = output_groups[:args.limit]
    for i, g in enumerate(output_groups, 1):
        g["group_id"] = i
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
        print(f"\nFound {len(output_groups)} judgment group(s) (threshold={args.threshold})")
        print(f"  High confidence:   {high}")
        print(f"  Medium confidence: {medium}")
        if n_auto_merged:
            print(f"  Auto-merged:       {n_auto_merged}")
        print(f"Output: {OUTPUT_FILE}")
        if output_groups:
            print(f"FILES: {n_files}")
    else:
        print(json.dumps({"auto_merged": n_auto_merged, "groups_found": len(output_groups),
                          "files": n_files}, indent=2))

    sys.exit(2 if output_groups else 0)


if __name__ == "__main__":
    main()
