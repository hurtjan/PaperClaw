#!/usr/bin/env python3
"""
Detect potential duplicate papers in data/db/papers.json using
shared authors and shared citation signals.

Usage:
  .venv/bin/python3 scripts/build/find_duplicates.py [--threshold N] [--limit N] [--json]
  --threshold   Minimum combined score to report (default: 4.0)
  --limit       Max groups to output (default: 50)
  --json        JSON-only output (no human-readable summary)

Output: data/tmp/duplicate_candidates.json
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import normalize_author_lastname, export_json

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.json"
TXT_OUTPUT_FILE = ROOT / "data" / "tmp" / "duplicate_candidates.txt"

# Type priority for canonical selection: higher = preferred
TYPE_PRIORITY = {"owned": 3, "external_owned": 2, "stub": 1}


def get_author_lastname_set(paper: dict) -> set:
    """Get set of primary normalized lastnames for all authors of a paper."""
    result = set()
    for author in paper.get("authors", []):
        variants = normalize_author_lastname(str(author))
        if variants:
            result.add(variants[0])
    return result


def get_first_author_key(paper: dict) -> str | None:
    """Get first author's primary normalized lastname for bucketing."""
    authors = paper.get("authors", [])
    if not authors:
        return None
    variants = normalize_author_lastname(str(authors[0]))
    return variants[0] if variants else None


def compute_author_jaccard(set_a: set, set_b: set) -> float:
    """Compute Jaccard similarity between two author lastname sets."""
    if not set_a or not set_b:
        return 0.0
    union = set_a | set_b
    return len(set_a & set_b) / len(union)


def compute_citation_jaccard(set_a: set, set_b: set) -> float:
    """Compute Jaccard similarity between two citation ID sets."""
    if not set_a or not set_b:
        return 0.0
    union = set_a | set_b
    return len(set_a & set_b) / len(union)


def score_pair(a: dict, b: dict,
               author_set_a: set, author_set_b: set) -> dict:
    """
    Score a candidate pair of papers.

    Returns dict with score, signals, shared_citers, shared_cites,
    shared_authors, and Jaccard values.
    """
    score = 0.0
    signals = []

    # --- Author scoring ---
    shared_authors = author_set_a & author_set_b
    author_jaccard = compute_author_jaccard(author_set_a, author_set_b)

    if author_set_a and author_set_b:
        if author_set_a == author_set_b:
            score += 4.0
            signals.append("all_authors")
        elif author_jaccard >= 0.75:
            score += 3.0
            signals.append("most_authors")
        elif author_jaccard >= 0.5 or len(shared_authors) >= 2:
            score += 2.0
            signals.append("some_authors")
        else:
            score += 1.0
            signals.append("first_author_only")

    # --- cited_by scoring ---
    cited_by_a = set(a.get("cited_by", []))
    cited_by_b = set(b.get("cited_by", []))
    shared_citers = cited_by_a & cited_by_b
    cited_by_jaccard = compute_citation_jaccard(cited_by_a, cited_by_b)

    if cited_by_a and cited_by_b:
        if cited_by_jaccard >= 0.5:
            score += 4.0
            signals.append("high_cited_by")
        elif len(shared_citers) >= 2 or cited_by_jaccard >= 0.25:
            score += 2.0
            signals.append("some_cited_by")
        elif len(shared_citers) >= 1:
            score += 1.0
            signals.append("any_cited_by")

    # --- cites scoring (skip when either has empty cites) ---
    cites_a = set(a.get("cites", []))
    cites_b = set(b.get("cites", []))
    shared_cites = cites_a & cites_b
    cites_jaccard = compute_citation_jaccard(cites_a, cites_b)

    if cites_a and cites_b:
        if cites_jaccard >= 0.5:
            score += 3.0
            signals.append("high_cites")
        elif cites_jaccard >= 0.25:
            score += 1.5
            signals.append("some_cites")

    return {
        "score": round(score, 1),
        "signals": signals,
        "shared_citers": sorted(shared_citers),
        "shared_cites": sorted(shared_cites),
        "shared_authors": sorted(shared_authors),
        "author_jaccard": round(author_jaccard, 3),
        "cited_by_jaccard": round(cited_by_jaccard, 3),
        "cites_jaccard": round(cites_jaccard, 3),
    }


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


def select_canonical(papers: dict, paper_ids: list) -> str:
    """
    Choose the canonical paper from a group.
    Preference: owned > external_owned > stub,
    then: has DOI, has abstract, more cited_by, more cites, longer title.
    """
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
        "abstract_snippet": snippet,
        "cites_count": len(paper.get("cites", [])),
        "cited_by_count": len(paper.get("cited_by", [])),
    }


def format_candidates_txt(result: dict) -> str:
    """Render duplicate groups as human-readable text for agent review."""
    lines = []
    generated = result["generated"]
    threshold = result["threshold"]
    groups = result["groups"]
    n_groups = len(groups)

    lines.append("=== DUPLICATE GROUPS ===")
    lines.append(f"# Generated: {generated} | Threshold: {threshold} | Groups: {n_groups}")

    if not groups:
        lines.append("")
        lines.append("No duplicate groups found.")
        return "\n".join(lines)

    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    for group in groups:
        group_id = group["group_id"]
        confidence = group["confidence"].upper()
        papers_in_group = group["papers"]
        pairwise = group["pairwise_scores"]
        canonical_id = group["recommended_canonical"]

        max_score = max((p["score"] for p in pairwise), default=0.0)

        lines.append("")
        lines.append(
            f"--- GROUP {group_id} ({confidence} confidence, max score: {max_score}) ---"
        )
        lines.append(f"Recommended canonical: {canonical_id}")
        lines.append("")

        paper_to_letter: dict[str, str] = {}
        for idx, paper in enumerate(papers_in_group):
            letter = letters[idx] if idx < len(letters) else str(idx + 1)
            paper_to_letter[paper["id"]] = letter

            authors_str = (
                ", ".join(str(a) for a in paper.get("authors", [])) or "(unknown)"
            )
            doi_str = paper.get("doi") or "(none)"
            year_str = str(paper.get("year", "")) if paper.get("year") else "(unknown)"
            title = paper.get("title") or "(no title)"

            lines.append(f"  Paper {letter}: {paper['id']}")
            lines.append(f"    Title: \"{title}\"")
            lines.append(f"    Authors: {authors_str}")
            lines.append(
                f"    Year: {year_str} | Type: {paper.get('type', 'unknown')} "
                f"| DOI: {doi_str}"
            )
            lines.append(
                f"    Cites: {paper.get('cites_count', 0)} "
                f"| Cited by: {paper.get('cited_by_count', 0)}"
            )
            lines.append("")

        for pw in pairwise:
            a_letter = paper_to_letter.get(pw["a"], pw["a"])
            b_letter = paper_to_letter.get(pw["b"], pw["b"])
            signals = ", ".join(pw["signals"]) if pw["signals"] else "(none)"
            lines.append(
                f"  Pairwise: {a_letter}-{b_letter} score={pw['score']} "
                f"signals=[{signals}]"
            )
            if pw["shared_authors"]:
                lines.append(f"    Shared authors: {', '.join(pw['shared_authors'])}")
            if pw["shared_citers"]:
                lines.append(f"    Shared citers: {', '.join(pw['shared_citers'])}")
            if pw["shared_cites"]:
                lines.append(f"    Shared cites: {', '.join(pw['shared_cites'])}")
            lines.append(
                f"    Author Jaccard: {pw['author_jaccard']:.3f} "
                f"| Cited-by Jaccard: {pw['cited_by_jaccard']:.3f}"
            )
            lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Detect duplicate papers in the DB")
    parser.add_argument("--threshold", type=float, default=4.0,
                        help="Minimum score to report (default: 4.0)")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max groups to output (default: 50)")
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

    # Build indexes: first-author bucket and author lastname sets
    author_index: dict[str, list[str]] = {}
    author_sets: dict[str, set] = {}

    for pid, paper in papers.items():
        if paper.get("superseded_by"):
            continue
        first_key = get_first_author_key(paper)
        if first_key:
            author_index.setdefault(first_key, []).append(pid)
        author_sets[pid] = get_author_lastname_set(paper)

    # Generate candidate pairs from first-author buckets
    candidate_pairs: set[frozenset] = set()

    for lastname, pids in author_index.items():
        if len(pids) < 2:
            continue
        for i in range(len(pids)):
            for j in range(i + 1, len(pids)):
                a_id, b_id = pids[i], pids[j]
                a, b = papers[a_id], papers[b_id]
                # Skip already-linked pairs
                if a.get("superseded_by") == b_id or b.get("superseded_by") == a_id:
                    continue
                if a_id in b.get("aliases", []) or b_id in a.get("aliases", []):
                    continue
                candidate_pairs.add(frozenset([a_id, b_id]))

    if not args.json:
        print(f"Candidate pairs: {len(candidate_pairs)}")

    # Score each candidate pair
    scored_pairs: list[dict] = []
    pair_score_map: dict[frozenset, dict] = {}

    for pair in candidate_pairs:
        a_id, b_id = tuple(pair)
        a, b = papers[a_id], papers[b_id]
        a_authors = author_sets.get(a_id, set())
        b_authors = author_sets.get(b_id, set())

        result = score_pair(a, b, a_authors, b_authors)
        result["a"] = a_id
        result["b"] = b_id

        if result["score"] >= args.threshold:
            scored_pairs.append(result)
            pair_score_map[pair] = result

    if not args.json:
        print(f"Pairs above threshold {args.threshold}: {len(scored_pairs)}")

    # Transitive grouping via Union-Find
    uf = UnionFind()
    for pr in scored_pairs:
        uf.union(pr["a"], pr["b"])

    groups_by_root: dict[str, set] = {}
    for pr in scored_pairs:
        root = uf.find(pr["a"])
        groups_by_root.setdefault(root, set())
        groups_by_root[root].add(pr["a"])
        groups_by_root[root].add(pr["b"])

    # Build output groups
    output_groups: list[dict] = []

    for root, pids_set in groups_by_root.items():
        pids = sorted(pids_set)
        canonical_id = select_canonical(papers, pids)

        # Collect pairwise scores within this group
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
                        "shared_citers": pr["shared_citers"],
                        "shared_cites": pr["shared_cites"],
                        "shared_authors": pr["shared_authors"],
                        "author_jaccard": pr["author_jaccard"],
                        "cited_by_jaccard": pr["cited_by_jaccard"],
                        "cites_jaccard": pr["cites_jaccard"],
                    })

        max_score = max((p["score"] for p in pairwise), default=0.0)
        confidence = "high" if max_score >= 6.0 else "medium"

        output_groups.append({
            "group_id": 0,  # renumbered below
            "confidence": confidence,
            "_max_score": max_score,  # internal sort key, removed before output
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
        "groups_found": len(output_groups),
        "groups": output_groups,
    }

    output_file = OUTPUT_FILE
    output_file.parent.mkdir(parents=True, exist_ok=True)
    export_json(result, output_file, track=False)

    # Write txt output for agent-mediated review
    txt_content = format_candidates_txt(result)
    TXT_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    TXT_OUTPUT_FILE.write_text(txt_content)

    if not args.json:
        high = sum(1 for g in output_groups if g["confidence"] == "high")
        medium = sum(1 for g in output_groups if g["confidence"] == "medium")
        print(f"\nFound {len(output_groups)} duplicate group(s) (threshold={args.threshold})")
        print(f"  High confidence:   {high}")
        print(f"  Medium confidence: {medium}")
        print(f"Output: {output_file}")
        if output_groups:
            print("NEXT: Use the Read tool to read data/tmp/duplicate_candidates.txt")
    else:
        print(json.dumps({"groups_found": len(output_groups)}, indent=2))

    sys.exit(2 if output_groups else 0)


if __name__ == "__main__":
    main()
