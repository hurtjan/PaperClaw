#!/usr/bin/env python3
"""
Unit tests for find_matches.py internals.
Runs standalone without needing the full DB. Exits 0 on pass, 1 on failure.

Usage: .venv/bin/python3 scripts/build/test_find_matches.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "build"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from find_matches import (
    score_paper_pair,
    is_auto_match,
    select_canonical,
    UnionFind,
    get_author_lastname_set,
)

PASS = 0
FAIL = 0


def ok(name: str, condition: bool):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}")


# ---------------------------------------------------------------------------
# Test 1: score_paper_pair — DOI/S2 ID signals
# ---------------------------------------------------------------------------
print("\nTest 1: score_paper_pair — identifier signals")

paper_a = {"title": "Climate Change Effects", "doi": "10.1234/abc", "authors": ["Smith, John"],
            "year": 2020, "cites": [], "cited_by": []}
paper_b = {"title": "Climate Change Effects", "doi": "10.1234/abc", "authors": ["Smith, John"],
            "year": 2020, "cites": [], "cited_by": []}
score, signals, details = score_paper_pair(paper_a, paper_b)
ok("DOI match → doi_match signal", "doi_match" in signals)
ok("DOI match → +4.0", score >= 4.0)
ok("title_exact signal present", "title_exact" in signals)
ok("title_similarity near 1.0", details["title_similarity"] > 0.95)

paper_c = {"title": "Climate Change Effects", "s2_paper_id": "abc123",
            "authors": ["Smith, John"], "year": 2020, "cites": [], "cited_by": []}
paper_d = {"title": "Climate Change Effects", "s2_paper_id": "abc123",
            "authors": ["Smith, John"], "year": 2020, "cites": [], "cited_by": []}
score2, signals2, _ = score_paper_pair(paper_c, paper_d)
ok("S2 ID match → s2_id_match signal", "s2_id_match" in signals2)
ok("S2 ID match → +4.0", score2 >= 4.0)

# ---------------------------------------------------------------------------
# Test 2: score_paper_pair — title signals
# ---------------------------------------------------------------------------
print("\nTest 2: score_paper_pair — title signals")

paper_e = {"title": "Monetary Policy and Financial Stability", "authors": [], "cites": [], "cited_by": []}
paper_f = {"title": "Monetary Policy and Financial Stability: A Review", "authors": [], "cites": [], "cited_by": []}
score3, signals3, details3 = score_paper_pair(paper_e, paper_f)
ok("similar titles → title signal", "title_high" in signals3 or "title_mid_high" in signals3 or "title_mid" in signals3)
ok("title_similarity > 0.7", details3["title_similarity"] > 0.7)

paper_g = {"title": "Completely Different Paper About Cats", "authors": [], "cites": [], "cited_by": []}
score4, signals4, details4 = score_paper_pair(paper_e, paper_g)
ok("different titles → no title signal",
   "title_exact" not in signals4 and "title_high" not in signals4
   and "title_mid_high" not in signals4 and "title_mid" not in signals4)
ok("low title_similarity", details4["title_similarity"] < 0.5)

# ---------------------------------------------------------------------------
# Test 3: score_paper_pair — author signals
# ---------------------------------------------------------------------------
print("\nTest 3: score_paper_pair — author signals")

paper_h = {"title": "Paper H", "authors": ["Smith, John", "Jones, Mary"], "year": 2020,
            "cites": [], "cited_by": []}
paper_i = {"title": "Paper I", "authors": ["Smith, John", "Kim, Lee"], "year": 2020,
            "cites": [], "cited_by": []}
score5, signals5, details5 = score_paper_pair(paper_h, paper_i)
ok("shared first author → first_author signal", "first_author" in signals5)

paper_j = {"title": "Paper J", "authors": ["Smith, John", "Jones, Mary", "Lee, Bob"],
            "cites": [], "cited_by": []}
paper_k = {"title": "Paper K", "authors": ["Smith, John", "Jones, Mary", "Kim, Sue"],
            "cites": [], "cited_by": []}
score6, signals6, details6 = score_paper_pair(paper_j, paper_k)
ok("author_overlap with jaccard >= 0.5", "author_overlap" in signals6)
ok("author_jaccard > 0", details6["author_jaccard"] > 0)

# ---------------------------------------------------------------------------
# Test 4: score_paper_pair — citation overlap
# ---------------------------------------------------------------------------
print("\nTest 4: score_paper_pair — citation overlap")

paper_l = {"title": "Paper L", "authors": ["Smith, John"],
            "cited_by": ["x", "y", "z"], "cites": ["p", "q", "r"]}
paper_m = {"title": "Paper M", "authors": ["Smith, John"],
            "cited_by": ["x", "y", "w"], "cites": ["p", "q", "s"]}
score7, signals7, details7 = score_paper_pair(paper_l, paper_m)
ok("cited_by_overlap signal present", "cited_by_overlap" in signals7)
ok("cites_overlap_mid signal present", "cites_overlap_mid" in signals7)
ok("shared_citers has 2 entries", len(details7["shared_citers"]) == 2)
ok("shared_cites has 2 entries", len(details7["shared_cites"]) == 2)

# No citation data → no overlap signals
paper_n = {"title": "Paper N", "authors": [], "cites": [], "cited_by": []}
paper_o = {"title": "Paper O", "authors": [], "cites": [], "cited_by": []}
score8, signals8, _ = score_paper_pair(paper_n, paper_o)
ok("no citation signals when both empty",
   "cited_by_overlap" not in signals8 and "cites_overlap" not in signals8)

# ---------------------------------------------------------------------------
# Test 5: is_auto_match
# ---------------------------------------------------------------------------
print("\nTest 5: is_auto_match")

ok("DOI + title > 0.90 → auto (path 1)",
   is_auto_match(["doi_match", "title_high"], {"title_similarity": 0.95, "_title_len": 40}))
ok("S2 + title > 0.90 → auto (path 1)",
   is_auto_match(["s2_id_match", "title_high"], {"title_similarity": 0.92, "_title_len": 40}))
ok("DOI but title < 0.90 → not auto",
   not is_auto_match(["doi_match", "title_mid"], {"title_similarity": 0.85, "_title_len": 40}))
ok("no ID match, no overlap → not auto",
   not is_auto_match(["title_exact", "first_author"], {"title_similarity": 0.98, "_title_len": 40}))
ok("near-exact title + first_author + author_overlap → auto (path 2)",
   is_auto_match(["title_exact", "first_author", "author_overlap"], {"title_similarity": 0.98, "_title_len": 40}))
ok("near-exact but short title → not auto (path 2 length guard)",
   not is_auto_match(["title_exact", "first_author", "author_overlap"], {"title_similarity": 0.98, "_title_len": 20}))

# ---------------------------------------------------------------------------
# Test 6: select_canonical
# ---------------------------------------------------------------------------
print("\nTest 6: select_canonical")

papers = {
    "owned_doi": {
        "type": "owned", "doi": "10.1/x", "abstract": "...",
        "cited_by": ["a", "b"], "cites": [], "title": "Paper A",
    },
    "owned_nodoi": {
        "type": "owned", "doi": None, "abstract": "",
        "cited_by": ["a"], "cites": [], "title": "Paper B",
    },
    "external_doi": {
        "type": "external_owned", "doi": "10.1/y", "abstract": "...",
        "cited_by": ["a", "b", "c"], "cites": [], "title": "Paper C",
    },
    "stub_id": {
        "type": "stub", "doi": None, "abstract": "",
        "cited_by": ["a"], "cites": [], "title": "Paper D",
    },
}

ok("owned beats external_owned",
   select_canonical(papers, ["owned_doi", "external_doi"]) == "owned_doi")
ok("owned beats stub",
   select_canonical(papers, ["owned_nodoi", "stub_id"]) == "owned_nodoi")
ok("external_owned beats stub",
   select_canonical(papers, ["external_doi", "stub_id"]) == "external_doi")
ok("owned with doi beats owned without doi",
   select_canonical(papers, ["owned_doi", "owned_nodoi"]) == "owned_doi")

# ---------------------------------------------------------------------------
# Test 7: UnionFind transitive grouping
# ---------------------------------------------------------------------------
print("\nTest 7: UnionFind")

uf = UnionFind()
uf.union("A", "B")
uf.union("B", "C")
ok("A and B same root", uf.find("A") == uf.find("B"))
ok("B and C same root", uf.find("B") == uf.find("C"))
ok("A and C transitive", uf.find("A") == uf.find("C"))

uf2 = UnionFind()
uf2.union("X", "Y")
uf2.union("P", "Q")
ok("XY and PQ different groups", uf2.find("X") != uf2.find("P"))

uf3 = UnionFind()
ok("singleton find returns itself", uf3.find("Z") == "Z")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 40}")
print(f"Results: {PASS} passed, {FAIL} failed")
sys.exit(0 if FAIL == 0 else 1)
