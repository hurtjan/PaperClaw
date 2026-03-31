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
    is_auto_skip,
    is_s2_mismatch,
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
# Test 3: score_paper_pair — author signals (reduced weights)
# ---------------------------------------------------------------------------
print("\nTest 3: score_paper_pair — author signals")

paper_h = {"title": "Macroeconomic Fluctuations in Open Economies", "authors": ["Smith, John", "Jones, Mary"], "year": 2020,
            "cites": [], "cited_by": []}
paper_i = {"title": "Neural Network Architectures for Image Classification", "authors": ["Smith, John", "Kim, Lee"], "year": 2020,
            "cites": [], "cited_by": []}
score5, signals5, details5 = score_paper_pair(paper_h, paper_i)
ok("shared first author → first_author signal", "first_author" in signals5)
ok("no title signal for very different titles",
   "title_exact" not in signals5 and "title_high" not in signals5
   and "title_mid_high" not in signals5 and "title_mid" not in signals5)
# First author (+0.5) + author overlap_low at ~0.33 Jaccard (+0.25) + year match (+0.5) = 1.25
ok("author-only pair scores low (< 2.0)", score5 < 2.0)

paper_j = {"title": "Bayesian Methods for Causal Inference in Economics", "authors": ["Smith, John", "Jones, Mary", "Lee, Bob"],
            "cites": [], "cited_by": []}
paper_k = {"title": "Deep Reinforcement Learning for Autonomous Vehicles", "authors": ["Smith, John", "Jones, Mary", "Kim, Sue"],
            "cites": [], "cited_by": []}
score6, signals6, details6 = score_paper_pair(paper_j, paper_k)
ok("author_overlap with jaccard >= 0.5", "author_overlap" in signals6)
ok("author_jaccard > 0", details6["author_jaccard"] > 0)
# First author (+0.5) + author overlap 0.5 Jaccard (+0.75) = 1.25, capped at 1.5
ok("author signals capped at 1.5", score6 <= 1.5)

# ---------------------------------------------------------------------------
# Test 4: score_paper_pair — citation overlap
# ---------------------------------------------------------------------------
print("\nTest 4: score_paper_pair — citation overlap")

paper_l = {"title": "Paper L", "authors": ["Smith, John"],
            "cited_by": ["x", "y", "z", "w"], "cites": ["p", "q", "r"]}
paper_m = {"title": "Paper M", "authors": ["Smith, John"],
            "cited_by": ["x", "y", "z", "v"], "cites": ["p", "q", "s"]}
score7, signals7, details7 = score_paper_pair(paper_l, paper_m)
# cited_by: 3 shared out of 5 union → Jaccard 0.6, and >= 3 shared → cited_by_overlap fires
ok("cited_by_overlap signal present", "cited_by_overlap" in signals7)
ok("cites_overlap_mid signal present", "cites_overlap_mid" in signals7)
ok("shared_citers has 3 entries", len(details7["shared_citers"]) == 3)
ok("shared_cites has 2 entries", len(details7["shared_cites"]) == 2)

# No citation data → no overlap signals
paper_n = {"title": "Paper N", "authors": [], "cites": [], "cited_by": []}
paper_o = {"title": "Paper O", "authors": [], "cites": [], "cited_by": []}
score8, signals8, _ = score_paper_pair(paper_n, paper_o)
ok("no citation signals when both empty",
   "cited_by_overlap" not in signals8 and "cites_overlap" not in signals8)

# New cites_overlap_strong tier (Jaccard >= 0.60)
refs_shared = [f"ref_{i}" for i in range(7)]
paper_cites_strong_a = {"title": "Paper CS-A", "authors": ["Smith"],
                         "cites": refs_shared + ["unique_a", "unique_b", "unique_c"], "cited_by": []}
paper_cites_strong_b = {"title": "Paper CS-B", "authors": ["Smith"],
                         "cites": refs_shared + ["unique_x", "unique_y", "unique_z"], "cited_by": []}
# 7 shared out of 13 union → Jaccard ~0.54 — should NOT trigger strong
_, sigs_cs, dets_cs = score_paper_pair(paper_cites_strong_a, paper_cites_strong_b)
ok("cites_overlap_mid at Jaccard ~0.54", "cites_overlap_mid" in sigs_cs)

refs_shared2 = [f"ref_{i}" for i in range(8)]
paper_cites_strong_c = {"title": "Paper CS-C", "authors": ["Smith"],
                         "cites": refs_shared2 + ["ua", "ub"], "cited_by": []}
paper_cites_strong_d = {"title": "Paper CS-D", "authors": ["Smith"],
                         "cites": refs_shared2 + ["ux", "uy", "uz"], "cited_by": []}
# 8 shared out of 13 union → Jaccard ~0.62 — should trigger strong
_, sigs_cs2, _ = score_paper_pair(paper_cites_strong_c, paper_cites_strong_d)
ok("cites_overlap_strong at Jaccard ~0.62", "cites_overlap_strong" in sigs_cs2)

# ---------------------------------------------------------------------------
# Test 5: is_auto_match
# ---------------------------------------------------------------------------
print("\nTest 5: is_auto_match")

# Path 1: identifier match (unconditional)
ok("DOI match → auto (unconditional)",
   is_auto_match(["doi_match", "title_high"], {"title_similarity": 0.95, "_title_len": 40, "cites_jaccard": 0.0}, 8.0))
ok("S2 match → auto (unconditional)",
   is_auto_match(["s2_id_match", "title_mid"], {"title_similarity": 0.70, "_title_len": 40, "cites_jaccard": 0.0}, 5.0))
ok("DOI match even with low title → auto",
   is_auto_match(["doi_match"], {"title_similarity": 0.50, "_title_len": 40, "cites_jaccard": 0.0}, 4.0))

# Path 2: near-exact long title (>= 40 chars) + score > 6.0
ok("title_sim 0.97 + long + score > 6 → auto (path 2)",
   is_auto_match(["title_exact", "first_author"], {"title_similarity": 0.97, "_title_len": 50, "cites_jaccard": 0.0}, 7.0))
ok("title_sim 0.97 + long but score <= 6 → not auto",
   not is_auto_match(["title_exact"], {"title_similarity": 0.97, "_title_len": 50, "cites_jaccard": 0.0}, 6.0))
ok("title_sim 0.97 but short (< 40 chars) → not auto",
   not is_auto_match(["title_exact", "first_author"], {"title_similarity": 0.97, "_title_len": 35, "cites_jaccard": 0.0}, 8.0))
ok("title_sim 0.95 (below 0.97) → not auto via path 2",
   not is_auto_match(["title_high", "first_author"], {"title_similarity": 0.95, "_title_len": 40, "cites_jaccard": 0.0}, 9.0))

# Path 3: high cites overlap + first author + score > 5.0
ok("cites_j 0.80 + first_author + score > 5 → auto (path 3)",
   is_auto_match(["first_author", "cites_overlap_high"], {"title_similarity": 0.40, "_title_len": 40, "cites_jaccard": 0.85}, 8.0))
ok("cites_j 0.80 but no first_author → not auto via path 3",
   not is_auto_match(["cites_overlap_high"], {"title_similarity": 0.40, "_title_len": 40, "cites_jaccard": 0.85}, 8.0))

# Path 4: high cites overlap + title >= 0.70
ok("cites_j 0.80 + title 0.75 → auto (path 4)",
   is_auto_match(["cites_overlap_high"], {"title_similarity": 0.75, "_title_len": 40, "cites_jaccard": 0.85}, 8.0))
ok("cites_j 0.80 + title 0.65 → not auto via path 4",
   not is_auto_match(["cites_overlap_high"], {"title_similarity": 0.65, "_title_len": 40, "cites_jaccard": 0.85}, 8.0))
ok("cites_j 0.70 (below 0.80) + title 0.75 → not auto",
   not is_auto_match([], {"title_similarity": 0.75, "_title_len": 40, "cites_jaccard": 0.70}, 8.0))

# ---------------------------------------------------------------------------
# Test 5b: is_auto_skip
# ---------------------------------------------------------------------------
print("\nTest 5b: is_auto_skip")

# Rule 1: low title + no cites + low score
ok("title 0.35, cites 0, score 3.5 → auto-skip",
   is_auto_skip(["first_author", "author_overlap_high"], {"title_similarity": 0.35, "cites_jaccard": 0.0}, 3.5))
ok("title 0.55, cites 0, score 3.5 → NOT auto-skip (title >= 0.50)",
   not is_auto_skip(["first_author"], {"title_similarity": 0.55, "cites_jaccard": 0.0}, 3.5))
ok("title 0.35, cites 0, score 4.5 → NOT auto-skip (score >= 4.0)",
   not is_auto_skip(["first_author"], {"title_similarity": 0.35, "cites_jaccard": 0.0}, 4.5))

# Rule 2: very low title + no identifiers + low cites
ok("title 0.25, no identifiers, cites 0 → auto-skip",
   is_auto_skip(["first_author"], {"title_similarity": 0.25, "cites_jaccard": 0.0}, 5.0))
ok("title 0.25, doi_match → NOT auto-skip (identifier present)",
   not is_auto_skip(["first_author", "doi_match"], {"title_similarity": 0.25, "cites_jaccard": 0.0}, 5.0))
ok("title 0.25, cites 0.6 → NOT auto-skip (cites evidence)",
   not is_auto_skip(["first_author"], {"title_similarity": 0.25, "cites_jaccard": 0.60}, 5.0))

# ---------------------------------------------------------------------------
# Test 5c: is_s2_mismatch
# ---------------------------------------------------------------------------
print("\nTest 5c: is_s2_mismatch")

ok("both have S2 but differ → mismatch",
   is_s2_mismatch({"s2_paper_id": "abc"}, {"s2_paper_id": "def"}))
ok("both have S2 and match → no mismatch",
   not is_s2_mismatch({"s2_paper_id": "abc"}, {"s2_paper_id": "abc"}))
ok("only one has S2 → no mismatch",
   not is_s2_mismatch({"s2_paper_id": "abc"}, {}))
ok("neither has S2 → no mismatch",
   not is_s2_mismatch({}, {}))

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
