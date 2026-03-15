#!/usr/bin/env python3
"""
Unit tests for find_duplicates.py internals.
Runs standalone without needing the full DB. Exits 0 on pass, 1 on failure.

Usage: .venv/bin/python3 scripts/build/test_find_duplicates.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "build"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from find_duplicates import (
    compute_author_jaccard,
    compute_citation_jaccard,
    score_pair,
    select_canonical,
    UnionFind,
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
# Test 1: compute_author_jaccard
# ---------------------------------------------------------------------------
print("\nTest 1: compute_author_jaccard")

ok("identical sets",
   compute_author_jaccard({"smith", "jones"}, {"smith", "jones"}) == 1.0)
ok("partial overlap (2/4 = 0.5)",
   compute_author_jaccard({"smith", "jones", "lee"}, {"smith", "jones", "kim"}) == 0.5)
ok("disjoint",
   compute_author_jaccard({"smith"}, {"jones"}) == 0.0)
ok("compound name identical",
   compute_author_jaccard({"van_der_berg"}, {"van_der_berg"}) == 1.0)
ok("empty set a",
   compute_author_jaccard(set(), {"smith"}) == 0.0)
ok("empty set b",
   compute_author_jaccard({"smith"}, set()) == 0.0)
ok("both empty",
   compute_author_jaccard(set(), set()) == 0.0)
ok("single shared (1/3)",
   abs(compute_author_jaccard({"smith", "jones"}, {"smith", "kim"}) - 1/3) < 1e-9)

# ---------------------------------------------------------------------------
# Test 2: compute_citation_jaccard
# ---------------------------------------------------------------------------
print("\nTest 2: compute_citation_jaccard")

ok("identical sets",
   compute_citation_jaccard({"a", "b", "c"}, {"a", "b", "c"}) == 1.0)
ok("partial overlap (1/3)",
   abs(compute_citation_jaccard({"a", "b"}, {"b", "c"}) - 1/3) < 1e-9)
ok("no overlap",
   compute_citation_jaccard({"a"}, {"b"}) == 0.0)
ok("both empty",
   compute_citation_jaccard(set(), set()) == 0.0)
ok("one empty",
   compute_citation_jaccard({"a"}, set()) == 0.0)
ok("single shared (1/4)",
   abs(compute_citation_jaccard({"a", "b"}, {"a", "c", "d"}) - 1/4) < 1e-9)

# ---------------------------------------------------------------------------
# Test 3: score_pair
# ---------------------------------------------------------------------------
print("\nTest 3: score_pair")

# Case A: Identical author sets + high cited_by + high cites → high score
paper_a = {"id": "a", "cited_by": ["x", "y", "z"], "cites": ["p", "q", "r"]}
paper_b = {"id": "b", "cited_by": ["x", "y", "z"], "cites": ["p", "q", "r"]}
auth_a = {"smith", "jones", "lee"}
auth_b = {"smith", "jones", "lee"}
result = score_pair(paper_a, paper_b, auth_a, auth_b)
ok("identical authors+cites → score >= 6.0", result["score"] >= 6.0)
ok("all_authors signal present", "all_authors" in result["signals"])
ok("high_cited_by signal present", "high_cited_by" in result["signals"])
ok("high_cites signal present", "high_cites" in result["signals"])
ok("shared_authors populated", len(result["shared_authors"]) == 3)
ok("shared_citers populated", sorted(result["shared_citers"]) == ["x", "y", "z"])
ok("author_jaccard == 1.0", result["author_jaccard"] == 1.0)
ok("cited_by_jaccard == 1.0", result["cited_by_jaccard"] == 1.0)

# Case B: First author only + no citations → score 1.0
paper_c = {"id": "c", "cited_by": [], "cites": []}
paper_d = {"id": "d", "cited_by": [], "cites": []}
auth_c = {"smith", "jones"}
auth_d = {"smith", "kim"}   # first author matches, others don't
result2 = score_pair(paper_c, paper_d, auth_c, auth_d)
ok("first_author_only + no cites → score 1.0", result2["score"] == 1.0)
ok("first_author_only signal", "first_author_only" in result2["signals"])
ok("no cited_by signals when both empty",
   not any(s in result2["signals"] for s in ["high_cited_by", "some_cited_by", "any_cited_by"]))
ok("no cites signals when both empty",
   not any(s in result2["signals"] for s in ["high_cites", "some_cites"]))

# Case C: some_authors (jaccard=0.5) + any_cited_by (1 shared out of many)
# need large cited_by sets so jaccard < 0.25 but len(shared) == 1
paper_e = {"id": "e", "cited_by": ["x", "y", "z", "w"], "cites": []}
paper_f = {"id": "f", "cited_by": ["x", "a", "b", "c", "d"], "cites": []}
auth_e = {"smith", "jones", "lee"}
auth_f = {"smith", "jones", "kim"}   # jaccard = 2/4 = 0.5
result3 = score_pair(paper_e, paper_f, auth_e, auth_f)
ok("some_authors signal (jaccard=0.5)", "some_authors" in result3["signals"])
ok("any_cited_by signal (1 shared, jaccard 1/8)", "any_cited_by" in result3["signals"])
ok("score == 3.0 (some_authors + any_cited_by)", result3["score"] == 3.0)

# Case D: most_authors (jaccard=0.75)
auth_g = {"smith", "jones", "lee", "kim"}
auth_h = {"smith", "jones", "lee", "wang"}   # 3/5 = 0.6... wait
# 3 shared, 5 union → 0.6. For 0.75 need e.g. 3/4:
auth_g2 = {"smith", "jones", "lee", "kim"}
auth_h2 = {"smith", "jones", "lee"}   # jaccard = 3/4 = 0.75
paper_g = {"id": "g", "cited_by": [], "cites": []}
result4 = score_pair(paper_g, paper_g, auth_g2, auth_h2)
ok("most_authors signal (jaccard=0.75)", "most_authors" in result4["signals"])
ok("most_authors score 3.0", result4["score"] == 3.0)

# Case E: empty author sets → no author signal
result5 = score_pair(paper_c, paper_d, set(), set())
ok("empty author sets → score 0.0", result5["score"] == 0.0)
ok("no signals for empty authors+cites", result5["signals"] == [])

# ---------------------------------------------------------------------------
# Test 4: select_canonical
# ---------------------------------------------------------------------------
print("\nTest 4: select_canonical")

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
# Test 5: UnionFind transitive grouping
# ---------------------------------------------------------------------------
print("\nTest 5: UnionFind")

uf = UnionFind()
uf.union("A", "B")
uf.union("B", "C")
ok("A and B same root", uf.find("A") == uf.find("B"))
ok("B and C same root", uf.find("B") == uf.find("C"))
ok("A and C transitive", uf.find("A") == uf.find("C"))

uf2 = UnionFind()
uf2.union("X", "Y")
uf2.union("P", "Q")
ok("X and Y same root", uf2.find("X") == uf2.find("Y"))
ok("P and Q same root", uf2.find("P") == uf2.find("Q"))
ok("XY and PQ different groups", uf2.find("X") != uf2.find("P"))

# Single element in its own group
uf3 = UnionFind()
ok("singleton find returns itself", uf3.find("Z") == "Z")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 40}")
print(f"Results: {PASS} passed, {FAIL} failed")
sys.exit(0 if FAIL == 0 else 1)
