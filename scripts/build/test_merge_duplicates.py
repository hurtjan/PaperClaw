#!/usr/bin/env python3
"""
Unit tests for merge_duplicates.py internals.
Uses in-memory paper dicts. Exits 0 on pass, 1 on failure.

Usage: .venv/bin/python3 scripts/build/test_merge_duplicates.py
"""

import copy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "build"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from merge_duplicates import _enrich_fields, do_merge

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


def make_papers():
    """Fresh set of test papers for each test."""
    return {
        "paper_a": {
            "id": "paper_a", "type": "owned",
            "title": "Main Paper", "authors": ["Smith, J.", "Jones, A."],
            "year": 2020, "doi": None, "abstract": None, "journal": None,
            "cites": ["paper_c"], "cited_by": ["paper_d"],
            "aliases": [],
        },
        "paper_b": {
            "id": "paper_b", "type": "stub",
            "title": "Working Paper: Main Paper", "authors": ["Smith, J.", "Jones, A."],
            "year": 2019, "doi": "10.1234/test", "abstract": "An abstract here.",
            "journal": "Preprint", "s2_paper_id": "abc123",
            "cites": ["paper_c", "paper_e"], "cited_by": ["paper_d", "paper_f"],
            "aliases": [],
        },
        "paper_c": {
            "id": "paper_c", "type": "stub",
            "title": "Reference Paper", "authors": ["Brown, B."],
            "year": 2015, "doi": None, "abstract": None,
            "cites": [], "cited_by": ["paper_a", "paper_b"],
        },
        "paper_d": {
            "id": "paper_d", "type": "owned",
            "title": "Citing Paper D", "authors": ["Lee, C."],
            "year": 2021, "doi": None, "abstract": None,
            "cites": ["paper_a", "paper_b"], "cited_by": [],
        },
        "paper_e": {
            "id": "paper_e", "type": "stub",
            "title": "Another Reference", "authors": ["Kim, D."],
            "year": 2018, "doi": None, "abstract": None,
            "cites": [], "cited_by": ["paper_b"],
        },
        "paper_f": {
            "id": "paper_f", "type": "stub",
            "title": "Another Citer", "authors": ["Wang, E."],
            "year": 2022, "doi": None, "abstract": None,
            "cites": ["paper_b"], "cited_by": [],
        },
    }


# ---------------------------------------------------------------------------
# Test 1: _enrich_fields
# ---------------------------------------------------------------------------
print("\nTest 1: _enrich_fields")

canonical = {
    "id": "a", "title": "My Title", "doi": None, "abstract": None,
    "year": 2020, "authors": [], "journal": None, "s2_paper_id": None,
}
alias = {
    "doi": "10.1/x", "abstract": "Abstract text.", "year": 2019,
    "journal": "Nature", "s2_paper_id": "xyz", "title": "Other Title",
}
_enrich_fields(canonical, alias)
ok("doi filled from alias", canonical.get("doi") == "10.1/x")
ok("abstract filled from alias", canonical.get("abstract") == "Abstract text.")
ok("year not overwritten (existing 2020)", canonical.get("year") == 2020)
ok("journal filled from alias", canonical.get("journal") == "Nature")
ok("s2_paper_id filled from alias", canonical.get("s2_paper_id") == "xyz")
ok("title not overwritten", canonical.get("title") == "My Title")

# Empty list is treated as missing
canonical2 = {"id": "b", "authors": []}
alias2 = {"authors": ["Smith, J.", "Jones, A."]}
_enrich_fields(canonical2, alias2)
ok("empty list filled from alias", canonical2.get("authors") == ["Smith, J.", "Jones, A."])

# dry_run=True returns field list but doesn't modify
canonical3 = {"id": "c", "doi": None}
alias3 = {"doi": "10.2/y"}
fields = _enrich_fields(canonical3, alias3, dry_run=True)
ok("dry_run returns field list", "doi" in fields)
ok("dry_run does not modify canonical", canonical3.get("doi") is None)

# ---------------------------------------------------------------------------
# Test 2: Graph edge merge
# ---------------------------------------------------------------------------
print("\nTest 2: Graph edge merge")

papers2 = make_papers()
summary2 = do_merge(papers2, "paper_a", ["paper_b"])

ok("cites includes paper_e (from paper_b)",
   "paper_e" in papers2["paper_a"]["cites"])
ok("cites still has paper_c",
   "paper_c" in papers2["paper_a"]["cites"])
ok("cited_by includes paper_f (from paper_b)",
   "paper_f" in papers2["paper_a"]["cited_by"])
ok("cited_by still has paper_d",
   "paper_d" in papers2["paper_a"]["cited_by"])
ok("no self-reference in cites",
   "paper_a" not in papers2["paper_a"]["cites"])
ok("no alias in cites",
   "paper_b" not in papers2["paper_a"]["cites"])
ok("edges_merged > 0", summary2["edges_merged"] > 0)
ok("edges_merged == 2 (paper_e + paper_f)", summary2["edges_merged"] == 2)

# ---------------------------------------------------------------------------
# Test 3: Reference rewriting
# ---------------------------------------------------------------------------
print("\nTest 3: Reference rewriting")

papers3 = make_papers()
do_merge(papers3, "paper_a", ["paper_b"])

ok("paper_d.cites: paper_b replaced with paper_a",
   "paper_b" not in papers3["paper_d"]["cites"] and "paper_a" in papers3["paper_d"]["cites"])
ok("paper_d.cites: no duplicates (had paper_a already)",
   papers3["paper_d"]["cites"].count("paper_a") == 1)
ok("paper_f.cites: paper_b replaced with paper_a",
   "paper_b" not in papers3["paper_f"]["cites"] and "paper_a" in papers3["paper_f"]["cites"])
ok("paper_c.cited_by: paper_b replaced with paper_a",
   "paper_b" not in papers3["paper_c"]["cited_by"] and "paper_a" in papers3["paper_c"]["cited_by"])
ok("paper_c.cited_by: no duplicates (had paper_a already)",
   papers3["paper_c"]["cited_by"].count("paper_a") == 1)
ok("paper_e.cited_by: paper_b replaced with paper_a",
   "paper_b" not in papers3["paper_e"]["cited_by"] and "paper_a" in papers3["paper_e"]["cited_by"])

# ---------------------------------------------------------------------------
# Test 4: Version links
# ---------------------------------------------------------------------------
print("\nTest 4: Version links")

papers4 = make_papers()
do_merge(papers4, "paper_a", ["paper_b"])

ok("paper_b.superseded_by = paper_a",
   papers4["paper_b"].get("superseded_by") == "paper_a")
ok("paper_a.aliases contains paper_b",
   "paper_b" in papers4["paper_a"].get("aliases", []))
ok("paper_a not in its own aliases",
   "paper_a" not in papers4["paper_a"].get("aliases", []))

# ---------------------------------------------------------------------------
# Test 5: Transitive alias handling
# ---------------------------------------------------------------------------
print("\nTest 5: Transitive alias handling")

papers5 = make_papers()
# paper_b already has paper_e as a sub-alias
papers5["paper_b"]["aliases"] = ["paper_e"]
papers5["paper_e"]["superseded_by"] = "paper_b"

do_merge(papers5, "paper_a", ["paper_b"])

ok("paper_b.superseded_by = paper_a",
   papers5["paper_b"].get("superseded_by") == "paper_a")
ok("paper_a.aliases contains paper_b",
   "paper_b" in papers5["paper_a"].get("aliases", []))
ok("paper_a.aliases contains transitive alias paper_e",
   "paper_e" in papers5["paper_a"].get("aliases", []))
ok("paper_e.superseded_by = paper_a (redirected)",
   papers5["paper_e"].get("superseded_by") == "paper_a")
ok("version_links_set contains both",
   set(papers5["paper_a"]["aliases"]) >= {"paper_b", "paper_e"})

# ---------------------------------------------------------------------------
# Test 6: Enrich canonical from alias
# ---------------------------------------------------------------------------
print("\nTest 6: Enrich canonical in do_merge")

papers6 = make_papers()
# paper_a has no doi/abstract; paper_b does
do_merge(papers6, "paper_a", ["paper_b"])

ok("canonical doi filled from alias",
   papers6["paper_a"].get("doi") == "10.1234/test")
ok("canonical abstract filled from alias",
   papers6["paper_a"].get("abstract") == "An abstract here.")
ok("canonical year not overwritten (2020 stays)",
   papers6["paper_a"].get("year") == 2020)

# ---------------------------------------------------------------------------
# Test 7: Dry run — no writes
# ---------------------------------------------------------------------------
print("\nTest 7: Dry run")

papers7 = make_papers()
orig7 = copy.deepcopy(papers7)
summary7 = do_merge(papers7, "paper_a", ["paper_b"], dry_run=True)

ok("dry_run: edges_merged count > 0", summary7["edges_merged"] > 0)
ok("dry_run: version_links_set populated", len(summary7["version_links_set"]) > 0)
ok("dry_run: superseded_by NOT set",
   papers7["paper_b"].get("superseded_by") is None)
ok("dry_run: aliases NOT populated",
   "paper_b" not in papers7["paper_a"].get("aliases", []))
ok("dry_run: cites NOT modified",
   papers7["paper_a"]["cites"] == orig7["paper_a"]["cites"])
ok("dry_run: cited_by NOT modified",
   papers7["paper_a"]["cited_by"] == orig7["paper_a"]["cited_by"])
ok("dry_run: paper_d.cites NOT rewritten",
   "paper_b" in papers7["paper_d"]["cites"])
ok("dry_run: doi NOT filled on canonical",
   papers7["paper_a"].get("doi") is None)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 40}")
print(f"Results: {PASS} passed, {FAIL} failed")
sys.exit(0 if FAIL == 0 else 1)
