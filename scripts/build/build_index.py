#!/usr/bin/env python3
"""
Build data/db/contexts.json from extractions.

Usage: .venv/bin/python3 scripts/build/build_index.py
"""

import json
import re
from datetime import date
from pathlib import Path
from collections import defaultdict

import sys
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
from litdb import export_json

EXTRACTIONS_DIR = ROOT / "data" / "extractions"
EXTERNAL_DIR = ROOT / "data" / "db_imports"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
INDEX_FILE = ROOT / "data" / "db" / "contexts.json"

SKIP_PATTERNS = [
    r'\.analysis\.json$', r'\.contexts(\.\d+)?\.json$',
    r'\.sections(\.\d+)?\.json$', r'\.refs\.json$',
]


def is_main_extraction(filename):
    return not any(re.search(p, filename) for p in SKIP_PATTERNS)


def main():
    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    by_cited = defaultdict(list)
    by_purpose = defaultdict(list)

    local = [f for f in sorted(EXTRACTIONS_DIR.glob("*.json")) if is_main_extraction(f.name)]
    external = [f for f in sorted(EXTERNAL_DIR.glob("*/extractions/*.json"))
                if is_main_extraction(f.name)] if EXTERNAL_DIR.exists() else []
    all_extractions = local + external
    print(f"Indexing {len(local)} local + {len(external)} external extractions...")

    for ext_path in all_extractions:
        ext = json.loads(ext_path.read_text())
        citing_id = ext["id"]
        for cit in ext.get("citations", []):
            cited_id = cit.get("id", "")
            for ctx in cit.get("contexts", []):
                purpose = ctx.get("purpose", "")
                entry = {
                    "citing": citing_id, "cited": cited_id,
                    "cited_title": cit.get("title", ""),
                    "purpose": purpose, "section": ctx.get("section", ""),
                    "quote": ctx.get("quote", ""),
                    "explanation": ctx.get("explanation", ""),
                }
                by_cited[cited_id].append(entry)
                by_purpose[purpose].append(entry)

    citation_counts = {pid: len(p.get("cited_by", [])) for pid, p in papers.items()}

    owned_papers = sorted([
        {
            "id": p["id"], "title": p["title"],
            "authors": p.get("authors", []), "year": p.get("year"),
            "journal": p.get("journal"), "doi": p.get("doi"),
            "cites_count": len(p.get("cites", [])),
            "cited_by_count": len(p.get("cited_by", [])),
            "cited_by": p.get("cited_by", []),
        }
        for p in papers.values()
        if p.get("type") in ("owned", "external_owned")
    ], key=lambda x: x["id"])

    index = {
        "generated": str(date.today()),
        "owned_count": len(owned_papers),
        "total_papers": len(papers),
        "owned_papers": owned_papers,
        "by_cited": dict(by_cited),
        "by_purpose": dict(by_purpose),
        "citation_counts": citation_counts,
    }

    total_contexts = sum(len(v) for v in by_cited.values())
    export_json(index, INDEX_FILE,
                description=f"build contexts.json: {len(owned_papers)} owned, {total_contexts} contexts")

    print(f"Contexts: {len(owned_papers)} owned, {total_contexts} contexts, {len(by_purpose)} purpose types")


if __name__ == "__main__":
    main()
