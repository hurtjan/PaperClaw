#!/usr/bin/env python3
"""
Naive add: add paper extraction(s) to papers.json without fuzzy matching.

Creates the owned paper entry (or upgrades an existing stub/external_owned),
creates stubs for each citation that doesn't already exist, wires bidirectional
cites/cited_by edges, and rebuilds the index.

Supports batch mode: pass multiple extraction paths to load the DB once,
add all papers, save once, and rebuild once.

Usage: python3 scripts/py.py scripts/link/add_paper.py data/extractions/{id}.json [...]
"""

import json
import sys
import subprocess
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import normalize_doi, export_json, is_owned, fast_loads

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def add_one(ext, papers):
    """Add a single extraction to the in-memory papers dict. Returns (from_id, new_stubs, linked)."""
    from_id = ext["id"]
    print(f"Adding paper: {from_id}")

    source_file = Path(ext.get("source_file", "")).name
    ext_citations = {c.get("id", ""): c for c in ext.get("citations", []) if c.get("id")}

    # Build owned paper fields from extraction
    owned_fields = {
        "type": "owned",
        "title": ext.get("title", ""),
        "authors": ext.get("authors", []),
        "year": ext.get("year"),
        "journal": ext.get("journal", ""),
        "doi": normalize_doi(ext.get("doi")),
        "abstract": ext.get("abstract", ""),
        "pdf_file": ext.get("pdf_file") or f"data/pdfs/{Path(source_file).stem}.pdf",
        "text_file": f"data/text/{source_file}",
        "extraction_file": f"data/extractions/{from_id}.json",
    }
    if ext.get("sections"):
        owned_fields["sections"] = [
            {"heading": s.get("heading", ""), "summary": s.get("summary", "")}
            for s in ext["sections"]
        ]
    if ext.get("extraction_meta"):
        owned_fields["extraction_meta"] = ext["extraction_meta"]

    # Add or upgrade the paper entry
    if from_id not in papers:
        papers[from_id] = {"id": from_id, **owned_fields, "cites": [], "cited_by": [], "dedup_pending": True}
        print(f"  Added new owned paper")
    else:
        existing = papers[from_id]
        old_type = existing.get("type", "stub")
        existing.update(owned_fields)
        if old_type == "stub":
            print(f"  Upgraded stub -> owned")
        elif old_type == "external_owned":
            existing.pop("source_db", None)
            print(f"  Upgraded external_owned -> owned")
        else:
            print(f"  Updated existing owned paper")

    owned_entry = papers[from_id]

    # Add new stub entries for citations
    new_entries = 0
    for cit_id, cit in ext_citations.items():
        if cit_id in papers:
            continue
        papers[cit_id] = {
            "id": cit_id, "type": "stub",
            "title": cit.get("title", ""),
            "authors": cit.get("authors") or [],
            "year": cit.get("year"),
            "journal": cit.get("journal", ""),
            "doi": normalize_doi(cit.get("doi")),
            "cites": [], "cited_by": [],
            "dedup_pending": True,
        }
        new_entries += 1
    print(f"  New stub entries: {new_entries}")

    # Wire cites/cited_by edges
    linked = 0
    for cit_id in ext_citations:
        if cit_id == from_id or cit_id not in papers:
            continue
        if cit_id not in owned_entry.get("cites", []):
            owned_entry.setdefault("cites", []).append(cit_id)
        if from_id not in papers[cit_id].get("cited_by", []):
            papers[cit_id].setdefault("cited_by", []).append(from_id)
        linked += 1
    print(f"  Linked: {linked}")

    # Clean stale cited_by (edges from a previous run that are no longer in cites)
    current_cites = set(owned_entry.get("cites", []))
    for pid, p in list(papers.items()):
        if pid == from_id:
            continue
        if from_id in p.get("cited_by", []) and pid not in current_cites:
            p["cited_by"].remove(from_id)
            if p["type"] == "stub" and not p["cited_by"]:
                del papers[pid]

    return from_id, new_entries, linked


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/py.py scripts/link/add_paper.py data/extractions/{id}.json [...]")
        sys.exit(1)

    extraction_paths = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        if not p.is_absolute():
            p = ROOT / p
        extraction_paths.append(p)

    batch = len(extraction_paths) > 1
    if batch:
        print(f"Batch mode: adding {len(extraction_paths)} papers")

    if not PAPERS_FILE.exists():
        PAPERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PAPERS_FILE.write_text('{"metadata": {}, "papers": {}}')
    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    total_stubs = 0
    total_linked = 0
    added_ids = []
    for extraction_path in extraction_paths:
        ext = fast_loads(extraction_path.read_text())
        from_id, new_stubs, linked = add_one(ext, papers)
        added_ids.append(from_id)
        total_stubs += new_stubs
        total_linked += linked

    # Update metadata
    owned_count = sum(1 for p in papers.values() if is_owned(p))
    stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
    db["metadata"]["last_updated"] = str(date.today())
    db["metadata"]["owned_count"] = owned_count
    db["metadata"]["stub_count"] = stub_count

    if batch:
        desc = f"add_paper batch ({len(added_ids)}): {total_stubs} new stubs, {total_linked} citations"
    else:
        desc = f"add_paper {added_ids[0]}: {total_stubs} new stubs, {total_linked} citations"
    export_json(db, PAPERS_FILE, description=desc)
    print(f"\nUpdated papers.json: {owned_count} owned + {stub_count} stub")

    # Rebuild index (once)
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode == 0:
        print("Index rebuilt")
    else:
        print(f"ERROR: index rebuild failed: {result.stderr.strip()}", file=sys.stderr)

    # Consistency check (once)
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "check_db.py")],
        cwd=ROOT, capture_output=True, text=True
    )
    print(result.stdout.strip())
    print(f"\nDONE - {'batch' if batch else 'paper'} added successfully.")


if __name__ == "__main__":
    main()
