#!/usr/bin/env python3
"""
Incremental Step 3: Apply link decisions to papers.json.

Combines link_candidates.json + link_resolved.json, creates/updates paper
entries, wires cites/cited_by, and rebuilds index.

Reads:  data/tmp/link_candidates.json, data/tmp/link_resolved.json, data/extractions/{id}.json, data/db/papers.json
Writes: data/db/papers.json (updated)
"""

import json
import sys
import subprocess
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import normalize_doi, export_json, is_owned

LINK_CANDIDATES_FILE = ROOT / "data" / "tmp" / "link_candidates.json"
LINK_RESOLVED_FILE = ROOT / "data" / "tmp" / "link_resolved.json"
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def main():
    with open(LINK_CANDIDATES_FILE) as f:
        candidates = json.load(f)
    with open(LINK_RESOLVED_FILE) as f:
        resolved = json.load(f)
    with open(PAPERS_FILE) as f:
        db = json.load(f)

    papers = db["papers"]
    from_id = candidates["from_paper"]
    print(f"Integrating: {from_id}")

    ext_path = EXTRACTIONS_DIR / f"{from_id}.json"
    with open(ext_path) as f:
        ext = json.load(f)

    source_file = Path(ext.get("source_file", "")).name
    ext_citations = {c.get("id", ""): c for c in ext.get("citations", [])}

    # Build citation_map
    judgments = resolved.get("judgments", {})
    overrides = resolved.get("overrides", {})
    citation_map = {}

    for entry in candidates.get("auto_matched", []):
        cit_id = entry["citation_id"]
        if cit_id in overrides:
            val = overrides[cit_id]
            citation_map[cit_id] = cit_id if val == "new" else val
        else:
            citation_map[cit_id] = entry["candidate_id"]

    for entry in candidates.get("needs_judgment", []):
        cit_id = entry["citation_id"]
        if cit_id in judgments:
            decision = judgments[cit_id]
            citation_map[cit_id] = cit_id if decision == "new" else decision
        else:
            print(f"  WARNING: no judgment for {cit_id} — treating as new")
            citation_map[cit_id] = cit_id

    for entry in candidates.get("new_citations", []):
        citation_map[entry["citation_id"]] = entry["citation_id"]

    print(f"  Citation map: {len(citation_map)} entries")

    # Add/update owned paper
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

    if from_id not in papers:
        papers[from_id] = {"id": from_id, **owned_fields, "cites": [], "cited_by": []}
        print(f"  Added new owned paper")
    else:
        existing = papers[from_id]
        if existing.get("type") == "stub":
            existing.update(owned_fields)
            print(f"  Upgraded stub → owned")

    owned_entry = papers[from_id]

    # Add new stub entries
    new_entries = 0
    for cit_id, canonical_id in citation_map.items():
        if canonical_id in papers:
            continue
        ext_cit = ext_citations.get(cit_id, {})
        papers[canonical_id] = {
            "id": canonical_id, "type": "stub",
            "title": ext_cit.get("title", ""),
            "authors": ext_cit.get("authors", []),
            "year": ext_cit.get("year"),
            "journal": ext_cit.get("journal", ""),
            "doi": normalize_doi(ext_cit.get("doi")),
            "cites": [], "cited_by": [],
        }
        new_entries += 1
    print(f"  New stub entries: {new_entries}")

    # Wire cites/cited_by
    linked = 0
    for cit_id, canonical_id in citation_map.items():
        if canonical_id == from_id or canonical_id not in papers:
            continue
        if canonical_id not in owned_entry["cites"]:
            owned_entry["cites"].append(canonical_id)
        if from_id not in papers[canonical_id]["cited_by"]:
            papers[canonical_id]["cited_by"].append(from_id)
        linked += 1
    print(f"  Linked: {linked}")

    # Version links
    for link in resolved.get("version_links", []):
        canonical_id = link["canonical_id"]
        alias_id = link["alias_id"]
        if canonical_id not in papers or alias_id not in papers:
            continue
        canonical = papers[canonical_id]
        alias = papers[alias_id]
        canonical.setdefault("aliases", [])
        if alias_id not in canonical["aliases"]:
            canonical["aliases"].append(alias_id)
        alias["superseded_by"] = canonical_id
        for citing_id in alias.get("cited_by", []):
            if citing_id not in canonical.get("cited_by", []):
                canonical.setdefault("cited_by", []).append(citing_id)

    # Clean stale cited_by
    current_cites = set(owned_entry["cites"])
    for pid, p in list(papers.items()):
        if pid == from_id:
            continue
        if from_id in p.get("cited_by", []) and pid not in current_cites:
            p["cited_by"].remove(from_id)
            if p["type"] == "stub" and not p["cited_by"]:
                del papers[pid]

    # Update metadata
    owned_count = sum(1 for p in papers.values() if is_owned(p))
    stub_count = sum(1 for p in papers.values() if p["type"] == "stub")
    db["metadata"]["last_updated"] = str(date.today())
    db["metadata"]["owned_count"] = owned_count
    db["metadata"]["stub_count"] = stub_count

    export_json(db, PAPERS_FILE,
                description=f"linked {from_id}: {new_entries} new stubs, {linked} citations")
    print(f"\nUpdated papers.json: {owned_count} owned + {stub_count} stub")

    # Rebuild index
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode == 0:
        print("Index rebuilt")
    else:
        print(f"ERROR: index rebuild failed: {result.stderr.strip()}", file=sys.stderr)

    # Consistency check
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "check_db.py")],
        cwd=ROOT, capture_output=True, text=True
    )
    print(result.stdout.strip())


if __name__ == "__main__":
    main()
