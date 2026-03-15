#!/usr/bin/env python3
"""
Bootstrap Stage 3: Assemble data/db/papers.json from resolved data.

Reads:  data/tmp/resolved.json, data/extractions/*.json
Writes: data/db/papers.json
"""

import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import score_match, is_owned, normalize_doi, export_json

ROOT = Path(__file__).resolve().parent.parent.parent
RESOLVED_FILE = ROOT / "data" / "tmp" / "resolved.json"
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
OUTPUT_FILE = ROOT / "data" / "db" / "papers.json"


def main():
    with open(RESOLVED_FILE) as f:
        resolved = json.load(f)

    extractions = {}
    for path in sorted(EXTRACTIONS_DIR.glob("*.json")):
        name = path.name
        if any(x in name for x in ['.contexts.', '.analysis.', '.sections.', '.refs.']):
            continue
        with open(path) as f:
            data = json.load(f)
        extractions[data["id"]] = data

    papers_db = {}
    owned_ids = set()

    for owned in resolved["owned_papers"]:
        pid = owned["id"]
        ext = extractions.get(pid, {})
        source_file = Path(ext.get("source_file", "")).name

        entry = {
            "id": pid, "type": "owned",
            "title": ext.get("title", owned.get("title", "")),
            "authors": ext.get("authors", owned.get("authors", [])),
            "year": ext.get("year", owned.get("year")),
            "journal": ext.get("journal", owned.get("journal", "")),
            "doi": normalize_doi(ext.get("doi") or owned.get("doi")),
            "abstract": ext.get("abstract", ""),
            "pdf_file": f"data/pdfs/{source_file.replace('.txt', '.pdf')}",
            "text_file": f"data/text/{source_file}",
            "extraction_file": f"data/extractions/{pid}.json",
            "cites": [], "cited_by": [],
        }
        if ext.get("sections"):
            entry["sections"] = [
                {"heading": s.get("heading", ""), "summary": s.get("summary", "")}
                for s in ext["sections"]
            ]
        if ext.get("extraction_meta"):
            entry["extraction_meta"] = ext["extraction_meta"]
        papers_db[pid] = entry
        owned_ids.add(pid)

    for cited in resolved["cited_papers"]:
        pid = cited["id"]
        if pid in papers_db:
            for from_id in cited.get("cited_by", []):
                if from_id not in papers_db[pid]["cited_by"]:
                    papers_db[pid]["cited_by"].append(from_id)
            continue
        papers_db[pid] = {
            "id": pid, "type": "stub",
            "title": cited.get("title", ""),
            "authors": cited.get("authors", []),
            "year": cited.get("year"),
            "journal": cited.get("journal", ""),
            "doi": normalize_doi(cited.get("doi")),
            "cites": [], "cited_by": list(cited.get("cited_by", [])),
        }

    # Wire cites using citation_map
    citation_map = resolved.get("citation_map", {})
    for from_id, id_mapping in citation_map.items():
        if from_id not in papers_db:
            continue
        for agent_id, canonical_id in id_mapping.items():
            if canonical_id not in papers_db or canonical_id == from_id:
                continue
            if canonical_id not in papers_db[from_id]["cites"]:
                papers_db[from_id]["cites"].append(canonical_id)
            if from_id not in papers_db[canonical_id]["cited_by"]:
                papers_db[canonical_id]["cited_by"].append(from_id)

    owned_count = sum(1 for p in papers_db.values() if is_owned(p))
    stub_count = sum(1 for p in papers_db.values() if p["type"] == "stub")

    output = {
        "metadata": {
            "last_updated": str(date.today()),
            "owned_count": owned_count,
            "stub_count": stub_count,
        },
        "papers": papers_db,
    }

    export_json(output, OUTPUT_FILE,
                description=f"build papers.json: {owned_count} owned + {stub_count} stubs")

    print(f"Built papers.json: {owned_count} owned + {stub_count} stub = {len(papers_db)} total")


if __name__ == "__main__":
    main()
