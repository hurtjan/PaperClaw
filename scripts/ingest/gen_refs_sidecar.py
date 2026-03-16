#!/usr/bin/env python3
"""Generate a refs sidecar JSON from a paper extraction.

Usage: gen_refs_sidecar.py <paper_id>

Reads:  data/extractions/{paper_id}.json
Writes: data/extractions/{paper_id}.refs.json
"""
import json
import sys
from pathlib import Path

paper_id = sys.argv[1]
extraction = json.loads(Path(f"data/extractions/{paper_id}.json").read_text())
refs = [
    {
        "id": c["id"],
        "citation_key": c.get("citation_key", ""),
        "title": c.get("title", ""),
        "authors": c.get("authors", ""),
        "year": c.get("year", ""),
    }
    for c in extraction["citations"]
]
Path(f"data/extractions/{paper_id}.refs.json").write_text(json.dumps(refs, indent=2))
