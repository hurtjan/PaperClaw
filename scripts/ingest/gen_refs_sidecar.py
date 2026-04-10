#!/usr/bin/env python3
"""Generate refs sidecar JSON(s) from paper extraction(s).

Usage: gen_refs_sidecar.py <paper_id> [paper_id ...]

Reads:  data/extractions/{paper_id}.json
Writes: data/extractions/{paper_id}.refs.json
"""
import json
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: gen_refs_sidecar.py <paper_id> [paper_id ...]", file=sys.stderr)
    sys.exit(1)

for paper_id in sys.argv[1:]:
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
