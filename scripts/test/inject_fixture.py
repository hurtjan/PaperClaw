#!/usr/bin/env python3
"""Inject a synthetic _v2 duplicate stub into the test DB for Phase E testing.

Reads/Writes: data/db/papers.json
Prints: "Injected: {original_id} + {v2_id}"
"""
import json
from pathlib import Path

db = json.loads(Path("data/db/papers.json").read_text())
papers = db["papers"]
stub = next(
    (p for p in papers.values()
     if p.get("type") == "stub" and p.get("cited_by") and p.get("authors")),
    None,
)
if not stub:
    print("ERROR: no suitable stub found for injection")
    raise SystemExit(1)
v2_id = stub["id"] + "_v2"
v2 = {
    "id": v2_id,
    "type": "stub",
    "title": "Working paper: " + (stub.get("title") or ""),
    "authors": list(stub.get("authors", [])),
    "year": str(int(stub.get("year") or 2020) - 1),
    "doi": None,
    "abstract": None,
    "cites": [],
    "cited_by": stub["cited_by"][:1],
}
papers[v2_id] = v2
for citing_id in v2["cited_by"]:
    if citing_id in papers and v2_id not in papers[citing_id].get("cites", []):
        papers[citing_id].setdefault("cites", []).append(v2_id)
db["metadata"]["stub_count"] = sum(1 for p in papers.values() if p["type"] == "stub")
Path("data/db/papers.json").write_text(
    json.dumps(db, indent=2, ensure_ascii=False, sort_keys=True)
)
print(f"Injected: {stub['id']} + {v2_id}")
