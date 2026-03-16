#!/usr/bin/env python3
"""
Consistency checker for data/db/papers.json.

Checks: bidirectional cites/cited_by, dangling refs, self-citations,
required fields, metadata counts, version links, duplicates, orphans.

Usage: .venv/bin/python3 scripts/build/check_db.py [--quiet]
Exit code: 0 if clean, 1 if errors.
"""

import json
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import is_owned

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def run_checks(db: dict) -> tuple[list[str], list[str]]:
    papers = db.get("papers", {})
    metadata = db.get("metadata", {})
    errors = []
    warnings = []

    for pid, p in papers.items():
        for cited_id in p.get("cites", []):
            if cited_id in papers and pid not in papers[cited_id].get("cited_by", []):
                errors.append(f"Bidi: {pid} cites {cited_id} but not in cited_by")
        for citing_id in p.get("cited_by", []):
            if citing_id in papers and pid not in papers[citing_id].get("cites", []):
                errors.append(f"Bidi: {pid} has {citing_id} in cited_by but not in cites")

    for pid, p in papers.items():
        for cited_id in p.get("cites", []):
            if cited_id not in papers:
                errors.append(f"Dangling: {pid}.cites → {cited_id}")
        for citing_id in p.get("cited_by", []):
            if citing_id not in papers:
                errors.append(f"Dangling: {pid}.cited_by → {citing_id}")

    for pid, p in papers.items():
        if pid in p.get("cites", []) or pid in p.get("cited_by", []):
            errors.append(f"Self-citation: {pid}")

    for pid, p in papers.items():
        if p.get("type") == "owned":
            for field in ("extraction_file", "text_file"):
                if not p.get(field):
                    errors.append(f"Owned {pid} missing {field}")

    actual_owned = sum(1 for p in papers.values() if is_owned(p))
    actual_stub = sum(1 for p in papers.values() if p.get("type") == "stub")
    if metadata.get("owned_count") != actual_owned:
        errors.append(f"Metadata owned_count={metadata.get('owned_count')} actual={actual_owned}")
    if metadata.get("stub_count") != actual_stub:
        errors.append(f"Metadata stub_count={metadata.get('stub_count')} actual={actual_stub}")

    for pid, p in papers.items():
        for alias_id in p.get("aliases", []):
            if alias_id not in papers:
                errors.append(f"Version: {pid}.aliases → unknown {alias_id}")
            elif papers[alias_id].get("superseded_by") != pid:
                errors.append(f"Version mismatch: {pid}.aliases has {alias_id}")

    for pid, p in papers.items():
        cites = p.get("cites", [])
        if len(cites) != len(set(cites)):
            warnings.append(f"Duplicates in {pid}.cites")
        cited_by = p.get("cited_by", [])
        if len(cited_by) != len(set(cited_by)):
            warnings.append(f"Duplicates in {pid}.cited_by")

    for pid, p in papers.items():
        if p.get("type") == "stub" and not p.get("cited_by") and not p.get("superseded_by"):
            warnings.append(f"Orphaned: {pid}")

    # ID consistency: key must match entry's id field
    for pid, p in papers.items():
        if p.get("id") and p["id"] != pid:
            errors.append(f"ID mismatch: key={pid} id={p['id']}")

    # Valid types
    valid_types = {"owned", "external_owned", "stub"}
    for pid, p in papers.items():
        ptype = p.get("type", "")
        if ptype not in valid_types:
            errors.append(f"Invalid type: {pid} has type={ptype!r}")

    # external_owned invariants
    for pid, p in papers.items():
        if p.get("type") == "external_owned":
            if not p.get("source_db"):
                errors.append(f"External {pid} missing source_db")
            for field in ("extraction_file", "text_file"):
                if p.get(field):
                    warnings.append(f"External {pid} has local {field}")

    # Required fields for owned types
    for pid, p in papers.items():
        if is_owned(p) and not p.get("title"):
            errors.append(f"Owned {pid} missing title")

    # Stale schema fields (should not be in DB)
    stale_fields = ("author_lastnames", "title_normalized")
    for pid, p in papers.items():
        for field in stale_fields:
            if field in p:
                warnings.append(f"Stale field: {pid}.{field}")

    return errors, warnings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if not PAPERS_FILE.exists():
        PAPERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PAPERS_FILE.write_text('{"metadata": {}, "papers": {}}')

    db = json.loads(PAPERS_FILE.read_text())
    errors, warnings = run_checks(db)

    if warnings and not args.quiet:
        print(f"Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  WARN  {w}")

    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(f"  ERROR {e}")
        sys.exit(1)
    else:
        total = len(db.get("papers", {}))
        print(f"OK — {total} papers, no errors" + (f", {len(warnings)} warning(s)" if warnings else ""))


if __name__ == "__main__":
    main()
