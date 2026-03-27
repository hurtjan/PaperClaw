#!/usr/bin/env python3
"""
Export the local PaperClaw DB as a shareable .paperclaw archive.

Creates a zip containing manifest.json, DB files, and extraction JSONs.
Recipients can import via:  /merge path/to/file.paperclaw

Usage:
  .venv/bin/python3 scripts/enrich/export_db.py [--output FILE] [--no-extractions]
"""

import argparse
import re
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
from litdb import fast_loads, fast_dumps

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
CONTEXTS_FILE = ROOT / "data" / "db" / "contexts.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
CONFIG_FILE = ROOT / "project.yaml"

SKIP_PATTERNS = [
    r'\.analysis\.json$', r'\.contexts(\.\d+)?\.json$',
    r'\.sections(\.\d+)?\.json$', r'\.refs\.json$',
]

LOCAL_ONLY_FIELDS = {"pdf_file", "text_file"}


def _is_main_extraction(filename: str) -> bool:
    return not any(re.search(p, filename) for p in SKIP_PATTERNS)


def _strip_local_fields(paper: dict) -> dict:
    return {k: v for k, v in paper.items() if k not in LOCAL_ONLY_FIELDS}


def _load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


def main():
    parser = argparse.ArgumentParser(description="Export PaperClaw DB as .paperclaw archive")
    parser.add_argument("--output", "-o", help="Output file path (default: {user}_{date}.paperclaw)")
    parser.add_argument("--no-extractions", action="store_true",
                        help="Exclude extraction JSONs (lighter export)")
    args = parser.parse_args()

    if not PAPERS_FILE.exists():
        print("ERROR: papers.json not found — nothing to export", file=sys.stderr)
        sys.exit(1)

    config = _load_config()
    user_name = config.get("user", {}).get("name", "unknown")

    # Load papers
    papers_db = fast_loads(PAPERS_FILE.read_text())
    papers = papers_db.get("papers", {})

    # Strip local-only fields from paper entries
    cleaned_papers = {pid: _strip_local_fields(p) for pid, p in papers.items()}
    cleaned_db = dict(papers_db)
    cleaned_db["papers"] = cleaned_papers

    owned_ids = {pid for pid, p in papers.items() if p.get("type") == "owned"}
    stub_ids = {pid for pid, p in papers.items() if p.get("type") == "stub"}

    # Collect extraction files
    extraction_files = []
    if not args.no_extractions and EXTRACTIONS_DIR.exists():
        extraction_files = [f for f in sorted(EXTRACTIONS_DIR.glob("*.json"))
                            if _is_main_extraction(f.name)]

    # Determine output path
    if args.output:
        out_path = Path(args.output)
    else:
        out_path = ROOT / f"{user_name}_{date.today().isoformat()}.paperclaw"

    # Build manifest
    manifest = {
        "format_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_user": user_name,
        "stats": {
            "owned_count": len(owned_ids),
            "stub_count": len(stub_ids),
            "extraction_count": len(extraction_files),
        },
        "schema_version": "2026.1",
    }

    # Write zip
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", fast_dumps(manifest))
        zf.writestr("db/papers.json", fast_dumps(cleaned_db))

        if CONTEXTS_FILE.exists():
            zf.writestr("db/contexts.json", CONTEXTS_FILE.read_text())

        if AUTHORS_FILE.exists():
            zf.writestr("db/authors.json", AUTHORS_FILE.read_text())

        for ext_file in extraction_files:
            zf.writestr(f"extractions/{ext_file.name}", ext_file.read_text())

    size_kb = out_path.stat().st_size / 1024
    print(f"Exported: {out_path.name}")
    print(f"  Size:         {size_kb:.1f} KB")
    print(f"  Owned papers: {len(owned_ids)}")
    print(f"  Stub papers:  {len(stub_ids)}")
    print(f"  Extractions:  {len(extraction_files)}")
    print(f"  Path:         {out_path}")


if __name__ == "__main__":
    main()
