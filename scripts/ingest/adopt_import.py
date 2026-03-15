#!/usr/bin/env python3
"""
Promote an external_owned paper into the local corpus.

After running ingest.py on a PDF that matched an external_owned paper,
this script promotes the paper to 'owned' by updating papers.json with
the local pdf_file and text_file paths.

Since extraction files are not imported, run /extract then /link after adopting.

Usage:
  .venv/bin/python3 scripts/ingest/adopt_import.py <paper_id>
"""

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import export_json

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
STORAGE_DIR = ROOT / "data" / "pdfs"
TEXT_DIR = ROOT / "data" / "text"


def find_pdf(paper_id: str, title: str) -> Path | None:
    """Find PDF in storage by paper_id stem or title words."""
    exact = STORAGE_DIR / f"{paper_id}.pdf"
    if exact.exists():
        return exact

    # Prefix match on paper_id
    for f in STORAGE_DIR.glob("*.pdf"):
        if f.stem == paper_id or f.stem.startswith(paper_id[:30]):
            return f

    # Fallback: title word match
    if title:
        words = [w.lower() for w in title.split() if len(w) > 4][:3]
        for f in STORAGE_DIR.glob("*.pdf"):
            stem_lower = f.stem.lower()
            if len(words) >= 2 and all(w in stem_lower for w in words[:2]):
                return f

    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 adopt_import.py <paper_id>", file=sys.stderr)
        sys.exit(1)

    paper_id = sys.argv[1]
    db = json.loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    if paper_id not in papers:
        print(f"ERROR: '{paper_id}' not found in papers.json", file=sys.stderr)
        sys.exit(1)

    paper = papers[paper_id]
    if paper.get("type") != "external_owned":
        print(f"ERROR: '{paper_id}' is type '{paper.get('type')}', expected 'external_owned'",
              file=sys.stderr)
        sys.exit(1)

    source_db = paper.get("source_db", "?")
    title = paper.get("title", paper_id)
    print(f"Adopting: {title}")
    print(f"  Source DB: {source_db}")

    # Find PDF
    pdf_path = find_pdf(paper_id, title)
    if pdf_path is None:
        print(f"\nERROR: PDF not found in data/pdfs/ for '{paper_id}'", file=sys.stderr)
        print(f"  Stage the PDF and run ingest.py first.", file=sys.stderr)
        sys.exit(1)

    # Find text file (same stem as PDF)
    text_path = TEXT_DIR / f"{pdf_path.stem}.txt"
    if not text_path.exists():
        print(f"\nERROR: text file not found: {text_path}", file=sys.stderr)
        print(f"  Run ingest.py to extract text from the PDF first.", file=sys.stderr)
        sys.exit(1)

    print(f"  PDF:  data/pdfs/{pdf_path.name}")
    print(f"  Text: data/text/{text_path.name}")

    # Promote entry
    paper["type"] = "owned"
    paper["pdf_file"] = f"data/pdfs/{pdf_path.name}"
    paper["text_file"] = f"data/text/{text_path.name}"
    paper.pop("source_db", None)

    owned_count = sum(1 for p in papers.values()
                      if p.get("type") in ("owned", "external_owned"))
    stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
    db["metadata"]["last_updated"] = str(date.today())
    db["metadata"]["owned_count"] = owned_count
    db["metadata"]["stub_count"] = stub_count

    export_json(db, PAPERS_FILE,
                description=f"adopted {paper_id} from external_owned to owned")
    print(f"\nPromoted '{paper_id}' → owned")

    print(f"\nNext steps:")
    print(f"  1. /ingest   — extract and link the paper")


if __name__ == "__main__":
    main()
