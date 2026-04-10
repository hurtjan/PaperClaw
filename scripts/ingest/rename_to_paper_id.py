#!/usr/bin/env python3
"""
Rename text and PDF files to match the paper ID from extraction.

After Pass 1 generates a paper_id, the text file and PDF may still have
their original filenames. This script renames them to {paper_id}.txt/.pdf
and updates the extraction JSON paths.

Usage:
  python3 scripts/py.py scripts/ingest/rename_to_paper_id.py <paper_id>
  python3 scripts/py.py scripts/ingest/rename_to_paper_id.py --all   # rename all mismatched
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import resolve_text_file

EXT_DIR = ROOT / "data" / "extractions"
TEXT_DIR = ROOT / "data" / "text"
PDF_DIR = ROOT / "data" / "pdfs"


def rename_paper(paper_id: str) -> bool:
    ext_path = EXT_DIR / f"{paper_id}.json"
    if not ext_path.exists():
        print(f"  skip: no extraction for {paper_id}", flush=True)
        return False

    data = json.loads(ext_path.read_text())
    source_file = data.get("source_file", "")
    pdf_file = data.get("pdf_file", "")

    source_stem = Path(source_file).stem if source_file else ""
    changed = False

    # Rename text file (within same stage directory)
    if source_stem and source_stem != paper_id:
        old_text = resolve_text_file(source_stem)
        if old_text is not None:
            new_text = old_text.parent / f"{paper_id}.txt"
            if not new_text.exists():
                old_text.rename(new_text)
                # Also rename part files in the same directory
                for part_file in sorted(old_text.parent.glob(f"{source_stem}.part*.txt")):
                    part_target = old_text.parent / part_file.name.replace(source_stem, paper_id, 1)
                    part_file.rename(part_target)
                data["source_file"] = f"{paper_id}.txt"
                print(f"  text: {old_text.name} -> {new_text.name}", flush=True)
                changed = True
            else:
                data["source_file"] = f"{paper_id}.txt"
                changed = True
        elif resolve_text_file(paper_id) is not None:
            data["source_file"] = f"{paper_id}.txt"
            changed = True

    # Rename PDF
    pdf_stem = Path(pdf_file).stem if pdf_file else source_stem
    if pdf_stem and pdf_stem != paper_id:
        old_pdf = PDF_DIR / f"{pdf_stem}.pdf"
        new_pdf = PDF_DIR / f"{paper_id}.pdf"
        if old_pdf.exists() and not new_pdf.exists():
            old_pdf.rename(new_pdf)
            data["pdf_file"] = f"data/pdfs/{paper_id}.pdf"
            print(f"  pdf:  {old_pdf.name} -> {new_pdf.name}", flush=True)
            changed = True
        elif new_pdf.exists():
            data["pdf_file"] = f"data/pdfs/{paper_id}.pdf"
            changed = True

    if changed:
        ext_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

    return changed


def main():
    if len(sys.argv) < 2:
        print("Usage: rename_to_paper_id.py <paper_id> | --all", file=sys.stderr)
        sys.exit(1)

    if sys.argv[1] == "--all":
        count = 0
        for ext_path in sorted(EXT_DIR.glob("*.json")):
            if any(ext_path.stem.endswith(s) for s in
                   (".refs", ".contexts", ".analysis", ".sections", ".meta")):
                continue
            paper_id = ext_path.stem
            data = json.loads(ext_path.read_text())
            source_stem = Path(data.get("source_file", "")).stem
            if source_stem and source_stem != paper_id:
                print(f"[{paper_id}]", flush=True)
                if rename_paper(paper_id):
                    count += 1
        print(f"\nRenamed {count} papers.", flush=True)
    else:
        paper_id = sys.argv[1]
        print(f"[{paper_id}]", flush=True)
        rename_paper(paper_id)


if __name__ == "__main__":
    main()
