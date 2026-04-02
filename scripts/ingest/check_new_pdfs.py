#!/usr/bin/env python3
"""
Check which PDFs in data/pdfs/ have not yet been extracted.

Usage:
  python3 scripts/py.py scripts/ingest/check_new_pdfs.py
  python3 scripts/py.py scripts/ingest/check_new_pdfs.py -q        # filenames only
  python3 scripts/py.py scripts/ingest/check_new_pdfs.py --all      # also show processed

Exit code: 1 if unprocessed PDFs exist, 0 otherwise.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
PDF_DIR = ROOT / "data" / "pdfs"
TEXT_DIR = ROOT / "data" / "text"


def main():
    parser = argparse.ArgumentParser(description="Check for unprocessed PDFs")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    pdfs = {p.stem: p for p in sorted(PDF_DIR.glob("*.pdf"))}
    texts = {t.stem for t in TEXT_DIR.glob("*.txt")}

    new = {stem: path for stem, path in pdfs.items() if stem not in texts}
    done = {stem: path for stem, path in pdfs.items() if stem in texts}

    if args.quiet:
        for path in new.values():
            print(path.name)
        sys.exit(0 if not new else 1)

    if not pdfs:
        print("No PDFs in data/pdfs/.")
        sys.exit(0)

    if new:
        print(f"{len(new)} unprocessed PDF(s):")
        for path in new.values():
            print(f"  {path.name}")
    else:
        print("All PDFs processed.")

    if args.all and done:
        print(f"\n{len(done)} already processed:")
        for path in done.values():
            print(f"  {path.name}")

    sys.exit(0 if not new else 1)


if __name__ == "__main__":
    main()
