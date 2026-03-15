#!/usr/bin/env python3
"""
Split a paper text file into page-range chunks for parallel extraction.

Usage:
  .venv/bin/python3 scripts/ingest/split_paper.py <text_file> [--max-pages N]

Prints one chunk path per line. If <= max-pages, prints original path.
"""

import re
import sys
import argparse
from pathlib import Path

PAGE_MARKER = re.compile(r'(={80}\nPAGE \d+\n={80}\n)', re.MULTILINE)


def split_pages(text: str) -> list[tuple[int, str]]:
    parts = PAGE_MARKER.split(text)
    pages = []
    i = 0
    while i < len(parts) and not PAGE_MARKER.match(parts[i]):
        i += 1
    while i + 1 < len(parts):
        separator = parts[i]
        body = parts[i + 1] if i + 1 < len(parts) else ''
        m = re.search(r'PAGE (\d+)', separator)
        page_num = int(m.group(1)) if m else len(pages) + 1
        pages.append((page_num, separator + body))
        i += 2
    return pages


def main():
    parser = argparse.ArgumentParser(description="Split paper into chunks")
    parser.add_argument('text_file')
    parser.add_argument('--max-pages', type=int, default=20)
    args = parser.parse_args()

    text_path = Path(args.text_file)
    if not text_path.exists():
        print(f'ERROR: file not found: {text_path}', file=sys.stderr)
        sys.exit(1)

    text = text_path.read_text(encoding='utf-8')
    pages = split_pages(text)

    if not pages or len(pages) <= args.max_pages:
        print(str(text_path))
        sys.exit(0)

    for i in range(0, len(pages), args.max_pages):
        chunk = pages[i:i + args.max_pages]
        chunk_text = ''.join(page_text for _, page_text in chunk)
        chunk_num = i // args.max_pages + 1
        chunk_path = text_path.parent / f'{text_path.stem}.part{chunk_num}.txt'
        chunk_path.write_text(chunk_text, encoding='utf-8')
        print(str(chunk_path))


if __name__ == '__main__':
    main()
