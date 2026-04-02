#!/usr/bin/env python3
"""
Split a paper text file into chunks for parallel extraction.

Supports two formats:
  - Legacy PAGE-marker format (from PyMuPDF): splits on ===PAGE N=== boundaries
  - Markdown format (from docling): splits by character count at natural boundaries

Usage:
  python3 scripts/py.py scripts/ingest/split_paper.py <text_file> [--max-pages N] [--max-chars N]

Prints one chunk path per line. If file is small enough, prints original path.
"""

import re
import sys
import argparse
from pathlib import Path

PAGE_MARKER = re.compile(r'(={80}\nPAGE \d+\n={80}\n)', re.MULTILINE)
HEADING_RE = re.compile(r'^#{1,3}\s+', re.MULTILINE)

# ~8000 chars/page is a reasonable estimate for docling markdown
DEFAULT_MAX_CHARS = 160_000  # ~20 pages


def split_pages_legacy(text: str) -> list[tuple[int, str]]:
    """Split on PAGE markers (legacy PyMuPDF format)."""
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


def split_by_chars(text: str, max_chars: int) -> list[str]:
    """Split markdown text at natural boundaries (headings > paragraphs > lines)."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    remaining = text

    while len(remaining) > max_chars:
        # Search for a heading boundary in the last 30% of the allowed range
        search_start = int(max_chars * 0.7)
        search_zone = remaining[search_start:max_chars]

        # Prefer splitting before a heading
        heading_positions = [m.start() for m in HEADING_RE.finditer(search_zone)]
        if heading_positions:
            split_at = search_start + heading_positions[-1]
        else:
            # Fall back to paragraph boundary (\n\n)
            para_pos = search_zone.rfind('\n\n')
            if para_pos != -1:
                split_at = search_start + para_pos + 1
            else:
                # Last resort: line boundary
                line_pos = search_zone.rfind('\n')
                if line_pos != -1:
                    split_at = search_start + line_pos + 1
                else:
                    split_at = max_chars

        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:]

    if remaining.strip():
        chunks.append(remaining)

    return chunks


def main():
    parser = argparse.ArgumentParser(description="Split paper into chunks")
    parser.add_argument('text_file')
    parser.add_argument('--max-pages', type=int, default=20)
    parser.add_argument('--max-chars', type=int, default=DEFAULT_MAX_CHARS)
    args = parser.parse_args()

    text_path = Path(args.text_file)
    if not text_path.exists():
        print(f'ERROR: file not found: {text_path}', file=sys.stderr)
        sys.exit(1)

    text = text_path.read_text(encoding='utf-8')

    # Auto-detect format: PAGE markers present → legacy mode
    has_page_markers = bool(PAGE_MARKER.search(text))

    if has_page_markers:
        pages = split_pages_legacy(text)
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
    else:
        # Markdown format — split by character count
        chunks = split_by_chars(text, args.max_chars)
        if len(chunks) <= 1:
            print(str(text_path))
            sys.exit(0)

        for i, chunk_text in enumerate(chunks, 1):
            chunk_path = text_path.parent / f'{text_path.stem}.part{i}.txt'
            chunk_path.write_text(chunk_text, encoding='utf-8')
            print(str(chunk_path))


if __name__ == '__main__':
    main()
