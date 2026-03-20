#!/usr/bin/env python3
"""
Ingest new PDFs: extract text, check for duplicates, move to storage.

Workflow:
  1. Scan pdf-staging/ for new PDFs
  2. Extract text from each PDF using PyMuPDF
  3. Parse title + authors from extracted text (heuristic)
  4. Fuzzy-match against existing data/db/papers.json to detect duplicates
  5. If NOT duplicate: move PDF to data/pdfs/, keep text in data/text/
  6. If duplicate: report and leave in pdf-staging/ for user decision

Usage:
  .venv/bin/python3 scripts/ingest/ingest.py                 # process all staged PDFs
  .venv/bin/python3 scripts/ingest/ingest.py --dry-run       # show what would happen
  .venv/bin/python3 scripts/ingest/ingest.py --force          # skip duplicate check
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

import fitz  # PyMuPDF

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "build"))

from litdb import normalize_doi
from find_matches import score_paper_pair

STAGING_DIR = ROOT / "pdf-staging"
STORAGE_DIR = ROOT / "data" / "pdfs"
TEXT_DIR = ROOT / "data" / "text"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def extract_text(pdf_path: Path) -> str:
    """Extract text from PDF with PAGE markers."""
    doc = fitz.open(str(pdf_path))
    parts = []
    for i, page in enumerate(doc, 1):
        parts.append(f"\n{'=' * 80}\nPAGE {i}\n{'=' * 80}\n")
        parts.append(page.get_text())
    doc.close()
    return "".join(parts)


def heuristic_metadata(text: str) -> dict:
    """Extract approximate title and authors from first pages for duplicate detection."""
    # Take first ~3000 chars (usually title page)
    header = text[:3000]
    lines = [l.strip() for l in header.split("\n") if l.strip()]

    # Skip PAGE markers, short lines, and known non-title patterns to find title
    import re
    SKIP_PATTERNS = [
        re.compile(r"^arXiv:", re.IGNORECASE),
        re.compile(r"^\d{4}\.\d{4,5}"),           # bare arXiv ID
        re.compile(r"^(doi|http|https|www\.)", re.IGNORECASE),
        re.compile(r"^(received|accepted|published|submitted)", re.IGNORECASE),
        re.compile(r"^\d+$"),                      # page numbers
        re.compile(r".*:\s*$"),                               # lines ending with colon (author attributions)
        re.compile(r"^[A-Z][\w.]+\s+and\s+[A-Z]", re.IGNORECASE),  # "Author and Author"
    ]
    title = ""
    authors_hint = []
    for line in lines:
        if line.startswith("=") or line.startswith("PAGE"):
            continue
        if any(p.match(line) for p in SKIP_PATTERNS):
            continue
        if len(line) > 15 and not title:
            title = line
            continue
        # Lines with commas after title might be authors
        if title and "," in line and len(line) < 200:
            authors_hint.append(line)
            if len(authors_hint) >= 3:
                break

    return {"title": title, "authors_raw": authors_hint}


def check_duplicate(metadata: dict, papers: dict) -> dict | None:
    """Check if paper likely already exists in DB. Returns best match or None."""
    if not papers or not metadata.get("title"):
        return None

    record = {"title": metadata["title"]}

    best_score = 0
    best_match = None
    for pid, paper in papers.items():
        s, sigs, details = score_paper_pair(record, paper)
        sim = details.get("title_similarity", 0)
        if s > best_score:
            best_score = s
            best_match = {
                "id": pid,
                "title": paper.get("title", ""),
                "authors": paper.get("authors", []),
                "year": paper.get("year"),
                "journal": paper.get("journal", ""),
                "doi": paper.get("doi"),
                "source_db": paper.get("source_db"),
                "score": s,
                "signals": sigs,
                "similarity": round(sim, 3),
                "match_type": (
                    "external" if paper.get("type") == "external_owned"
                    else "stub" if paper.get("type") == "stub"
                    else "local"
                ),
                "extraction_meta": paper.get("extraction_meta"),
            }

    if best_match and best_match["score"] >= 2:
        return best_match
    return None


def main():
    parser = argparse.ArgumentParser(description="Ingest PDFs from staging")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    parser.add_argument("--force", action="store_true", help="Skip duplicate check")
    args = parser.parse_args()

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    staged = sorted(STAGING_DIR.glob("*.pdf"))
    if not staged:
        print("No PDFs in pdf-staging/.")
        return

    # Load existing DB for duplicate check
    papers = {}
    if PAPERS_FILE.exists() and not args.force:
        with open(PAPERS_FILE) as f:
            papers = json.load(f).get("papers", {})
        print(f"Loaded {len(papers)} existing papers for duplicate check")

    accepted = []
    duplicates = []
    errors = []

    for pdf_path in staged:
        print(f"\nProcessing: {pdf_path.name}")

        # Check if text already exists
        text_path = TEXT_DIR / f"{pdf_path.stem}.txt"
        if text_path.exists():
            storage_path = STORAGE_DIR / pdf_path.name
            if storage_path.exists():
                print(f"  Already processed (text + storage exist)")
                continue

        # Extract text
        try:
            text = extract_text(pdf_path)
        except Exception as e:
            print(f"  ERROR extracting text: {e}")
            errors.append((pdf_path.name, str(e)))
            continue

        page_count = text.count("PAGE ")
        print(f"  Extracted {page_count} pages")

        # Duplicate check
        external_match = None
        if papers and not args.force:
            metadata = heuristic_metadata(text)
            match = check_duplicate(metadata, papers)
            if match:
                if match["match_type"] == "external":
                    new_title = metadata.get("title", pdf_path.stem)
                    print(f"  EXTERNAL MATCH: {match['title'][:80]}")
                    print(f"    Source DB: {match.get('source_db', '?')}")
                    print(f"    Paper ID:  {match['id']}")
                    print(f"  → PDF accepted for adoption. After ingest completes, run:")
                    print(f"      .venv/bin/python3 scripts/ingest/adopt_import.py {match['id']}")
                    external_match = match
                    # Fall through to normal accept flow
                elif match["match_type"] == "stub":
                    new_title = metadata.get("title", pdf_path.stem)
                    print(f"  STUB MATCH: {match['title'][:80]}")
                    print(f"    Paper ID:  {match['id']}")
                    print(f"  → PDF accepted for stub promotion. After ingest completes, run:")
                    print(f"      .venv/bin/python3 scripts/ingest/adopt_import.py {match['id']}")
                    external_match = match
                    # Fall through to normal accept flow
                else:
                    new_title = metadata.get("title", pdf_path.stem)
                    print(f"  DUPLICATE FOUND")
                    print(f"    New PDF:  {new_title[:80]}")
                    print(f"    In DB:    {match['title'][:80]} ({match['id']})")
                    print(f"  → PDF left in staging. Remove it manually if confirmed duplicate.")
                    em = match.get("extraction_meta")
                    if not em or set(em.get("passes_completed", [])) < {1, 2, 3, 4}:
                        completed = em.get("passes_completed", []) if em else []
                        print(f"    ⚠ Existing entry has incomplete extractions (passes: {completed or 'none'})")
                        print(f"    → If this is the same paper, consider running missing passes on the existing entry")
                    duplicates.append((pdf_path.name, match, text[:3000]))

                    # Clean up extracted text if it was written by a previous partial run
                    if text_path.exists():
                        text_path.unlink()
                        print(f"  → Deleted partial text file: {text_path.name}")
                    continue

        if args.dry_run:
            print(f"  [dry-run] Would accept and move to data/pdfs/")
            accepted.append((pdf_path.name, external_match["id"] if external_match else None))
            continue

        # Write text — rename to paper_id if adopting
        if external_match:
            text_path = TEXT_DIR / f"{external_match['id']}.txt"
        text_path.write_text(text, encoding="utf-8")
        print(f"  Text saved: {text_path}")

        # Move PDF to storage — rename to paper_id if adopting
        if external_match:
            storage_path = STORAGE_DIR / f"{external_match['id']}.pdf"
        else:
            storage_path = STORAGE_DIR / pdf_path.name
        shutil.move(str(pdf_path), str(storage_path))
        print(f"  PDF moved: {storage_path}")

        if external_match:
            accepted.append((pdf_path.name, external_match["id"]))
        else:
            accepted.append((pdf_path.name, None))

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Accepted: {len(accepted)}")
    adoptions = []
    for name, adopt_id in accepted:
        if adopt_id:
            print(f"  {name}  (adopt: {adopt_id})")
            adoptions.append(adopt_id)
        else:
            print(f"  {name}")
    if duplicates:
        print(f"Duplicates (left in staging): {len(duplicates)}")
        for name, match, _ in duplicates:
            print(f"  {name} → {match['id']} (score {match['score']})")
        tmp_dir = ROOT / "data" / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        pending = [
            {
                "pdf_name": name,
                "text_preview": preview,
                "match": match,
            }
            for name, match, preview in duplicates
        ]
        (tmp_dir / "pending_duplicates.json").write_text(
            json.dumps(pending, indent=2), encoding="utf-8"
        )
        print(f"  → Wrote data/tmp/pending_duplicates.json ({len(pending)} entries)")
    if errors:
        print(f"Errors: {len(errors)}")
        for name, err in errors:
            print(f"  {name}: {err}")

    if accepted and not args.dry_run:
        regular = [n for n, aid in accepted if not aid]
        if regular:
            print(f"\nNext: run paper-extractor agent on the new text files")
        if adoptions:
            print(f"\nAdoption pending — run for each external match:")
            for aid in adoptions:
                print(f"  .venv/bin/python3 scripts/ingest/adopt_import.py {aid}")


if __name__ == "__main__":
    main()
