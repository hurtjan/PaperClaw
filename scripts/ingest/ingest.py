#!/usr/bin/env python3
"""
Ingest new PDFs: extract text with docling, check for duplicates, move to storage.

Workflow:
  1. Scan pdf-staging/ for new PDFs
  2. Extract markdown text from each PDF using docling (one at a time, with progress)
  3. Parse title + authors from extracted text (heuristic)
  4. Fuzzy-match against existing data/db/papers.json to detect duplicates
  5. If NOT duplicate: move PDF to data/pdfs/, save text to data/text/
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
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "build"))

from litdb import normalize_doi
from find_matches import score_paper_pair

STAGING_DIR = ROOT / "pdf-staging"
STORAGE_DIR = ROOT / "data" / "pdfs"
TEXT_DIR = ROOT / "data" / "text"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def log(msg=""):
    """Print with immediate flush so progress is visible even when piped."""
    print(msg, flush=True)


def create_converter():
    """Create a docling DocumentConverter (expensive — call once)."""
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.pipeline_options import PdfPipelineOptions, AcceleratorOptions
    from docling.datamodel.accelerator_options import AcceleratorDevice
    from docling.datamodel.base_models import InputFormat

    pipeline_opts = PdfPipelineOptions()
    pipeline_opts.do_ocr = False
    pipeline_opts.do_table_structure = True
    pipeline_opts.do_formula_enrichment = False
    try:
        pipeline_opts.accelerator_options = AcceleratorOptions(device=AcceleratorDevice.MPS)
    except Exception:
        pipeline_opts.accelerator_options = AcceleratorOptions(device=AcceleratorDevice.CPU)

    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts)}
    )


def extract_text(pdf_path: Path, converter) -> tuple[str, int]:
    """Extract markdown text from PDF using docling. Returns (text, page_count)."""
    t0 = time.time()
    result = converter.convert(str(pdf_path.resolve()))
    elapsed = time.time() - t0

    doc = result.document
    page_count = len(list(doc.pages))
    content_md = doc.export_to_markdown()

    log(f"    {page_count} pages, {len(content_md)} chars in {elapsed:.1f}s ({elapsed/max(page_count,1):.1f}s/page)")
    return content_md, page_count


def heuristic_metadata(text: str) -> dict:
    """Extract approximate title and authors from first pages for duplicate detection."""
    # Take first ~3000 chars (usually title page)
    header = text[:3000]
    lines = [l.strip() for l in header.split("\n") if l.strip()]

    import re
    SKIP_PATTERNS = [
        re.compile(r"^arXiv:", re.IGNORECASE),
        re.compile(r"^\d{4}\.\d{4,5}"),           # bare arXiv ID
        re.compile(r"^(doi|http|https|www\.)", re.IGNORECASE),
        re.compile(r"^(received|accepted|published|submitted)", re.IGNORECASE),
        re.compile(r"^\d+$"),                      # page numbers
        re.compile(r".*:\s*$"),                     # lines ending with colon
        re.compile(r"^[A-Z][\w.]+\s+and\s+[A-Z]", re.IGNORECASE),  # "Author and Author"
        re.compile(r"^<!--.*-->$"),                 # HTML comments (docling images)
        re.compile(r"^Contents\s+lists", re.IGNORECASE),  # journal boilerplate
        re.compile(r"^j\s*o\s*u\s*r\s*n\s*a\s*l", re.IGNORECASE),  # spaced journal text
        re.compile(r"^\[.*\]\(.*\)$"),              # markdown links (journal URLs)
        re.compile(r"^(Journal|Review|Proceedings)\s+of\b", re.IGNORECASE),  # journal names
    ]
    title = ""
    authors_hint = []
    for line in lines:
        # Strip markdown heading markers
        clean = re.sub(r"^#{1,6}\s+", "", line)
        if not clean:
            continue
        if line.startswith("=") or line.startswith("PAGE"):
            continue
        if any(p.match(clean) for p in SKIP_PATTERNS):
            continue
        if len(clean) > 15 and not title:
            title = clean
            continue
        # Lines with commas after title might be authors
        if title and "," in clean and len(clean) < 200:
            authors_hint.append(clean)
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
    parser.add_argument("--max-size-mb", type=int, default=50, help="Skip PDFs larger than this (MB)")
    args = parser.parse_args()

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    staged = sorted(STAGING_DIR.glob("*.pdf"))
    if not staged:
        log("No PDFs in pdf-staging/.")
        return

    total = len(staged)
    log(f"Found {total} PDFs in pdf-staging/")

    # Initialize docling converter once (expensive)
    log("Initializing docling converter...")
    t_init = time.time()
    converter = create_converter()
    log(f"Converter ready ({time.time() - t_init:.1f}s)")

    # Load existing DB for duplicate check
    papers = {}
    if PAPERS_FILE.exists() and not args.force:
        with open(PAPERS_FILE) as f:
            papers = json.load(f).get("papers", {})
        log(f"Loaded {len(papers)} existing papers for duplicate check")

    accepted = []
    duplicates = []
    errors = []
    t_start = time.time()

    for idx, pdf_path in enumerate(staged, 1):
        log(f"\n[{idx}/{total}] {pdf_path.name}")

        # Skip if PDF was removed between glob and processing
        if not pdf_path.exists():
            log(f"  -> skip (file missing)")
            continue

        # Check if text already exists
        text_path = TEXT_DIR / f"{pdf_path.stem}.txt"
        if text_path.exists():
            storage_path = STORAGE_DIR / pdf_path.name
            if storage_path.exists():
                log(f"  -> skip (already processed)")
                continue

        # Skip oversized PDFs
        size_mb = pdf_path.stat().st_size / (1024 * 1024)
        if size_mb > args.max_size_mb:
            log(f"  -> skip (too large: {size_mb:.0f}MB > {args.max_size_mb}MB limit)")
            errors.append((pdf_path.name, f"too large: {size_mb:.0f}MB"))
            continue

        # Extract text
        try:
            text, page_count = extract_text(pdf_path, converter)
        except Exception as e:
            log(f"  ERROR: {e}")
            errors.append((pdf_path.name, str(e)))
            continue

        # Duplicate check
        external_match = None
        if papers and not args.force:
            metadata = heuristic_metadata(text)
            match = check_duplicate(metadata, papers)
            if match:
                if match["match_type"] == "external":
                    log(f"  EXTERNAL MATCH: {match['title'][:80]}")
                    log(f"    Source DB: {match.get('source_db', '?')}  |  Paper ID: {match['id']}")
                    external_match = match
                elif match["match_type"] == "stub":
                    log(f"  STUB MATCH: {match['title'][:80]}")
                    log(f"    Paper ID: {match['id']}")
                    external_match = match
                else:
                    log(f"  DUPLICATE: {match['title'][:80]} ({match['id']})")
                    em = match.get("extraction_meta")
                    if not em or set(em.get("passes_completed", [])) < {1, 2, 3, 4}:
                        completed = em.get("passes_completed", []) if em else []
                        log(f"    incomplete extractions (passes: {completed or 'none'})")
                    duplicates.append((pdf_path.name, match, text[:3000]))
                    if text_path.exists():
                        text_path.unlink()
                    continue

        if args.dry_run:
            log(f"  -> dry-run, would accept")
            accepted.append((pdf_path.name, external_match["id"] if external_match else None))
            continue

        # Save text immediately
        if external_match:
            text_path = TEXT_DIR / f"{external_match['id']}.txt"
        text_path.write_text(text, encoding="utf-8")
        log(f"  -> saved: {text_path.name}")

        # Move PDF to storage
        if external_match:
            storage_path = STORAGE_DIR / f"{external_match['id']}.pdf"
        else:
            storage_path = STORAGE_DIR / pdf_path.name
        shutil.move(str(pdf_path), str(storage_path))
        log(f"  -> moved: {storage_path.name}")

        if external_match:
            accepted.append((pdf_path.name, external_match["id"]))
        else:
            accepted.append((pdf_path.name, None))

    # Summary
    elapsed_total = time.time() - t_start
    log(f"\n{'=' * 60}")
    log(f"Done in {elapsed_total:.0f}s")
    log(f"Accepted: {len(accepted)}  |  Duplicates: {len(duplicates)}  |  Errors: {len(errors)}")
    adoptions = []
    for name, adopt_id in accepted:
        if adopt_id:
            log(f"  {name}  (adopt: {adopt_id})")
            adoptions.append(adopt_id)
        else:
            log(f"  {name}")
    if duplicates:
        log(f"\nDuplicates (left in staging):")
        for name, match, _ in duplicates:
            log(f"  {name} -> {match['id']} (score {match['score']})")
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
        log(f"  -> Wrote data/tmp/pending_duplicates.json ({len(pending)} entries)")
    if errors:
        log(f"\nErrors:")
        for name, err in errors:
            log(f"  {name}: {err}")

    if accepted and not args.dry_run:
        regular = [n for n, aid in accepted if not aid]
        if regular:
            log(f"\nNext: run paper-extractor agent on the new text files")
        if adoptions:
            log(f"\nAdoption pending — run for each external match:")
            for aid in adoptions:
                log(f"  .venv/bin/python3 scripts/ingest/adopt_import.py {aid}")


if __name__ == "__main__":
    main()
