#!/usr/bin/env python3
"""
Prepare batches of PDFs for ingestion from temp_store.

Workflow:
  1. Scans pdf-staging/temp_store/ for unprocessed PDFs
  2. Filters out papers with >N pages (moves to pdf-staging/on_hold/)
  3. Clears pdf-staging/ top level of already-processed PDFs
  4. Copies the next batch to pdf-staging/ for ingest.py

Usage:
  python3 scripts/py.py scripts/ingest/batch_prep.py                # prepare next batch of 30
  python3 scripts/py.py scripts/ingest/batch_prep.py --batch-size 20
  python3 scripts/py.py scripts/ingest/batch_prep.py --max-pages 40  # stricter page limit
  python3 scripts/py.py scripts/ingest/batch_prep.py --status        # show queue status
  python3 scripts/py.py scripts/ingest/batch_prep.py --hold paper.pdf [paper2.pdf ...]
  python3 scripts/py.py scripts/ingest/batch_prep.py --clear         # remove processed PDFs from staging
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

STAGING_DIR = ROOT / "pdf-staging"
TEMP_STORE = STAGING_DIR / "temp_store"
ON_HOLD = STAGING_DIR / "on_hold"
STORAGE_DIR = ROOT / "data" / "pdfs"
TEXT_DIR = ROOT / "data" / "text"

HAS_FITZ = False
try:
    import fitz  # noqa: F401
    HAS_FITZ = True
except ImportError:
    pass


def log(msg=""):
    print(msg, flush=True)


def get_page_count(pdf_path: Path) -> int:
    """Get page count using PyMuPDF."""
    if not HAS_FITZ:
        return -1
    try:
        doc = fitz.open(str(pdf_path))
        n = len(doc)
        doc.close()
        return n
    except Exception:
        return -1


def get_processed_stems() -> set[str]:
    """Get stems of already-processed papers (have text or stored PDF)."""
    stems = set()
    if TEXT_DIR.is_dir():
        for subdir in ("staging", "in_process", "done"):
            stage_dir = TEXT_DIR / subdir
            if stage_dir.is_dir():
                stems.update(p.stem for p in stage_dir.glob("*.txt"))
        # Legacy fallback: flat root
        stems.update(p.stem for p in TEXT_DIR.glob("*.txt") if p.is_file())
    if STORAGE_DIR.is_dir():
        stems.update(p.stem for p in STORAGE_DIR.glob("*.pdf"))
    return stems


def get_unprocessed_pdfs() -> list[Path]:
    """Get PDFs in temp_store that haven't been processed yet."""
    if not TEMP_STORE.is_dir():
        return []
    processed = get_processed_stems()
    holding_names = set()
    holding_dir = STAGING_DIR / "holding"
    if holding_dir.is_dir():
        holding_names = {f.name for f in holding_dir.iterdir()}
    unprocessed = []
    for pdf in sorted(TEMP_STORE.glob("*.pdf")):
        if pdf.stem not in processed and pdf.name not in holding_names:
            unprocessed.append(pdf)
    return unprocessed


def get_staged_pdfs() -> list[Path]:
    """Get PDFs currently in top-level staging."""
    return sorted(STAGING_DIR.glob("*.pdf"))


def show_status():
    """Show queue status."""
    unprocessed = get_unprocessed_pdfs()
    staged = get_staged_pdfs()
    on_hold_pdfs = sorted(ON_HOLD.glob("*.pdf")) if ON_HOLD.is_dir() else []
    processed = get_processed_stems()

    log(f"Queue status:")
    log(f"  Unprocessed in temp_store:  {len(unprocessed)}")
    log(f"  Currently staged:           {len(staged)}")
    log(f"  On hold:                    {len(on_hold_pdfs)}")
    log(f"  Already processed (text):   {len(processed)}")

    if on_hold_pdfs:
        log(f"\nOn hold:")
        for pdf in on_hold_pdfs:
            pages = get_page_count(pdf)
            pg = f"{pages}p" if pages > 0 else "?"
            log(f"  {pg:>5s}  {pdf.name}")

    batches_remaining = (len(unprocessed) + 29) // 30 if unprocessed else 0
    log(f"\nBatches remaining (at 30/batch): {batches_remaining}")


def clear_processed():
    """Remove already-processed PDFs from top-level staging."""
    processed = get_processed_stems()
    holding_names = set()
    holding_dir = STAGING_DIR / "holding"
    if holding_dir.is_dir():
        holding_names = {f.name for f in holding_dir.iterdir()}
    staged = get_staged_pdfs()
    removed = 0
    for pdf in staged:
        if pdf.stem in processed or pdf.name in holding_names:
            pdf.unlink()
            removed += 1
    log(f"Cleared {removed} already-processed PDFs from staging")
    return removed


def hold_papers(names: list[str]):
    """Move specific papers to on_hold."""
    ON_HOLD.mkdir(parents=True, exist_ok=True)
    for name in names:
        # Search in temp_store and staging
        for search_dir in [TEMP_STORE, STAGING_DIR]:
            src = search_dir / name
            if src.is_file():
                shutil.move(str(src), str(ON_HOLD / name))
                log(f"  -> on_hold: {name}")
                break
        else:
            log(f"  not found: {name}")


def prep_batch(batch_size: int, max_pages: int):
    """Prepare next batch for ingestion."""
    ON_HOLD.mkdir(parents=True, exist_ok=True)

    # First clear processed PDFs from staging
    clear_processed()

    # Count already-staged PDFs toward batch size
    already_staged = get_staged_pdfs()
    slots = batch_size - len(already_staged)
    if already_staged:
        log(f"\n{len(already_staged)} PDFs already in staging (counting toward batch)")
    if slots <= 0:
        log(f"Batch already full ({len(already_staged)} staged). Run /ingest first.")
        return

    # Get unprocessed queue
    unprocessed = get_unprocessed_pdfs()
    if not unprocessed:
        log("\nNo unprocessed PDFs remaining in temp_store.")
        return

    log(f"\n{len(unprocessed)} unprocessed PDFs in queue")

    # Filter by page count, take batch
    batch = []
    skipped_pages = []
    for pdf in unprocessed:
        if len(batch) >= slots:
            break
        pages = get_page_count(pdf)
        if pages > max_pages:
            # Move to on_hold
            shutil.move(str(pdf), str(ON_HOLD / pdf.name))
            skipped_pages.append((pdf.name, pages))
            continue
        batch.append((pdf, pages))

    if skipped_pages:
        log(f"\nMoved {len(skipped_pages)} papers to on_hold (>{max_pages} pages):")
        for name, pages in skipped_pages:
            log(f"  {pages:4d}p  {name}")

    if not batch:
        log("\nNo papers left for this batch after filtering.")
        remaining = get_unprocessed_pdfs()
        if remaining:
            log(f"({len(remaining)} still in queue — may need to adjust --max-pages)")
        return

    # Copy batch to staging
    log(f"\nStaging batch of {len(batch)}:")
    for pdf, pages in batch:
        dest = STAGING_DIR / pdf.name
        shutil.copy2(str(pdf), str(dest))
        pg = f"{pages}p" if pages > 0 else "?"
        log(f"  {pg:>5s}  {pdf.name}")

    remaining = len(unprocessed) - len(batch) - len(skipped_pages)
    log(f"\nBatch ready: {len(batch)} PDFs staged")
    log(f"Remaining after this batch: ~{remaining}")
    log(f"\nNext: cd .. && claude, then /ingest")


def done_batch():
    """Clean up after successful /ingest: remove ingested PDFs from staging and temp_store."""
    staged = get_staged_pdfs()
    if not staged:
        log("No PDFs in staging to clean up.")
        return

    processed = get_processed_stems()
    cleaned_staging = 0
    cleaned_temp = 0

    for pdf in staged:
        # Remove from staging
        pdf.unlink()
        cleaned_staging += 1
        # Also remove from temp_store if present
        temp_copy = TEMP_STORE / pdf.name
        if temp_copy.exists():
            temp_copy.unlink()
            cleaned_temp += 1

    log(f"Cleaned up: {cleaned_staging} from staging, {cleaned_temp} from temp_store")
    show_status()


def main():
    parser = argparse.ArgumentParser(description="Prepare PDF batches for ingestion")
    parser.add_argument("--batch-size", type=int, default=30, help="PDFs per batch (default: 30)")
    parser.add_argument("--max-pages", type=int, default=50, help="Max pages before on-hold (default: 50)")
    parser.add_argument("--status", action="store_true", help="Show queue status")
    parser.add_argument("--clear", action="store_true", help="Clear processed PDFs from staging")
    parser.add_argument("--hold", nargs="+", metavar="PDF", help="Move specific PDFs to on_hold")
    parser.add_argument("--done", action="store_true", help="Clean up after successful /ingest")
    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.clear:
        clear_processed()
    elif args.hold:
        hold_papers(args.hold)
    elif args.done:
        done_batch()
    else:
        prep_batch(args.batch_size, args.max_pages)


if __name__ == "__main__":
    main()
