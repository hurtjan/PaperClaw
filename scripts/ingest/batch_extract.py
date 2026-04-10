"""
Batch-extract text from all PDFs in pdf-staging/ (recursively).
Saves .txt files to txt-staging/ preserving subfolder structure.
Skips PDFs that already have a corresponding .txt.

Usage: python3 scripts/py.py scripts/ingest/batch_extract.py [--max-size-mb N]
"""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "ingest"))

PDF_DIR = ROOT / "pdf-staging"
TXT_DIR = ROOT / "txt-staging"


def log(msg=""):
    print(msg, flush=True)


HAS_DOCLING = False
try:
    import docling  # noqa: F401
    HAS_DOCLING = True
except ImportError:
    pass


def create_converter():
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.pipeline_options import PdfPipelineOptions, AcceleratorOptions
    from docling.datamodel.accelerator_options import AcceleratorDevice
    from docling.datamodel.base_models import InputFormat

    opts = PdfPipelineOptions()
    opts.do_ocr = False
    opts.do_table_structure = True
    opts.do_formula_enrichment = False
    try:
        opts.accelerator_options = AcceleratorOptions(device=AcceleratorDevice.MPS)
    except Exception:
        opts.accelerator_options = AcceleratorOptions(device=AcceleratorDevice.CPU)

    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
    )


def extract_docling(pdf_path, converter):
    t0 = time.time()
    result = converter.convert(str(pdf_path.resolve()))
    doc = result.document
    pages = len(list(doc.pages))
    text = doc.export_to_markdown()
    elapsed = time.time() - t0
    log(f"    [docling] {pages}p, {len(text)} chars, {elapsed:.1f}s")
    return text


def extract_pymupdf(pdf_path):
    import fitz
    t0 = time.time()
    doc = fitz.open(str(pdf_path))
    parts = []
    for i, page in enumerate(doc, 1):
        parts.append(f"\n{'=' * 80}\nPAGE {i}\n{'=' * 80}\n")
        parts.append(page.get_text())
    doc.close()
    text = "".join(parts)
    elapsed = time.time() - t0
    log(f"    [pymupdf] {len(doc)}p, {len(text)} chars, {elapsed:.1f}s")
    return text


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch extract text from staged PDFs")
    parser.add_argument("--max-size-mb", type=int, default=50)
    args = parser.parse_args()

    pdfs = sorted(PDF_DIR.rglob("*.pdf"))
    if not pdfs:
        log("No PDFs found in pdf-staging/")
        return

    # Check which already have txt
    todo = []
    for pdf in pdfs:
        rel = pdf.relative_to(PDF_DIR)
        txt_path = TXT_DIR / rel.with_suffix(".txt")
        if not txt_path.exists():
            todo.append((pdf, rel, txt_path))

    log(f"Found {len(pdfs)} PDFs, {len(pdfs) - len(todo)} already extracted, {len(todo)} to do")
    if not todo:
        return

    converter = None
    if HAS_DOCLING:
        log("Initializing docling...")
        t0 = time.time()
        converter = create_converter()
        log(f"Ready ({time.time() - t0:.1f}s)")
    else:
        log("Using PyMuPDF fallback")

    done = 0
    errors = 0
    t_start = time.time()

    for i, (pdf, rel, txt_path) in enumerate(todo, 1):
        log(f"\n[{i}/{len(todo)}] {rel}")

        size_mb = pdf.stat().st_size / (1024 * 1024)
        if size_mb > args.max_size_mb:
            log(f"  skip ({size_mb:.0f}MB > {args.max_size_mb}MB)")
            errors += 1
            continue

        try:
            if converter:
                text = extract_docling(pdf, converter)
            else:
                text = extract_pymupdf(pdf)
        except Exception as e:
            log(f"  ERROR: {e}")
            errors += 1
            continue

        txt_path.parent.mkdir(parents=True, exist_ok=True)
        txt_path.write_text(text, encoding="utf-8")
        done += 1

        if i % 10 == 0:
            elapsed = time.time() - t_start
            rate = done / elapsed * 60 if elapsed > 0 else 0
            log(f"  -- progress: {done} done, {errors} errors, {rate:.1f}/min --")

    elapsed = time.time() - t_start
    log(f"\nDone: {done} extracted, {errors} errors, {elapsed:.0f}s total")


if __name__ == "__main__":
    main()
