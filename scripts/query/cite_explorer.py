#!/usr/bin/env python3
"""
cite_explorer.py — Explore how a paper cites others.

Reads data/extractions/{paper_id}.json for citation contexts and
data/db/papers.json for cross-reference metadata. Use this to understand
what a paper cites, why it cites each work, and in what context.

USAGE
-----
  python3 scripts/query/cite_explorer.py <paper_id> [options]

ARGUMENTS
---------
  paper_id
      ID of the owned paper to explore (e.g. semieniuk_2022_stranded).
      Partial match is supported: 'semieniuk_2022' will match if unique.

OPTIONS
-------
  --detail minimal|summary|normal|full
      Controls how much output is shown per citation. Default: normal.

        minimal  — cited paper ID only (one per line). Good for piping.
        summary  — one line per citation context: ID | purpose | explanation.
                   Compact overview for scanning what a paper cites and why.
                   Only shows citations that have contexts.
        normal   — ID, title, year, first author, journal, all purposes.
                   No quotes shown.
        full     — Everything in normal, plus every context quote and
                   the section it appears in.

  --purpose PURPOSE [PURPOSE ...]
      Filter to citations with at least one context matching this purpose.
      Can specify multiple: --purpose methodology data_source
      Purposes: background, motivation, methodology, data_source,
                supporting_evidence, contrasting_evidence,
                comparison, extension, tool_software

  --search PHRASE
      Filter to citations whose title, quote, or section contains PHRASE
      (case-insensitive). Applied after --purpose filter.

  --limit N
      Show only the first N citations after filtering. Default: no limit.

  --sort {id,year,purpose,appearances}
      Sort order for citations. Default: id (alphabetical).
        id          — alphabetical by citation ID
        year        — chronological
        purpose     — grouped by most-frequent purpose
        appearances — most-cited contexts first

  --owned-only
      Show only citations that are owned papers in the database.

  --json
      Output as JSON instead of formatted text. Useful for piping to
      other scripts. Outputs a list of citation objects, each with:
        id, title, authors, year, journal, doi, purposes[], contexts[]

EXAMPLES
--------
  # Overview of all citations (normal detail)
  python3 scripts/query/cite_explorer.py semieniuk_2022_stranded

  # Full detail for a specific paper
  python3 scripts/query/cite_explorer.py semieniuk_2022_stranded --detail full

  # Only methodology and data_source citations, with quotes
  python3 scripts/query/cite_explorer.py baer_2022_trisk --purpose methodology data_source --detail full

  # Search for carbon mentions across all citation contexts
  python3 scripts/query/cite_explorer.py monasterolo_2020_climate --search carbon --detail full

  # Just the IDs of contrasting citations
  python3 scripts/query/cite_explorer.py semieniuk_2022_stranded --purpose contrasting_evidence --detail minimal

  # Top 5 most-cited works in a paper
  python3 scripts/query/cite_explorer.py way_2022_empirically --sort appearances --limit 5 --detail full

  # All citations as JSON
  python3 scripts/query/cite_explorer.py pichler_2026_five --json
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"

ALL_PURPOSES = [
    "background", "motivation", "methodology", "data_source",
    "supporting_evidence", "contrasting_evidence",
    "comparison", "extension", "tool_software",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_extraction(paper_id: str) -> dict:
    """Load extraction JSON, supporting partial ID match."""
    exact = EXTRACTIONS_DIR / f"{paper_id}.json"
    if exact.exists():
        return json.loads(exact.read_text())

    # Partial match
    candidates = [p for p in EXTRACTIONS_DIR.glob("*.json")
                  if paper_id.lower() in p.stem.lower()]
    if not candidates:
        print(f"ERROR: No extraction found for '{paper_id}'", file=sys.stderr)
        print(f"Available: {sorted(p.stem for p in EXTRACTIONS_DIR.glob('*.json'))}",
              file=sys.stderr)
        sys.exit(1)
    if len(candidates) > 1:
        print(f"ERROR: Ambiguous ID '{paper_id}', matches:", file=sys.stderr)
        for c in sorted(candidates):
            print(f"  {c.stem}", file=sys.stderr)
        sys.exit(1)
    return json.loads(candidates[0].read_text())


def load_db_paper(paper_id: str, papers: dict) -> dict:
    """Return db entry for a cited paper, or empty dict."""
    return papers.get(paper_id, {})


def format_authors(authors: list, max_authors: int = 3) -> str:
    if not authors:
        return ""
    names = [str(a).split(",")[0].strip() for a in authors]
    if len(names) <= max_authors:
        return ", ".join(names)
    return ", ".join(names[:max_authors]) + " et al."


def sort_key(cit: dict, sort_by: str):
    if sort_by == "year":
        return str(cit.get("year", "9999"))
    if sort_by == "purpose":
        purposes = [ctx["purpose"] for ctx in cit.get("contexts", [])]
        return purposes[0] if purposes else "zzz"
    if sort_by == "appearances":
        return -len(cit.get("contexts", []))
    return cit.get("id", "")  # default: id


# ── Filtering ─────────────────────────────────────────────────────────────────

def filter_citations(citations: list, purposes: list, search: str, owned_only: bool,
                     papers: dict) -> list:
    result = []
    for cit in citations:
        contexts = cit.get("contexts", [])

        # --owned-only
        if owned_only and papers.get(cit.get("id", {}), {}).get("type") != "owned":
            continue

        # --purpose filter: keep citation only if at least one context matches
        if purposes:
            contexts = [ctx for ctx in contexts if ctx.get("purpose") in purposes]
            if not contexts:
                continue

        # --search filter: keep citation if phrase found in title, quote, or section
        if search:
            phrase = search.lower()
            title_match = phrase in cit.get("title", "").lower()
            ctx_match = any(
                phrase in ctx.get("quote", "").lower() or
                phrase in ctx.get("section", "").lower()
                for ctx in contexts
            )
            if not title_match and not ctx_match:
                continue
            # Narrow contexts to matching ones (if phrase only in some)
            if not title_match:
                contexts = [ctx for ctx in contexts
                            if phrase in ctx.get("quote", "").lower()
                            or phrase in ctx.get("section", "").lower()]

        result.append({**cit, "contexts": contexts})
    return result


# ── Output formatters ─────────────────────────────────────────────────────────

def print_minimal(citations: list):
    for cit in citations:
        print(cit.get("id", ""))


def print_summary(citations: list, papers: dict):
    """One line per context: id | purpose | explanation. Skip citations with no contexts."""
    for cit in citations:
        pid = cit.get("id", "")
        contexts = cit.get("contexts", [])
        if not contexts:
            continue
        db_entry = papers.get(pid, {})
        owned_flag = " [OWNED]" if db_entry.get("type") in ("owned", "external_owned") else ""
        for ctx in contexts:
            purpose = ctx.get("purpose", "")
            expl = ctx.get("explanation", "")
            print(f"  {pid}{owned_flag} | {purpose} | {expl}")


def print_normal(citations: list, papers: dict):
    for cit in citations:
        pid = cit.get("id", "")
        title = cit.get("title", "")
        year = cit.get("year", "")
        authors = format_authors(cit.get("authors", []))
        journal = cit.get("journal") or ""
        contexts = cit.get("contexts", [])

        # Collect unique purposes and their counts
        purpose_counts: dict = {}
        for ctx in contexts:
            p = ctx.get("purpose", "")
            purpose_counts[p] = purpose_counts.get(p, 0) + 1

        db_entry = papers.get(pid, {})
        owned_flag = " [OWNED]" if db_entry.get("type") in ("owned", "external_owned") else ""

        print(f"  {pid}{owned_flag}")
        print(f"    title   : {title}")
        if authors:
            print(f"    authors : {authors}{f' ({year})' if year else ''}")
        if journal:
            print(f"    journal : {journal}")
        if purpose_counts:
            purposes_str = ", ".join(
                f"{p} ×{n}" if n > 1 else p
                for p, n in sorted(purpose_counts.items())
            )
            print(f"    cited as: {purposes_str}")
        print()


def print_full(citations: list, papers: dict):
    for cit in citations:
        pid = cit.get("id", "")
        title = cit.get("title", "")
        year = cit.get("year", "")
        authors = format_authors(cit.get("authors", []))
        journal = cit.get("journal") or ""
        doi = cit.get("doi") or ""
        contexts = cit.get("contexts", [])

        db_entry = papers.get(pid, {})
        owned_flag = " [OWNED]" if db_entry.get("type") in ("owned", "external_owned") else ""

        print(f"  {pid}{owned_flag}")
        print(f"    title   : {title}")
        if authors:
            print(f"    authors : {authors}{f' ({year})' if year else ''}")
        if journal:
            print(f"    journal : {journal}")
        if doi:
            print(f"    doi     : {doi}")
        print(f"    contexts: {len(contexts)}")
        for ctx in contexts:
            print(f"      [{ctx.get('purpose', '')}] {ctx.get('section', '')}")
            quote = ctx.get("quote", "")
            if quote:
                # Wrap long quotes
                words = quote.split()
                line, lines = [], []
                for w in words:
                    line.append(w)
                    if len(" ".join(line)) > 100:
                        lines.append(" ".join(line))
                        line = []
                if line:
                    lines.append(" ".join(line))
                print(f"      \"{lines[0]}\"")
                for l in lines[1:]:
                    print(f"       {l}")
        print()


def print_json(citations: list, paper_meta: dict, papers: dict):
    out = []
    for cit in citations:
        pid = cit.get("id", "")
        db_entry = papers.get(pid, {})
        out.append({
            "id": pid,
            "title": cit.get("title", ""),
            "authors": cit.get("authors", []),
            "year": cit.get("year"),
            "journal": cit.get("journal"),
            "doi": cit.get("doi"),
            "type": db_entry.get("type", "unknown"),
            "purposes": sorted({ctx.get("purpose", "") for ctx in cit.get("contexts", [])}),
            "contexts": cit.get("contexts", []),
        })
    print(json.dumps(out, indent=2, ensure_ascii=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="cite_explorer.py",
        description="Explore how an owned paper cites others.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("paper_id", help="Owned paper ID (partial match supported)")
    parser.add_argument("--detail", choices=["minimal", "summary", "normal", "full"],
                        default="normal",
                        help="Output detail level (default: normal)")
    parser.add_argument("--purpose", nargs="+", metavar="PURPOSE",
                        choices=ALL_PURPOSES,
                        help="Filter by citation purpose(s)")
    parser.add_argument("--search", metavar="PHRASE",
                        help="Filter by phrase in title, quote, or section")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="Show only first N citations after filtering")
    parser.add_argument("--sort", choices=["id", "year", "purpose", "appearances"],
                        default="id",
                        help="Sort order (default: id)")
    parser.add_argument("--owned-only", action="store_true",
                        help="Only show citations that are owned papers")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")

    args = parser.parse_args()

    # Load data
    ext = load_extraction(args.paper_id)
    papers = json.loads(PAPERS_FILE.read_text())["papers"]

    paper_id = ext["id"]
    citations = ext.get("citations", [])

    # Filter
    citations = filter_citations(
        citations,
        purposes=args.purpose or [],
        search=args.search or "",
        owned_only=args.owned_only,
        papers=papers,
    )

    # Sort
    citations = sorted(citations, key=lambda c: sort_key(c, args.sort))

    # Limit
    total = len(citations)
    if args.limit:
        citations = citations[:args.limit]

    # Header (not in json or minimal mode)
    if not args.json and args.detail != "minimal":
        print(f"Paper : {paper_id}")
        print(f"Title : {ext.get('title', '')}")
        authors = format_authors(ext.get("authors", []))
        year = ext.get("year", "")
        print(f"By    : {authors} ({year})")
        filters = []
        if args.purpose:
            filters.append(f"purpose={','.join(args.purpose)}")
        if args.search:
            filters.append(f"search='{args.search}'")
        if args.owned_only:
            filters.append("owned-only")
        filter_str = f"  [filters: {', '.join(filters)}]" if filters else ""
        shown = len(citations)
        print(f"Citing: {total} citation(s){filter_str}" +
              (f", showing first {shown}" if args.limit else ""))
        print()

    # Output
    if args.json:
        print_json(citations, ext, papers)
    elif args.detail == "minimal":
        print_minimal(citations)
    elif args.detail == "summary":
        print_summary(citations, papers)
    elif args.detail == "normal":
        print_normal(citations, papers)
    else:
        print_full(citations, papers)


if __name__ == "__main__":
    main()
