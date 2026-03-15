#!/usr/bin/env python3
"""
corpus.py — Query extraction-level data across the literature corpus.

Exposes Pass 3/4 metadata (methodology, claims, topics, sections) and
full-text search that are NOT available in query_db.py or cite_explorer.py.

PAPER-LEVEL QUERIES
-------------------
  python3 scripts/query/corpus.py abstract <paper_id>
      Show paper abstract. Partial ID match supported.

  python3 scripts/query/corpus.py methodology <paper_id>
      Show methodology details: type, approach, data sources, scenarios,
      temporal/geographic scope, unit of analysis.

  python3 scripts/query/corpus.py claims <paper_id> [--type TYPE] [--json]
      List claims with evidence basis and supporting citations.
      Types: empirical_finding, theoretical_argument,
             methodological_contribution, policy_recommendation

  python3 scripts/query/corpus.py keywords <paper_id>
      Show keywords and topic classification (themes, sectors,
      geographic focus, policy context).

  python3 scripts/query/corpus.py sections <paper_id>
      List section headings with summaries.

  python3 scripts/query/corpus.py section <paper_id> <heading_substring>
      Show full section detail (summary + annotated text) for one section.

  python3 scripts/query/corpus.py questions <paper_id>
      Show research questions.

  python3 scripts/query/corpus.py data-sources <paper_id>
      Show data sources used (from methodology.data_sources).

CROSS-CORPUS QUERIES
--------------------
  python3 scripts/query/corpus.py search-all <phrase> [--limit N]
      Unified search across topics, claims, keywords, abstracts, and
      sections in one pass. Shows summary counts then details.
      Best first query for broad topics — gives you an overview before
      drilling down. Default limit: 30 results.

  python3 scripts/query/corpus.py search-methods <type>
      Papers by methodology type. Types: simulation, empirical,
      theoretical, review, mixed. Partial match supported.

  python3 scripts/query/corpus.py search-topics <phrase> [--limit N]
      Papers whose themes, sectors, geographic_focus, or policy_context
      contain phrase (case-insensitive).

  python3 scripts/query/corpus.py search-claims <phrase> [--type TYPE] [--limit N]
      Search claims text across all papers. Shows matching claims with
      paper attribution.

  python3 scripts/query/corpus.py search-data <phrase> [--limit N]
      Search data source names/types across all papers.

  python3 scripts/query/corpus.py search-abstracts <phrase> [--limit N]
      Search abstracts across all owned papers.

  python3 scripts/query/corpus.py search-keywords <phrase> [--limit N]
      Papers whose keywords contain phrase.

  python3 scripts/query/corpus.py search-sections <phrase> [--limit N]
      Search section headings and summaries across all papers.
      Uses sections embedded in papers.json (falls back to scanning
      extractions if sections not present).

  python3 scripts/query/corpus.py search-text <phrase> [--context N]
      Full-text search across data/text/*.txt. Shows matching lines
      with surrounding context. Default context: 2 lines.

  All search-* commands support --limit N to cap output.

CORPUS OVERVIEW
---------------
  python3 scripts/query/corpus.py stats
      Corpus summary: paper counts, methodology breakdown, year range,
      top themes, top sectors.

  python3 scripts/query/corpus.py list [--year-min Y] [--year-max Y] [--method TYPE] [--has FIELD] [--json]
      List owned papers with optional filters.
      --has: methodology, claims, sections, keywords, abstract

  python3 scripts/query/corpus.py purposes
      List all citation purpose tags with counts across corpus.

ALL COMMANDS support partial paper_id matching (unique prefix/substring).
"""

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
INDEX_FILE = ROOT / "data" / "db" / "contexts.json"
TEXT_DIR = ROOT / "data" / "text"


# ── Helpers ──────────────────────────────────────────────────────────────────


def load_papers():
    return json.loads(PAPERS_FILE.read_text())["papers"]


def load_index():
    if not INDEX_FILE.exists():
        return None
    return json.loads(INDEX_FILE.read_text())


def load_sections_from_papers():
    """Load sections from papers.json (sections are embedded in owned paper entries)."""
    papers = load_papers()
    entries = []
    for pid, p in papers.items():
        if p.get("type") != "owned":
            continue
        for sec in p.get("sections", []):
            entries.append({
                "paper_id": pid,
                "paper_title": p.get("title", ""),
                "heading": sec.get("heading", ""),
                "summary": sec.get("summary", ""),
            })
    return {"sections": entries} if entries else None


def load_extraction(paper_id: str) -> dict | None:
    """Load extraction JSON for a paper. Returns None if not found."""
    exact = EXTRACTIONS_DIR / f"{paper_id}.json"
    if exact.exists():
        return json.loads(exact.read_text())
    return None


def resolve_paper_id(paper_id: str) -> str:
    """Resolve partial paper_id to full ID using extraction files."""
    exact = EXTRACTIONS_DIR / f"{paper_id}.json"
    if exact.exists():
        return paper_id

    candidates = [p.stem for p in EXTRACTIONS_DIR.glob("*.json")
                  if paper_id.lower() in p.stem.lower()]
    if not candidates:
        print(f"ERROR: No extraction found for '{paper_id}'", file=sys.stderr)
        sys.exit(1)
    if len(candidates) > 1:
        print(f"ERROR: Ambiguous ID '{paper_id}', matches:", file=sys.stderr)
        for c in sorted(candidates):
            print(f"  {c}", file=sys.stderr)
        sys.exit(1)
    return candidates[0]


def all_extractions():
    """Yield (paper_id, data_dict) for all extractions."""
    for f in sorted(EXTRACTIONS_DIR.glob("*.json")):
        # Skip intermediate files (refs, contexts, sections, analysis)
        if any(part in f.stem for part in (".refs", ".contexts", ".sections", ".analysis")):
            continue
        try:
            data = json.loads(f.read_text())
            if not isinstance(data, dict):
                continue
            yield f.stem, data
        except Exception:
            continue


def wrap_text(text: str, prefix: str = "    ", width: int = 100) -> str:
    """Word-wrap text with a prefix."""
    words = text.split()
    lines = []
    line = prefix
    for w in words:
        if len(line) + len(w) + 1 > width and line != prefix:
            lines.append(line)
            line = prefix + w
        else:
            line += (" " if line != prefix else "") + w
    if line != prefix:
        lines.append(line)
    return "\n".join(lines)


def format_authors(authors: list, max_n: int = 3) -> str:
    if not authors:
        return ""
    names = [str(a).split(",")[0].strip() for a in authors]
    if len(names) <= max_n:
        return ", ".join(names)
    return ", ".join(names[:max_n]) + " et al."


# ── Paper-level commands ─────────────────────────────────────────────────────


def cmd_abstract(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    abstract = ext.get("abstract", "")
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print(f"By:    {format_authors(ext.get('authors', []))} ({ext.get('year', '')})")
    print()
    if abstract:
        print(wrap_text(abstract, prefix="  ", width=100))
    else:
        print("  (no abstract available)")


def cmd_methodology(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    meth = ext.get("methodology")
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    if not meth:
        print("  (no methodology data — run Pass 3 extraction for this paper)")
        return

    print(f"  Type            : {meth.get('type', '')}")
    if meth.get("model_name"):
        print(f"  Model           : {meth['model_name']}")
    if meth.get("approach"):
        print(f"  Approach        :")
        print(wrap_text(meth["approach"], prefix="    ", width=100))
    if meth.get("temporal_scope"):
        print(f"  Temporal scope  : {meth['temporal_scope']}")
    geo = meth.get("geographic_scope")
    if geo:
        if isinstance(geo, list):
            print(f"  Geographic scope: {', '.join(str(g) for g in geo)}")
        else:
            print(f"  Geographic scope: {geo}")
    if meth.get("unit_of_analysis"):
        print(f"  Unit of analysis: {meth['unit_of_analysis']}")

    scenarios = meth.get("scenarios", [])
    if scenarios:
        print(f"\n  Scenarios ({len(scenarios)}):")
        for s in scenarios:
            print(f"    • {s.get('name', '')}: {s.get('description', '')[:80]}")

    ds = meth.get("data_sources", [])
    if ds:
        print(f"\n  Data sources ({len(ds)}):")
        for d in ds:
            role = f" — {d['role']}" if d.get("role") else ""
            dtype = f" [{d['type']}]" if d.get("type") else ""
            print(f"    • {d.get('name', '')}{dtype}{role}")


def cmd_claims(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    claims = ext.get("claims", [])
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    if not claims:
        print("  (no claims data — run Pass 3 extraction for this paper)")
        return

    # Filter by type if specified
    if args.type:
        claims = [c for c in claims if c.get("type") == args.type]
        if not claims:
            print(f"  No claims of type '{args.type}' found.")
            return

    if args.json:
        print(json.dumps(claims, indent=2, ensure_ascii=False))
        return

    print(f"  Claims ({len(claims)}):\n")
    for i, c in enumerate(claims, 1):
        ctype = c.get("type", "")
        confidence = c.get("confidence", "")
        evidence = c.get("evidence_basis", "")
        quant = c.get("quantification", "")
        supporting = c.get("supporting_citations", [])

        print(f"  {i}. [{ctype}] (confidence: {confidence})")
        print(wrap_text(c.get("claim", ""), prefix="     ", width=100))
        if evidence:
            print(f"     Evidence: {evidence}")
        if quant:
            print(f"     Quantification: {quant}")
        if supporting:
            print(f"     Supporting: {', '.join(str(s) for s in supporting)}")
        print()


def cmd_keywords(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    kw = ext.get("keywords", [])
    topics = ext.get("topics")

    if kw:
        print(f"  Keywords: {', '.join(kw)}")
        print()

    if topics:
        if topics.get("themes"):
            print(f"  Themes:")
            for t in topics["themes"]:
                print(f"    • {t}")
        if topics.get("sectors"):
            print(f"\n  Sectors:")
            for s in topics["sectors"]:
                print(f"    • {s}")
        if topics.get("geographic_focus"):
            print(f"\n  Geographic focus:")
            for g in topics["geographic_focus"]:
                print(f"    • {g}")
        if topics.get("policy_context"):
            print(f"\n  Policy context:")
            for p in topics["policy_context"]:
                print(f"    • {p}")
    elif not kw:
        print("  (no keywords/topics data — run Pass 3 extraction for this paper)")


def cmd_sections(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    sections = ext.get("sections", [])
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    if not sections:
        print("  (no sections data — run Pass 4 extraction for this paper)")
        return

    print(f"  Sections ({len(sections)}):\n")
    for i, s in enumerate(sections, 1):
        heading = s.get("heading", "(untitled)")
        summary = s.get("summary", "")
        print(f"  {i}. {heading}")
        if summary:
            print(wrap_text(summary, prefix="     ", width=100))
        print()


def cmd_section_detail(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    sections = ext.get("sections", [])
    if not sections:
        print("  (no sections data — run Pass 4 extraction for this paper)")
        return

    query = args.heading.lower()
    matches = [s for s in sections if query in s.get("heading", "").lower()]

    if not matches:
        print(f"No section matching '{args.heading}'. Available sections:")
        for s in sections:
            print(f"  • {s.get('heading', '')}")
        return

    for s in matches:
        print(f"Section: {s.get('heading', '')}")
        print()
        summary = s.get("summary", "")
        if summary:
            print("Summary:")
            print(wrap_text(summary, prefix="  ", width=100))
            print()
        text = s.get("annotated_text", "")
        if text:
            print("Annotated text:")
            print(wrap_text(text, prefix="  ", width=100))
        print()


def cmd_questions(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    rqs = ext.get("research_questions", [])
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    if not rqs:
        print("  (no research questions — run Pass 3 extraction for this paper)")
        return

    print("  Research questions:")
    for i, q in enumerate(rqs, 1):
        print(f"    {i}. {q}")


def cmd_data_sources(args):
    pid = resolve_paper_id(args.paper_id)
    ext = load_extraction(pid)
    if not ext:
        print(f"No extraction for {pid}")
        return

    meth = ext.get("methodology")
    print(f"Paper: {pid}")
    print(f"Title: {ext.get('title', '')}")
    print()

    if not meth or not meth.get("data_sources"):
        print("  (no data sources — run Pass 3 extraction for this paper)")
        return

    print(f"  Data sources ({len(meth['data_sources'])}):\n")
    for d in meth["data_sources"]:
        print(f"    • {d.get('name', '')}")
        if d.get("type"):
            print(f"      Type: {d['type']}")
        if d.get("role"):
            print(f"      Role: {d['role']}")
        print()


# ── Cross-corpus commands ────────────────────────────────────────────────────


def cmd_search_methods(args):
    query = args.type.lower()
    results = []
    for pid, ext in all_extractions():
        meth = ext.get("methodology")
        if not meth:
            continue
        mtype = (meth.get("type") or "").lower()
        if query in mtype:
            results.append((pid, ext, meth))

    if not results:
        # Show available types
        types = {}
        for pid, ext in all_extractions():
            meth = ext.get("methodology")
            if meth and meth.get("type"):
                types.setdefault(meth["type"], []).append(pid)
        print(f"No papers with methodology type matching '{args.type}'.")
        if types:
            print(f"Available types:")
            for t, pids in sorted(types.items()):
                print(f"  {t}: {len(pids)} paper(s)")
        return

    print(f"Papers with methodology type matching '{args.type}' ({len(results)} papers):\n")
    for pid, ext, meth in results:
        print(f"  {pid}")
        print(f"    Title    : {ext.get('title', '')[:70]}")
        print(f"    Type     : {meth.get('type', '')}")
        approach = meth.get("approach", "")
        if approach:
            print(f"    Approach : {approach[:120]}")
        geo = meth.get("geographic_scope")
        if geo:
            gstr = ", ".join(geo) if isinstance(geo, list) else str(geo)
            print(f"    Geography: {gstr}")
        temporal = meth.get("temporal_scope")
        if temporal:
            print(f"    Temporal : {temporal}")
        print()


def cmd_search_topics(args):
    phrase = args.phrase.lower()
    results = []
    for pid, ext in all_extractions():
        topics = ext.get("topics")
        if not topics:
            continue
        # Search all topic fields
        haystack = " ".join([
            " ".join(topics.get("themes", [])),
            " ".join(topics.get("sectors", [])),
            " ".join(str(g) for g in topics.get("geographic_focus", [])),
            " ".join(topics.get("policy_context", [])),
        ]).lower()
        if phrase in haystack:
            # Collect which fields matched
            matched_in = []
            for field in ["themes", "sectors", "geographic_focus", "policy_context"]:
                items = topics.get(field, [])
                matching = [str(item) for item in items if phrase in str(item).lower()]
                if matching:
                    matched_in.append((field, matching))
            results.append((pid, ext, matched_in))

    if not results:
        print(f"No papers with topics matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Papers with topics matching '{args.phrase}' ({total} papers){suffix}:\n")
    for pid, ext, matched_in in results:
        print(f"  {pid}")
        print(f"    Title: {ext.get('title', '')[:70]}")
        for field, items in matched_in:
            print(f"    {field}: {', '.join(items)}")
        print()


def cmd_search_claims(args):
    phrase = args.phrase.lower()
    results = []
    for pid, ext in all_extractions():
        claims = ext.get("claims", [])
        for c in claims:
            if args.type and c.get("type") != args.type:
                continue
            claim_text = c.get("claim", "")
            if phrase in claim_text.lower():
                results.append((pid, ext, c))

    if not results:
        print(f"No claims matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Claims matching '{args.phrase}' ({total} across corpus){suffix}:\n")
    for pid, ext, c in results:
        print(f"  [{pid}] {ext.get('title', '')[:60]}")
        print(f"    Type: {c.get('type', '')} | Confidence: {c.get('confidence', '')}")
        print(wrap_text(c.get("claim", ""), prefix="    ", width=100))
        quant = c.get("quantification", "")
        if quant:
            print(f"    Quantification: {quant}")
        print()


def cmd_search_data(args):
    phrase = args.phrase.lower()
    results = []
    for pid, ext in all_extractions():
        meth = ext.get("methodology")
        if not meth:
            continue
        for ds in meth.get("data_sources", []):
            haystack = f"{ds.get('name', '')} {ds.get('type', '')} {ds.get('role', '')}".lower()
            if phrase in haystack:
                results.append((pid, ext, ds))

    if not results:
        print(f"No data sources matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Data sources matching '{args.phrase}' ({total} across corpus){suffix}:\n")
    for pid, ext, ds in results:
        print(f"  [{pid}] {ext.get('title', '')[:55]}")
        print(f"    Source: {ds.get('name', '')}")
        if ds.get("type"):
            print(f"    Type  : {ds['type']}")
        if ds.get("role"):
            print(f"    Role  : {ds['role']}")
        print()


def cmd_search_abstracts(args):
    phrase = args.phrase.lower()
    results = []
    for pid, ext in all_extractions():
        abstract = ext.get("abstract", "")
        if abstract and phrase in abstract.lower():
            results.append((pid, ext, abstract))

    if not results:
        print(f"No abstracts matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Abstracts matching '{args.phrase}' ({total} papers){suffix}:\n")
    for pid, ext, abstract in results:
        print(f"  {pid}")
        print(f"    Title: {ext.get('title', '')[:70]}")
        # Show the matching portion with context
        idx = abstract.lower().find(phrase)
        start = max(0, idx - 80)
        end = min(len(abstract), idx + len(phrase) + 80)
        snippet = ("..." if start > 0 else "") + abstract[start:end] + ("..." if end < len(abstract) else "")
        print(f"    ...{snippet.strip()}...")
        print()


def cmd_search_keywords(args):
    phrase = args.phrase.lower()
    results = []
    for pid, ext in all_extractions():
        kw = ext.get("keywords", [])
        matching = [k for k in kw if phrase in k.lower()]
        if matching:
            results.append((pid, ext, matching))

    if not results:
        print(f"No papers with keywords matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Papers with keywords matching '{args.phrase}' ({total} papers){suffix}:\n")
    for pid, ext, matching in results:
        print(f"  {pid}")
        print(f"    Title   : {ext.get('title', '')[:70]}")
        print(f"    Matching: {', '.join(matching)}")
        print()


def cmd_search_sections(args):
    phrase = args.phrase.lower()
    sidx = load_sections_from_papers()

    if not sidx:
        # Fall back to scanning extractions directly
        results = []
        for pid, ext in all_extractions():
            for sec in ext.get("sections", []):
                heading = sec.get("heading", "")
                summary = sec.get("summary", "")
                if phrase in heading.lower() or phrase in summary.lower():
                    results.append((pid, ext.get("title", ""), heading, summary))
    else:
        results = []
        for entry in sidx["sections"]:
            heading = entry.get("heading", "")
            summary = entry.get("summary", "")
            if phrase in heading.lower() or phrase in summary.lower():
                results.append((
                    entry["paper_id"],
                    entry.get("paper_title", ""),
                    heading,
                    summary,
                ))

    if not results:
        print(f"No sections matching '{args.phrase}'.")
        return

    limit = getattr(args, 'limit', None)
    total = len(results)
    if limit:
        results = results[:limit]
    suffix = f" (showing {len(results)})" if limit and limit < total else ""
    print(f"Sections matching '{args.phrase}' ({total} across corpus){suffix}:\n")
    current_paper = None
    for pid, ptitle, heading, summary in results:
        if pid != current_paper:
            current_paper = pid
            print(f"  [{pid}] {ptitle[:65]}")
        print(f"    {heading}")
        if summary:
            print(wrap_text(summary, prefix="      ", width=100))
        print()


def cmd_search_all(args):
    """Unified search across topics, claims, keywords, abstracts, and sections."""
    phrase = args.phrase.lower()
    limit = getattr(args, 'limit', None) or 30

    # Collect matches per category, tracking unique paper IDs
    topic_papers = []
    claim_papers = []
    keyword_papers = []
    abstract_papers = []
    section_papers = []

    for pid, ext in all_extractions():
        title = ext.get("title", "")

        # Topics
        topics = ext.get("topics")
        if topics:
            haystack = " ".join([
                " ".join(topics.get("themes", [])),
                " ".join(topics.get("sectors", [])),
                " ".join(str(g) for g in topics.get("geographic_focus", [])),
                " ".join(topics.get("policy_context", [])),
            ]).lower()
            if phrase in haystack:
                matched = []
                for field in ["themes", "sectors", "geographic_focus", "policy_context"]:
                    for item in topics.get(field, []):
                        if phrase in str(item).lower():
                            matched.append(f"{field}: {item}")
                topic_papers.append((pid, title, matched))

        # Claims
        for c in ext.get("claims", []):
            if phrase in c.get("claim", "").lower():
                claim_papers.append((pid, title, c.get("type", ""), c.get("claim", "")[:120]))

        # Keywords
        kw_matches = [k for k in ext.get("keywords", []) if phrase in k.lower()]
        if kw_matches:
            keyword_papers.append((pid, title, kw_matches))

        # Abstract
        abstract = ext.get("abstract", "")
        if abstract and phrase in abstract.lower():
            idx = abstract.lower().find(phrase)
            start = max(0, idx - 60)
            end = min(len(abstract), idx + len(phrase) + 60)
            snippet = abstract[start:end]
            abstract_papers.append((pid, title, snippet))

        # Sections
        for sec in ext.get("sections", []):
            heading = sec.get("heading", "")
            summary = sec.get("summary", "")
            if phrase in heading.lower() or phrase in summary.lower():
                section_papers.append((pid, title, heading, summary[:100]))

    # Collect all unique paper IDs
    all_pids = set()
    for items in [topic_papers, claim_papers, keyword_papers, abstract_papers]:
        for item in items:
            all_pids.add(item[0])
    for item in section_papers:
        all_pids.add(item[0])

    if not all_pids:
        print(f"No results for '{args.phrase}' across topics, claims, keywords, abstracts, or sections.")
        return

    # Summary header
    print(f"=== Search-all: '{args.phrase}' ===\n")
    print(f"  Topics    : {len(topic_papers)} paper(s)")
    print(f"  Claims    : {len(claim_papers)} claim(s)")
    print(f"  Keywords  : {len(keyword_papers)} paper(s)")
    print(f"  Abstracts : {len(abstract_papers)} paper(s)")
    print(f"  Sections  : {len(section_papers)} section(s)")
    print(f"  Total     : {len(all_pids)} unique paper(s)")
    print()

    # Detail — limit output
    shown = 0

    if topic_papers:
        print("── Topics ──")
        for pid, title, matched in topic_papers[:limit]:
            print(f"  [{pid}] {title[:65]}")
            for m in matched[:3]:
                print(f"    {m}")
            shown += 1
        print()

    if claim_papers and shown < limit:
        print("── Claims ──")
        for pid, title, ctype, claim_text in claim_papers[:limit - shown]:
            print(f"  [{pid}] {title[:55]}")
            print(f"    [{ctype}] {claim_text}")
            shown += 1
        print()

    if keyword_papers and shown < limit:
        print("── Keywords ──")
        for pid, title, kws in keyword_papers[:limit - shown]:
            print(f"  [{pid}] {title[:55]}  →  {', '.join(kws)}")
            shown += 1
        print()

    if abstract_papers and shown < limit:
        print("── Abstracts ──")
        for pid, title, snippet in abstract_papers[:limit - shown]:
            print(f"  [{pid}] {title[:55]}")
            print(f"    ...{snippet.strip()}...")
            shown += 1
        print()

    if section_papers and shown < limit:
        print("── Sections ──")
        for pid, title, heading, summary in section_papers[:limit - shown]:
            print(f"  [{pid}] {heading}")
            if summary:
                print(f"    {summary}")
            shown += 1
        print()


def cmd_search_text(args):
    phrase = args.phrase.lower()
    context_lines = args.context or 2

    if not TEXT_DIR.exists():
        print("data/text/ directory not found.", file=sys.stderr)
        sys.exit(1)

    results = []
    for txt_file in sorted(TEXT_DIR.glob("*.txt")):
        # Skip chunk files
        if ".part" in txt_file.name:
            continue
        try:
            lines = txt_file.read_text(errors="replace").splitlines()
        except Exception:
            continue
        for i, line in enumerate(lines):
            if phrase in line.lower():
                start = max(0, i - context_lines)
                end = min(len(lines), i + context_lines + 1)
                snippet = lines[start:end]
                results.append((txt_file.stem, i + 1, snippet, i - start))
                if len(results) >= 50:  # cap results
                    break
        if len(results) >= 50:
            break

    if not results:
        print(f"No matches for '{args.phrase}' in paper texts.")
        return

    print(f"Full-text matches for '{args.phrase}' (showing up to 50):\n")
    current_file = None
    for stem, lineno, snippet, match_offset in results:
        if stem != current_file:
            current_file = stem
            print(f"── {stem} ──")
        print(f"  line {lineno}:")
        for j, sline in enumerate(snippet):
            marker = ">>>" if j == match_offset else "   "
            print(f"    {marker} {sline[:150]}")
        print()


# ── Overview commands ────────────────────────────────────────────────────────


def cmd_stats(_args):
    papers = load_papers()
    owned = {pid: p for pid, p in papers.items() if p.get("type") in ("owned", "external_owned")}
    cited = {pid: p for pid, p in papers.items() if p.get("type") == "stub"}

    # Year range
    years = [p.get("year") for p in owned.values() if p.get("year")]
    int_years = [int(y) for y in years if str(y).isdigit()]

    print("=== Corpus Statistics ===\n")
    print(f"  Total papers   : {len(papers)}")
    print(f"  Owned papers   : {len(owned)}")
    print(f"  Cited-only     : {len(cited)}")
    if int_years:
        print(f"  Year range     : {min(int_years)} – {max(int_years)} (owned papers)")
    print()

    # Methodology breakdown from extractions
    meth_types = {}
    has_pass3 = 0
    has_pass4 = 0
    total_claims = 0
    total_data_sources = 0
    all_themes = {}
    all_sectors = {}
    all_keywords = {}

    for pid, ext in all_extractions():
        meth = ext.get("methodology")
        if meth:
            has_pass3 += 1
            mtype = meth.get("type", "unknown")
            meth_types.setdefault(mtype, []).append(pid)
            for ds in meth.get("data_sources", []):
                total_data_sources += 1
        claims = ext.get("claims", [])
        total_claims += len(claims)

        if ext.get("sections"):
            has_pass4 += 1

        topics = ext.get("topics")
        if topics:
            for t in topics.get("themes", []):
                all_themes[t] = all_themes.get(t, 0) + 1
            for s in topics.get("sectors", []):
                all_sectors[s] = all_sectors.get(s, 0) + 1

        for k in ext.get("keywords", []):
            all_keywords[k] = all_keywords.get(k, 0) + 1

    print(f"  Extractions    : {sum(1 for _ in EXTRACTIONS_DIR.glob('*.json'))}")
    print(f"  With Pass 3    : {has_pass3} (methodology, claims, topics)")
    print(f"  With Pass 4    : {has_pass4} (sections)")
    print(f"  Total claims   : {total_claims}")
    print(f"  Data sources   : {total_data_sources}")
    print()

    if meth_types:
        print("  Methodology types:")
        for mtype, pids in sorted(meth_types.items(), key=lambda x: -len(x[1])):
            print(f"    {mtype:<15} {len(pids):>3} papers")
        print()

    if all_themes:
        top_themes = sorted(all_themes.items(), key=lambda x: -x[1])[:15]
        print("  Top themes:")
        for theme, count in top_themes:
            print(f"    ({count}x) {theme}")
        print()

    if all_sectors:
        top_sectors = sorted(all_sectors.items(), key=lambda x: -x[1])[:10]
        print("  Top sectors:")
        for sector, count in top_sectors:
            print(f"    ({count}x) {sector}")
        print()

    if all_keywords:
        top_kw = sorted(all_keywords.items(), key=lambda x: -x[1])[:15]
        print("  Top keywords:")
        for kw, count in top_kw:
            print(f"    ({count}x) {kw}")


def cmd_list(args):
    results = []
    for pid, ext in all_extractions():
        year = ext.get("year")
        int_year = int(year) if year and str(year).isdigit() else None

        if args.year_min and (not int_year or int_year < args.year_min):
            continue
        if args.year_max and (not int_year or int_year > args.year_max):
            continue
        if args.method:
            meth = ext.get("methodology")
            if not meth or args.method.lower() not in (meth.get("type") or "").lower():
                continue
        if args.has:
            field = args.has
            if field == "abstract" and not ext.get("abstract"):
                continue
            elif field == "methodology" and not ext.get("methodology"):
                continue
            elif field == "claims" and not ext.get("claims"):
                continue
            elif field == "sections" and not ext.get("sections"):
                continue
            elif field == "keywords" and not ext.get("keywords"):
                continue
        results.append((pid, ext))

    if not results:
        print("No papers match the given filters.")
        return

    if args.json:
        out = []
        for pid, ext in results:
            out.append({
                "id": pid,
                "title": ext.get("title", ""),
                "authors": ext.get("authors", []),
                "year": ext.get("year"),
                "journal": ext.get("journal"),
                "has_methodology": bool(ext.get("methodology")),
                "has_claims": bool(ext.get("claims")),
                "has_sections": bool(ext.get("sections")),
                "has_keywords": bool(ext.get("keywords")),
                "methodology_type": (ext.get("methodology") or {}).get("type"),
                "citation_count": len(ext.get("citations", [])),
            })
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return

    filters = []
    if args.year_min:
        filters.append(f"year≥{args.year_min}")
    if args.year_max:
        filters.append(f"year≤{args.year_max}")
    if args.method:
        filters.append(f"method={args.method}")
    if args.has:
        filters.append(f"has={args.has}")
    filter_str = f" [{', '.join(filters)}]" if filters else ""
    print(f"Owned papers ({len(results)}){filter_str}:\n")

    for pid, ext in results:
        authors = format_authors(ext.get("authors", []))
        year = ext.get("year", "")
        title = ext.get("title", "")[:65]
        meth = ext.get("methodology")
        mtype = f" [{meth['type']}]" if meth and meth.get("type") else ""
        ncites = len(ext.get("citations", []))
        print(f"  {pid}")
        print(f"    {authors} ({year}) — {title}")
        print(f"    {ncites} refs{mtype}")
        print()


def cmd_purposes(_args):
    index = load_index()
    if not index:
        print("data/db/contexts.json not found. Run: python3 scripts/query/query_db.py --rebuild")
        return

    by_purpose = index.get("by_purpose", {})
    print(f"Citation purposes across corpus ({len(by_purpose)} tags):\n")
    for purpose, entries in sorted(by_purpose.items(), key=lambda x: -len(x[1])):
        print(f"  {purpose:<25} {len(entries):>4} contexts")


def cmd_methods(_args):
    types = {}
    for pid, ext in all_extractions():
        meth = ext.get("methodology")
        if meth and meth.get("type"):
            types.setdefault(meth["type"], []).append(pid)

    if not types:
        print("No methodology data found. Run Pass 3 extraction on papers.")
        return

    print(f"Methodology types across corpus ({sum(len(v) for v in types.values())} papers with data):\n")
    for mtype, pids in sorted(types.items(), key=lambda x: -len(x[1])):
        print(f"  {mtype:<15} ({len(pids)} papers)")
        for pid in pids:
            print(f"    • {pid}")
        print()


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        prog="corpus.py",
        description="Query extraction-level data across the literature corpus.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # Paper-level
    p = sub.add_parser("abstract", help="Show paper abstract")
    p.add_argument("paper_id")

    p = sub.add_parser("methodology", help="Show methodology details")
    p.add_argument("paper_id")

    p = sub.add_parser("claims", help="List claims with evidence")
    p.add_argument("paper_id")
    p.add_argument("--type", choices=["empirical_finding", "theoretical_argument",
                                       "methodological_contribution", "policy_recommendation"])
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("keywords", help="Show keywords and topics")
    p.add_argument("paper_id")

    p = sub.add_parser("sections", help="List section headings with summaries")
    p.add_argument("paper_id")

    p = sub.add_parser("section", help="Show full section detail")
    p.add_argument("paper_id")
    p.add_argument("heading", help="Substring of section heading to match")

    p = sub.add_parser("questions", help="Show research questions")
    p.add_argument("paper_id")

    p = sub.add_parser("data-sources", help="Show data sources used")
    p.add_argument("paper_id")

    # Cross-corpus
    p = sub.add_parser("search-all", help="Unified search across topics, claims, keywords, abstracts, sections")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, default=30, help="Max results to show (default 30)")

    p = sub.add_parser("search-methods", help="Papers by methodology type")
    p.add_argument("type", help="Methodology type (simulation, empirical, etc.)")

    p = sub.add_parser("search-topics", help="Papers by topic phrase")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, help="Max papers to show")

    p = sub.add_parser("search-claims", help="Search claims across corpus")
    p.add_argument("phrase")
    p.add_argument("--type", choices=["empirical_finding", "theoretical_argument",
                                       "methodological_contribution", "policy_recommendation"])
    p.add_argument("--limit", type=int, help="Max claims to show")

    p = sub.add_parser("search-data", help="Search data sources across corpus")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, help="Max results to show")

    p = sub.add_parser("search-abstracts", help="Search abstracts across corpus")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, help="Max papers to show")

    p = sub.add_parser("search-keywords", help="Search keywords across corpus")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, help="Max papers to show")

    p = sub.add_parser("search-sections", help="Search section headings and summaries")
    p.add_argument("phrase")
    p.add_argument("--limit", type=int, help="Max sections to show")

    p = sub.add_parser("search-text", help="Full-text search in paper texts")
    p.add_argument("phrase")
    p.add_argument("--context", type=int, default=2, help="Context lines (default 2)")

    # Overview
    sub.add_parser("stats", help="Corpus summary statistics")

    p = sub.add_parser("list", help="List owned papers with filters")
    p.add_argument("--year-min", type=int)
    p.add_argument("--year-max", type=int)
    p.add_argument("--method", help="Filter by methodology type")
    p.add_argument("--has", choices=["methodology", "claims", "sections", "keywords", "abstract"])
    p.add_argument("--json", action="store_true")

    sub.add_parser("purposes", help="List citation purpose tags with counts")
    sub.add_parser("methods", help="List methodology types with paper counts")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cmd_map = {
        "abstract": cmd_abstract,
        "methodology": cmd_methodology,
        "claims": cmd_claims,
        "keywords": cmd_keywords,
        "sections": cmd_sections,
        "section": cmd_section_detail,
        "questions": cmd_questions,
        "data-sources": cmd_data_sources,
        "search-all": cmd_search_all,
        "search-methods": cmd_search_methods,
        "search-topics": cmd_search_topics,
        "search-claims": cmd_search_claims,
        "search-data": cmd_search_data,
        "search-abstracts": cmd_search_abstracts,
        "search-keywords": cmd_search_keywords,
        "search-sections": cmd_search_sections,
        "search-text": cmd_search_text,
        "stats": cmd_stats,
        "list": cmd_list,
        "purposes": cmd_purposes,
        "methods": cmd_methods,
    }

    fn = cmd_map.get(args.command)
    if fn:
        fn(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
