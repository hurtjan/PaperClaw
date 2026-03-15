#!/usr/bin/env python3
"""
Query the literature database. Reads data/db/contexts.json (fast) and data/db/papers.json.

Usage:
  python3 scripts/query/query_db.py --cites <paper_id>
      Which owned papers cite this paper, with purpose and quote.

  python3 scripts/query/query_db.py --purpose <purpose>
      All citation contexts with this purpose tag across the corpus.
      Purposes: background, motivation, methodology, data_source,
                supporting_evidence, contrasting_evidence, comparison,
                extension, tool_software

  python3 scripts/query/query_db.py --top-cited [N]
      Top N most cited papers by cited_by count (default 15).

  python3 scripts/query/query_db.py --search <phrase> [--filter-purpose TAG] [--limit N]
      Search for a phrase in citation quotes and titles across all contexts.
      --filter-purpose narrows to a specific purpose tag (compound filter).
      --limit caps output to N results.

  python3 scripts/query/query_db.py --paper <paper_id>
      Summary for one paper: metadata, cites, cited_by, top citers.

  python3 scripts/query/query_db.py --owned
      List all owned papers with cites/cited_by counts.

  python3 scripts/query/query_db.py --author <name>
      Search authors by name. Shows papers, coauthors, and name variants.

  python3 scripts/query/query_db.py --text-file <paper_id>
      Print the text file path for an owned paper (from papers.json text_file field).
      Useful for grepping full text. Partial ID match supported.

  python3 scripts/query/query_db.py --purpose <purpose> [--limit N]
      --limit caps output to N results.

  python3 scripts/query/query_db.py --cites <paper_id> [--limit N]
      --limit caps output to N citing papers.

  python3 scripts/query/query_db.py --forward-cited-by <paper_id> [<paper_id> ...]
      Papers that cite these owned papers via Semantic Scholar forward citations.
      Ranked by relevance score (seed overlap, journal prestige, topic keywords, recency).
      Use --year-min to filter and --top to limit output.

  python3 scripts/query/query_db.py --rebuild
      Rebuild data/db/contexts.json from extractions (run after adding a new paper).
"""

import json
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
INDEX_FILE = ROOT / "data" / "db" / "contexts.json"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
AUTHORS_FILE = ROOT / "data" / "db" / "authors.json"


def load_index():
    if not INDEX_FILE.exists():
        print("data/db/contexts.json not found. Run: python3 scripts/query/query_db.py --rebuild")
        sys.exit(1)
    return json.loads(INDEX_FILE.read_text())


def load_papers():
    return json.loads(PAPERS_FILE.read_text())["papers"]


def cmd_cites(args, index, papers):
    """Which owned papers cite a given paper_id."""
    target = args.cites
    entries = index["by_cited"].get(target)

    if not entries:
        # Try partial match
        matches = [k for k in index["by_cited"] if target.lower() in k.lower()]
        if not matches:
            print(f"No citation contexts found for: {target}")
            return
        if len(matches) == 1:
            target = matches[0]
            entries = index["by_cited"][target]
            print(f"(matched: {target})\n")
        else:
            print(f"Multiple matches for '{target}':")
            for m in matches:
                print(f"  {m}")
            return

    p = papers.get(target, {})
    title = p.get("title", target)
    authors = p.get("authors", [])
    year = p.get("year", "")
    print(f"Citations to: {target}")
    print(f"  Title:   {title}")
    if authors:
        print(f"  Authors: {', '.join(str(a) for a in authors[:3])}{' et al.' if len(authors) > 3 else ''}")
    if year:
        print(f"  Year:    {year}")
    print(f"  Cited {len(entries)} time(s) across the corpus\n")

    # Group by citing paper
    by_citing = {}
    for e in entries:
        by_citing.setdefault(e["citing"], []).append(e)

    limit = getattr(args, 'limit', None)
    items = sorted(by_citing.items())
    total = len(items)
    if limit:
        items = items[:limit]
    if limit and limit < total:
        print(f"  (showing {len(items)} of {total} citing papers)\n")

    for citing_id, ctxs in items:
        cp = papers.get(citing_id, {})
        print(f"  [{citing_id}] {cp.get('title', citing_id)[:70]}")
        for ctx in ctxs:
            print(f"    purpose     : {ctx['purpose']}")
            print(f"    section     : {ctx['section']}")
            print(f"    quote       : \"{ctx['quote'][:120]}\"")
            if ctx.get('explanation'):
                print(f"    explanation : {ctx['explanation']}")
            print()


def cmd_purpose(args, index, papers):
    """All citation contexts with a given purpose tag."""
    purpose = args.purpose
    entries = index["by_purpose"].get(purpose)

    if not entries:
        available = sorted(index["by_purpose"].keys())
        print(f"No contexts found for purpose: '{purpose}'")
        print(f"Available: {', '.join(available)}")
        return

    limit = getattr(args, 'limit', None)
    total = len(entries)
    if limit:
        entries = entries[:limit]
    shown = len(entries)
    suffix = f" (showing {shown} of {total})" if limit and limit < total else ""
    print(f"Purpose: {purpose} — {total} context(s){suffix}\n")
    for e in entries:
        citing_p = papers.get(e["citing"], {})
        cited_p = papers.get(e["cited"], {})
        print(f"  {e['citing']} → {e['cited']}")
        print(f"    cited title : {cited_p.get('title', e['cited_title'])[:70]}")
        print(f"    section     : {e['section']}")
        print(f"    quote       : \"{e['quote'][:120]}\"")
        if e.get('explanation'):
            print(f"    explanation : {e['explanation']}")
        print()


def cmd_top_cited(args, index, papers):
    """Top N most cited papers."""
    n = args.top_cited if args.top_cited else 15
    counts = index["citation_counts"]
    top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]

    print(f"Top {n} most cited papers (by cited_by count):\n")
    for i, (pid, count) in enumerate(top, 1):
        p = papers.get(pid, {})
        ptype = p.get("type", "?")
        title = p.get("title", pid)
        year = p.get("year", "")
        authors = p.get("authors", [])
        first_author = str(authors[0]).split(",")[0] if authors else ""
        label = f"{first_author} {year}".strip() if first_author else pid
        print(f"  {i:2d}. [{count:2d}x] {label} — {title[:60]}")
        if ptype in ("owned", "external_owned"):
            print(f"        (owned paper)")


def cmd_search(args, index, papers):
    """Search for a phrase across all citation quotes and titles."""
    phrase = args.search.lower()
    filter_purpose = getattr(args, 'filter_purpose', None)
    matches = []

    for entries in index["by_cited"].values():
        for e in entries:
            if filter_purpose and e.get("purpose") != filter_purpose:
                continue
            if (phrase in e["quote"].lower() or
                    phrase in e["cited_title"].lower() or
                    phrase in e["section"].lower()):
                matches.append(e)

    if not matches:
        extra = f" with purpose={filter_purpose}" if filter_purpose else ""
        print(f"No matches for: '{args.search}'{extra}")
        return

    limit = getattr(args, 'limit', None)
    total = len(matches)

    # Deduplicate
    deduped = []
    seen = set()
    for e in matches:
        key = (e["citing"], e["cited"], e["quote"][:40])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)

    total_deduped = len(deduped)
    if limit:
        deduped = deduped[:limit]
    shown = len(deduped)
    filter_str = f" [purpose={filter_purpose}]" if filter_purpose else ""
    suffix = f" (showing {shown} of {total_deduped})" if limit and limit < total_deduped else ""
    print(f"Search: '{args.search}'{filter_str} — {total_deduped} match(es){suffix}\n")

    for e in deduped:
        cited_p = papers.get(e["cited"], {})
        owned_tag = "[OWNED]" if cited_p.get("type") in ("owned", "external_owned") else "[cited]"
        print(f"  {e['citing']} → {e['cited']} {owned_tag}")
        print(f"    title       : {cited_p.get('title', e['cited_title'])[:70]}")
        print(f"    purpose     : {e['purpose']}")
        print(f"    quote       : \"{e['quote'][:120]}\"")
        if e.get('explanation'):
            print(f"    explanation : {e['explanation']}")
        print()


def cmd_paper(args, index, papers):
    """Summary for a single paper."""
    pid = args.paper
    if pid not in papers:
        matches = [k for k in papers if args.paper.lower() in k.lower()]
        if not matches:
            print(f"Paper not found: {pid}")
            return
        if len(matches) == 1:
            pid = matches[0]
            print(f"(matched: {pid})\n")
        else:
            print(f"Multiple matches:")
            for m in matches:
                print(f"  {m}")
            return

    p = papers[pid]
    print(f"Paper: {pid}")
    print(f"  Type    : {p.get('type')}")
    print(f"  Title   : {p.get('title', '')}")
    authors = p.get("authors", [])
    print(f"  Authors : {', '.join(str(a) for a in authors)}")
    print(f"  Year    : {p.get('year', '')}")
    print(f"  Journal : {p.get('journal', '')}")
    print(f"  DOI     : {p.get('doi', '')}")
    tf = p.get("text_file", "")
    if tf:
        print(f"  Text    : {tf}")
    print()

    cites = p.get("cites", [])
    cited_by = p.get("cited_by", [])
    print(f"  Cites    : {len(cites)} papers")
    print(f"  Cited by : {len(cited_by)} papers")
    if cited_by:
        print(f"    → {', '.join(cited_by)}")
    print()

    # Show how this paper is cited (if it appears in index)
    ctxs = index["by_cited"].get(pid, [])
    if ctxs:
        purposes = {}
        for c in ctxs:
            purposes.setdefault(c["purpose"], 0)
            purposes[c["purpose"]] += 1
        print(f"  Citation contexts ({len(ctxs)} total):")
        for purpose, count in sorted(purposes.items(), key=lambda x: -x[1]):
            print(f"    {purpose}: {count}")


def cmd_owned(args, index, papers):
    """List all owned papers."""
    owned = index["owned_papers"]
    print(f"Owned papers ({len(owned)}):\n")
    for p in owned:
        authors = p.get("authors", [])
        first_author = str(authors[0]).split(",")[0] if authors else ""
        print(f"  {p['id']}")
        print(f"    {first_author} et al. ({p.get('year', '')}) — {p.get('title', '')[:65]}")
        print(f"    cites {p['cites_count']} | cited_by {p['cited_by_count']}")
        print()


def cmd_pdfs(papers):
    """List owned papers and whether their PDF exists on disk."""
    owned = {pid: p for pid, p in papers.items() if p.get("type") in ("owned", "external_owned")}
    have, missing = [], []
    for pid, p in sorted(owned.items()):
        pdf = p.get("pdf_file", "")
        exists = pdf and (ROOT / pdf).exists()
        (have if exists else missing).append((pid, p, pdf))

    print(f"Owned papers: {len(owned)} total, {len(have)} with PDF, {len(missing)} missing\n")

    if missing:
        print("MISSING PDFs:")
        for pid, p, pdf in missing:
            authors = p.get("authors", [])
            first = str(authors[0]).split(",")[0] if authors else ""
            print(f"  ✗ {pid}  ({first}, {p.get('year','')}) — {p.get('title','')[:60]}")
            if pdf:
                print(f"      expected: {pdf}")
        print()

    print("Have PDFs:")
    for pid, p, pdf in have:
        authors = p.get("authors", [])
        first = str(authors[0]).split(",")[0] if authors else ""
        print(f"  ✓ {pid}  ({first}, {p.get('year','')}) — {p.get('title','')[:60]}")


def cmd_author(args, papers):
    """Search authors by name and show their papers and coauthors."""
    if not AUTHORS_FILE.exists():
        print("data/db/authors.json not found. Run: .venv/bin/python3 scripts/build/build_authors.py")
        return

    authors_data = json.loads(AUTHORS_FILE.read_text())
    persons = authors_data["persons"]
    query = args.author.lower()

    # Find matching authors
    matches = []
    for aid, a in persons.items():
        if (query in aid or
            query in a["canonical_name"].lower() or
            any(query in v.lower() for v in a["name_variants"])):
            matches.append(a)

    if not matches:
        # Try institutions
        institutions = authors_data.get("institutions", {})
        inst_matches = [inst for iid, inst in institutions.items()
                        if query in iid or query in inst["name"].lower()]
        if inst_matches:
            for inst in inst_matches:
                print(f"Institution: {inst['name']}")
                print(f"  Papers ({inst['paper_count']}):")
                for pid in inst["papers"]:
                    p = papers.get(pid, {})
                    print(f"    {p.get('title', pid)[:70]} ({p.get('year', '')})")
                print()
            return
        print(f"No authors matching: '{args.author}'")
        return

    for a in sorted(matches, key=lambda x: -x["paper_count"]):
        owned_tag = f" ({a['owned_paper_count']} owned)" if a["owned_paper_count"] else ""
        print(f"Author: {a['canonical_name']}  [{a['id']}]")
        print(f"  Papers: {a['paper_count']}{owned_tag}")
        if len(a["name_variants"]) > 1:
            print(f"  Name variants: {', '.join(a['name_variants'])}")
        print()

        # Show papers grouped by owned/cited
        if a["owned_papers"]:
            print(f"  Owned papers:")
            for pid in a["owned_papers"]:
                p = papers.get(pid, {})
                print(f"    {p.get('title', pid)[:70]} ({p.get('year', '')})")

        cited = [pid for pid in a["papers"] if pid not in a["owned_papers"]]
        if cited:
            print(f"  Cited-only papers ({len(cited)}):")
            for pid in cited[:10]:
                p = papers.get(pid, {})
                print(f"    {p.get('title', pid)[:70]} ({p.get('year', '')})")
            if len(cited) > 10:
                print(f"    ... and {len(cited) - 10} more")
        print()

        # Top coauthors
        if a["coauthors"]:
            coauth_details = []
            for caid in a["coauthors"]:
                ca = persons.get(caid)
                if ca:
                    # Count shared papers
                    shared = len(set(a["papers"]) & set(ca["papers"]))
                    coauth_details.append((ca["canonical_name"], shared))
            coauth_details.sort(key=lambda x: -x[1])
            print(f"  Top coauthors ({len(a['coauthors'])} total):")
            for name, shared in coauth_details[:10]:
                print(f"    {name:30s}  ({shared} shared papers)")
        print()


def cmd_text_file(args, papers):
    """Print the text file path for an owned paper."""
    pid = args.text_file
    if pid not in papers:
        matches = [k for k in papers if pid.lower() in k.lower()]
        if not matches:
            print(f"Paper not found: {pid}")
            return
        if len(matches) == 1:
            pid = matches[0]
        else:
            print(f"Multiple matches:")
            for m in matches:
                print(f"  {m}")
            return

    p = papers[pid]
    tf = p.get("text_file", "")
    if tf:
        full_path = ROOT / tf
        print(full_path)
    else:
        print(f"No text_file for {pid}", file=sys.stderr)
        sys.exit(1)


def _load_scoring_config():
    """Load prestige_journals and topic_keywords from project.yaml (if present)."""
    try:
        import yaml
        config_path = ROOT / "project.yaml"
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}
            scoring = config.get("scoring", {})
            return (
                scoring.get("prestige_journals", []),
                scoring.get("topic_keywords", []),
            )
    except Exception:
        pass
    return [], []

_PRESTIGE_JOURNALS, _TOPIC_KEYWORDS = _load_scoring_config()


def _resolve_paper_id(pid, papers):
    """Return canonical paper ID or exit with an error message."""
    if pid in papers:
        return pid
    matches = [k for k in papers if pid.lower() in k.lower()]
    if not matches:
        print(f"Paper not found: {pid}", file=sys.stderr)
        sys.exit(1)
    if len(matches) == 1:
        print(f"(matched '{pid}' → {matches[0]})")
        return matches[0]
    print(f"Ambiguous ID '{pid}', matches:", file=sys.stderr)
    for m in matches:
        print(f"  {m}", file=sys.stderr)
    sys.exit(1)


def cmd_forward_cited_by(args, papers):
    """Papers that cite these owned papers via Semantic Scholar forward citations."""
    seeds = [_resolve_paper_id(sid, papers) for sid in args.forward_cited_by]

    # Collect forward_cited_by entries across all seeds
    fwd_seeds: dict[str, list[str]] = {}
    for sid in seeds:
        fwd = papers.get(sid, {}).get("forward_cited_by", [])
        if not fwd:
            print(f"  (no forward citations recorded for {sid})")
        for fid in fwd:
            fwd_seeds.setdefault(fid, []).append(sid)

    if not fwd_seeds:
        print("No forward citations found for any seed paper.")
        return

    year_min = args.year_min or 0
    results = []
    for fid, seed_list in fwd_seeds.items():
        p = papers.get(fid, {})
        try:
            yr = int(p.get("year", 0))
        except (ValueError, TypeError):
            yr = 0
        if year_min and yr < year_min:
            continue
        results.append({
            "id": fid,
            "title": p.get("title", ""),
            "authors": p.get("authors", []),
            "year": yr,
            "journal": p.get("journal", ""),
            "doi": p.get("doi", ""),
            "type": p.get("type", ""),
            "seeds": seed_list,
        })

    if not results:
        msg = f"year_min={year_min}" if year_min else "no filters"
        print(f"No forward-cited papers found ({msg}).")
        return

    def score(r):
        j = r["journal"].lower()
        t = r["title"].lower()
        pres = sum(3 for pj in _PRESTIGE_JOURNALS if pj in j)
        rel = sum(1 for k in _TOPIC_KEYWORDS if k in t or k in j)
        yr_bonus = max(0, r["year"] - 2022) if r["year"] else 0
        return len(r["seeds"]) * 5 + pres + rel + yr_bonus

    for r in results:
        r["score"] = score(r)
    results.sort(key=lambda x: (-x["score"], -(x["year"] or 0), x["title"]))

    top_n = args.top or 20
    results = results[:top_n]

    # Short label per seed for display
    seed_label = {}
    for sid in seeds:
        parts = sid.split("_")
        seed_label[sid] = parts[2] if len(parts) >= 3 else sid

    print(f"Forward citations for: {', '.join(seeds)}")
    total = len(fwd_seeds)
    shown = len(results)
    print(f"  Total forward-cited papers: {total}", end="")
    if year_min:
        print(f" ({shown} from {year_min}+)", end="")
    print(f"  |  Showing top {shown} by score\n")

    for i, r in enumerate(results, 1):
        seed_short = "+".join(seed_label[s] for s in r["seeds"])
        authors = r["authors"]
        if isinstance(authors, list):
            first = str(authors[0]).split(",")[0] if authors else ""
        else:
            first = str(authors).split(";")[0].split(",")[0] if authors else ""
        label = f"{first} {r['year']}".strip() if first else r["id"]
        owned_tag = " [OWNED]" if r["type"] in ("owned", "external_owned") else ""
        print(f"  {i:2d}. [score {r['score']:2d}] {label} — {r['title'][:65]}{owned_tag}")
        print(f"        Seeds: {seed_short} | Journal: {r['journal'][:55]}")
        if r["doi"]:
            print(f"        DOI: {r['doi']}")
        print()


def cmd_rebuild():
    """Rebuild the index."""
    import subprocess
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
        cwd=ROOT
    )
    sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Query the literature database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--cites", metavar="PAPER_ID",
                       help="Which owned papers cite this paper")
    group.add_argument("--purpose", metavar="PURPOSE",
                       help="All contexts with this purpose tag")
    group.add_argument("--top-cited", metavar="N", nargs="?", const=15, type=int,
                       help="Top N most cited papers (default 15)")
    group.add_argument("--search", metavar="PHRASE",
                       help="Search phrase in quotes, titles, sections")
    group.add_argument("--paper", metavar="PAPER_ID",
                       help="Summary for one paper")
    group.add_argument("--owned", action="store_true",
                       help="List all owned papers")
    group.add_argument("--author", metavar="NAME",
                       help="Search authors by name")
    group.add_argument("--pdfs", action="store_true",
                       help="List owned papers and whether their PDF exists on disk")
    group.add_argument("--text-file", metavar="PAPER_ID",
                       help="Print the text file path for an owned paper")
    group.add_argument("--forward-cited-by", metavar="PAPER_ID", nargs="+",
                       help="Papers citing these owned papers (S2 forward citations)")
    group.add_argument("--rebuild", action="store_true",
                       help="Rebuild data/db/contexts.json from extractions")

    parser.add_argument("--year-min", metavar="N", type=int,
                        help="Filter --forward-cited-by to papers from this year onwards")
    parser.add_argument("--top", metavar="N", type=int,
                        help="Limit --forward-cited-by output to top N results (default 20)")
    parser.add_argument("--limit", metavar="N", type=int,
                        help="Limit output to N results (works with --search, --purpose, --cites)")
    parser.add_argument("--filter-purpose", metavar="TAG",
                        help="Filter --search results to a specific purpose tag")

    args = parser.parse_args()

    if args.rebuild:
        cmd_rebuild()
        return

    if args.forward_cited_by:
        cmd_forward_cited_by(args, load_papers())
        return

    if args.pdfs:
        cmd_pdfs(load_papers())
        return

    if args.text_file:
        cmd_text_file(args, load_papers())
        return

    if args.author:
        cmd_author(args, load_papers())
        return

    index = load_index()

    if args.cites:
        cmd_cites(args, index, load_papers())
    elif args.purpose:
        cmd_purpose(args, index, load_papers())
    elif args.top_cited is not None:
        cmd_top_cited(args, index, load_papers())
    elif args.search:
        cmd_search(args, index, load_papers())
    elif args.paper:
        cmd_paper(args, index, load_papers())
    elif args.owned:
        cmd_owned(args, index, None)


if __name__ == "__main__":
    main()
