#!/usr/bin/env python3
"""
Query the research findings store. Reads research/*.json files.

Usage:
  python3 scripts/query/research.py list
      Table: id | title | date | #papers | tags

  python3 scripts/query/research.py show <id>
      Full finding: metadata + paper list with groups and relevance notes

  python3 scripts/query/research.py search <term>
      Findings where term appears in title/question/notes/paper titles

  python3 scripts/query/research.py papers <id>
      Paper list with in_db/owned flags, year, relevance note

  python3 scripts/query/research.py missing <id>
      Papers in finding that are NOT in data/db/papers.json (candidates to add)

  python3 scripts/query/research.py for-paper <paper_id>
      Which findings mention this paper, with its relevance note in each

  python3 scripts/query/research.py overlap <id1> <id2>
      Papers appearing in both findings

  python3 scripts/query/research.py tags
      All tags across all findings with finding counts
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
RESEARCH_DIR = ROOT / "research"
PAPERS_FILE = ROOT / "data" / "db" / "papers.json"


def load_findings():
    if not RESEARCH_DIR.exists():
        return {}
    findings = {}
    for f in sorted(RESEARCH_DIR.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            findings[data["id"]] = data
        except Exception as e:
            print(f"Warning: could not load {f.name}: {e}", file=sys.stderr)
    return findings


def load_db_papers():
    if not PAPERS_FILE.exists():
        return {}
    return json.loads(PAPERS_FILE.read_text())["papers"]


def resolve_db_flags(paper_entry, db_papers):
    """Check in_db and owned against the live DB (overrides stored values)."""
    pid = paper_entry.get("paper_id")
    if not pid:
        return False, False
    p = db_papers.get(pid)
    if p is None:
        return False, False
    return True, p.get("type") in ("owned", "external_owned")


def cmd_list(findings):
    if not findings:
        print("No findings found in research/")
        return
    print(f"{'ID':<35} {'Updated':<12} {'#Papers':<8} Tags")
    print("-" * 85)
    for fid, f in findings.items():
        tags = ", ".join(f.get("tags", []))[:40]
        n = len(f.get("papers", []))
        updated = f.get("updated", f.get("created", ""))
        print(f"{fid:<35} {updated:<12} {n:<8} {tags}")
    print()
    for fid, f in findings.items():
        print(f"  {fid}: {f.get('title', '')}")


def cmd_show(fid, findings, db_papers):
    f = findings.get(fid)
    if f is None:
        matches = [k for k in findings if fid.lower() in k.lower()]
        if len(matches) == 1:
            f = findings[matches[0]]
            fid = matches[0]
            print(f"(matched: {fid})\n")
        elif matches:
            print(f"Multiple matches for '{fid}':")
            for m in matches:
                print(f"  {m}")
            return
        else:
            print(f"Finding not found: {fid}")
            return

    print(f"Finding: {f['id']}")
    print(f"  Title   : {f.get('title', '')}")
    print(f"  Question: {f.get('question', '')}")
    print(f"  Method  : {f.get('method', '')}  |  Created: {f.get('created', '')}  |  Updated: {f.get('updated', '')}")
    print(f"  Tags    : {', '.join(f.get('tags', []))}")
    print()

    papers = f.get("papers", [])
    # Group by group field
    groups = {}
    for p in papers:
        g = p.get("group", "ungrouped")
        groups.setdefault(g, []).append(p)

    owned_count = sum(1 for p in papers if resolve_db_flags(p, db_papers)[1])
    in_db_count = sum(1 for p in papers if resolve_db_flags(p, db_papers)[0])
    print(f"  Papers  : {len(papers)} total, {owned_count} owned, {in_db_count - owned_count} cited-only, {len(papers) - in_db_count} not in DB")
    print()

    for group, group_papers in groups.items():
        print(f"  [{group}]")
        for p in group_papers:
            in_db, owned = resolve_db_flags(p, db_papers)
            if owned:
                flag = "[OWNED]"
            elif in_db:
                flag = "[cited]"
            else:
                flag = "[!DB ]"
            pid_str = p.get("paper_id") or "(no ID)"
            authors = p.get("authors", "")
            year = p.get("year", "")
            title = p.get("title", "")[:65]
            print(f"    {flag} {authors} ({year}) — {title}")
            print(f"           {pid_str}")
            note = p.get("relevance_note", "")
            if note:
                # Wrap at 90 chars
                words = note.split()
                lines = []
                current = "           Note: "
                for w in words:
                    if len(current) + len(w) + 1 > 100:
                        lines.append(current)
                        current = "                 " + w
                    else:
                        current += (" " if current.endswith(": ") or current.endswith("  ") else " ") + w if not current.endswith(": ") else w
                lines.append(current)
                for line in lines:
                    print(line)
            print()
        print()

    summary = f.get("summary", "")
    if summary:
        print("  Summary:")
        words = summary.split()
        line = "    "
        for w in words:
            if len(line) + len(w) + 1 > 100:
                print(line)
                line = "    " + w
            else:
                line += (" " if line != "    " else "") + w
        print(line)
        print()

    notes = f.get("notes", "")
    if notes:
        print("  Notes:")
        words = notes.split()
        line = "    "
        for w in words:
            if len(line) + len(w) + 1 > 100:
                print(line)
                line = "    " + w
            else:
                line += (" " if line != "    " else "") + w
        print(line)
        print()


def cmd_search(term, findings):
    term_lower = term.lower()
    results = []
    for fid, f in findings.items():
        haystack = " ".join([
            f.get("title", ""),
            f.get("question", ""),
            f.get("notes", ""),
            f.get("summary", ""),
            " ".join(p.get("title", "") for p in f.get("papers", [])),
            " ".join(p.get("relevance_note", "") for p in f.get("papers", [])),
            " ".join(f.get("tags", [])),
        ]).lower()
        if term_lower in haystack:
            results.append(f)

    if not results:
        print(f"No findings matching: '{term}'")
        return

    print(f"Search: '{term}' — {len(results)} finding(s)\n")
    for f in results:
        n = len(f.get("papers", []))
        tags = ", ".join(f.get("tags", []))
        print(f"  {f['id']}")
        print(f"    {f.get('title', '')}")
        print(f"    {n} papers | {f.get('method', '')} | {f.get('updated', '')}")
        print(f"    Tags: {tags}")
        print()


def cmd_papers(fid, findings, db_papers):
    f = findings.get(fid)
    if f is None:
        matches = [k for k in findings if fid.lower() in k.lower()]
        if len(matches) == 1:
            f = findings[matches[0]]
            fid = matches[0]
        else:
            print(f"Finding not found: {fid}")
            return

    papers = f.get("papers", [])
    print(f"Papers in '{fid}' ({len(papers)} total):\n")
    print(f"  {'Flag':<8} {'Year':<6} {'Authors':<25} Title")
    print("  " + "-" * 80)
    for p in papers:
        in_db, owned = resolve_db_flags(p, db_papers)
        if owned:
            flag = "[OWNED]"
        elif in_db:
            flag = "[cited]"
        else:
            flag = "[ ??? ]"
        authors = (p.get("authors") or "")[:24]
        year = str(p.get("year") or "")
        title = (p.get("title") or "")[:50]
        print(f"  {flag:<8} {year:<6} {authors:<25} {title}")
    print()
    print(f"  Legend: [OWNED] = owned paper with PDF+extraction | [cited] = in DB, cited-only | [ ??? ] = not in DB")


def cmd_missing(fid, findings, db_papers):
    f = findings.get(fid)
    if f is None:
        matches = [k for k in findings if fid.lower() in k.lower()]
        if len(matches) == 1:
            f = findings[matches[0]]
            fid = matches[0]
        else:
            print(f"Finding not found: {fid}")
            return

    missing = []
    for p in f.get("papers", []):
        in_db, owned = resolve_db_flags(p, db_papers)
        if not in_db:
            missing.append(p)

    if not missing:
        print(f"All papers in '{fid}' are in data/db/papers.json.")
        return

    print(f"Papers in '{fid}' NOT in data/db/papers.json ({len(missing)} total):\n")
    for p in missing:
        authors = p.get("authors", "")
        year = p.get("year", "")
        title = p.get("title", "")
        note = p.get("relevance_note", "")
        print(f"  {authors} ({year})")
        print(f"    {title}")
        if note:
            print(f"    Note: {note[:100]}")
        print()


def cmd_for_paper(pid, findings):
    results = []
    for fid, f in findings.items():
        for p in f.get("papers", []):
            if p.get("paper_id") == pid:
                results.append((f, p))

    if not results:
        # Try partial match on paper title
        pid_lower = pid.lower()
        for fid, f in findings.items():
            for p in f.get("papers", []):
                if pid_lower in (p.get("title") or "").lower():
                    results.append((f, p))
        if not results:
            print(f"Paper '{pid}' not found in any finding.")
            return

    print(f"Findings mentioning '{pid}' ({len(results)} occurrence(s)):\n")
    for f, p in results:
        print(f"  Finding: {f['id']}")
        print(f"    {f.get('title', '')}")
        print(f"    Group: {p.get('group', '')}")
        note = p.get("relevance_note", "")
        if note:
            print(f"    Note: {note[:120]}")
        print()


def cmd_overlap(id1, id2, findings, db_papers):
    f1 = findings.get(id1)
    f2 = findings.get(id2)
    if f1 is None:
        print(f"Finding not found: {id1}")
        return
    if f2 is None:
        print(f"Finding not found: {id2}")
        return

    ids1 = {p["paper_id"] for p in f1.get("papers", []) if p.get("paper_id")}
    ids2 = {p["paper_id"] for p in f2.get("papers", []) if p.get("paper_id")}
    shared = ids1 & ids2

    if not shared:
        print(f"No overlapping papers between '{id1}' and '{id2}'.")
        return

    print(f"Overlapping papers between '{id1}' and '{id2}' ({len(shared)} papers):\n")

    p1_map = {p["paper_id"]: p for p in f1.get("papers", []) if p.get("paper_id")}
    p2_map = {p["paper_id"]: p for p in f2.get("papers", []) if p.get("paper_id")}

    for pid in sorted(shared):
        p1 = p1_map[pid]
        p2 = p2_map[pid]
        in_db, owned = resolve_db_flags(p1, db_papers)
        flag = "[OWNED]" if owned else "[cited]"
        print(f"  {flag} {p1.get('authors', '')} ({p1.get('year', '')}) — {p1.get('title', '')[:60]}")
        print(f"         {pid}")
        print(f"    In '{id1}' ({p1.get('group', '')}): {(p1.get('relevance_note') or '')[:80]}")
        print(f"    In '{id2}' ({p2.get('group', '')}): {(p2.get('relevance_note') or '')[:80]}")
        print()


def cmd_tags(findings):
    tag_counts = {}
    for fid, f in findings.items():
        for tag in f.get("tags", []):
            tag_counts.setdefault(tag, []).append(fid)

    if not tag_counts:
        print("No tags found.")
        return

    print(f"Tags across all findings ({len(tag_counts)} unique):\n")
    for tag, fids in sorted(tag_counts.items(), key=lambda x: -len(x[1])):
        print(f"  {tag:<35} ({len(fids)}x)  {', '.join(fids)}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    findings = load_findings()
    db_papers = load_db_papers()

    if cmd == "list":
        cmd_list(findings)

    elif cmd == "show":
        if len(sys.argv) < 3:
            print("Usage: research.py show <id>")
            sys.exit(1)
        cmd_show(sys.argv[2], findings, db_papers)

    elif cmd == "search":
        if len(sys.argv) < 3:
            print("Usage: research.py search <term>")
            sys.exit(1)
        cmd_search(" ".join(sys.argv[2:]), findings)

    elif cmd == "papers":
        if len(sys.argv) < 3:
            print("Usage: research.py papers <id>")
            sys.exit(1)
        cmd_papers(sys.argv[2], findings, db_papers)

    elif cmd == "missing":
        if len(sys.argv) < 3:
            print("Usage: research.py missing <id>")
            sys.exit(1)
        cmd_missing(sys.argv[2], findings, db_papers)

    elif cmd == "for-paper":
        if len(sys.argv) < 3:
            print("Usage: research.py for-paper <paper_id>")
            sys.exit(1)
        cmd_for_paper(sys.argv[2], findings)

    elif cmd == "overlap":
        if len(sys.argv) < 4:
            print("Usage: research.py overlap <id1> <id2>")
            sys.exit(1)
        cmd_overlap(sys.argv[2], sys.argv[3], findings, db_papers)

    elif cmd == "tags":
        cmd_tags(findings)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
