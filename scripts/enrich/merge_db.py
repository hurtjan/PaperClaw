#!/usr/bin/env python3
"""
Import an external PaperClaw corpus into the local DB.

Copies papers.json and contexts.json from the external DB for reference,
and merges paper metadata into the local papers.json as external_owned entries.
Extraction files are NOT imported.

Usage:
  .venv/bin/python3 scripts/enrich/merge_db.py <source_dir>
  .venv/bin/python3 scripts/enrich/merge_db.py <source_dir> --name <label>
  .venv/bin/python3 scripts/enrich/merge_db.py <source_dir> --force    # overwrite existing external_owned
  .venv/bin/python3 scripts/enrich/merge_db.py <source_dir> --enrich   # enrich local with external metadata
  .venv/bin/python3 scripts/enrich/merge_db.py <source_dir> --resolved data/tmp/merge_resolved.txt
"""

import argparse
import json
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import export_json

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
CONTEXTS_FILE = ROOT / "data" / "db" / "contexts.json"
DB_IMPORTS_DIR = ROOT / "data" / "db_imports"

# Fields that are derived/internal in old schema, not in local schema
STRIP_FIELDS = {"author_lastnames", "title_normalized", "discovered_via"}

# Metadata fields eligible for enrichment
ENRICHABLE_FIELDS = ("doi", "s2_paper_id", "forward_cited_by", "abstract",
                     "journal", "authors", "year", "title", "aliases", "superseded_by")


def _strip_fields(entry: dict) -> dict:
    return {k: v for k, v in entry.items() if k not in STRIP_FIELDS}


def _union_list(a: list, b: list) -> list:
    seen = set(a)
    result = list(a)
    for item in b:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _enrich_fields(local: dict, external: dict):
    for f in ENRICHABLE_FIELDS:
        ext_val = external.get(f)
        if ext_val is None:
            continue
        local_val = local.get(f)
        if local_val is None or local_val == "" or local_val == []:
            local[f] = ext_val


def _parse_resolved(path: Path) -> dict:
    """Parse merge_resolved.txt → {ext_id: local_id | None}. None means 'new'."""
    remap = {}
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line or line.startswith("FROM_SOURCE:"):
            continue
        parts = [p.strip() for p in line.split(",", 1)]
        if len(parts) != 2:
            continue
        ext_id, decision = parts[0], parts[1]
        if not ext_id:
            continue
        remap[ext_id] = None if decision == "new" else decision
    return remap


def _apply_id_remap(source_papers: dict, remap: dict) -> tuple:
    """
    Separate fuzzy-matched papers from source_papers and rewrite cross-refs.

    Returns (remaining_source_papers, matched_papers_dict).
    - matched_papers: ext_id → paper, for entries where remap value is not None
    - remaining: all others (including 'new' decisions), with cites/cited_by rewritten
    """
    ref_remap = {k: v for k, v in remap.items() if v is not None}

    remaining = {}
    matched = {}
    for pid, paper in source_papers.items():
        if pid in ref_remap:
            matched[pid] = paper
        else:
            remaining[pid] = paper

    # Rewrite cross-references in remaining papers so they point to local IDs
    for paper in remaining.values():
        if "cites" in paper:
            paper["cites"] = [ref_remap.get(c, c) for c in paper["cites"]]
        if "cited_by" in paper:
            paper["cited_by"] = [ref_remap.get(c, c) for c in paper["cited_by"]]

    return remaining, matched


def _merge_paper_enrich(local_papers, pid, local_entry, ext_entry, name):
    """Merge with --enrich logic. Returns action string."""
    local_type = local_entry.get("type", "")
    ext_type = ext_entry.get("type", "")
    ext_clean = _strip_fields(ext_entry)

    if local_type == "owned":
        _enrich_fields(local_entry, ext_clean)
        local_entry["cites"] = _union_list(local_entry.get("cites", []), ext_clean.get("cites", []))
        local_entry["cited_by"] = _union_list(local_entry.get("cited_by", []), ext_clean.get("cited_by", []))
        return "enriched_owned"

    if local_type == "external_owned":
        _enrich_fields(local_entry, ext_clean)
        local_entry["cites"] = _union_list(local_entry.get("cites", []), ext_clean.get("cites", []))
        local_entry["cited_by"] = _union_list(local_entry.get("cited_by", []), ext_clean.get("cited_by", []))
        return "enriched_external_owned"

    if local_type == "stub":
        if ext_type == "owned":
            old_cited_by = local_entry.get("cited_by", [])
            old_superseded_by = local_entry.get("superseded_by")
            new_entry = {k: v for k, v in ext_clean.items()
                         if k not in ("pdf_file", "text_file", "extraction_file")}
            new_entry["type"] = "external_owned"
            new_entry["source_db"] = name
            new_entry["cited_by"] = _union_list(old_cited_by, ext_clean.get("cited_by", []))
            if old_superseded_by:
                new_entry["superseded_by"] = old_superseded_by
            local_papers[pid] = new_entry
            return "upgraded_stub"
        else:
            _enrich_fields(local_entry, ext_clean)
            local_entry["cited_by"] = _union_list(
                local_entry.get("cited_by", []), ext_clean.get("cited_by", []))
            return "enriched_stub"

    return "skipped"


def _repair_bidi(papers):
    """Enforce bidirectional cites/cited_by, remove dangling refs, deduplicate."""
    all_ids = set(papers.keys())

    # Remove dangling references and self-citations
    for p in papers.values():
        pid = p.get("id", "")
        if "cites" in p:
            p["cites"] = [c for c in p["cites"] if c in all_ids and c != pid]
        if "cited_by" in p:
            p["cited_by"] = [c for c in p["cited_by"] if c in all_ids and c != pid]

    # Forward: A.cites B → B.cited_by must include A
    for pid, p in papers.items():
        for cited_id in p.get("cites", []):
            cited = papers[cited_id]
            if pid not in cited.get("cited_by", []):
                cited.setdefault("cited_by", []).append(pid)

    # Reverse: A in B.cited_by → A.cites must include B
    for pid, p in papers.items():
        for citing_id in p.get("cited_by", []):
            citing = papers[citing_id]
            if pid not in citing.get("cites", []):
                citing.setdefault("cites", []).append(pid)

    # Deduplicate
    for p in papers.values():
        if "cites" in p:
            p["cites"] = list(dict.fromkeys(p["cites"]))
        if "cited_by" in p:
            p["cited_by"] = list(dict.fromkeys(p["cited_by"]))


def _merge_contexts(source_contexts_file):
    """Merge external contexts into local contexts.json."""
    if not source_contexts_file.exists():
        print("  No source contexts.json to merge")
        return

    # Build local contexts from extraction files first
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / "build_index.py")],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode == 0:
        print(result.stdout.strip())
    else:
        print(f"  ERROR in build_index.py: {result.stderr.strip()}", file=sys.stderr)

    # Load local contexts (freshly built)
    if CONTEXTS_FILE.exists():
        local_ctx = json.loads(CONTEXTS_FILE.read_text())
    else:
        local_ctx = {"generated": str(date.today()), "by_cited": {}, "by_purpose": {}}

    ext_ctx = json.loads(source_contexts_file.read_text())
    ext_by_cited = ext_ctx.get("by_cited", {})

    # Track existing (citing, cited) pairs to avoid duplicates
    existing_pairs = set()
    for cited_id, entries in local_ctx.get("by_cited", {}).items():
        for e in entries:
            existing_pairs.add((e.get("citing", ""), cited_id))

    merged_count = 0
    for cited_id, entries in ext_by_cited.items():
        for e in entries:
            pair = (e.get("citing", ""), cited_id)
            if pair not in existing_pairs:
                local_ctx.setdefault("by_cited", {}).setdefault(cited_id, []).append(e)
                existing_pairs.add(pair)
                merged_count += 1

    # Rebuild by_purpose from merged by_cited
    by_purpose = defaultdict(list)
    for entries in local_ctx.get("by_cited", {}).values():
        for e in entries:
            by_purpose[e.get("purpose", "")].append(e)
    local_ctx["by_purpose"] = dict(by_purpose)

    # Rebuild owned_papers and counts from papers.json
    papers_db = json.loads(PAPERS_FILE.read_text())
    papers = papers_db["papers"]
    owned_papers = sorted([
        {
            "id": p["id"], "title": p["title"],
            "authors": p.get("authors", []), "year": p.get("year"),
            "journal": p.get("journal"), "doi": p.get("doi"),
            "cites_count": len(p.get("cites", [])),
            "cited_by_count": len(p.get("cited_by", [])),
            "cited_by": p.get("cited_by", []),
        }
        for p in papers.values()
        if p.get("type") in ("owned", "external_owned")
    ], key=lambda x: x["id"])

    local_ctx["generated"] = str(date.today())
    local_ctx["owned_count"] = len(owned_papers)
    local_ctx["total_papers"] = len(papers)
    local_ctx["owned_papers"] = owned_papers
    local_ctx["citation_counts"] = {pid: len(p.get("cited_by", [])) for pid, p in papers.items()}

    total_contexts = sum(len(v) for v in local_ctx.get("by_cited", {}).values())
    export_json(local_ctx, CONTEXTS_FILE,
                description=f"merge contexts: {merged_count} external contexts added, {total_contexts} total")
    print(f"  Context merge: {merged_count} external contexts added, {total_contexts} total")


def main():
    parser = argparse.ArgumentParser(description="Import external PaperClaw corpus")
    parser.add_argument("source_dir", help="Path to external PaperClaw root or db_imports dir")
    parser.add_argument("--name", help="Label for this import (defaults to source dirname)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing external_owned entries")
    parser.add_argument("--enrich", action="store_true",
                        help="Enrich local papers with external metadata where local is missing")
    parser.add_argument("--resolved", metavar="PATH",
                        help="Path to merge_resolved.txt from merge-resolver agent")
    args = parser.parse_args()

    source = Path(args.source_dir).resolve()
    if not source.exists():
        print(f"ERROR: source dir not found: {source}", file=sys.stderr)
        sys.exit(1)

    name = args.name or source.name

    # Source path: try data/db/ first, then top-level
    source_papers_file = source / "data" / "db" / "papers.json"
    source_index_file = source / "data" / "db" / "contexts.json"
    if not source_papers_file.exists():
        alt = source / "papers.json"
        if alt.exists():
            source_papers_file = alt
            source_index_file = source / "contexts.json"

    if not source_papers_file.exists():
        print(f"ERROR: papers.json not found in {source}", file=sys.stderr)
        sys.exit(1)

    print(f"Importing from: {source_papers_file}")
    print(f"Import name:    {name}")

    source_db = json.loads(source_papers_file.read_text())
    source_papers = source_db.get("papers", {})
    print(f"Source papers:  {len(source_papers)}")

    local_db = json.loads(PAPERS_FILE.read_text())
    local_papers = local_db["papers"]

    added_owned = 0
    added_cites = 0
    skipped = 0
    updated = 0
    enriched = 0
    merged_fuzzy = 0

    # Apply fuzzy-match resolutions from merge-resolver agent (if provided)
    if args.resolved:
        resolved_path = Path(args.resolved)
        if not resolved_path.exists():
            print(f"ERROR: resolved file not found: {resolved_path}", file=sys.stderr)
            sys.exit(1)

        id_remap = _parse_resolved(resolved_path)
        ref_remap = {k: v for k, v in id_remap.items() if v is not None}
        print(f"Resolved decisions: {len(id_remap)} ({len(ref_remap)} matches, "
              f"{len(id_remap) - len(ref_remap)} new)")

        source_papers, matched_papers = _apply_id_remap(source_papers, id_remap)

        # Enrichment pass: merge matched external papers into their local counterparts
        for ext_id, local_id in id_remap.items():
            if local_id is None:
                continue
            ext_paper = matched_papers.get(ext_id)
            if ext_paper is None:
                continue

            local_entry = local_papers.get(local_id)
            if local_entry is None:
                print(f"  WARNING: MERGE target {local_id} not found in local DB "
                      f"— adding {ext_id} as external_owned")
                clean = _strip_fields(ext_paper)
                entry = {k: v for k, v in clean.items()
                         if k not in ("pdf_file", "text_file", "extraction_file")}
                entry["type"] = "external_owned"
                entry["source_db"] = name
                local_papers[ext_id] = entry
                added_owned += 1
                continue

            _enrich_fields(local_entry, _strip_fields(ext_paper))

            # Rewrite cites/cited_by in ext_paper before unioning
            ext_cites = [ref_remap.get(c, c) for c in ext_paper.get("cites", [])]
            ext_cited_by = [ref_remap.get(c, c) for c in ext_paper.get("cited_by", [])]
            local_entry["cites"] = _union_list(local_entry.get("cites", []), ext_cites)
            local_entry["cited_by"] = _union_list(local_entry.get("cited_by", []), ext_cited_by)

            print(f"  MERGED: {ext_id} → {local_id}")
            merged_fuzzy += 1

    for pid, paper in source_papers.items():
        ptype = paper.get("type", "")
        clean_paper = _strip_fields(paper)

        if ptype == "owned":
            existing = local_papers.get(pid)

            if existing and args.enrich:
                action = _merge_paper_enrich(local_papers, pid, existing, paper, name)
                if action.startswith("enriched"):
                    enriched += 1
                elif action == "upgraded_stub":
                    updated += 1
                else:
                    skipped += 1
                continue

            if existing and existing.get("type") == "owned":
                print(f"  SKIP (locally owned): {pid}")
                skipped += 1
                continue
            if existing and existing.get("type") == "external_owned" and not args.force:
                skipped += 1
                continue

            was_existing = pid in local_papers
            entry = {k: v for k, v in clean_paper.items()
                     if k not in ("pdf_file", "text_file", "extraction_file")}
            entry["type"] = "external_owned"
            entry["source_db"] = name
            local_papers[pid] = entry

            if was_existing:
                updated += 1
            else:
                added_owned += 1

        elif ptype in ("stub", "cited_only"):
            existing = local_papers.get(pid)

            if existing and args.enrich:
                action = _merge_paper_enrich(local_papers, pid, existing, paper, name)
                if action.startswith("enriched"):
                    enriched += 1
                else:
                    skipped += 1
                continue

            if existing:
                skipped += 1
                continue

            entry = dict(clean_paper)
            entry["type"] = "stub"
            local_papers[pid] = entry
            added_cites += 1

    # Post-merge repairs
    _repair_bidi(local_papers)

    owned_count = sum(1 for p in local_papers.values()
                      if p.get("type") in ("owned", "external_owned"))
    stub_count = sum(1 for p in local_papers.values() if p.get("type") == "stub")
    local_db["metadata"]["last_updated"] = str(date.today())
    local_db["metadata"]["owned_count"] = owned_count
    local_db["metadata"]["stub_count"] = stub_count

    export_json(local_db, PAPERS_FILE,
                description=f"merge {name}: {added_owned} added, {updated} updated, {enriched} enriched")
    print(f"\nMerge results:")
    print(f"  Added external_owned: {added_owned}")
    if merged_fuzzy:
        print(f"  Fuzzy-merged:         {merged_fuzzy}")
    if updated:
        print(f"  Updated:              {updated}")
    if enriched:
        print(f"  Enriched:             {enriched}")
    print(f"  Added stub:           {added_cites}")
    print(f"  Skipped:              {skipped}")
    print(f"  Total papers:         {len(local_papers)} ({owned_count} owned, {stub_count} stubs)")

    # Copy reference files (skip if source is already in import_dir)
    import_dir = DB_IMPORTS_DIR / name
    import_dir.mkdir(parents=True, exist_ok=True)
    if source_papers_file.resolve() != (import_dir / "papers.json").resolve():
        shutil.copy2(str(source_papers_file), str(import_dir / "papers.json"))
        print(f"\nCopied papers.json → data/db_imports/{name}/papers.json")
    if source_index_file.exists() and source_index_file.resolve() != (import_dir / "contexts.json").resolve():
        shutil.copy2(str(source_index_file), str(import_dir / "contexts.json"))
        print(f"Copied contexts.json  → data/db_imports/{name}/contexts.json")

    # Context merging
    ctx_source = import_dir / "contexts.json"
    if not ctx_source.exists() and source_index_file.exists():
        ctx_source = source_index_file
    if ctx_source.exists():
        print("\nMerging contexts...")
        _merge_contexts(ctx_source)


if __name__ == "__main__":
    main()
