#!/usr/bin/env python3
"""
Merge duplicate paper groups into their canonical entries.

Usage:
  python3 scripts/py.py scripts/build/merge_duplicates.py [--plan FILE] [--dry-run]
  --plan      Path to merge plan (default: data/tmp/duplicate_merge_plan.json)
  --dry-run   Preview changes without writing

Input: data/tmp/duplicate_merge_plan.json
  {
    "merges": [
      {"canonical_id": "paper_a", "alias_ids": ["paper_b", "paper_c"]}
    ]
  }
"""

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from litdb import export_json, is_owned, fast_loads, derive_detail_level
from merge_extractions import merge_extraction_files, _merge_extraction_meta

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
DEFAULT_PLAN_FILE = ROOT / "data" / "tmp" / "duplicate_merge_plan.json"

# Fields eligible for one-way enrichment (fill if canonical is empty/None)
ENRICHABLE_FIELDS = (
    "doi", "s2_paper_id", "forward_cited_by", "abstract",
    "journal", "authors", "year", "title", "extraction_file",
)


def _enrich_fields(canonical: dict, alias: dict, dry_run: bool = False) -> list[str]:
    """
    Fill missing fields on canonical from alias.
    Returns list of field names that were (or would be) filled.
    """
    enriched = []
    for f in ENRICHABLE_FIELDS:
        alias_val = alias.get(f)
        if alias_val is None:
            continue
        canon_val = canonical.get(f)
        if canon_val is None or canon_val == "" or canon_val == []:
            enriched.append(f)
            if not dry_run:
                canonical[f] = alias_val
    return enriched


def do_merge(papers: dict, canonical_id: str, alias_ids: list,
             dry_run: bool = False) -> dict:
    """
    Merge alias_ids into canonical_id in-place (unless dry_run).

    Returns a summary dict describing changes made or previewed.
    """
    canonical = papers[canonical_id]
    summary = {
        "canonical_id": canonical_id,
        "alias_ids": alias_ids,
        "enriched_fields": [],
        "edges_merged": 0,
        "references_rewritten": 0,
        "version_links_set": [],
    }

    # Collect transitive aliases: if alias B already has its own aliases, include them
    all_alias_ids = list(alias_ids)
    for alias_id in alias_ids:
        alias = papers.get(alias_id, {})
        for sub_alias in alias.get("aliases", []):
            if sub_alias not in all_alias_ids and sub_alias != canonical_id:
                all_alias_ids.append(sub_alias)

    alias_id_set = set(all_alias_ids)

    # Step 1: Enrich canonical with missing fields from aliases
    for alias_id in alias_ids:
        alias = papers.get(alias_id, {})
        enriched = _enrich_fields(canonical, alias, dry_run=dry_run)
        summary["enriched_fields"].extend(
            [f"{f} (from {alias_id})" for f in enriched]
        )

    # Step 1b: Upgrade canonical type if any alias has a richer type
    TYPE_UP = {"owned": 3, "external_owned": 2, "stub": 1}
    canon_priority = TYPE_UP.get(canonical.get("type", "stub"), 0)
    best_type = canonical.get("type", "stub")
    for alias_id in all_alias_ids:
        alias = papers.get(alias_id, {})
        alias_type = alias.get("type", "stub")
        alias_priority = TYPE_UP.get(alias_type, 0)
        # Upgrade to external_owned but not to owned (owned implies local PDF/text)
        if alias_priority > canon_priority and alias_type != "owned":
            best_type = alias_type
            canon_priority = alias_priority
    if best_type != canonical.get("type", "stub"):
        if not dry_run:
            canonical["type"] = best_type
        summary["enriched_fields"].append(f"type → {best_type}")

    # Step 1c: Merge extraction_meta from aliases
    canon_em = canonical.get("extraction_meta") or {}
    em_changed = False
    for alias_id in alias_ids:
        alias = papers.get(alias_id, {})
        alias_em = alias.get("extraction_meta")
        if alias_em:
            canon_em = _merge_extraction_meta(canon_em, alias_em, alias_id)
            em_changed = True
    if em_changed and canon_em.get("passes_completed"):
        canon_em["detail_level"] = derive_detail_level(canon_em["passes_completed"])
        if not dry_run:
            canonical["extraction_meta"] = canon_em
        summary["enriched_fields"].append("extraction_meta")

    # Step 2: Merge graph edges from all aliases (including transitive) into canonical
    # Track current state to avoid double-counting (matters in dry_run too)
    current_cites = set(canonical.get("cites", []))
    current_cited_by = set(canonical.get("cited_by", []))

    for alias_id in all_alias_ids:
        alias = papers.get(alias_id, {})
        for cited_id in alias.get("cites", []):
            if cited_id == canonical_id or cited_id in alias_id_set:
                continue
            if cited_id not in current_cites:
                current_cites.add(cited_id)
                summary["edges_merged"] += 1
                if not dry_run:
                    canonical.setdefault("cites", []).append(cited_id)
        for citing_id in alias.get("cited_by", []):
            if citing_id == canonical_id or citing_id in alias_id_set:
                continue
            if citing_id not in current_cited_by:
                current_cited_by.add(citing_id)
                summary["edges_merged"] += 1
                if not dry_run:
                    canonical.setdefault("cited_by", []).append(citing_id)

    # Step 3: Reference rewriting is done in a single batch pass after all merges
    # (see _batch_rewrite_references below)

    # Step 4: Set version links
    summary["version_links_set"] = list(all_alias_ids)
    if not dry_run:
        canonical.setdefault("aliases", [])
        for alias_id in all_alias_ids:
            if alias_id not in canonical["aliases"]:
                canonical["aliases"].append(alias_id)
            alias = papers.get(alias_id, {})
            alias["superseded_by"] = canonical_id

    # Step 5: Merge extraction files
    ext_summary = merge_extraction_files(canonical_id, all_alias_ids, dry_run=dry_run)
    summary["extraction_merge"] = ext_summary
    if ext_summary["action"] != "noop" and ext_summary["canonical_file"] and not dry_run:
        canonical["extraction_file"] = ext_summary["canonical_file"]

    return summary


def _batch_rewrite_references(papers: dict, merges: list[dict]) -> int:
    """Rewrite all alias→canonical references in a single pass over all papers.

    Returns the number of papers whose references were rewritten.
    """
    # Build unified remap: alias_id → canonical_id (including transitive aliases)
    remap: dict[str, str] = {}
    all_alias_ids: set[str] = set()
    all_canonical_ids: set[str] = set()

    for merge in merges:
        canonical_id = merge["canonical_id"]
        all_canonical_ids.add(canonical_id)
        for alias_id in merge["alias_ids"]:
            remap[alias_id] = canonical_id
            all_alias_ids.add(alias_id)
        # Also remap transitive aliases from papers
        for alias_id in merge["alias_ids"]:
            alias = papers.get(alias_id, {})
            for sub_alias in alias.get("aliases", []):
                if sub_alias not in all_canonical_ids:
                    remap[sub_alias] = canonical_id
                    all_alias_ids.add(sub_alias)

    skip_ids = all_alias_ids | all_canonical_ids
    rewritten = 0

    for pid, paper in papers.items():
        if pid in skip_ids:
            continue

        needs_rewrite = False
        for c in paper.get("cites", []):
            if c in remap:
                needs_rewrite = True
                break
        if not needs_rewrite:
            for c in paper.get("cited_by", []):
                if c in remap:
                    needs_rewrite = True
                    break

        if needs_rewrite:
            rewritten += 1
            new_cites = []
            seen = set()
            for c in paper.get("cites", []):
                target = remap.get(c, c)
                if target not in seen:
                    new_cites.append(target)
                    seen.add(target)
            paper["cites"] = new_cites

            new_cited_by = []
            seen = set()
            for c in paper.get("cited_by", []):
                target = remap.get(c, c)
                if target not in seen:
                    new_cited_by.append(target)
                    seen.add(target)
            paper["cited_by"] = new_cited_by

    return rewritten


def _run_script(script_name: str) -> bool:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build" / script_name)],
        cwd=ROOT, capture_output=True, text=True
    )
    output = result.stdout.strip()
    if output:
        print(output)
    if result.returncode != 0:
        err = result.stderr.strip()
        if err:
            print(f"ERROR in {script_name}: {err}", file=sys.stderr)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Merge duplicate papers per merge plan")
    parser.add_argument("--plan", default=str(DEFAULT_PLAN_FILE),
                        help="Path to merge plan JSON")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    args = parser.parse_args()

    plan_file = Path(args.plan)
    if not plan_file.exists():
        print(f"ERROR: merge plan not found: {plan_file}", file=sys.stderr)
        sys.exit(1)

    plan = fast_loads(plan_file.read_text())
    merges = plan.get("merges", [])

    if not merges:
        print("No merges in plan.")
        sys.exit(0)

    if not PAPERS_FILE.exists():
        print("ERROR: data/db/papers.json not found.", file=sys.stderr)
        sys.exit(1)

    db = fast_loads(PAPERS_FILE.read_text())
    papers = db["papers"]

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"{prefix}Merge plan: {len(merges)} group(s)")

    # Validate all groups before executing any
    canonical_set: set[str] = set()
    alias_set: set[str] = set()

    for merge in merges:
        canonical_id = merge["canonical_id"]
        alias_ids = merge["alias_ids"]

        if canonical_id not in papers:
            print(f"ERROR: canonical {canonical_id} not in papers.json", file=sys.stderr)
            sys.exit(1)

        for alias_id in alias_ids:
            if alias_id not in papers:
                print(f"ERROR: alias {alias_id} not in papers.json", file=sys.stderr)
                sys.exit(1)
            if alias_id == canonical_id:
                print(f"ERROR: alias {alias_id} same as canonical", file=sys.stderr)
                sys.exit(1)

        if is_owned(papers[canonical_id]):
            for alias_id in alias_ids:
                if is_owned(papers[alias_id]):
                    print(f"  WARN: merging two owned papers: {canonical_id} ← {alias_id}")

        if canonical_id in canonical_set:
            print(f"ERROR: canonical {canonical_id} appears in multiple groups", file=sys.stderr)
            sys.exit(1)
        if canonical_id in alias_set:
            print(f"ERROR: canonical {canonical_id} is alias in another group", file=sys.stderr)
            sys.exit(1)

        canonical_set.add(canonical_id)
        for alias_id in alias_ids:
            if alias_id in canonical_set:
                print(f"ERROR: alias {alias_id} is canonical in another group", file=sys.stderr)
                sys.exit(1)
            alias_set.add(alias_id)

    # Execute merges
    total_enriched = 0
    total_edges = 0
    total_refs = 0

    for merge in merges:
        canonical_id = merge["canonical_id"]
        alias_ids = merge["alias_ids"]
        canon_paper = papers[canonical_id]
        canon_title = canon_paper.get("title") or canonical_id

        print(f"\n{prefix}Merge: {canonical_id} ← {', '.join(alias_ids)}")
        print(f"  Canonical: \"{canon_title}\"")

        summary = do_merge(papers, canonical_id, alias_ids, dry_run=args.dry_run)

        if summary["enriched_fields"]:
            print(f"  Enrich: {', '.join(summary['enriched_fields'])}")
        if summary["edges_merged"]:
            print(f"  Edges merged: {summary['edges_merged']}")
        if summary["references_rewritten"]:
            print(f"  References rewritten in {summary['references_rewritten']} paper(s)")
        if summary["version_links_set"]:
            links = ", ".join(summary["version_links_set"])
            print(f"  Version links: {links} → superseded_by {canonical_id}")

        ext_merge = summary.get("extraction_merge", {})
        if ext_merge.get("action") != "noop":
            action = ext_merge["action"]
            archived = ext_merge.get("archived", [])
            print(f"  Extraction: {action} → {ext_merge.get('canonical_file', '?')}")
            if archived:
                print(f"  Archived: {len(archived)} alias extraction(s)")

        total_enriched += len(summary["enriched_fields"])
        total_edges += summary["edges_merged"]
        total_refs += summary["references_rewritten"]

    # Batch rewrite all alias→canonical references in one pass
    if not args.dry_run:
        total_refs = _batch_rewrite_references(papers, merges)
        if total_refs:
            print(f"\nBatch reference rewrite: {total_refs} paper(s) updated")

    if args.dry_run:
        print(
            f"\n[DRY RUN] Would enrich {total_enriched} field(s), "
            f"merge {total_edges} edge(s)"
        )
        return

    # Update metadata counts
    owned_count = sum(1 for p in papers.values() if is_owned(p))
    stub_count = sum(1 for p in papers.values() if p.get("type") == "stub")
    db["metadata"]["last_updated"] = str(date.today())
    db["metadata"]["owned_count"] = owned_count
    db["metadata"]["stub_count"] = stub_count

    n_groups = len(merges)
    n_aliases = len(alias_set)
    export_json(
        db, PAPERS_FILE,
        description=f"merge_duplicates: {n_groups} group(s), {n_aliases} alias(es) merged"
    )
    print(f"\nUpdated papers.json ({owned_count} owned + {stub_count} stubs)")

    # Post-merge rebuild
    for script in ["build_authors.py", "build_index.py"]:
        _run_script(script)

    _run_script("check_db.py")


if __name__ == "__main__":
    main()
