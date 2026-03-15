#!/usr/bin/env python3
"""
Merge duplicate paper groups into their canonical entries.

Usage:
  .venv/bin/python3 scripts/build/merge_duplicates.py [--plan FILE] [--dry-run]
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

from litdb import export_json, is_owned

PAPERS_FILE = ROOT / "data" / "db" / "papers.json"
DEFAULT_PLAN_FILE = ROOT / "data" / "tmp" / "duplicate_merge_plan.json"

# Fields eligible for one-way enrichment (fill if canonical is empty/None)
ENRICHABLE_FIELDS = (
    "doi", "s2_paper_id", "forward_cited_by", "abstract",
    "journal", "authors", "year", "title",
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

    # Step 3: Rewrite references globally — replace alias IDs with canonical ID
    for pid, paper in papers.items():
        if pid in alias_id_set or pid == canonical_id:
            continue

        needs_cites_rewrite = any(c in alias_id_set for c in paper.get("cites", []))
        needs_cited_by_rewrite = any(c in alias_id_set for c in paper.get("cited_by", []))

        if needs_cites_rewrite or needs_cited_by_rewrite:
            summary["references_rewritten"] += 1
            if not dry_run:
                new_cites = []
                seen = set()
                for c in paper.get("cites", []):
                    target = canonical_id if c in alias_id_set else c
                    if target not in seen:
                        new_cites.append(target)
                        seen.add(target)
                paper["cites"] = new_cites

                new_cited_by = []
                seen = set()
                for c in paper.get("cited_by", []):
                    target = canonical_id if c in alias_id_set else c
                    if target not in seen:
                        new_cited_by.append(target)
                        seen.add(target)
                paper["cited_by"] = new_cited_by

    # Step 4: Set version links
    summary["version_links_set"] = list(all_alias_ids)
    if not dry_run:
        canonical.setdefault("aliases", [])
        for alias_id in all_alias_ids:
            if alias_id not in canonical["aliases"]:
                canonical["aliases"].append(alias_id)
            alias = papers.get(alias_id, {})
            alias["superseded_by"] = canonical_id

    return summary


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

    plan = json.loads(plan_file.read_text())
    merges = plan.get("merges", [])

    if not merges:
        print("No merges in plan.")
        sys.exit(0)

    if not PAPERS_FILE.exists():
        print("ERROR: data/db/papers.json not found.", file=sys.stderr)
        sys.exit(1)

    db = json.loads(PAPERS_FILE.read_text())
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

        total_enriched += len(summary["enriched_fields"])
        total_edges += summary["edges_merged"]
        total_refs += summary["references_rewritten"]

    if args.dry_run:
        print(
            f"\n[DRY RUN] Would enrich {total_enriched} field(s), "
            f"merge {total_edges} edge(s), rewrite refs in {total_refs} paper(s)"
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
