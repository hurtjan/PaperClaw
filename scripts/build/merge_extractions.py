#!/usr/bin/env python3
"""
Merge extraction files when duplicate papers are merged.

Called by merge_duplicates.py after the papers.json merge is complete.
Handles renaming, field-level merging, and archiving of alias extractions.
"""

import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTIONS_DIR = ROOT / "data" / "extractions"
ARCHIVE_DIR = EXTRACTIONS_DIR / "_superseded"

# Top-level metadata fields — fill canonical blanks from alias
_META_FIELDS = ("title", "authors", "year", "journal", "doi", "abstract",
                "source_file", "pdf_file")


def _dedup_strings(existing: list, incoming: list, case_insensitive: bool = False) -> list:
    """Union two string lists, preserving order."""
    if case_insensitive:
        seen = {s.lower() for s in existing}
        result = list(existing)
        for s in incoming:
            if s.lower() not in seen:
                seen.add(s.lower())
                result.append(s)
        return result
    seen = set(existing)
    result = list(existing)
    for s in incoming:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


def _merge_citations(canon_cits: list, alias_cits: list) -> list:
    """Merge citations by id key; union contexts within shared citations."""
    by_id = {}
    for c in canon_cits:
        by_id[c["id"]] = c

    for ac in alias_cits:
        cid = ac["id"]
        if cid not in by_id:
            by_id[cid] = ac
        else:
            existing = by_id[cid]
            # Union contexts using (section, purpose, quote) as dedup key
            existing_keys = set()
            for ctx in existing.get("contexts", []):
                key = (ctx.get("section", ""), ctx.get("purpose", ""), ctx.get("quote", ""))
                existing_keys.add(key)
            for ctx in ac.get("contexts", []):
                key = (ctx.get("section", ""), ctx.get("purpose", ""), ctx.get("quote", ""))
                if key not in existing_keys:
                    existing.setdefault("contexts", []).append(ctx)
                    existing_keys.add(key)

    # Preserve original ordering: canon first, then new from alias
    result = list(canon_cits)
    canon_ids = {c["id"] for c in canon_cits}
    for ac in alias_cits:
        if ac["id"] not in canon_ids:
            result.append(by_id[ac["id"]])
    return result


def _merge_claims(canon_claims: list, alias_claims: list) -> list:
    """Concatenate claims, deduplicate by exact claim text."""
    seen = {c.get("claim", "") for c in canon_claims}
    result = list(canon_claims)
    for c in alias_claims:
        if c.get("claim", "") not in seen:
            seen.add(c.get("claim", ""))
            result.append(c)
    return result


def _merge_topics(canon_topics: dict, alias_topics: dict) -> dict:
    """Union each sub-key in topics."""
    all_keys = set(list(canon_topics.keys()) + list(alias_topics.keys()))
    merged = {}
    for key in all_keys:
        canon_vals = canon_topics.get(key, []) or []
        alias_vals = alias_topics.get(key, []) or []
        if isinstance(canon_vals, list) and isinstance(alias_vals, list):
            merged[key] = _dedup_strings(canon_vals, alias_vals, case_insensitive=True)
        elif canon_vals:
            merged[key] = canon_vals
        else:
            merged[key] = alias_vals
    return merged


def _merge_extraction_meta(canon_meta: dict, alias_meta: dict, alias_id: str) -> dict:
    """Union passes_completed, merge models dicts, track merged_from."""
    merged = dict(canon_meta)
    # Union passes
    canon_passes = set(canon_meta.get("passes_completed", []))
    alias_passes = set(alias_meta.get("passes_completed", []))
    merged["passes_completed"] = sorted(canon_passes | alias_passes)
    # Merge models
    canon_models = dict(canon_meta.get("models", {}))
    for k, v in alias_meta.get("models", {}).items():
        if k not in canon_models:
            canon_models[k] = v
    merged["models"] = canon_models
    # Track merged_from
    merged_from = list(canon_meta.get("merged_from", []))
    if alias_id not in merged_from:
        merged_from.append(alias_id)
    merged["merged_from"] = merged_from
    return merged


def _load_extraction(paper_id: str, extractions_dir: Path) -> dict | None:
    """Load the main extraction file for a paper ID, if it exists."""
    path = extractions_dir / f"{paper_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, OSError):
        return None


def _merge_two(canonical: dict, alias: dict, alias_id: str) -> dict:
    """Merge alias extraction data into canonical extraction."""
    # Top-level metadata: fill blanks
    for f in _META_FIELDS:
        if not canonical.get(f) and alias.get(f):
            canonical[f] = alias[f]

    # Citations
    if alias.get("citations"):
        canonical["citations"] = _merge_citations(
            canonical.get("citations", []), alias["citations"])

    # Claims
    if alias.get("claims"):
        canonical["claims"] = _merge_claims(
            canonical.get("claims", []), alias["claims"])

    # Keywords
    if alias.get("keywords"):
        canonical["keywords"] = _dedup_strings(
            canonical.get("keywords", []), alias["keywords"], case_insensitive=True)

    # Topics
    if alias.get("topics") and isinstance(alias["topics"], dict):
        canonical["topics"] = _merge_topics(
            canonical.get("topics", {}) or {}, alias["topics"])

    # Research questions
    if alias.get("research_questions"):
        canonical["research_questions"] = _dedup_strings(
            canonical.get("research_questions", []), alias["research_questions"])

    # Sections — prefer canonical; use alias only if canonical has none
    if not canonical.get("sections") and alias.get("sections"):
        canonical["sections"] = alias["sections"]

    # Methodology — prefer canonical; use alias only if canonical has none
    if not canonical.get("methodology") and alias.get("methodology"):
        canonical["methodology"] = alias["methodology"]

    # Extraction meta
    canonical["extraction_meta"] = _merge_extraction_meta(
        canonical.get("extraction_meta", {}),
        alias.get("extraction_meta", {}),
        alias_id)

    return canonical


def merge_extraction_files(canonical_id: str, alias_ids: list[str],
                           extractions_dir: Path | None = None,
                           archive_dir: Path | None = None,
                           dry_run: bool = False) -> dict:
    """
    Merge extraction files for a duplicate merge group.

    Returns summary dict with keys:
      action: "adopted" | "merged" | "noop"
      canonical_file: path to result file (or None)
      archived: list of archived alias files
    """
    if extractions_dir is None:
        extractions_dir = EXTRACTIONS_DIR
    if archive_dir is None:
        archive_dir = extractions_dir / "_superseded"

    canon_ext = _load_extraction(canonical_id, extractions_dir)
    alias_exts = []
    for aid in alias_ids:
        ext = _load_extraction(aid, extractions_dir)
        if ext is not None:
            alias_exts.append((aid, ext))

    summary = {"action": "noop", "canonical_file": None, "archived": []}

    if not alias_exts:
        # No alias extractions to merge
        if canon_ext:
            summary["canonical_file"] = str(extractions_dir / f"{canonical_id}.json")
        return summary

    canon_path = extractions_dir / f"{canonical_id}.json"

    if canon_ext is None:
        # Only alias has extraction — adopt the first one as canonical base
        first_aid, first_ext = alias_exts[0]
        first_ext["id"] = canonical_id
        canon_ext = first_ext
        summary["action"] = "adopted"

        # Merge remaining aliases if any
        for aid, aext in alias_exts[1:]:
            canon_ext = _merge_two(canon_ext, aext, aid)
            summary["action"] = "merged"

        # Also track the first alias in merged_from
        canon_ext["extraction_meta"] = _merge_extraction_meta(
            canon_ext.get("extraction_meta", {}), {}, first_aid)
    else:
        # Both have extractions — merge alias data into canonical
        for aid, aext in alias_exts:
            canon_ext = _merge_two(canon_ext, aext, aid)
        summary["action"] = "merged"

    summary["canonical_file"] = str(canon_path)

    if dry_run:
        for aid, _ in alias_exts:
            summary["archived"].append(str(extractions_dir / f"{aid}.json"))
        return summary

    # Write merged canonical
    canon_ext["id"] = canonical_id
    with open(canon_path, "w") as f:
        json.dump(canon_ext, f, indent=2, ensure_ascii=False)

    # Archive alias extraction files
    archive_dir.mkdir(parents=True, exist_ok=True)
    for aid, _ in alias_exts:
        alias_path = extractions_dir / f"{aid}.json"
        if alias_path.exists():
            dest = archive_dir / f"{aid}.json"
            shutil.move(str(alias_path), str(dest))
            summary["archived"].append(str(dest))

    # Also move sidecar files (.contexts.json, .analysis.json, .sections.json, .refs.json)
    for aid, _ in alias_exts:
        for sidecar in extractions_dir.glob(f"{aid}.*"):
            if sidecar.suffix == ".json" and sidecar.exists():
                dest = archive_dir / sidecar.name
                shutil.move(str(sidecar), str(dest))

    return summary
