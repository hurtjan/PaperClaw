#!/usr/bin/env python3
"""
Merge multi-pass extraction outputs into a single extraction JSON.

Reads:  data/extractions/{id}.json (Pass 1), .contexts.json (Pass 2),
        .analysis.json (Pass 3, optional), .sections.json (Pass 4, optional)
Writes: data/extractions/{id}.json (merged, overwrites Pass 1)

Also derives normalized fields: title_normalized, author_lastnames.
Deletes intermediate files after successful merge.

Usage: python3 scripts/py.py scripts/ingest/merge_extraction.py <paper_id>
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))
from litdb import (build_extraction_meta, derive_detail_level, get_agent_version, load_config,
                   DIACRITIC_MAP, COMPOUND_PREFIXES, ORG_EXPANSIONS, TITLE_STOP_WORDS,
                   transliterate, normalize_title, normalize_author_lastname, derive_author_lastnames)


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

def load_contexts(base_dir: str, paper_id: str) -> tuple[dict, list[str]]:
    single_path = os.path.join(base_dir, f'{paper_id}.contexts.json')
    chunk_paths = sorted(
        p for p in (
            os.path.join(base_dir, f'{paper_id}.contexts.{i}.json')
            for i in range(1, 50)
        ) if os.path.exists(p)
    )

    if chunk_paths:
        paths = chunk_paths
    elif os.path.exists(single_path):
        paths = [single_path]
    else:
        # No sidecar — try recovering inline contexts from main JSON
        inline_path = os.path.join(base_dir, f'{paper_id}.json')
        if os.path.exists(inline_path):
            with open(inline_path) as f:
                main_data = json.load(f)
            contexts_by_id = {}
            # Pattern A: citation_contexts top-level key
            for entry in main_data.get('citation_contexts', []):
                if isinstance(entry, dict) and entry.get('id'):
                    contexts_by_id.setdefault(entry['id'], []).extend(entry.get('contexts', []))
            # Pattern B: citations[].contexts already populated
            if not contexts_by_id:
                for cit in main_data.get('citations', []):
                    cid = cit.get('id')
                    ctxs = cit.get('contexts', [])
                    if cid and ctxs:
                        contexts_by_id.setdefault(cid, []).extend(ctxs)
            if contexts_by_id:
                n = sum(len(v) for v in contexts_by_id.values())
                print(f'NOTE: recovered {n} inline contexts for {paper_id} (agent wrote to main JSON)', file=sys.stderr)
                return contexts_by_id, []
        print(f'WARNING: no contexts found for {paper_id} — skipping Pass 2', file=sys.stderr)
        return {}, []

    contexts_by_id = {}
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        for entry in data.get('citations', []):
            cid = entry.get('id')
            if not cid:
                continue
            contexts_by_id.setdefault(cid, []).extend(entry.get('contexts', []))

    return contexts_by_id, paths


def load_analysis(base_dir: str, paper_id: str) -> tuple:
    path = os.path.join(base_dir, f'{paper_id}.analysis.json')
    if not os.path.exists(path):
        return None, None
    with open(path) as f:
        return json.load(f), path


def load_sections(base_dir: str, paper_id: str) -> tuple:
    single_path = os.path.join(base_dir, f'{paper_id}.sections.json')
    chunk_paths = sorted(
        p for p in (
            os.path.join(base_dir, f'{paper_id}.sections.{i}.json')
            for i in range(1, 50)
        ) if os.path.exists(p)
    )

    if chunk_paths:
        paths = chunk_paths
    elif os.path.exists(single_path):
        paths = [single_path]
    else:
        return None, []

    sections = []
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        sections.extend(data.get('sections', []))
    return sections, paths


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _stamp_extraction_meta(extraction: dict, meta_sidecar: dict | None,
                           has_contexts: bool, has_analysis: bool,
                           has_sections: bool):
    """Build and stamp extraction_meta into the extraction dict."""
    if meta_sidecar:
        # Use sidecar data (written by /extract skill)
        passes_info = meta_sidecar.get("passes", {})
        passes_completed = sorted(int(p) for p in passes_info.keys())
        models = {str(p): info.get("model", "haiku") for p, info in passes_info.items()}
        extracted_at = meta_sidecar.get("extracted_at")
        meta = build_extraction_meta(passes_completed, models)
        if extracted_at:
            meta["extracted_at"] = extracted_at
    else:
        # Infer from what was merged
        passes = [1]
        if has_contexts:
            passes.append(2)
        if has_analysis:
            passes.append(3)
        if has_sections:
            passes.append(4)
        meta = build_extraction_meta(passes)

    extraction["extraction_meta"] = meta


def load_meta_sidecar(base_dir: str, paper_id: str) -> tuple[dict | None, str | None]:
    """Load the .meta.json sidecar written by /extract."""
    path = os.path.join(base_dir, f'{paper_id}.meta.json')
    if not os.path.exists(path):
        return None, None
    with open(path) as f:
        return json.load(f), path


def merge(paper_id: str):
    base_dir = 'data/extractions'
    pass1_path = os.path.join(base_dir, f'{paper_id}.json')

    if not os.path.exists(pass1_path):
        print(f'ERROR: Pass 1 file not found: {pass1_path}', file=sys.stderr)
        sys.exit(1)

    with open(pass1_path) as f:
        extraction = json.load(f)

    # Strip non-standard key that agents sometimes create
    extraction.pop('citation_contexts', None)

    contexts_by_id, contexts_paths = load_contexts(base_dir, paper_id)

    citation_count = 0
    context_count = 0
    has_contexts = bool(contexts_by_id)
    for citation in extraction.get('citations', []):
        cid = citation.get('id')
        citation['author_lastnames'] = derive_author_lastnames(citation.get('authors', []))
        citation['title_normalized'] = normalize_title(citation.get('title', ''))
        citation['contexts'] = contexts_by_id.get(cid, [])
        citation_count += 1
        context_count += len(citation['contexts'])

    total_loaded = sum(len(v) for v in contexts_by_id.values())
    if total_loaded > 0 and context_count == 0:
        p1_ids = sorted(c.get("id") for c in extraction.get("citations", []) if c.get("id"))
        p2_ids = sorted(contexts_by_id.keys())
        print(f"WARNING: Loaded {total_loaded} contexts from Pass 2 but matched 0 — "
              f"possible citation ID mismatch between passes", file=sys.stderr)
        print(f"  Pass 1 IDs ({len(p1_ids)}): {p1_ids[:5]}{'...' if len(p1_ids) > 5 else ''}", file=sys.stderr)
        print(f"  Pass 2 IDs ({len(p2_ids)}): {p2_ids[:5]}{'...' if len(p2_ids) > 5 else ''}", file=sys.stderr)

        # Fuzzy fallback: map ref_N IDs → Pass 1 IDs via citation_key
        # Handles numbered references where agent still generated ref_1, ref_2, etc.
        import re
        key_to_p1_id = {}
        for cit in extraction.get('citations', []):
            ck = cit.get('citation_key', '')
            cid = cit.get('id', '')
            if ck and cid:
                key_to_p1_id[str(ck)] = cid

        if key_to_p1_id:
            remapped = 0
            for p2_id in list(contexts_by_id.keys()):
                m = re.fullmatch(r'ref_(\d+)', p2_id)
                if m:
                    p1_id = key_to_p1_id.get(m.group(1))
                    if p1_id:
                        contexts_by_id.setdefault(p1_id, []).extend(contexts_by_id.pop(p2_id))
                        remapped += 1
            if remapped:
                print(f"  Fuzzy fallback remapped {remapped} ref_N entries via citation_key", file=sys.stderr)
                # Re-apply contexts after remapping
                context_count = 0
                for citation in extraction.get('citations', []):
                    cid = citation.get('id')
                    citation['contexts'] = contexts_by_id.get(cid, [])
                    context_count += len(citation['contexts'])
                has_contexts = context_count > 0
                if context_count > 0:
                    print(f"  Fuzzy fallback succeeded: matched {context_count} contexts", file=sys.stderr)

    analysis_data, analysis_path = load_analysis(base_dir, paper_id)
    if analysis_data:
        for key in ('research_questions', 'methodology', 'claims', 'keywords', 'topics'):
            if key in analysis_data:
                extraction[key] = analysis_data[key]

    sections_data, sections_paths = load_sections(base_dir, paper_id)
    if sections_data is not None:
        extraction['sections'] = sections_data

    # Stamp extraction_meta
    meta_sidecar, meta_path = load_meta_sidecar(base_dir, paper_id)
    _stamp_extraction_meta(extraction, meta_sidecar, has_contexts,
                           analysis_data is not None, sections_data is not None)

    with open(pass1_path, 'w') as f:
        json.dump(extraction, f, indent=2, ensure_ascii=False)
        f.write('\n')

    # Delete intermediates
    for path in contexts_paths:
        os.remove(path)
    if analysis_path:
        os.remove(analysis_path)
    for path in sections_paths:
        os.remove(path)
    refs_path = os.path.join(base_dir, f'{paper_id}.refs.json')
    if os.path.exists(refs_path):
        os.remove(refs_path)
    if meta_path:
        os.remove(meta_path)

    analysis_status = 'yes' if analysis_data else 'no'
    sections_status = f'{len(sections_data)} sections' if sections_data is not None else 'no'
    print(f'MERGED paper_id={paper_id} citations={citation_count} contexts={context_count} analysis={analysis_status} sections={sections_status}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: python3 scripts/py.py scripts/ingest/merge_extraction.py <paper_id>', file=sys.stderr)
        sys.exit(1)
    merge(sys.argv[1])
