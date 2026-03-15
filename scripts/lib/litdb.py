#!/usr/bin/env python3
"""Shared utilities for the literature database pipeline."""

import json
import re
import subprocess
from datetime import date
from pathlib import Path
from rapidfuzz.fuzz import ratio as rapidfuzz_ratio

import yaml

OWNED_TYPES = ("owned", "external_owned")


def is_owned(paper: dict) -> bool:
    """Return True if paper is owned (locally or externally imported)."""
    return paper.get("type") in OWNED_TYPES


ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_FILE = ROOT / "project.yaml"

_config_cache = None


def load_config() -> dict:
    """Load project.yaml config (cached)."""
    global _config_cache
    if _config_cache is None:
        with open(CONFIG_FILE) as f:
            _config_cache = yaml.safe_load(f)
    return _config_cache


def get_agent_version(agent_name: str) -> str:
    """Get short git SHA of an agent .md file. Returns 'sha-dirty' if uncommitted changes."""
    agent_path = f".claude/agents/{agent_name}.md"
    result = subprocess.run(
        ["git", "log", "-1", "--format=%h", "--", agent_path],
        capture_output=True, text=True, cwd=ROOT
    )
    sha = result.stdout.strip()
    diff = subprocess.run(
        ["git", "diff", "--name-only", "--", agent_path],
        capture_output=True, text=True, cwd=ROOT
    )
    if diff.stdout.strip():
        return f"{sha}-dirty" if sha else "uncommitted"
    return sha or "unknown"


def derive_detail_level(passes: list[int]) -> str:
    """Derive human-readable detail level from completed passes."""
    s = set(passes)
    if s >= {1, 2, 3, 4}:
        return "full"
    if s >= {1, 2, 3}:
        return "analysis"
    if s >= {1, 2}:
        return "contexts"
    return "metadata"


def build_extraction_meta(passes_completed: list[int],
                          models: dict[str, str] | None = None) -> dict:
    """Build an extraction_meta block for stamping into extraction JSONs."""
    config = load_config()
    ext_config = config.get("extraction", {})
    pass_agents = ext_config.get("pass_agents", {})

    if models is None:
        models = {str(p): ext_config.get("default_model", "haiku")
                  for p in passes_completed}

    agent_versions = {}
    for p in passes_completed:
        agent_name = pass_agents.get(p) or pass_agents.get(str(p))
        if agent_name:
            agent_versions[agent_name] = get_agent_version(agent_name)

    return {
        "extractor": ext_config.get("extractor_name", "paperclaw_extractor"),
        "passes_completed": sorted(passes_completed),
        "detail_level": derive_detail_level(passes_completed),
        "models": models,
        "agent_versions": agent_versions,
        "extracted_at": str(date.today()),
    }


def normalize_doi(doi) -> str | None:
    """Strip URL prefix from DOI, lowercase."""
    if not doi:
        return None
    doi = str(doi).strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    return doi.lower() if doi else None


def _get_first_lastname(record: dict) -> str | None:
    if record.get("author_lastnames"):
        return str(record["author_lastnames"][0]).lower()
    authors = record.get("authors", [])
    if authors:
        first = str(authors[0])
        if "," in first:
            return first.split(",")[0].strip().lower()
        parts = first.split()
        if parts:
            return parts[-1].lower()
    return None


def _get_title_text(record: dict) -> str:
    tn = record.get("title_normalized", "").strip()
    if tn:
        return tn.lower()
    t = record.get("title") or ""
    t = t.strip()
    if t:
        t = re.sub(r"[^\w\s]", " ", t.lower())
        t = re.sub(r"\s+", " ", t).strip()
    return t


def _title_prefix_fp(title_text: str) -> str:
    return "_".join(title_text.split()[:5])


def _raw_title_prefix(record: dict) -> str:
    t = record.get("title") or ""
    t = t.strip()
    if not t:
        return ""
    t = re.sub(r"[^\w\s]", " ", t.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return _title_prefix_fp(t)


def score_match(a: dict, b: dict) -> tuple[int, list[str], float]:
    """
    Score how likely two records refer to the same work.
    Returns (total_score, signal_list, title_similarity).

    Signals:
      exact_id      +4  — agent-generated IDs match
      exact_doi     +4  — normalized DOIs match
      author_year   +2  — first author lastname + year match
      title_high    +2  — fuzzy ratio >= 0.9
      title_mid     +1  — fuzzy ratio 0.7–0.9
      title_prefix  +1  — 5-word prefix fingerprint matches
    """
    score = 0
    signals = []
    title_sim = 0.0

    a_id = a.get("id", "")
    b_id = b.get("id", "")
    if a_id and b_id and a_id == b_id:
        score += 4
        signals.append("exact_id")

    a_doi = normalize_doi(a.get("doi"))
    b_doi = normalize_doi(b.get("doi"))
    if a_doi and b_doi and a_doi == b_doi:
        score += 4
        signals.append("exact_doi")

    a_ln = _get_first_lastname(a)
    b_ln = _get_first_lastname(b)
    a_yr = str(a.get("year", "")).strip()
    b_yr = str(b.get("year", "")).strip()
    if a_ln and b_ln and a_yr and b_yr and a_ln == b_ln and a_yr == b_yr:
        score += 2
        signals.append("author_year")

    a_title = _get_title_text(a)
    b_title = _get_title_text(b)
    if a_title and b_title:
        ratio = rapidfuzz_ratio(a_title, b_title) / 100.0
        title_sim = ratio
        if ratio >= 0.9:
            score += 2
            signals.append("title_high")
        elif ratio >= 0.7:
            score += 1
            signals.append("title_mid")

        a_fp = _title_prefix_fp(a_title)
        b_fp = _title_prefix_fp(b_title)
        if len(a_fp) > 8 and a_fp == b_fp and "title_high" not in signals:
            score += 1
            signals.append("title_prefix")

    return score, signals, title_sim


class PaperIndex:
    """Pre-computed lookup indexes over paper dicts for fast candidate matching."""

    def __init__(self, papers: list[dict]):
        self.papers = papers
        self.by_id: dict[str, dict] = {}
        self.by_doi: dict[str, list[dict]] = {}
        self.by_author_year: dict[tuple[str, str], list[dict]] = {}
        self.by_title_prefix: dict[str, list[dict]] = {}

        for p in papers:
            pid = p.get("id", "")
            if pid:
                self.by_id[pid] = p

            doi = normalize_doi(p.get("doi"))
            if doi:
                self.by_doi.setdefault(doi, []).append(p)

            ln = _get_first_lastname(p)
            yr = str(p.get("year", "")).strip()
            if ln and yr:
                self.by_author_year.setdefault((ln, yr), []).append(p)

            fp = _raw_title_prefix(p)
            if len(fp) > 8:
                self.by_title_prefix.setdefault(fp, []).append(p)


def find_candidates_indexed(citation: dict, index: PaperIndex, min_score: int = 1) -> list[dict]:
    """Score one citation against papers using pre-computed indexes."""
    candidate_ids: set[int] = set()
    candidate_papers: list[dict] = []

    def _add(p):
        pid = id(p)
        if pid not in candidate_ids:
            candidate_ids.add(pid)
            candidate_papers.append(p)

    cit_id = citation.get("id", "")
    if cit_id and cit_id in index.by_id:
        _add(index.by_id[cit_id])

    cit_doi = normalize_doi(citation.get("doi"))
    if cit_doi and cit_doi in index.by_doi:
        for p in index.by_doi[cit_doi]:
            _add(p)

    cit_ln = _get_first_lastname(citation)
    cit_yr = str(citation.get("year", "")).strip()
    if cit_ln and cit_yr and (cit_ln, cit_yr) in index.by_author_year:
        for p in index.by_author_year[(cit_ln, cit_yr)]:
            _add(p)

    cit_fp = _raw_title_prefix(citation)
    if len(cit_fp) > 8 and cit_fp in index.by_title_prefix:
        for p in index.by_title_prefix[cit_fp]:
            _add(p)

    results = []
    for p in candidate_papers:
        s, sigs, sim = score_match(citation, p)
        if s >= min_score:
            results.append({
                "id": p.get("id", ""),
                "title": p.get("title", ""),
                "authors": p.get("authors", []),
                "year": p.get("year"),
                "journal": p.get("journal"),
                "doi": p.get("doi"),
                "signals": sigs,
                "title_similarity": round(sim, 3),
                "score": s,
            })

    if not results:
        results = find_candidates(citation, index.papers, min_score)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def find_candidates(citation: dict, papers: list[dict], min_score: int = 1) -> list[dict]:
    """Score one citation against all papers (brute-force fallback)."""
    results = []
    for paper in papers:
        s, sigs, sim = score_match(citation, paper)
        if s >= min_score:
            results.append({
                "id": paper.get("id", ""),
                "title": paper.get("title", ""),
                "authors": paper.get("authors", []),
                "year": paper.get("year"),
                "journal": paper.get("journal"),
                "doi": paper.get("doi"),
                "signals": sigs,
                "title_similarity": round(sim, 3),
                "score": s,
            })
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def export_json(data, path: Path, indent: int = 2):
    """Write JSON with consistent formatting."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Title / author normalization
# ---------------------------------------------------------------------------

DIACRITIC_MAP = str.maketrans({
    'ü': 'u', 'ö': 'o', 'ä': 'a', 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
    'ñ': 'n', 'ç': 'c', 'ß': 'ss', 'ø': 'o', 'å': 'a', 'á': 'a', 'à': 'a',
    'â': 'a', 'ã': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o',
    'ò': 'o', 'ô': 'o', 'õ': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ý': 'y',
    'ÿ': 'y', 'ř': 'r', 'š': 's', 'ž': 'z', 'č': 'c', 'ě': 'e', 'ů': 'u',
    'ğ': 'g', 'ı': 'i', 'ł': 'l', 'ń': 'n', 'ś': 's', 'ź': 'z', 'ż': 'z',
    'Ü': 'u', 'Ö': 'o', 'Ä': 'a', 'É': 'e', 'Ñ': 'n', 'Ç': 'c',
    'Ø': 'o', 'Å': 'a', 'Á': 'a', 'À': 'a', 'Â': 'a', 'Ã': 'a',
    'Í': 'i', 'Ì': 'i', 'Î': 'i', 'Ï': 'i', 'Ó': 'o', 'Ò': 'o',
    'Ô': 'o', 'Õ': 'o', 'Ú': 'u', 'Ù': 'u', 'Û': 'u', 'Ý': 'y',
    'Ř': 'r', 'Š': 's', 'Ž': 'z', 'Č': 'c', 'Ě': 'e', 'Ů': 'u',
    'Ğ': 'g', 'Ł': 'l', 'Ń': 'n', 'Ś': 's', 'Ź': 'z', 'Ż': 'z',
})

COMPOUND_PREFIXES = ['van der', 'van de', 'van den', 'van', 'von', 'de la',
                     'de los', 'de las', 'del', 'de', 'da', 'du', 'le', 'la',
                     'der', 'den']

ORG_EXPANSIONS = {
    'iea': 'international_energy_agency',
    'ipcc': 'intergovernmental_panel_climate_change',
    'irena': 'international_renewable_energy_agency',
    'imf': 'international_monetary_fund',
    'oecd': 'organisation_economic_cooperation_development',
    'ecb': 'european_central_bank',
    'tcfd': 'task_force_climate_related_financial_disclosures',
}

TITLE_STOP_WORDS = frozenset([
    'a', 'an', 'the', 'of', 'in', 'on', 'for', 'to', 'by', 'at', 'from',
    'with', 'as', 'into', 'through', 'about', 'between', 'among', 'across',
    'during', 'near', 'under', 'within', 'without', 'and', 'or', 'but',
    'nor', 'yet', 'so', 'both', 'either', 'neither', 'is', 'are', 'be',
    'was', 'were', 'has', 'have', 'had', 'will', 'would', 'can', 'could',
    'do', 'does', 'did', 'may', 'might', 'shall', 'should', 'must',
    'its', 'it', 'this', 'that', 'these', 'those', 'which', 'who',
    'what', 'how', 'why', 'where', 'their', 'there', 'here',
])


def transliterate(text: str) -> str:
    return text.translate(DIACRITIC_MAP)


def normalize_title(title: str) -> str:
    if not title:
        return ''
    text = transliterate(title)
    text = re.sub(r'(\d+(?:\.\d+)?)\s*°\s*([CcFfKk])', r'\1\2', text)
    text = text.replace('-', ' ')
    text = re.sub(r'[^\w\s.]', ' ', text)
    text = re.sub(r'(?<!\d)\.(?!\d)', ' ', text)
    text = text.lower()
    words = [w for w in text.split() if w and w not in TITLE_STOP_WORDS]
    return ' '.join(words)


def normalize_author_lastname(author_str: str) -> list:
    s = author_str.strip()
    if not s:
        return []

    lastname_part = s.split(',')[0].strip() if ',' in s else s

    is_org = (',' not in s) and (
        lastname_part.upper() == lastname_part or
        len(lastname_part.split()) == 1 and lastname_part[0].isupper() and
        all(c.isupper() or c.isdigit() for c in lastname_part if c.isalpha())
    )

    if is_org:
        raw = transliterate(lastname_part)
        normalized = re.sub(r'[\s\-]+', '_', raw.lower())
        normalized = re.sub(r"'", '', normalized)
        if normalized in ORG_EXPANSIONS:
            return [normalized, ORG_EXPANSIONS[normalized]]
        return [normalized]

    lastname = lastname_part if ',' not in s else s.split(',')[0].strip()
    lastname = transliterate(lastname)
    lastname_lower = lastname.lower()

    matched_prefix = None
    remainder = lastname
    for prefix in sorted(COMPOUND_PREFIXES, key=len, reverse=True):
        if lastname_lower.startswith(prefix + ' '):
            matched_prefix = prefix
            remainder = lastname[len(prefix) + 1:]
            break

    if matched_prefix:
        prefix_norm = matched_prefix.replace(' ', '_').lower()
        remainder_norm = remainder.strip().lower()
        remainder_norm = re.sub(r"'", '', remainder_norm)
        remainder_norm = re.sub(r'-', '_', remainder_norm)
        normalized = prefix_norm + '_' + remainder_norm
    else:
        normalized = lastname.lower()
        normalized = re.sub(r"'", '', normalized)
        normalized = re.sub(r'-', '_', normalized)

    normalized = re.sub(r'[^\w]', '', normalized)
    return [normalized] if normalized else []


def derive_author_lastnames(authors: list) -> list:
    result = []
    for author in authors:
        result.extend(normalize_author_lastname(author))
    return result
