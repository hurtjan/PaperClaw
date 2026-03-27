#!/usr/bin/env python3
"""Shared utilities for the literature database pipeline."""

import gzip
import json
import re
import subprocess
from datetime import date
from pathlib import Path
import yaml

try:
    import orjson as _orjson

    def fast_loads(data):
        """Deserialize JSON bytes or str using orjson."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return _orjson.loads(data)

    def fast_dumps(obj, indent=2):
        """Serialize to JSON string using orjson."""
        flags = _orjson.OPT_SORT_KEYS | _orjson.OPT_NON_STR_KEYS
        if indent:
            flags |= _orjson.OPT_INDENT_2
        return _orjson.dumps(obj, option=flags).decode("utf-8")

except ImportError:
    def fast_loads(data):
        return json.loads(data)

    def fast_dumps(obj, indent=2):
        return json.dumps(obj, indent=indent, ensure_ascii=False, sort_keys=True)

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




class PaperIndex:
    """Pre-computed lookup indexes over paper dicts for fast candidate matching."""

    def __init__(self, papers: list[dict]):
        self.papers = papers
        self.by_id: dict[str, dict] = {}
        self.by_doi: dict[str, list[dict]] = {}
        self.by_s2_id: dict[str, list[dict]] = {}
        self.by_author_year: dict[tuple[str, str], list[dict]] = {}
        self.by_title_prefix: dict[str, list[dict]] = {}

        for p in papers:
            pid = p.get("id", "")
            if pid:
                self.by_id[pid] = p

            doi = normalize_doi(p.get("doi"))
            if doi:
                self.by_doi.setdefault(doi, []).append(p)

            s2_id = p.get("s2_paper_id")
            if s2_id:
                self.by_s2_id.setdefault(s2_id, []).append(p)

            ln = _get_first_lastname(p)
            yr = str(p.get("year", "")).strip()
            if ln and yr:
                self.by_author_year.setdefault((ln, yr), []).append(p)

            fp = _raw_title_prefix(p)
            if len(fp) > 8:
                self.by_title_prefix.setdefault(fp, []).append(p)




_TRACKED_FILES = {"papers.json", "authors.json", "contexts.json"}


def load_patch_file(patch_path: Path) -> dict:
    """Load a patch file, handling both gzipped (.json.gz) and legacy (.json) formats."""
    patch_path = Path(patch_path)
    if patch_path.suffix == ".gz" or patch_path.name.endswith(".json.gz"):
        with gzip.open(patch_path, "rt", encoding="utf-8") as f:
            return fast_loads(f.read())
    else:
        return fast_loads(patch_path.read_text())


def _record_patch(path: Path, new_data, source: str, description: str | None) -> None:
    """Compute forward JSON patch (gzipped) and append to data/db_history/."""
    try:
        import jsonpatch
    except ImportError:
        return
    import datetime

    if not path.exists():
        return  # no baseline to diff against

    try:
        old_data = fast_loads(path.read_text())
    except (json.JSONDecodeError, ValueError, OSError):
        return

    forward = jsonpatch.make_patch(old_data, new_data)
    if not forward.patch:
        return  # no-op

    # No reverse patch stored — recomputed on demand during rollback

    ts = datetime.datetime.now(datetime.timezone.utc)
    ts_tag = ts.strftime("%Y%m%dT%H%M%S")
    ts_iso = ts.isoformat(timespec="seconds")

    ops_add = sum(1 for op in forward.patch if op["op"] == "add")
    ops_replace = sum(1 for op in forward.patch if op["op"] == "replace")
    ops_remove = sum(1 for op in forward.patch if op["op"] == "remove")
    n_ops = len(forward.patch)

    if description is None:
        description = f"{n_ops} ops (+{ops_add} ~{ops_replace} -{ops_remove}) on {path.name}"

    # path layout: <project_root>/data/db/<file> → root is 3 levels up
    project_root = path.parent.parent.parent
    rel_target = str(path.relative_to(project_root))

    patches_dir = project_root / "data" / "db_history" / "patches"
    patches_dir.mkdir(parents=True, exist_ok=True)

    patch_filename = f"{ts_tag}_{source}_{path.name}.json.gz"
    patch_path = patches_dir / patch_filename

    patch_doc = {
        "version": 2,
        "timestamp": ts_iso,
        "source_script": source,
        "target_file": rel_target,
        "description": description,
        "stats": {
            "ops_add": ops_add,
            "ops_replace": ops_replace,
            "ops_remove": ops_remove,
            "patch_size_ops": n_ops,
        },
        "forward_patch": forward.patch,
    }

    with gzip.open(patch_path, "wt", encoding="utf-8") as f:
        f.write(fast_dumps(patch_doc))

    manifest_path = project_root / "data" / "db_history" / "manifest.jsonl"
    entry = {
        "timestamp": ts_iso,
        "source": source,
        "file": rel_target,
        "description": description,
        "patch_file": f"data/db_history/patches/{patch_filename}",
        "stats": patch_doc["stats"],
    }
    with open(manifest_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def export_json(data, path: Path, indent: int = 2, *,
                source: str | None = None,
                description: str | None = None,
                track: bool | None = None):
    """Write JSON with consistent formatting, optionally tracking patches."""
    import sys as _sys

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if source is None:
        source = Path(_sys.argv[0]).stem

    should_track = track
    if should_track is None:
        should_track = path.parent.name == "db" and path.name in _TRACKED_FILES

    if should_track:
        _record_patch(path, data, source, description)

    with open(path, "w") as f:
        f.write(fast_dumps(data, indent=indent))

    # Also record in DuckDB WAL history (non-blocking)
    if should_track:
        try:
            from db import record_change
            desc = description or f"{source}: updated {path.name}"
            record_change(source, desc, table_name=path.stem)
        except Exception:
            pass


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


def generate_paper_id(title: str, authors: list, year, existing_ids: set) -> str:
    """
    Generate {first_author_lastname}_{year}_{first_significant_title_word}.
    Appends _2, _3, ... on collision.
    """
    if authors:
        first = str(authors[0])
        lastname = first.split(",")[0].strip() if "," in first else (first.split() or ["unknown"])[-1]
    else:
        lastname = "unknown"

    lastname = re.sub(r"[^a-z0-9]", "_", transliterate(lastname).lower()).strip("_")
    lastname = re.sub(r"_+", "_", lastname) or "unknown"

    yr = str(year).strip() if year else "0000"

    title_word = "paper"
    if title:
        text = re.sub(r"[^\w\s]", " ", transliterate(title).lower())
        text = re.sub(r"\s+", " ", text).strip()
        for word in text.split():
            if word and word not in TITLE_STOP_WORDS and not word.isdigit():
                title_word = word
                break

    base = f"{lastname}_{yr}_{title_word}"
    if base not in existing_ids:
        return base
    for suffix in range(2, 1000):
        candidate = f"{base}_{suffix}"
        if candidate not in existing_ids:
            return candidate
    return f"{base}_x"
