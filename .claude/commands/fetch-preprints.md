---
description: Download PDFs from preprint servers (arXiv, bioRxiv, medRxiv, SSRN via S2) into pdf-staging/. Usage: /fetch-preprints [--id ID ...] [--paper ID ...] [--all-external] [--all-stubs] [--dry-run] [--force] [--max N]
---

You are downloading preprint PDFs from arXiv, bioRxiv, medRxiv, and SSRN (via Semantic Scholar) into `pdf-staging/` so they can be ingested into the literature database.

Arguments: $ARGUMENTS

## Rules

- Always use `.venv/bin/python3` for scripts.
- No agents needed — pure Python script.
- After downloading, remind the user to run `/ingest` to extract and integrate the PDFs.
- Setting `S2_API_KEY` is recommended: it enables higher rate limits and is the only path for SSRN PDFs.

---

## Resolution Strategy

**Tier 1 — Semantic Scholar `openAccessPdf`** (runs first for all identifiers)
Covers arXiv, bioRxiv, medRxiv, SSRN, PMC, and some publishers in a single API call.

**Tier 2 — Direct URL construction** (fallback when S2 has no open-access link)
- arXiv: constructs `https://arxiv.org/pdf/{id}`
- bioRxiv: constructs `https://www.biorxiv.org/content/{doi}.full.pdf`
- medRxiv: constructs `https://www.medrxiv.org/content/{doi}.full.pdf`
- SSRN: no direct fallback (login-walled) — only via Tier 1

---

## Common Workflows

### Download a single paper by arXiv ID

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --id 2303.14223
```

Accepts bare arXiv IDs, DOIs, full URLs, or DB paper IDs interchangeably:

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --id 10.48550/arxiv.2303.14223
.venv/bin/python3 scripts/enrich/fetch_preprints.py --id https://arxiv.org/abs/2303.14223
.venv/bin/python3 scripts/enrich/fetch_preprints.py --id 10.1101/2023.04.01.535123
```

### Download PDFs for all external_owned entries

These are papers already in the DB as `external_owned` (imported from another corpus) that lack local PDFs. Downloading them enables full text extraction.

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --all-external
```

### Download PDFs for specific DB papers (stubs or external_owned)

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --paper smith_2020_climate jones_2019_neural
```

### Batch download stubs with DOIs (preview first)

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --all-stubs --dry-run
.venv/bin/python3 scripts/enrich/fetch_preprints.py --all-stubs --max 20
```

### Re-attempt previously failed downloads

```
.venv/bin/python3 scripts/enrich/fetch_preprints.py --all-external --force
```

---

## Options

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | off | Show what would be downloaded, no actual downloads |
| `--force` | off | Re-attempt previously succeeded or failed downloads |
| `--max N` | 50 | Cap total downloads per run |
| `--output-dir DIR` | `pdf-staging/` | Override output directory |

---

## After Downloading

Run `/ingest` to extract text, match against the DB, and integrate:

```
/ingest
```

`adopt_import.py` will automatically promote any `external_owned` entries to `owned` when a matching PDF is found. The PDF filename must match the DB paper ID (the script handles this automatically for `--paper` and `--all-external` modes).

---

## Download Log

All attempts are recorded in `data/db/preprint_fetch_log.json`. Results:
- `ok` — downloaded successfully
- `not_found` — not found in S2 or 404
- `not_open_access` — found in S2 but no open-access PDF
- `download_failed` — URL resolved but download failed (non-PDF response, network error)

Previously-skipped `not_found` / `not_open_access` entries are not retried unless `--force` is set.
