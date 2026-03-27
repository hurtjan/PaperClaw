---
description: Bundle the local DB into a shareable .paperclaw file. Usage: /export [--output FILE] [--no-extractions]
---

You are exporting the local PaperClaw database as a shareable `.paperclaw` archive.

Arguments: $ARGUMENTS

- `--output <path>`: Custom output file path (default: `{user}_{date}.paperclaw` in project root).
- `--no-extractions`: Exclude extraction JSONs for a lighter export (just DB files).

## What this does

Creates a `.paperclaw` zip archive containing:
- `manifest.json` — export metadata (user, date, paper counts, format version)
- `db/papers.json` — full paper registry (with `pdf_file`/`text_file` stripped)
- `db/contexts.json` — citation context index
- `db/authors.json` — author/institution entities
- `extractions/*.json` — one per owned paper (unless `--no-extractions`)

Recipients can import via `/merge path/to/file.paperclaw`.

## Rules

- Always use `.venv/bin/python3` for scripts.
- Pass through any user arguments to the script.

---

## Step 1 — Run export

```
.venv/bin/python3 scripts/enrich/export_db.py $ARGUMENTS
```

Report the output: filename, size, and paper counts.

---

## Step 2 — Verify

Quick sanity check — list the archive contents:

```
unzip -l <output_file>
```

Report the file list to the user. Confirm the archive contains manifest.json, DB files, and (if applicable) extraction files.
