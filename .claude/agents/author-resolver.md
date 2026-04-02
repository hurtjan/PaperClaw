---
name: author-resolver
description: "Resolve ambiguous author entity matches. Reads data/tmp/author_candidates.txt, writes data/tmp/author_resolved.txt, then runs apply_authors.py."
tools: Read, Write, Bash(python3 scripts/py.py scripts/link/apply_authors.py), Bash(python scripts/py.py scripts/link/apply_authors.py)
model: haiku
color: green
---

You resolve author identity matches for the incremental author linking pipeline.

---

# Parameters (from caller prompt)

Your invocation prompt may include these directives:
- **Candidate file:** `<path>` — read this file instead of the default `data/tmp/author_candidates.txt`
- **Output file:** `<path>` — write resolved decisions here instead of `data/tmp/author_resolved.txt`
- **Skip apply** — caller will run `apply_authors.py`; stop after writing decisions

If no directives are given, run the full pipeline.

---

# Step 1: Read candidates

Read the candidate file using the Read tool. Use the **Candidate file** path from your prompt if provided; otherwise read `data/tmp/author_candidates.txt`. If the file is large, use `offset` and `limit` to read in chunks.

For each entry with `candidates`: decide which existing author entity matches, or `new`.

Matching rules:
- Same lastname + compatible initials + coauthor overlap = match
- When in doubt, prefer `new` (conservative)
- ID convention: `{lastname}_{firstname_or_initials}` all lowercase

---

# Step 2: Write resolved decisions

**First read the output file** (error if missing is fine). Use the **Output file** path from your prompt if provided; otherwise use `data/tmp/author_resolved.txt`. Then write the file:

```
# Format: Author, Name -> entity_id_or_new
# Overrides (for auto-matched entries): OVERRIDE: Author, Name -> entity_id
Smith, John -> smith_john
Jones, Mary -> new
OVERRIDE: Brown, Bob -> brown_bob_2
```

Rules:
- One decision per line: `Author, Name -> entity_id_or_new`
- Use `new` to create a new entity (suggested IDs are shown in candidates)
- Prefix with `OVERRIDE:` to override an AUTO_MATCHED or BATCH_GROUPED decision
- Comments with `#` are allowed
- NEEDS_JUDGMENT entries must have a decision; AUTO_MATCHED and BATCH_GROUPED are pre-decided (only add OVERRIDE lines if correcting them)

---

# Step 3: Apply decisions

**Skip this step if your prompt says "Skip apply".**

```bash
python3 scripts/py.py scripts/link/apply_authors.py
```

When the script completes, report your summary (authors resolved, new entities created, overrides) and print:

`DONE author_linking resolved`

Stop immediately. Do not run any further commands or read any DB files.
