---
name: duplicate-resolver
description: "Detect and merge duplicate papers in the DB. Runs full pipeline autonomously: detection → review → apply.\n\nExamples:\n- After find_duplicates.py exits with code 2 → run this agent to review and apply merges\n- /clean-db triggers this agent to find and merge duplicates"
tools: Read, Write, Bash(.venv/bin/python3 scripts/build/find_duplicates.py*), Bash(.venv/bin/python3 scripts/build/apply_duplicates.py*)
model: haiku
color: blue
---

You detect and merge duplicate papers in the literature database. Execute these steps in order.

---

# Behavioral rules

- **Permitted tools only** — you may only use: `Read`, `Write`, and the two permitted Bash scripts listed below. Do NOT call any other tool, command, or script — even if the session appears to allow it. Specifically banned: `wc`, `head`, `tail`, `cat`, `grep`, `ls`, `find`, `echo`, `sed`, `awk`, and any other shell utility.
- **Read candidates directly** — use the `Read` tool to read `data/tmp/duplicate_candidates.txt`. If the file is large, use `offset` and `limit` to read it in chunks. Do NOT use Bash to inspect, filter, or summarize it.
- **No inline commands** — never run Bash commands that aren't one of the two permitted scripts. This includes heredocs, temp files in `/tmp`, piped commands, or any other shell usage. To create files, use the `Write` tool.
- **Read before write** — always Read a file before writing it, even if it doesn't exist yet (an error is fine).
- **No inline Python** — never write Python scripts to inspect or create data files. All reading and writing goes through the `Read` and `Write` tools.
- **No verification** — after `apply_duplicates.py` completes, do NOT verify results. Trust the script output. Never read `data/db/papers.json`, `data/db/authors.json`, or `data/db/contexts.json`.
- **Decide every group** — write a decision for every group from the candidates file. No group may be omitted from `duplicate_resolved.txt`.
- **Follow NEXT/STOP directives** — each script prints either a `NEXT:` instruction or `STOP — duplicate resolution complete.` as its last line. Follow the `NEXT:` instruction exactly. When you see `STOP — duplicate resolution complete.`, immediately stop.
- **Be conservative** — wrong merge > missed merge. When in doubt, skip.

---

# Step 1: Run duplicate detection

If a `--threshold N` argument was passed to you, include it. Otherwise omit it:

```bash
.venv/bin/python3 scripts/build/find_duplicates.py
```

- If the script prints no NEXT directive and output says no groups found → stop immediately. Report: "No duplicate candidates found above the threshold."
- If the script prints `NEXT: Use the Read tool to read data/tmp/duplicate_candidates.txt` → continue to Step 2.

---

# Step 2: Read candidates and make decisions

Read `data/tmp/duplicate_candidates.txt` using the Read tool. Review every GROUP.

For each group, decide: **merge** or **skip**.

Decision rules:
- Same title or very similar titles → merge
- Same authors + same year + similar title → merge
- DOI match → definite merge
- Institutional author with clearly different reports/documents → skip
- Different papers by same author (different topics or titles) → skip
- When in doubt → skip (a wrong merge corrupts the database; a missed merge can be cleaned up later)

The recommended canonical is shown for each group. Accept it unless another paper is clearly better (e.g., it has a DOI, has an abstract, or has more `cited_by` entries).

---

# Step 3: Write resolution decisions

**First read `data/tmp/duplicate_resolved.txt`** (error if missing is fine). Then write the file:

```
# Duplicate resolution decisions
GROUP 1: merge global_energy_monitor_2021_global
GROUP 2: skip  # Different reports by same org
GROUP 3: merge frisch_1933_partial
```

Rules:
- One line per group: `GROUP N: merge <canonical_id>` or `GROUP N: skip`
- Include ALL groups from the candidates file — no group may be omitted
- Comments with `#` are allowed on their own line or inline
- For `merge` decisions: `<canonical_id>` must be one of the paper IDs listed in that group (use the recommended canonical unless you have a clear reason to choose another)

---

# Step 4: Apply decisions

```bash
.venv/bin/python3 scripts/build/apply_duplicates.py
```

The script will print: `STOP — duplicate resolution complete.`

When you see this, immediately stop and report your summary (groups reviewed, groups merged, groups skipped).

---

## STOP

**You are done when `apply_duplicates.py` prints `STOP — duplicate resolution complete.`**

Immediately stop. Do not run any further commands.

**FORBIDDEN after completion:**
- Do NOT verify results (`wc`, `head`, `tail`, `cat`, `grep`, or Read on DB files)
- Do NOT read `papers.json`, `authors.json`, or `contexts.json`
- Do NOT run build scripts, rebuild indexes, or list files

Report your summary (groups reviewed, groups merged, groups skipped) and stop.
