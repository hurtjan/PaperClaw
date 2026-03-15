---
name: paper-extractor-contexts
description: "Pass 2: Extract citation contexts from paper text. Run after Pass 1 to add how/where each citation is used.\n\nExamples:\n- After paper-extractor completes → run this (possibly per chunk) to add contexts"
tools: Read, Write
model: haiku
color: green
---

You extract **citation contexts** from a paper's text. You are given:
- A text file path (possibly a chunk of the original)
- A refs file path (`.refs.json`) listing all citation IDs from Pass 1
- A paper ID
- An output path

## Your Task

1. Read the text file and the refs file. If the refs file does not exist, read citation IDs from the `citations` array in `data/extractions/{paper_id}.json` instead.
2. For each citation that appears in the text:
   - Identify the **section** where it appears
   - Determine the **purpose** (one of: `background`, `motivation`, `methodology`, `data_source`, `supporting_evidence`, `contrasting_evidence`, `comparison`, `extension`, `tool_software`)
   - Extract a **quote** (the sentence containing the citation)
   - Write an **explanation** (1-2 sentences on why the author cites this work)
3. A citation may appear multiple times → create one context per appearance
4. **Output path:** Write output to `data/extractions/{paper_id}.contexts.json` ONLY. Do NOT modify `data/extractions/{paper_id}.json`.

## Output Schema

The top-level key MUST be `citations`. Write this JSON and nothing else to the output path.

```json
{
  "citations": [
    {
      "id": "watts_1998_collective",
      "contexts": [
        {
          "section": "Introduction",
          "purpose": "background",
          "quote": "Scale-free networks have been shown to emerge...",
          "explanation": "Cited to establish the theoretical framework of scale-free networks."
        }
      ]
    }
  ]
}
```

After writing, print: `DONE paper_id={id} contexts={N}`
