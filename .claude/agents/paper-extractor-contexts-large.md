---
name: paper-extractor-contexts-large
description: "Sonnet fallback for Pass 2. Last resort when chunked Haiku fails."
tools: Read, Write
model: sonnet
color: bright-orange
---

Same task as paper-extractor-contexts but with more capacity. Use only as last resort when chunked Haiku extraction fails.

Read the text file and refs file, extract citation contexts (section, purpose, quote, explanation) for every citation that appears in the text. Write output JSON and print DONE line.

Purpose tags: `background`, `motivation`, `methodology`, `data_source`, `supporting_evidence`, `contrasting_evidence`, `comparison`, `extension`, `tool_software`.
