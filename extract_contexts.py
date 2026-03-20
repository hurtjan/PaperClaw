#!/usr/bin/env python3
import json
import re
import sys

# Read the text file
text_file = "/Users/jhurt/Documents/claw_testing/PaperClaw/data/text/1-s2.0-S0095069618307083-main.txt"
refs_file = "/Users/jhurt/Documents/claw_testing/PaperClaw/data/extractions/sen_2020_climate.refs.json"

# Load refs
with open(refs_file, 'r') as f:
    refs_list = json.load(f)

# Create a mapping from citation_key to id
citation_key_to_id = {}
for ref in refs_list:
    citation_key_to_id[ref['citation_key']] = ref['id']

# Read text file
with open(text_file, 'r', encoding='utf-8') as f:
    text = f.read()

# Split text into sections
sections = {}
current_section = "Introduction"
section_pattern = r'^\d+\.\s+([A-Za-z\s]+)$'

lines = text.split('\n')
current_text = []

for line in lines:
    section_match = re.match(section_pattern, line.strip())
    if section_match:
        if current_text:
            sections[current_section] = '\n'.join(current_text)
        current_section = section_match.group(1).strip()
        current_text = []
    else:
        current_text.append(line)

if current_text:
    sections[current_section] = '\n'.join(current_text)

# Find citations - looking for [N] pattern
# Also handle author-year citations like (Author, Year)
citation_pattern = r'\[(\d+)\]'
author_year_pattern = r'\(([A-Za-z\s\.]+),\s*(\d{4})\)'

contexts = {}

for section_name, section_text in sections.items():
    # Find all numbered citations
    for match in re.finditer(citation_pattern, section_text):
        citation_key = match.group(1)
        if citation_key in citation_key_to_id:
            citation_id = citation_key_to_id[citation_key]

            # Extract the sentence containing the citation
            citation_pos = match.start()

            # Find sentence boundaries - go backwards to find period or start
            start = citation_pos
            for i in range(citation_pos - 1, max(0, citation_pos - 500), -1):
                if section_text[i] in '.!?\n':
                    start = i + 1
                    break
                if i == max(0, citation_pos - 500):
                    start = 0

            # Find sentence end - go forward to find period
            end = citation_pos
            for i in range(citation_pos, min(len(section_text), citation_pos + 500)):
                if section_text[i] in '.!?\n':
                    end = i + 1
                    break
                if i == min(len(section_text), citation_pos + 500) - 1:
                    end = i

            quote = section_text[start:end].strip()
            quote = ' '.join(quote.split())  # Normalize whitespace

            if citation_id not in contexts:
                contexts[citation_id] = []

            contexts[citation_id].append({
                "section": section_name,
                "purpose": "background",  # Default - should be manually assigned
                "quote": quote,
                "explanation": ""
            })

# Build output
output = {
    "citations": []
}

for citation_id, citation_contexts in sorted(contexts.items()):
    output["citations"].append({
        "id": citation_id,
        "contexts": citation_contexts
    })

# Write output
output_file = "/Users/jhurt/Documents/claw_testing/PaperClaw/data/extractions/sen_2020_climate.contexts.json"
with open(output_file, 'w') as f:
    json.dump(output, f, indent=2)

print(f"Extracted {len(output['citations'])} citations with {sum(len(c['contexts']) for c in output['citations'])} context instances")
