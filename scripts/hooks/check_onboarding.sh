#!/bin/bash
# SessionStart hook: check if onboarding is needed
PROJECT_YAML="project.yaml"

if [ ! -f "$PROJECT_YAML" ] || ! grep -q "^user:" "$PROJECT_YAML"; then
  echo "[AUTO-START] No user profile found. Run /onboarding to set up PaperClaw."
fi
