#!/usr/bin/env python3
"""SessionStart hook: check if onboarding is needed.

Uses only stdlib — no venv dependency since this runs before venv may exist.
"""
import re
from pathlib import Path

p = Path("project.yaml")
if not p.exists() or not re.search(r"^user:", p.read_text(encoding="utf-8"), re.MULTILINE):
    print("[AUTO-START] No user profile found. Run /onboarding to set up PaperClaw.")
