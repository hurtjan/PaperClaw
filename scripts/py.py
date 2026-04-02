#!/usr/bin/env python3
"""Cross-platform venv Python forwarder.

Usage: python3 scripts/py.py scripts/build/check_db.py --flag
Resolves .venv/bin/python3 (Unix) or .venv/Scripts/python.exe (Windows).
"""
import sys
import platform
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

if platform.system() == "Windows":
    venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
else:
    venv_python = ROOT / ".venv" / "bin" / "python3"

if not venv_python.exists():
    print(
        f"ERROR: venv not found at {venv_python}. Run /onboarding first.",
        file=sys.stderr,
    )
    sys.exit(1)

sys.exit(subprocess.call([str(venv_python)] + sys.argv[1:]))
