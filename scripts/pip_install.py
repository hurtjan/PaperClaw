#!/usr/bin/env python3
"""Cross-platform venv pip forwarder.

Usage: python3 scripts/pip_install.py install -r requirements.txt
Resolves .venv/bin/pip (Unix) or .venv/Scripts/pip.exe (Windows).
"""
import sys
import platform
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

if platform.system() == "Windows":
    pip = ROOT / ".venv" / "Scripts" / "pip.exe"
else:
    pip = ROOT / ".venv" / "bin" / "pip"

if not pip.exists():
    print(
        f"ERROR: venv not found at {pip}. Run /onboarding first.",
        file=sys.stderr,
    )
    sys.exit(1)

sys.exit(subprocess.call([str(pip)] + sys.argv[1:]))
