#!/usr/bin/env python3
"""PreToolUse hook: block reads outside the query subproject.

Receives JSON on stdin with tool_name and tool_input.
Exits 2 (block) if the target path resolves outside the project root.
Exits 0 (allow) otherwise.

Cross-platform (macOS, Linux, Windows) — stdlib only.
"""
import json
import os
import sys

# Project root = parent of hooks/ directory
PROJECT_ROOT = os.path.normcase(
    os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
)


def is_outside_project(path):
    if not path:
        return False
    resolved = os.path.normcase(os.path.realpath(path))
    # Allow if path is within the project root
    return not resolved.startswith(PROJECT_ROOT + os.sep) and resolved != PROJECT_ROOT


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # can't parse → allow

    tool_input = data.get("tool_input", {})

    # Read tool uses file_path; Grep/Glob use path
    path = tool_input.get("file_path") or tool_input.get("path") or ""

    if is_outside_project(path):
        print(
            "BLOCKED: Reading files outside the query environment is not allowed. "
            "All data access must go through the query scripts:\n"
            "  python3 scripts/py.py scripts/query/duckdb_query.py <command>\n"
            "  python3 scripts/py.py scripts/query/research.py <command>",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
