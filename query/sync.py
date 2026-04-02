#!/usr/bin/env python3
"""Sync the query subproject from the parent PaperClaw database.

Copies lit.duckdb and research findings so the query environment stays current.
Run this after /ingest, /clean-db, or any DB rebuild in the parent project.

Usage:
    python3 sync.py
"""
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent

SRC_DB = PARENT / "data" / "db" / "lit.duckdb"
DST_DB = HERE / "data" / "db" / "lit.duckdb"

SRC_RESEARCH = PARENT / "research"
DST_RESEARCH = HERE / "research"


def sync_file(src, dst):
    if not src.exists():
        print(f"  SKIP {src.name} (not found)")
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    size_mb = src.stat().st_size / (1024 * 1024)
    print(f"  OK   {src.name} ({size_mb:.1f} MB)")
    return True


def sync_dir(src, dst):
    if not src.exists():
        print(f"  SKIP {src.name}/ (not found)")
        return 0
    dst.mkdir(parents=True, exist_ok=True)
    count = 0
    for f in src.glob("*.json"):
        shutil.copy2(f, dst / f.name)
        count += 1
    # Remove files in dst that no longer exist in src
    src_names = {f.name for f in src.glob("*.json")}
    for f in dst.glob("*.json"):
        if f.name not in src_names:
            f.unlink()
            count += 1
    print(f"  OK   {src.name}/ ({count} file(s))")
    return count


def ensure_git_marker():
    """Create a .git file so Claude Code treats query/ as its own project root."""
    git_path = HERE / ".git"
    if git_path.exists():
        if git_path.is_dir():
            shutil.rmtree(git_path)
        else:
            return
    git_path.write_text("gitdir: .\n")
    print("  OK   .git marker created (Claude Code project root)")


def main():
    print("Syncing query environment from parent project...\n")

    ensure_git_marker()
    ok = sync_file(SRC_DB, DST_DB)
    if not ok:
        print("\nERROR: Parent database not found. Build it first:")
        print("  cd .. && python3 scripts/py.py scripts/build/build_duckdb.py --fts")
        sys.exit(1)

    sync_dir(SRC_RESEARCH, DST_RESEARCH)

    print("\nDone. Query environment is up to date.")


if __name__ == "__main__":
    main()
