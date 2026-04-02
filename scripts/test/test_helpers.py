#!/usr/bin/env python3
"""Cross-platform shell command replacements for /test and /clean-db.

Replaces Unix-only commands (mkdir -p, cp, rm -f, truncate, wc -l, ls, cat)
with portable Python equivalents.

Usage:
    python3 scripts/test/test_helpers.py mkdir dir1 dir2 ...
    python3 scripts/test/test_helpers.py cp src dst
    python3 scripts/test/test_helpers.py rm path1 path2 ...
    python3 scripts/test/test_helpers.py truncate file
    python3 scripts/test/test_helpers.py wc file
    python3 scripts/test/test_helpers.py ls glob_pattern
    python3 scripts/test/test_helpers.py cat file1 file2 ... > output
    python3 scripts/test/test_helpers.py cat-to output_file file1 file2 ...
"""
import glob
import shutil
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        print("Usage: test_helpers.py <command> [args...]", file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd == "mkdir":
        for p in args:
            Path(p).mkdir(parents=True, exist_ok=True)

    elif cmd == "cp":
        if len(args) < 2:
            print("Usage: test_helpers.py cp <src> <dst>", file=sys.stderr)
            sys.exit(1)
        src, dst = Path(args[0]), Path(args[1])
        if dst.is_dir():
            dst = dst / src.name
        try:
            shutil.copy2(str(src), str(dst))
        except FileNotFoundError:
            pass  # match `cp ... 2>/dev/null || true` behavior

    elif cmd == "rm":
        for p in args:
            pp = Path(p)
            if pp.is_dir():
                shutil.rmtree(pp, ignore_errors=True)
            elif pp.exists():
                pp.unlink()
            # else: silently ignore, matching `rm -f` behavior

    elif cmd == "truncate":
        if not args:
            print("Usage: test_helpers.py truncate <file>", file=sys.stderr)
            sys.exit(1)
        p = Path(args[0])
        try:
            p.write_text("")
        except FileNotFoundError:
            pass  # match `truncate ... 2>/dev/null || true` behavior

    elif cmd == "wc":
        if not args:
            print("Usage: test_helpers.py wc <file>", file=sys.stderr)
            sys.exit(1)
        p = Path(args[0])
        if p.exists():
            print(len(p.read_text().splitlines()))
        else:
            print(0)

    elif cmd == "ls":
        if not args:
            print("Usage: test_helpers.py ls <glob_pattern>", file=sys.stderr)
            sys.exit(1)
        for f in sorted(glob.glob(args[0])):
            print(f)

    elif cmd == "cat":
        # Print file contents to stdout (pipe-friendly)
        for p in args:
            pp = Path(p)
            if pp.exists():
                sys.stdout.write(pp.read_text())

    elif cmd == "cat-to":
        # Concatenate files into an output file: cat-to output f1 f2 ...
        if len(args) < 2:
            print("Usage: test_helpers.py cat-to <output> <file1> [file2 ...]", file=sys.stderr)
            sys.exit(1)
        output = Path(args[0])
        with output.open("w") as out:
            for p in args[1:]:
                pp = Path(p)
                if pp.exists():
                    out.write(pp.read_text())

    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
