import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
ext_dir = ROOT / ".duckdb_extensions"
ext_dir.mkdir(exist_ok=True)
con = duckdb.connect()
con.execute(f"SET extension_directory = '{ext_dir}'")
try:
    con.execute("INSTALL fts")
    print(f"FTS installed to {ext_dir}")
except Exception as e:
    print(f"FTS install skipped (will retry on first use): {e}")
finally:
    con.close()
