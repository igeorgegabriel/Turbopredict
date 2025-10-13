#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/check_tag_latest.py <UNIT> <TAG>")
        return 2
    unit = argv[0]
    tag = argv[1]
    try:
        from pi_monitor.parquet_database import ParquetDatabase
    except Exception:
        # Try adding project root
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        try:
            from pi_monitor.parquet_database import ParquetDatabase  # type: ignore
        except Exception as e:
            print(f"Failed to import ParquetDatabase: {e}")
            return 1

    db = ParquetDatabase()
    df = db.get_unit_data(unit)
    if df.empty:
        print(f"{unit}: no rows in database")
        return 0
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    print(f"{unit}: total rows: {len(df):,}; latest: {df['time'].max()}")

    dft = df[df.get('tag','').astype(str) == str(tag)].copy()
    if dft.empty:
        print(f"Tag not found in unit: {tag}")
        # Show a few tag examples
        print("Sample tags:")
        try:
            print(df['tag'].astype(str).value_counts().head(10))
        except Exception:
            pass
        return 0
    dft['time'] = pd.to_datetime(dft['time'], errors='coerce')
    print(f"{unit}/{tag}: rows: {len(dft):,}; latest: {dft['time'].max()}")
    print(dft.tail(5))
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
