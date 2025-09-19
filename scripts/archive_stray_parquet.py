#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys
import argparse

# Ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_database import ParquetDatabase  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive stray (non‑unit) Parquet files from data/processed")
    ap.add_argument("--processed", type=Path, default=PROJECT_ROOT / "data" / "processed",
                    help="Processed directory (default: data/processed)")
    args = ap.parse_args()

    db = ParquetDatabase(data_dir=args.processed.parent)
    archived = db.archive_non_unit_parquet()
    if archived:
        print(f"Archived {len(archived)} file(s) to {args.processed / 'archive'}:")
        for p in archived:
            print(f" - {p.name}")
    else:
        print("No stray Parquet files found. Nothing to archive.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

