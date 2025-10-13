#!/usr/bin/env python3
"""Check the health of existing unit data and determine if refresh is needed."""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def check_unit_health(unit: str):
    """Check if unit data exists and is fresh."""
    import pandas as pd

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

    print(f"\n{'='*80}")
    print(f"HEALTH CHECK: {unit}")
    print(f"{'='*80}\n")

    if not parquet_file.exists():
        print(f"[X] No data file found: {parquet_file}")
        return False

    print(f"[OK] Data file exists: {parquet_file}")
    print(f"  Size: {parquet_file.stat().st_size / (1024*1024):.2f} MB")

    try:
        df = pd.read_parquet(parquet_file)
        print(f"  Records: {len(df):,}")

        if 'tag' in df.columns:
            print(f"  Unique tags: {df['tag'].nunique()}")

        if 'time' in df.columns:
            min_time = pd.to_datetime(df['time'].min())
            max_time = pd.to_datetime(df['time'].max())

            print(f"\n  Date Range:")
            print(f"    Earliest: {min_time}")
            print(f"    Latest:   {max_time}")

            # Check for invalid dates (1970)
            if min_time.year == 1970:
                print(f"\n  [WARN] Contains invalid dates from 1970 - data quality issue")

            # Check freshness
            now = datetime.now()
            age = now - max_time.to_pydatetime().replace(tzinfo=None)

            print(f"\n  Freshness:")
            print(f"    Data age: {age}")

            if age < timedelta(hours=2):
                print(f"    Status: [OK] FRESH (< 2 hours old)")
                return True
            elif age < timedelta(days=1):
                print(f"    Status: [WARN] STALE (< 1 day old)")
                return True
            else:
                print(f"    Status: [X] VERY STALE (> 1 day old)")
                return False

        return True

    except Exception as e:
        print(f"[X] Error reading data: {e}")
        return False


def main() -> int:
    units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]

    results = {}
    for unit in units:
        results[unit] = check_unit_health(unit)

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}\n")

    for unit, healthy in results.items():
        status = "[OK] HEALTHY" if healthy else "[X] NEEDS REFRESH"
        print(f"  {unit}: {status}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
