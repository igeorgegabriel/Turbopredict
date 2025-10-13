#!/usr/bin/env python3
"""Fix invalid 1970 dates in K-12-01 and K-16-01 Parquet files."""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def fix_unit_dates(unit: str, min_valid_year: int = 2020) -> bool:
    """Remove records with invalid dates (before min_valid_year)."""
    import pandas as pd

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
    backup_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.backup.parquet"

    print(f"\n{'='*80}")
    print(f"FIX INVALID DATES: {unit}")
    print(f"{'='*80}\n")

    if not parquet_file.exists():
        print(f"[X] No data file found: {parquet_file}")
        return False

    try:
        # Load data
        print(f"Loading {parquet_file.name}...")
        df = pd.read_parquet(parquet_file)
        print(f"  Original records: {len(df):,}")

        if 'time' not in df.columns:
            print(f"[X] No 'time' column found")
            return False

        # Check for invalid dates
        df['time'] = pd.to_datetime(df['time'])
        invalid_mask = df['time'].dt.year < min_valid_year

        invalid_count = invalid_mask.sum()
        print(f"\n  Invalid dates (< {min_valid_year}): {invalid_count:,}")

        if invalid_count == 0:
            print(f"[OK] No invalid dates found - data is clean!")
            return True

        # Show sample of invalid dates
        print(f"\n  Sample invalid dates:")
        invalid_samples = df[invalid_mask]['time'].unique()[:5]
        for dt in invalid_samples:
            print(f"    - {dt}")

        # Create backup
        print(f"\n  Creating backup: {backup_file.name}")
        df.to_parquet(backup_file, index=False, compression='snappy')

        # Filter out invalid dates
        print(f"  Filtering out invalid dates...")
        df_clean = df[~invalid_mask].copy()

        print(f"\n  Records after cleaning: {len(df_clean):,}")
        print(f"  Records removed: {len(df) - len(df_clean):,}")

        if len(df_clean) == 0:
            print(f"[X] All records would be removed - aborting!")
            return False

        # Save cleaned data
        print(f"\n  Saving cleaned data to {parquet_file.name}...")
        df_clean.to_parquet(parquet_file, index=False, compression='snappy')

        # Verify
        min_time = df_clean['time'].min()
        max_time = df_clean['time'].max()
        print(f"\n  New date range:")
        print(f"    Earliest: {min_time}")
        print(f"    Latest:   {max_time}")

        print(f"\n[OK] Successfully cleaned {unit} data!")
        print(f"[OK] Backup saved to: {backup_file.name}")

        return True

    except Exception as e:
        print(f"[X] Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> int:
    units_to_fix = ["K-12-01", "K-16-01"]

    print("="*80)
    print("INVALID DATE CLEANUP")
    print("="*80)
    print(f"\nThis script will remove records with dates before 2020")
    print(f"Units to process: {', '.join(units_to_fix)}")
    print(f"\nBackups will be created before making changes.")

    results = {}
    for unit in units_to_fix:
        results[unit] = fix_unit_dates(unit)

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}\n")

    for unit, success in results.items():
        status = "[OK] CLEANED" if success else "[X] FAILED"
        print(f"  {unit}: {status}")

    # Re-check health
    print(f"\n{'='*80}")
    print("VERIFYING CLEANED DATA")
    print(f"{'='*80}")

    import pandas as pd
    for unit in units_to_fix:
        if results[unit]:
            parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
            df = pd.read_parquet(parquet_file)
            df['time'] = pd.to_datetime(df['time'])
            min_year = df['time'].dt.year.min()
            print(f"\n  {unit}:")
            print(f"    Records: {len(df):,}")
            print(f"    Earliest year: {min_year}")
            print(f"    Status: {'[OK] CLEAN' if min_year >= 2020 else '[X] STILL HAS INVALID DATES'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
