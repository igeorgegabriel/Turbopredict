#!/usr/bin/env python3
"""Quick data analysis helper for existing Parquet data."""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def analyze_unit(unit: str, show_tags: bool = False, show_stats: bool = True):
    """Analyze a single unit's data."""
    import pandas as pd
    import numpy as np

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

    print(f"\n{'='*80}")
    print(f"ANALYSIS: {unit}")
    print(f"{'='*80}\n")

    if not parquet_file.exists():
        print(f"[X] No data file found: {parquet_file}")
        return

    try:
        df = pd.read_parquet(parquet_file)

        print(f"File: {parquet_file.name}")
        print(f"Size: {parquet_file.stat().st_size / (1024*1024):.2f} MB")
        print(f"\nDataFrame Info:")
        print(f"  Total records: {len(df):,}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")

        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            print(f"\nTime Range:")
            print(f"  Start: {df['time'].min()}")
            print(f"  End:   {df['time'].max()}")
            print(f"  Span:  {df['time'].max() - df['time'].min()}")

        if 'tag' in df.columns:
            print(f"\nTags:")
            print(f"  Unique tags: {df['tag'].nunique()}")

            if show_tags:
                print(f"\n  Tag list:")
                for tag in sorted(df['tag'].unique()):
                    tag_count = len(df[df['tag'] == tag])
                    print(f"    - {tag}: {tag_count:,} records")

            # Records per tag
            tag_counts = df['tag'].value_counts()
            print(f"\n  Records per tag:")
            print(f"    Min:    {tag_counts.min():,}")
            print(f"    Max:    {tag_counts.max():,}")
            print(f"    Mean:   {tag_counts.mean():,.0f}")
            print(f"    Median: {tag_counts.median():,.0f}")

        if 'value' in df.columns and show_stats:
            print(f"\nValue Statistics:")
            print(f"  Min:    {df['value'].min():,.2f}")
            print(f"  Max:    {df['value'].max():,.2f}")
            print(f"  Mean:   {df['value'].mean():,.2f}")
            print(f"  Median: {df['value'].median():,.2f}")
            print(f"  Std:    {df['value'].std():,.2f}")

            # Check for nulls
            null_count = df['value'].isnull().sum()
            if null_count > 0:
                print(f"\n  [WARN] Null values: {null_count:,} ({null_count/len(df)*100:.2f}%)")

        # Data quality checks
        print(f"\nData Quality:")
        total_nulls = df.isnull().sum().sum()
        if total_nulls > 0:
            print(f"  [WARN] Total null values: {total_nulls:,}")
        else:
            print(f"  [OK] No null values")

        # Check for duplicates
        if 'time' in df.columns and 'tag' in df.columns:
            dupes = df.duplicated(subset=['time', 'tag']).sum()
            if dupes > 0:
                print(f"  [WARN] Duplicate records (time+tag): {dupes:,}")
            else:
                print(f"  [OK] No duplicates")

        print(f"\n[OK] Analysis complete!")

    except Exception as e:
        print(f"[X] Error: {e}")
        import traceback
        traceback.print_exc()


def compare_units(units: list[str]):
    """Compare multiple units."""
    import pandas as pd

    print(f"\n{'='*80}")
    print(f"UNIT COMPARISON")
    print(f"{'='*80}\n")

    data = []
    for unit in units:
        parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
        if not parquet_file.exists():
            continue

        try:
            df = pd.read_parquet(parquet_file)
            df['time'] = pd.to_datetime(df['time'])

            data.append({
                'Unit': unit,
                'Records': len(df),
                'Tags': df['tag'].nunique() if 'tag' in df.columns else 'N/A',
                'Size (MB)': parquet_file.stat().st_size / (1024*1024),
                'Start': df['time'].min() if 'time' in df.columns else 'N/A',
                'End': df['time'].max() if 'time' in df.columns else 'N/A',
            })
        except Exception as e:
            print(f"[X] Error loading {unit}: {e}")

    if data:
        comparison_df = pd.DataFrame(data)
        print(comparison_df.to_string(index=False))
    else:
        print("[X] No data to compare")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Analyze PCFS unit data")
    parser.add_argument('--unit', type=str, help='Specific unit to analyze (e.g., K-31-01)')
    parser.add_argument('--all', action='store_true', help='Analyze all units')
    parser.add_argument('--compare', action='store_true', help='Compare all units')
    parser.add_argument('--show-tags', action='store_true', help='Show detailed tag list')
    parser.add_argument('--no-stats', action='store_true', help='Skip value statistics')

    args = parser.parse_args()

    if args.compare:
        units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        compare_units(units)
        return 0

    if args.unit:
        analyze_unit(args.unit, show_tags=args.show_tags, show_stats=not args.no_stats)
    elif args.all:
        units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        for unit in units:
            analyze_unit(unit, show_tags=args.show_tags, show_stats=not args.no_stats)
    else:
        # Default: show comparison
        units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        compare_units(units)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
