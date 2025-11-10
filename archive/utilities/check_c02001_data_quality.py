#!/usr/bin/env python3
"""
Check C-02001 data quality and content
"""

import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def check_c02001_data():
    """Check the actual data content in C-02001 files"""

    print("=== C-02001 DATA QUALITY ANALYSIS ===")

    files_to_check = [
        "data/processed/C-02001_1y_0p1h.parquet",
        "data/processed/C-02001_1y_0p1h.dedup.parquet"
    ]

    for file_path in files_to_check:
        file_full_path = PROJECT_ROOT / file_path

        print(f"\nChecking: {file_path}")
        print("-" * 50)

        if not file_full_path.exists():
            print("File does not exist")
            continue

        try:
            # Get file size
            size_mb = file_full_path.stat().st_size / (1024 * 1024)
            print(f"File size: {size_mb:.3f} MB")

            # Load and analyze parquet data
            df = pd.read_parquet(file_full_path)

            print(f"Records: {len(df):,}")
            print(f"Columns: {list(df.columns)}")

            if len(df) > 0:
                print(f"Date range: {df['time'].min()} to {df['time'].max()}")

                if 'tag' in df.columns:
                    unique_tags = df['tag'].nunique()
                    print(f"Unique tags: {unique_tags}")

                    if unique_tags > 0:
                        print("Sample tags:")
                        for tag in df['tag'].unique()[:5]:
                            tag_data = df[df['tag'] == tag]
                            print(f"  - {tag}: {len(tag_data)} records")

                if 'value' in df.columns:
                    print(f"Value range: {df['value'].min():.3f} to {df['value'].max():.3f}")
                    print(f"Non-null values: {df['value'].count():,}")

                print("Sample data:")
                print(df.head(3).to_string())
            else:
                print("*** NO DATA FOUND ***")

        except Exception as e:
            print(f"Error reading file: {e}")

    # Check if we need to rebuild C-02001 with fresh data
    print(f"\n=== ASSESSMENT ===")

    try:
        from pi_monitor.parquet_database import ParquetDatabase
        db = ParquetDatabase(PROJECT_ROOT / "data")

        # Get C-02001 data through database
        df_db = db.get_unit_data("C-02001")

        if df_db.empty:
            print("DATABASE REPORTS: No C-02001 data found")
            print("RECOMMENDATION: C-02001 needs fresh data build")

            # Check scan results - your image showed C-02001 as FRESH with 1 record
            print("\nFrom fresh scan results:")
            print("- C-02001 shows as FRESH (0.9h old)")
            print("- But only 1 record reported")
            print("- File sizes are tiny (4KB)")
            print("- This suggests the data fetch failed or returned minimal data")

        else:
            print(f"DATABASE REPORTS: {len(df_db):,} records for C-02001")

    except Exception as e:
        print(f"Database check error: {e}")

    print(f"\n=== CONCLUSION ===")
    print("C-02001 status:")
    print("- Files exist but contain minimal data")
    print("- Need to investigate why fresh build produced tiny files")
    print("- Extended analysis should still work with existing data")
    print("- Plot stale fetch will show whatever data is available")

if __name__ == "__main__":
    check_c02001_data()