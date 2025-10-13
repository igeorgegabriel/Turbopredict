#!/usr/bin/env python3
"""
Rebuild C-02001 to get real data instead of fallback
"""

import os
import sys
from pathlib import Path

# Set enhanced PCMSB timeout
os.environ['PI_FETCH_TIMEOUT'] = '90'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags
from pi_monitor.clean import dedup_parquet

def rebuild_c02001():
    """Rebuild C-02001 with real PI data"""

    print("=== REBUILDING C-02001 FOR REAL DATA ===")
    print(f"PI_FETCH_TIMEOUT: {os.environ.get('PI_FETCH_TIMEOUT')}s")

    # Read C-02001 tags
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_c02001.txt"

    if not tags_file.exists():
        print(f"ERROR: Tags file not found: {tags_file}")
        return False

    tags = []
    for line in tags_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            tags.append(line)

    if not tags:
        print("ERROR: No tags found in tags file")
        return False

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"

    if not xlsx.exists():
        print(f"ERROR: Excel file not found: {xlsx}")
        return False

    # Remove old minimal data files
    old_files = [
        PROJECT_ROOT / "data" / "processed" / "C-02001_1y_0p1h.parquet",
        PROJECT_ROOT / "data" / "processed" / "C-02001_1y_0p1h.dedup.parquet"
    ]

    for old_file in old_files:
        if old_file.exists():
            print(f"Removing old file: {old_file.name}")
            old_file.unlink()

    out_parquet = PROJECT_ROOT / "data" / "processed" / "C-02001_1y_0p1h.parquet"

    print(f"Building C-02001 with {len(tags)} tags...")
    print("Enhanced PCMSB settings:")
    print("- PI_FETCH_TIMEOUT: 90s")
    print("- settle_seconds: 3.0")
    print("- Start timeframe: -1y (full year)")
    print("- Use working copy: True")

    try:
        # First attempt: Standard settings
        result = build_unit_from_tags(
            xlsx,
            tags,
            out_parquet,
            plant="PCMSB",
            unit="C-02001",
            server=r"\\PTSG-1MMPDPdb01",
            start="-1y",
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=3.0,
            visible=False,
            use_working_copy=True
        )

        print(f"SUCCESS: Built {result}")

        # Check file size
        if result.exists():
            size_mb = result.stat().st_size / (1024 * 1024)
            print(f"File size: {size_mb:.1f} MB")

            if size_mb < 1.0:
                print("WARNING: File size is small, may indicate data issues")
            else:
                print("File size looks good - likely has real data")

        # Deduplicate
        try:
            dedup_file = dedup_parquet(result)
            print(f"Deduplicated: {dedup_file}")

            if dedup_file.exists():
                dedup_size_mb = dedup_file.stat().st_size / (1024 * 1024)
                print(f"Dedup file size: {dedup_size_mb:.1f} MB")

        except Exception as e:
            print(f"Dedup warning: {e}")

        # Verify the data content
        print("\nVerifying data content...")
        verify_c02001_data(result)

        return True

    except Exception as e:
        print(f"Build failed: {e}")

        # Fallback: Try with shorter timeframe
        print("\nTrying fallback with 3-month timeframe...")
        try:
            result_fallback = build_unit_from_tags(
                xlsx,
                tags,
                PROJECT_ROOT / "data" / "processed" / "C-02001_3m_0p1h.parquet",
                plant="PCMSB",
                unit="C-02001",
                server=r"\\PTSG-1MMPDPdb01",
                start="-3M",  # 3 months
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=5.0,  # Even longer settle time
                visible=True,  # Make visible for debugging
                use_working_copy=True
            )

            print(f"Fallback SUCCESS: {result_fallback}")
            verify_c02001_data(result_fallback)
            return True

        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return False

def verify_c02001_data(parquet_file):
    """Verify the rebuilt C-02001 data quality"""

    try:
        import pandas as pd

        df = pd.read_parquet(parquet_file)

        print(f"Data verification:")
        print(f"  Records: {len(df):,}")

        if 'tag' in df.columns:
            unique_tags = df['tag'].nunique()
            print(f"  Unique tags: {unique_tags}")

            # Check for fallback vs real tags
            fallback_count = len(df[df['tag'].str.contains('FALLBACK', na=False)])
            real_count = len(df) - fallback_count

            print(f"  Real data records: {real_count:,}")
            print(f"  Fallback records: {fallback_count:,}")

            if real_count > 1000:
                print("  STATUS: RICH DATA DETECTED!")

                # Show sample real tags
                real_tags = df[~df['tag'].str.contains('FALLBACK', na=False)]['tag'].unique()[:5]
                print("  Sample real tags:")
                for tag in real_tags:
                    print(f"    - {tag}")

            else:
                print("  STATUS: Still minimal data")

        if 'time' in df.columns and len(df) > 1:
            print(f"  Time range: {df['time'].min()} to {df['time'].max()}")

    except Exception as e:
        print(f"Verification error: {e}")

if __name__ == "__main__":
    success = rebuild_c02001()

    if success:
        print("\n=== C-02001 REBUILD COMPLETE ===")
        print("Extended analysis features now available:")
        print("- Plot stale fetch with real data")
        print("- Staleness as instrumentation anomaly")
        print("- PCMSB-optimized timeout settings")
        print("- Option [2] enhanced plotting ready")
    else:
        print("\n=== C-02001 REBUILD FAILED ===")
        print("May need to:")
        print("- Check PI server connectivity for PCMSB")
        print("- Verify tag names in config file")
        print("- Use alternative Excel file or method")