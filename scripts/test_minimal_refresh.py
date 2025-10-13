#!/usr/bin/env python3
"""Minimal test - fetch only 3 tags to quickly verify relative time format works."""
from __future__ import annotations

from pathlib import Path
import sys
import os
from datetime import datetime, timedelta

# Set PI timeouts BEFORE any imports
os.environ['PI_FETCH_TIMEOUT'] = '180'  # 3 minutes per tag
os.environ['PI_FETCH_LINGER'] = '60'    # 60s linger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags


def test_minimal_refresh() -> int:
    """Test with just 3 tags from K-31-01."""

    # Test with only 3 REAL tags from tags_k31_01.txt
    test_tags = [
        "PCFS.K-31-01.31KI-302.PV",
        "PCFS.K-31-01.31FI-003A.PV",
        "PCFS.K-31-01.31FI-003B.PV",
    ]

    unit = "K-31-01"
    plant = "PCFS"
    excel_file = "PCFS/PCFS_Automation.xlsx"
    server = r"\\PTSG-1MMPDPdb01"

    xlsx_path = PROJECT_ROOT / "excel" / excel_file
    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
    temp_file = PROJECT_ROOT / "tmp" / f"{unit}_minimal_test.parquet"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    print("="*80)
    print("MINIMAL TEST: 3 tags with relative time format")
    print("="*80)

    # Get latest timestamp to calculate gap
    import pandas as pd
    df = pd.read_parquet(parquet_file)
    latest_time = pd.to_datetime(df['time'].max())
    age = datetime.now() - latest_time.replace(tzinfo=None)

    hours_gap = age.total_seconds() / 3600
    hours_to_fetch = int(hours_gap * 1.1) + 1
    start_time = f"-{hours_to_fetch}h"

    print(f"\nTest Configuration:")
    print(f"  Tags: {len(test_tags)} (minimal test)")
    print(f"  Latest data: {latest_time}")
    print(f"  Data age: {age}")
    print(f"  Gap: {hours_gap:.1f} hours")
    print(f"  Fetch window: {start_time} to *")
    print(f"  Expected formula: =PISampDat(\"<tag>\",\"{start_time}\",\"*\",\"-0.1h\",1,\"{server}\")")

    print(f"\nStarting fetch (visible=True for debugging)...")

    try:
        build_unit_from_tags(
            xlsx_path,
            test_tags,
            temp_file,
            plant=plant,
            unit=unit,
            server=server,
            start=start_time,  # Relative format: "-8h" or similar
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=5.0,  # Much longer settle for PI DataLink initialization
            visible=True,  # Make Excel visible to see what's happening
        )

        if temp_file.exists() and temp_file.stat().st_size > 0:
            df_new = pd.read_parquet(temp_file)
            print(f"\n[OK] SUCCESS!")
            print(f"  Fetched: {len(df_new):,} records")
            print(f"  Time range: {df_new['time'].min()} to {df_new['time'].max()}")
            print(f"  Tags: {df_new['tag'].nunique()}")
            temp_file.unlink()
            return 0
        else:
            print(f"\n[X] FAILED - No data fetched")
            return 1

    except Exception as e:
        print(f"\n[X] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(test_minimal_refresh())
