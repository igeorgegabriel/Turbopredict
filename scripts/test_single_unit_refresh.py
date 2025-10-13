#!/usr/bin/env python3
"""Test incremental refresh on a single unit to verify relative time format fix."""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def get_latest_timestamp(parquet_file: Path) -> datetime | None:
    """Get the latest timestamp from existing Parquet file."""
    if not parquet_file.exists():
        return None

    import pandas as pd
    try:
        df = pd.read_parquet(parquet_file)
        # Check if index is DatetimeIndex (time is the index)
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 0:
            return df.index.max().to_pydatetime()
        # Fallback: check if 'time' or 'timestamp' is in columns
        elif 'time' in df.columns and len(df) > 0:
            return pd.to_datetime(df['time'].max()).to_pydatetime()
        elif 'timestamp' in df.columns and len(df) > 0:
            return pd.to_datetime(df['timestamp'].max()).to_pydatetime()
    except Exception:
        pass
    return None


def test_incremental_refresh() -> int:
    """Test with K-31-01 (known working unit)."""

    # Test configuration
    unit = "K-31-01"
    plant = "PCFS"
    tags_file = "tags_k31_01.txt"
    excel_file = "PCFS/PCFS_Automation.xlsx"
    server = r"\\PTSG-1MMPDPdb01"

    tags_path = PROJECT_ROOT / "config" / tags_file
    xlsx_path = PROJECT_ROOT / "excel" / excel_file
    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
    temp_file = PROJECT_ROOT / "tmp" / f"{unit}_test_incremental.parquet"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    print("="*80)
    print(f"TEST: Incremental Refresh for {unit}")
    print("="*80)

    # Check files exist
    if not tags_path.exists():
        print(f"[X] Tags file not found: {tags_path}")
        return 1

    if not xlsx_path.exists():
        print(f"[X] Excel file not found: {xlsx_path}")
        return 1

    # Read tags
    tags = read_tags(tags_path)
    if not tags:
        print(f"[X] No tags found in {tags_file}")
        return 1

    print(f"\nConfiguration:")
    print(f"  Plant: {plant}")
    print(f"  Unit: {unit}")
    print(f"  Tags: {len(tags)}")
    print(f"  Server: {server}")

    # Get latest timestamp
    latest_time = get_latest_timestamp(parquet_file)
    if not latest_time:
        print(f"\n[X] No existing data found - use full build first")
        return 1

    age = datetime.now() - latest_time.replace(tzinfo=None)
    print(f"\nExisting data:")
    print(f"  Latest timestamp: {latest_time}")
    print(f"  Data age: {age}")

    if age < timedelta(hours=1):
        print(f"\n[OK] Data is fresh (< 1 hour old) - no refresh needed")
        return 0

    # Calculate relative time format
    hours_gap = age.total_seconds() / 3600
    hours_to_fetch = int(hours_gap * 1.1) + 1  # 10% buffer + 1 hour
    start_time = f"-{hours_to_fetch}h"

    print(f"\nFilling data gap:")
    print(f"  Gap detected: {hours_gap:.1f} hours")
    print(f"  Fetching: {hours_to_fetch} hours (with 10% buffer)")
    print(f"  PI format: {start_time} to *")
    print(f"  Step: -0.1h")

    print(f"\nStarting PI DataLink fetch...")
    print(f"  This will test if relative time format works correctly")

    try:
        import pandas as pd

        build_unit_from_tags(
            xlsx_path,
            tags,
            temp_file,
            plant=plant,
            unit=unit,
            server=server,
            start=start_time,  # Relative time format (e.g., "-24h")
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=1.5,
            visible=False,
        )

        if not temp_file.exists() or temp_file.stat().st_size == 0:
            print(f"\n[X] No new data fetched - PI DataLink may have failed")
            return 1

        # Load and inspect new data
        df_new = pd.read_parquet(temp_file)
        print(f"\n[OK] Fetched {len(df_new):,} new records")

        if len(df_new) > 0:
            print(f"  Time range: {df_new['time'].min()} to {df_new['time'].max()}")
            print(f"  Unique tags: {df_new['tag'].nunique()}")

        # Cleanup temp file
        if temp_file.exists():
            temp_file.unlink()

        print(f"\n[OK] TEST PASSED - Relative time format works correctly!")
        return 0

    except Exception as e:
        print(f"\n[X] Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(test_incremental_refresh())
