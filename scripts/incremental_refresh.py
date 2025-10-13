#!/usr/bin/env python3
"""Incremental refresh - only fetch recent data to append to existing Parquet files."""
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


def incremental_refresh(
    unit: str,
    tags: list[str],
    xlsx: Path,
    plant: str,
    server: str,
    default_hours_back: int = 24
) -> bool:
    """Fetch data from last timestamp until now to prevent gaps."""
    import pandas as pd
    import pyarrow.parquet as pq

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
    temp_file = PROJECT_ROOT / "tmp" / f"{unit}_incremental.parquet"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*80}")
    print(f"INCREMENTAL REFRESH: {unit}")
    print(f"{'='*80}\n")

    # Get latest timestamp from existing data
    latest_time = get_latest_timestamp(parquet_file)

    if latest_time:
        age = datetime.now() - latest_time.replace(tzinfo=None)
        print(f"Existing data:")
        print(f"  Latest timestamp: {latest_time}")
        print(f"  Data age: {age}")

        if age < timedelta(hours=1):
            print(f"\n[OK] Data is fresh (< 1 hour old) - no refresh needed")
            return True

        # Calculate time window from last data until now
        start_time = latest_time.strftime("%Y-%m-%d %H:%M:%S")
        fetch_duration = age
        print(f"\nFetching from last data timestamp to now (gap: {age})...")
    else:
        print(f"No existing data found - will fetch last {default_hours_back} hours")
        start_time = f"-{default_hours_back}h"
        fetch_duration = timedelta(hours=default_hours_back)

    # Fetch data from last timestamp or default period
    print(f"  Tags: {len(tags)}")
    print(f"  Time window: {start_time if not latest_time else 'from last data'} to now")
    print(f"  Fetch duration: {fetch_duration}")

    try:
        build_unit_from_tags(
            xlsx,
            tags,
            temp_file,
            plant=plant,
            unit=unit,
            server=server,
            start=start_time,  # Fetch from last timestamp or default period
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=1.5,
            visible=False,
        )

        if not temp_file.exists() or temp_file.stat().st_size == 0:
            print(f"[X] No new data fetched")
            return False

        # Load new data
        df_new = pd.read_parquet(temp_file)
        print(f"\n[OK] Fetched {len(df_new):,} new records")

        # Merge with existing data
        if parquet_file.exists():
            print(f"\nMerging with existing data...")
            df_old = pd.read_parquet(parquet_file)
            print(f"  Existing records: {len(df_old):,}")

            # Combine
            df_combined = pd.concat([df_old, df_new], ignore_index=True)

            # Deduplicate
            if 'time' in df_combined.columns and 'tag' in df_combined.columns:
                print(f"  Deduplicating...")
                before = len(df_combined)
                df_combined = df_combined.drop_duplicates(subset=['time', 'tag'], keep='last')
                after = len(df_combined)
                print(f"  Removed {before - after:,} duplicates")

            # Sort
            df_combined = df_combined.sort_values('time')

        else:
            df_combined = df_new

        print(f"\n  Final records: {len(df_combined):,}")

        # Save
        print(f"\nSaving to {parquet_file.name}...")
        df_combined.to_parquet(parquet_file, index=False, compression='snappy')

        # Cleanup
        if temp_file.exists():
            temp_file.unlink()

        print(f"\n[OK] Incremental refresh completed!")
        return True

    except Exception as e:
        print(f"[X] Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> int:
    """Refresh all PCFS units incrementally."""
    units = [
        ("K-12-01", "tags_k12_01.txt", "PCFS_Automation.xlsx"),
        ("K-16-01", "tags_k16_01.txt", "PCFS_Automation.xlsx"),
        ("K-19-01", "tags_k19_01.txt", "PCFS_Automation.xlsx"),
        ("K-31-01", "tags_k31_01.txt", "PCFS_Automation.xlsx"),
    ]

    plant = "PCFS"
    server = r"\\PTSG-1MMPDPdb01"
    default_hours_back = 24  # Default if no existing data (24 hours)

    results = {}

    for unit, tags_file, excel_file in units:
        tags_path = PROJECT_ROOT / "config" / tags_file
        xlsx_path = PROJECT_ROOT / "excel" / "PCFS" / excel_file

        if not tags_path.exists():
            print(f"\n[X] Tags file not found: {tags_path}")
            results[unit] = False
            continue

        if not xlsx_path.exists():
            print(f"\n[X] Excel file not found: {xlsx_path}")
            results[unit] = False
            continue

        tags = read_tags(tags_path)
        if not tags:
            print(f"\n[X] No tags found in {tags_file}")
            results[unit] = False
            continue

        results[unit] = incremental_refresh(
            unit, tags, xlsx_path, plant, server, default_hours_back
        )

    # Summary
    print(f"\n{'='*80}")
    print("INCREMENTAL REFRESH SUMMARY")
    print(f"{'='*80}\n")

    for unit, success in results.items():
        status = "[OK] SUCCESS" if success else "[X] FAILED"
        print(f"  {unit}: {status}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
