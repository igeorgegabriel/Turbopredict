#!/usr/bin/env python3
"""
Incremental update for PCMSB Parquet files.
Extract new data from refreshed Excel and append to existing Parquet files.
"""

from pathlib import Path
import sys
import pandas as pd
import time
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags  # noqa: E402
from pi_monitor.clean import dedup_parquet  # noqa: E402


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def get_latest_parquet_timestamp(parquet_file: Path) -> pd.Timestamp:
    """Get the latest timestamp from existing Parquet file."""
    if not parquet_file.exists():
        return pd.Timestamp('1970-01-01')  # Very old date if file doesn't exist

    df = pd.read_parquet(parquet_file)
    if 'time' in df.columns:
        return pd.to_datetime(df['time'].max())
    return pd.Timestamp('1970-01-01')


def incremental_build_unit(xlsx: Path, tags_file: Path, out_parquet: Path,
                          plant: str, unit: str, hours_back: int = 24):
    """Build incremental data for the last N hours and append to existing Parquet."""

    print(f"Incremental update for {plant} {unit}...")

    # Get latest timestamp from existing file
    latest_timestamp = get_latest_parquet_timestamp(out_parquet)
    hours_old = (pd.Timestamp.now() - latest_timestamp).total_seconds() / 3600

    print(f"  Existing data latest: {latest_timestamp}")
    print(f"  Age: {hours_old:.1f} hours")

    if hours_old < 2.0:  # Less than 2 hours old
        print(f"  -> Data is fresh, skipping incremental update")
        return out_parquet

    # Create temporary file for incremental data
    temp_parquet = out_parquet.with_suffix('.incremental.parquet')

    tags = read_tags(tags_file)
    if not tags:
        print(f"  -> No tags found in {tags_file}")
        return out_parquet

    # Build only recent data (last 24 hours)
    start_time = f"-{hours_back}h"

    print(f"  -> Fetching incremental data ({start_time} to now)...")

    try:
        incremental_out = build_unit_from_tags(
            xlsx,
            tags,
            temp_parquet,
            plant=plant,
            unit=unit,
            server=r"\\PTSG-1MMPDPdb01",
            start=start_time,  # Only last N hours
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=1.5,
            visible=True,
        )

        # Append new data to existing Parquet
        if out_parquet.exists() and incremental_out.exists():
            print(f"  -> Merging incremental data with existing...")

            # Load both files
            existing_df = pd.read_parquet(out_parquet)
            new_df = pd.read_parquet(incremental_out)

            # Combine and remove duplicates
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['plant', 'unit', 'tag', 'time'])
            combined_df = combined_df.sort_values('time')

            # Save updated file
            combined_df.to_parquet(out_parquet, index=False)
            print(f"  -> Updated: {out_parquet} ({len(combined_df):,} total records)")

            # Clean up temporary file
            incremental_out.unlink()

        elif incremental_out.exists():
            # No existing file, just rename incremental to main
            incremental_out.rename(out_parquet)
            print(f"  -> Created new: {out_parquet}")

        # Create deduped version
        dedup = dedup_parquet(out_parquet)
        print(f"  -> Master (dedup) updated: {dedup}")

        return out_parquet

    except Exception as e:
        print(f"  -> Error in incremental update: {e}")
        if temp_parquet.exists():
            temp_parquet.unlink()
        return out_parquet


def main():
    """Run incremental updates for all PCMSB units."""

    print("=" * 60)
    print("INCREMENTAL PCMSB PARQUET UPDATE")
    print("Adding fresh data to existing master Parquet files")
    print("=" * 60)

    # PCMSB units with their config files
    pcmsb_units = [
        ("C-02001", "tags_pcmsb_c02001.txt"),
        ("C-104", "tags_pcmsb_c104.txt"),
        ("C-13001", "tags_pcmsb_c13001.txt"),
        ("C-1301", "tags_pcmsb_c1301.txt"),
        ("C-1302", "tags_pcmsb_c1302.txt"),
        ("C-201", "tags_pcmsb_c201.txt"),
        ("C-202", "tags_pcmsb_c202.txt"),
        ("XT-07002", "tags_pcmsb_xt07002.txt"),
    ]

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    updates_successful = 0

    for unit, tags_filename in pcmsb_units:
        tags_file = PROJECT_ROOT / "config" / tags_filename
        out_parquet = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.parquet"

        if not tags_file.exists():
            print(f"[SKIP] {unit}: Tags file not found - {tags_filename}")
            continue

        try:
            result = incremental_build_unit(
                xlsx=xlsx,
                tags_file=tags_file,
                out_parquet=out_parquet,
                plant="PCMSB",
                unit=unit,
                hours_back=24  # Get last 24 hours of data
            )

            if result == out_parquet:
                updates_successful += 1
                print(f"[SUCCESS] {unit}: Incremental update completed")

        except Exception as e:
            print(f"[ERROR] {unit}: Incremental update failed - {e}")

        print()

    print("=" * 60)
    print(f"INCREMENTAL UPDATE SUMMARY")
    print(f"Successful: {updates_successful}/{len(pcmsb_units)}")
    print("=" * 60)

    return updates_successful == len(pcmsb_units)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)