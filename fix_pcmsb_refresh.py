#!/usr/bin/env python3
"""
Fix PCMSB refresh issue by directly triggering the refresh mechanism
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.parquet_auto_scan import ParquetAutoScanner

def fix_pcmsb_refresh():
    """Fix PCMSB refresh by directly triggering updates"""

    print("FIXING PCMSB REFRESH MECHANISM")
    print("=" * 50)
    print(f"Fix Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Step 1: Check current PCMSB status
    print("STEP 1: Current PCMSB Status")
    print("-" * 30)

    db = ParquetDatabase()
    all_units = db.get_all_units()
    pcmsb_units = [unit for unit in all_units if unit.startswith('C-')]

    print(f"PCMSB units found: {len(pcmsb_units)}")
    print(f"Units: {', '.join(pcmsb_units)}")
    print()

    stale_units = []
    for unit in pcmsb_units:
        try:
            freshness_info = db.get_data_freshness_info(unit)
            data_age_hours = freshness_info.get('data_age_hours', 0)
            latest_time = freshness_info.get('latest_timestamp')

            is_stale = data_age_hours > 1.0
            if is_stale:
                stale_units.append(unit)

            status = "STALE" if is_stale else "FRESH"
            latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

            print(f"{unit:<12} | {status:<6} | {data_age_hours:<8.1f}h | {latest_str}")

        except Exception as e:
            print(f"{unit:<12} | ERROR  | Failed to get info: {e}")

    print(f"\nStale PCMSB units: {len(stale_units)} - {stale_units}")

    if not stale_units:
        print("✅ All PCMSB units are fresh. No fix needed.")
        return

    # Step 2: Force refresh stale PCMSB units
    print(f"\nSTEP 2: Force Refresh Stale PCMSB Units")
    print("-" * 40)

    scanner = ParquetAutoScanner()

    try:
        print("Triggering auto-scan with force_refresh=True...")
        results = scanner.scan_all_units(max_age_hours=1.0, force_refresh=True)

        print(f"\nScan Results:")
        print(f"  Units scanned: {len(results['units_scanned'])}")
        print(f"  Fresh units: {len(results['fresh_units'])}")
        print(f"  Stale units: {len(results['stale_units'])}")
        print(f"  Total records: {results['total_records']:,}")
        print(f"  Total size: {results['total_size_mb']:.1f} MB")

        # Check if PCMSB units are now fresh
        print(f"\nPost-refresh PCMSB status:")
        for unit in stale_units:
            try:
                freshness_info = db.get_data_freshness_info(unit)
                data_age_hours = freshness_info.get('data_age_hours', 0)
                latest_time = freshness_info.get('latest_timestamp')

                is_fresh = data_age_hours <= 1.0
                status = "FRESH" if is_fresh else "STALE"
                latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

                print(f"  {unit:<12} | {status:<6} | {data_age_hours:<8.1f}h | {latest_str}")

            except Exception as e:
                print(f"  {unit:<12} | ERROR  | Failed to get info: {e}")

    except Exception as e:
        print(f"❌ Error during auto-scan: {e}")
        import traceback
        traceback.print_exc()

    # Step 3: Verify parquet files were updated
    print(f"\nSTEP 3: Verify Parquet File Updates")
    print("-" * 35)

    data_dir = Path("data/processed")
    current_time = datetime.now()

    for unit in stale_units:
        unit_files = list(data_dir.glob(f"*{unit}*.parquet"))
        for file_path in unit_files:
            try:
                stat = file_path.stat()
                file_modified = datetime.fromtimestamp(stat.st_mtime)
                file_age_minutes = (current_time - file_modified).total_seconds() / 60

                recently_updated = file_age_minutes < 10  # Updated in last 10 minutes
                status = "UPDATED" if recently_updated else "OLD"

                print(f"  {file_path.name:<35} | {status:<8} | {file_age_minutes:<8.1f}min ago")

            except Exception as e:
                print(f"  {file_path.name:<35} | ERROR    | {e}")

    print(f"\nFIX COMPLETE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    fix_pcmsb_refresh()