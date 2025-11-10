#!/usr/bin/env python3
"""
Clean PCMSB status check without unicode characters
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime, timedelta
from pi_monitor.parquet_database import ParquetDatabase

def check_pcmsb_status():
    """Check PCMSB status cleanly"""

    print("PCMSB STATUS CHECK (CLEAN)")
    print("=" * 50)

    # Check PCMSB units
    db = ParquetDatabase()
    all_units = db.get_all_units()
    pcmsb_units = [unit for unit in all_units if unit.startswith('C-')]

    print(f"PCMSB units found: {len(pcmsb_units)}")
    print(f"Units: {', '.join(pcmsb_units)}")
    print()

    stale_count = 0
    fresh_count = 0

    for unit in pcmsb_units[:3]:  # Check first 3 units
        try:
            freshness_info = db.get_data_freshness_info(unit)
            latest_time = freshness_info.get('latest_timestamp')
            data_age_hours = freshness_info.get('data_age_hours', 0)
            total_records = freshness_info.get('total_records', 0)

            is_stale = data_age_hours > 2.0
            if is_stale:
                stale_count += 1
            else:
                fresh_count += 1

            status = "STALE" if is_stale else "FRESH"
            latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

            print(f"{unit:<12} | {status:<6} | {data_age_hours:<8.1f}h | {latest_str}")

        except Exception as e:
            print(f"{unit:<12} | ERROR  | Failed to get info: {e}")

    print()
    print(f"Fresh PCMSB units: {fresh_count}")
    print(f"Stale PCMSB units: {stale_count}")

    # Check Excel file status
    excel_path = Path("excel") / "PCMSB_Automation.xlsx"
    if excel_path.exists():
        stat = excel_path.stat()
        excel_modified = datetime.fromtimestamp(stat.st_mtime)
        excel_age = (datetime.now() - excel_modified).total_seconds() / 3600

        print(f"\nPCMSB_Automation.xlsx status:")
        print(f"  Last modified: {excel_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Age: {excel_age:.1f} hours")

        if excel_age < 1.0:
            print("  STATUS: Excel file recently refreshed")
        else:
            print("  STATUS: Excel file needs refresh")

    print("\nNEXT STEPS:")
    if fresh_count == 0 and stale_count > 0:
        print("1. Excel automation is working but parquet update pipeline is broken")
        print("2. Need to trigger parquet file regeneration from fresh Excel data")
        print("3. Check if auto-scan option [1] processes PCMSB units correctly")

if __name__ == "__main__":
    check_pcmsb_status()