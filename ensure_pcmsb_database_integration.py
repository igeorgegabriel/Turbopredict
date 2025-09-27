#!/usr/bin/env python3
"""
Ensure PCMSB data is properly integrated with master database
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime

def ensure_pcmsb_database_integration():
    """Ensure PCMSB units are properly integrated with the master database"""

    print("ENSURING PCMSB DATABASE INTEGRATION")
    print("=" * 40)

    # Check current state
    processed_dir = Path("data/processed")

    print("STEP 1: Current processed directory status")
    print("-" * 40)

    if not processed_dir.exists():
        print("ERROR: Processed directory doesn't exist!")
        return False

    # List all files
    all_files = list(processed_dir.glob("*.parquet"))
    pcmsb_files = [f for f in all_files if any(unit in f.name for unit in ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202'])]
    other_files = [f for f in all_files if f not in pcmsb_files]

    print(f"Total parquet files: {len(all_files)}")
    print(f"PCMSB files: {len(pcmsb_files)}")
    print(f"Other plant files: {len(other_files)}")

    if pcmsb_files:
        print("\nPCMSB files found:")
        for f in pcmsb_files:
            stat = f.stat()
            size_mb = stat.st_size / (1024 * 1024)
            modified = datetime.fromtimestamp(stat.st_mtime)
            print(f"  {f.name:<35} {size_mb:>8.1f}MB {modified.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("\nNo PCMSB files found - they need to be generated")

    print(f"\nOther plant files (sample):")
    for f in other_files[:5]:  # Show first 5
        stat = f.stat()
        size_mb = stat.st_size / (1024 * 1024)
        modified = datetime.fromtimestamp(stat.st_mtime)
        print(f"  {f.name:<35} {size_mb:>8.1f}MB {modified.strftime('%Y-%m-%d %H:%M:%S')}")

    # Check database connectivity
    print(f"\nSTEP 2: Database integration test")
    print("-" * 35)

    try:
        from pi_monitor.parquet_database import ParquetDatabase

        db = ParquetDatabase()
        all_units = db.get_all_units()

        print(f"Units found by database: {len(all_units)}")
        pcmsb_units_in_db = [unit for unit in all_units if unit.startswith('C-')]
        other_units_in_db = [unit for unit in all_units if not unit.startswith('C-')]

        print(f"PCMSB units in database: {len(pcmsb_units_in_db)}")
        print(f"Other units in database: {len(other_units_in_db)}")

        if pcmsb_units_in_db:
            print(f"\nPCMSB units: {pcmsb_units_in_db}")

            # Test data access for each unit
            for unit in pcmsb_units_in_db:
                try:
                    freshness_info = db.get_data_freshness_info(unit)
                    records = freshness_info.get('total_records', 0)
                    age_hours = freshness_info.get('data_age_hours', 0)
                    latest = freshness_info.get('latest_timestamp')

                    status = "FRESH" if age_hours <= 1.0 else "STALE" if age_hours <= 24.0 else "OLD"
                    latest_str = latest.strftime('%Y-%m-%d %H:%M:%S') if latest else "None"

                    print(f"  {unit:<12} {status:<6} {records:>10,} records, {age_hours:>6.1f}h old, latest: {latest_str}")

                except Exception as e:
                    print(f"  {unit:<12} ERROR  {str(e)}")
        else:
            print("\nNo PCMSB units found in database")

        print(f"\nOther units (sample):")
        for unit in other_units_in_db[:3]:
            try:
                freshness_info = db.get_data_freshness_info(unit)
                records = freshness_info.get('total_records', 0)
                age_hours = freshness_info.get('data_age_hours', 0)
                latest = freshness_info.get('latest_timestamp')

                status = "FRESH" if age_hours <= 1.0 else "STALE" if age_hours <= 24.0 else "OLD"
                latest_str = latest.strftime('%Y-%m-%d %H:%M:%S') if latest else "None"

                print(f"  {unit:<12} {status:<6} {records:>10,} records, {age_hours:>6.1f}h old, latest: {latest_str}")

            except Exception as e:
                print(f"  {unit:<12} ERROR  {str(e)}")

    except Exception as e:
        print(f"ERROR accessing database: {e}")
        return False

    # Check if PCMSB data needs to be generated
    print(f"\nSTEP 3: Integration status")
    print("-" * 25)

    expected_pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']
    missing_units = [unit for unit in expected_pcmsb_units if unit not in pcmsb_units_in_db]

    if missing_units:
        print(f"MISSING PCMSB units: {missing_units}")
        print(f"ACTION NEEDED: Run option [1] in turbopredict to generate PCMSB data")
        print(f"              This will create parquet files for all missing units")

        # Check if config files exist
        print(f"\nConfig file check:")
        for unit in missing_units:
            config_file = Path(f"config/tags_pcmsb_{unit.lower().replace('-', '')}.txt")
            if config_file.exists():
                try:
                    tags = [t.strip() for t in config_file.read_text().splitlines()
                           if t.strip() and not t.strip().startswith('#')]
                    print(f"  {unit:<12} config OK ({len(tags)} tags)")
                except Exception as e:
                    print(f"  {unit:<12} config ERROR: {e}")
            else:
                print(f"  {unit:<12} config MISSING: {config_file}")

        return False
    else:
        print(f"SUCCESS: All PCMSB units are integrated with master database")
        print(f"PCMSB units: {pcmsb_units_in_db}")
        return True

if __name__ == "__main__":
    success = ensure_pcmsb_database_integration()
    if success:
        print("\nPCMSB database integration complete!")
    else:
        print("\nPCMSB database integration needed - run option [1] to generate missing data")