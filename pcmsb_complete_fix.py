#!/usr/bin/env python3
"""
Complete PCMSB fix - refresh Excel then update parquet files
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime
import time

def pcmsb_complete_fix():
    """Complete fix for PCMSB data refresh"""

    print("PCMSB COMPLETE FIX")
    print("=" * 30)
    print(f"Fix started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Step 1: Refresh Excel file
    print("STEP 1: Refreshing PCMSB Excel file")
    print("-" * 35)

    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")

    if not excel_path.exists():
        print("ERROR: PCMSB_Automation.xlsx not found!")
        return False

    try:
        from pi_monitor.excel_refresh import refresh_excel_safe

        print(f"Refreshing {excel_path}...")

        # Refresh the Excel file with PI DataLink
        refresh_result = refresh_excel_safe(str(excel_path))

        if refresh_result:
            print("OK Excel refresh completed successfully")

            # Wait a moment for file system to update
            time.sleep(2)

            # Check if file was modified
            stat = excel_path.stat()
            excel_modified = datetime.fromtimestamp(stat.st_mtime)
            excel_age_minutes = (datetime.now() - excel_modified).total_seconds() / 60

            print(f"   Last modified: {excel_modified.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Age: {excel_age_minutes:.1f} minutes")

            if excel_age_minutes < 5:
                print("OK Excel file was recently updated")
            else:
                print("WARNING: Excel file was not recently updated")

        else:
            print("WARNING: Excel refresh may have failed")

    except Exception as e:
        print(f"ERROR refreshing Excel: {e}")
        print("Continuing with existing Excel file...")

    # Step 2: Update parquet files for PCMSB units
    print(f"\nSTEP 2: Updating PCMSB parquet files")
    print("-" * 38)

    # Get PCMSB units
    from pi_monitor.parquet_database import ParquetDatabase

    try:
        db = ParquetDatabase()
        all_units = db.get_all_units()
        pcmsb_units = [unit for unit in all_units if unit.startswith('C-')]

        print(f"Found {len(pcmsb_units)} PCMSB units: {pcmsb_units}")

        # Check current status
        stale_units = []
        for unit in pcmsb_units:
            try:
                freshness_info = db.get_data_freshness_info(unit)
                data_age_hours = freshness_info.get('data_age_hours', 0)

                if data_age_hours > 1.0:
                    stale_units.append(unit)

                print(f"   {unit}: {data_age_hours:.1f}h old ({'STALE' if data_age_hours > 1.0 else 'FRESH'})")

            except Exception as e:
                print(f"   {unit}: ERROR - {e}")

        if not stale_units:
            print("OK All PCMSB units are fresh")
            return True

        print(f"\nUpdating {len(stale_units)} stale units: {stale_units}")

        # Update each stale unit individually with timeout
        success_count = 0

        for unit in stale_units:
            print(f"\nUpdating {unit}...")

            try:
                # Check if tags file exists
                tags_file = Path(f"config/tags_pcmsb_{unit.lower().replace('-', '')}.txt")

                if not tags_file.exists():
                    print(f"   ERROR: Tags file not found: {tags_file}")
                    continue

                # Read tags
                tags = [t.strip() for t in tags_file.read_text(encoding="utf-8").splitlines()
                       if t.strip() and not t.strip().startswith('#')]

                if not tags:
                    print(f"   ERROR: No valid tags found in {tags_file}")
                    continue

                print(f"   Found {len(tags)} tags")

                # Try to update with timeout protection
                from pi_monitor.batch import build_unit_from_tags
                from pi_monitor.clean import dedup_parquet

                # Build output path
                safe_unit = unit.replace('-', '_')
                out_parquet = Path(f"data/processed/{unit}_1y_0p1h.parquet")

                print(f"   Building parquet from Excel...")

                # This is where it might hang - we'll need to implement timeout
                build_unit_from_tags(
                    xlsx=excel_path,
                    tags=tags,
                    out_parquet=out_parquet,
                    plant="PCMSB",
                    unit=unit,
                    work_sheet="DL_WORK"
                )

                # Dedup the file
                print(f"   Deduplicating...")
                dedup_path = dedup_parquet(out_parquet)

                # Check the result
                if dedup_path.exists():
                    size_mb = dedup_path.stat().st_size / (1024 * 1024)

                    # Quick check of data
                    df = pd.read_parquet(dedup_path)
                    latest_time = df['time'].max() if len(df) > 0 else None

                    if latest_time:
                        data_age = (datetime.now() - pd.to_datetime(latest_time)).total_seconds() / 3600
                        print(f"   SUCCESS: {len(df):,} rows, {size_mb:.1f}MB, latest: {data_age:.1f}h ago")
                        success_count += 1
                    else:
                        print(f"   WARNING: File created but no data found")
                else:
                    print(f"   ERROR: Dedup file was not created")

            except Exception as e:
                print(f"   ERROR updating {unit}: {e}")
                import traceback
                traceback.print_exc()

        print(f"\nSTEP 2 COMPLETE: {success_count}/{len(stale_units)} units updated successfully")

        # Final verification
        print(f"\nSTEP 3: Final verification")
        print("-" * 25)

        for unit in stale_units:
            try:
                freshness_info = db.get_data_freshness_info(unit)
                data_age_hours = freshness_info.get('data_age_hours', 0)
                latest_time = freshness_info.get('latest_timestamp')

                status = "FRESH" if data_age_hours <= 1.0 else "STALE"
                latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

                print(f"   {unit}: {status} ({data_age_hours:.1f}h old, latest: {latest_str})")

            except Exception as e:
                print(f"   {unit}: ERROR - {e}")

        return success_count > 0

    except Exception as e:
        print(f"ERROR in parquet update: {e}")
        import traceback
        traceback.print_exc()
        return False

    print(f"\nFix completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    success = pcmsb_complete_fix()
    if success:
        print("\nPCMSB fix completed successfully!")
    else:
        print("\nPCMSB fix encountered errors - check logs above")