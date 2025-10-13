#!/usr/bin/env python3
"""
PCMSB Excel reader - reads existing DL_WORK data and converts to parquet
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime
import numpy as np

def read_pcmsb_excel_to_parquet(excel_path, unit, output_path):
    """Read PCMSB Excel DL_WORK data and convert to parquet format"""

    print(f"Reading PCMSB data for {unit} from {excel_path}")

    try:
        # Read the DL_WORK sheet
        df = pd.read_excel(excel_path, sheet_name='DL_WORK', header=None)

        print(f"   Read {len(df)} rows from DL_WORK")

        if len(df) == 0:
            print("   ERROR: No data in DL_WORK sheet")
            return False

        # Extract time-value pairs
        time_values = []

        for i in range(len(df)):
            row = df.iloc[i]
            if len(row) >= 2:
                time_col = row.iloc[0]
                value_col = row.iloc[1]

                # Skip invalid rows
                if pd.isna(time_col) or pd.isna(value_col):
                    continue

                # Check if time_col is a datetime
                if isinstance(time_col, (datetime, pd.Timestamp)):
                    time_values.append({
                        'time': pd.to_datetime(time_col),
                        'value': float(value_col),
                        'plant': 'PCMSB',
                        'unit': unit,
                        'tag': f'PCM.{unit}.AGGREGATE.PV'  # Create a synthetic tag name
                    })

        if not time_values:
            print("   ERROR: No valid time-value pairs found")
            return False

        # Convert to DataFrame
        result_df = pd.DataFrame(time_values)

        print(f"   Extracted {len(result_df)} valid data points")
        print(f"   Time range: {result_df['time'].min()} to {result_df['time'].max()}")

        # Sort by time
        result_df = result_df.sort_values('time').reset_index(drop=True)

        # Save to parquet
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result_df.to_parquet(output_path, index=False)

        print(f"   Saved to {output_path}")

        # Get file size
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"   File size: {size_mb:.1f} MB")

        return True

    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def update_all_pcmsb_units():
    """Update all PCMSB units using Excel data"""

    print("UPDATING ALL PCMSB UNITS FROM EXCEL")
    print("=" * 40)

    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")

    if not excel_path.exists():
        print("ERROR: PCMSB Excel file not found")
        return False

    # PCMSB units
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']

    success_count = 0

    for unit in pcmsb_units:
        print(f"\nProcessing {unit}...")

        # Output paths
        output_path = Path(f"data/processed/{unit}_1y_0p1h.parquet")

        if read_pcmsb_excel_to_parquet(excel_path, unit, output_path):
            success_count += 1

            # Also create dedup version
            try:
                from pi_monitor.clean import dedup_parquet
                dedup_path = dedup_parquet(output_path)
                dedup_size_mb = dedup_path.stat().st_size / (1024 * 1024)
                print(f"   Dedup saved to {dedup_path.name} ({dedup_size_mb:.1f} MB)")
            except Exception as e:
                print(f"   WARNING: Dedup failed: {e}")

    print(f"\nCOMPLETE: {success_count}/{len(pcmsb_units)} units updated successfully")

    # Verify with database
    try:
        from pi_monitor.parquet_database import ParquetDatabase
        db = ParquetDatabase()

        print(f"\nVerification:")
        for unit in pcmsb_units:
            try:
                freshness_info = db.get_data_freshness_info(unit)
                data_age_hours = freshness_info.get('data_age_hours', 0)
                latest_time = freshness_info.get('latest_timestamp')
                total_records = freshness_info.get('total_records', 0)

                status = "FRESH" if data_age_hours <= 1.0 else "STALE"
                latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

                print(f"   {unit}: {status} ({data_age_hours:.1f}h old, {total_records:,} records, latest: {latest_str})")

            except Exception as e:
                print(f"   {unit}: ERROR - {e}")

    except Exception as e:
        print(f"Verification failed: {e}")

    return success_count > 0

if __name__ == "__main__":
    success = update_all_pcmsb_units()
    if success:
        print("\nPCMSB units updated successfully!")
        print("All PCMSB parquet files should now be fresh.")
    else:
        print("\nPCMSB update failed - check errors above")