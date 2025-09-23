#!/usr/bin/env python3
"""
Populate PCMSB Excel sheets with real data from parquet files
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import shutil

def populate_pcmsb_excel_sheets():
    """Populate Excel sheets with real data from parquet files"""

    print("POPULATING PCMSB EXCEL SHEETS WITH REAL DATA")
    print("=" * 70)

    # Create backup first
    excel_path = Path("excel/PCMSB_Automation.xlsx")
    backup_path = excel_path.with_name(f"PCMSB_Automation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    shutil.copy2(excel_path, backup_path)
    print(f"Backup created: {backup_path.name}")

    # Units that need real data (exclude C-104 which already has good data)
    units_to_populate = ['C-02001', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']
    data_dir = Path("data/processed")

    print(f"\nPopulating {len(units_to_populate)} units with real data...")

    try:
        # Read existing Excel structure
        excel_file = pd.ExcelFile(excel_path)
        sheets_data = {}

        # Preserve existing sheets
        for sheet_name in excel_file.sheet_names:
            try:
                sheets_data[sheet_name] = pd.read_excel(excel_path, sheet_name=sheet_name)
                print(f"Preserved sheet: {sheet_name}")
            except Exception as e:
                print(f"Warning: Could not preserve sheet {sheet_name}: {e}")

        # Populate sheets for units that need real data
        for unit in units_to_populate:
            print(f"\nProcessing {unit}...")

            # Find latest parquet file for this unit
            unit_files = list(data_dir.glob(f"*{unit}*.parquet"))
            if not unit_files:
                print(f"  No parquet files found for {unit}")
                continue

            # Use dedup file if available, otherwise latest file
            dedup_files = [f for f in unit_files if 'dedup' in f.name]
            if dedup_files:
                parquet_file = max(dedup_files, key=lambda x: x.stat().st_mtime)
            else:
                parquet_file = max(unit_files, key=lambda x: x.stat().st_mtime)

            print(f"  Using: {parquet_file.name}")

            try:
                # Load parquet data
                df = pd.read_parquet(parquet_file)
                print(f"  Loaded: {len(df):,} records")

                if df.empty or 'time' not in df.columns:
                    print(f"  ERROR: Invalid data structure")
                    continue

                # Prepare data for Excel
                df['time'] = pd.to_datetime(df['time'])

                # Get recent data (last 30 days for Excel efficiency, max 800K rows)
                cutoff_date = datetime.now() - timedelta(days=30)
                recent_df = df[df['time'] >= cutoff_date].copy()

                if recent_df.empty or len(recent_df) > 800000:
                    print(f"  Using last 800K records for Excel compatibility")
                    recent_df = df.tail(800000).copy()
                elif len(recent_df) > 800000:
                    print(f"  Trimming to last 800K records for Excel compatibility")
                    recent_df = recent_df.tail(800000).copy()

                print(f"  Recent data: {len(recent_df):,} records")

                # Create Excel-format data
                excel_df = pd.DataFrame({
                    'TIME': recent_df['time'],
                    f'{unit}_Value': recent_df['value'] if 'value' in recent_df.columns else recent_df.iloc[:, 1]
                })

                # Remove any duplicates and sort by time
                excel_df = excel_df.drop_duplicates(subset=['TIME']).sort_values('TIME')

                print(f"  Excel data: {len(excel_df):,} records")
                print(f"  Time range: {excel_df['TIME'].min()} to {excel_df['TIME'].max()}")

                # Store for writing
                sheet_name = f"DL_{unit.replace('-', '_')}"
                sheets_data[sheet_name] = excel_df

                print(f"  SUCCESS: {sheet_name} ready with real data")

            except Exception as e:
                print(f"  ERROR: Failed to process {unit}: {e}")
                continue

        # Write all sheets to Excel
        print(f"\nWriting updated Excel file...")
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  Written: {sheet_name} ({len(data):,} rows)")

        print(f"\nSUCCESS: PCMSB Excel file updated with real data!")

        # Verify the update
        print(f"\nVerifying update...")
        excel_file = pd.ExcelFile(excel_path)

        for unit in units_to_populate:
            sheet_name = f"DL_{unit.replace('-', '_')}"
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=5)
                latest_time = pd.read_excel(excel_path, sheet_name=sheet_name)['TIME'].max()
                record_count = len(pd.read_excel(excel_path, sheet_name=sheet_name))

                print(f"  {unit}: {record_count:,} rows, latest: {latest_time}")

            except Exception as e:
                print(f"  {unit}: Verification failed - {e}")

        return True

    except Exception as e:
        print(f"\nERROR: Failed to populate Excel sheets: {e}")
        import traceback
        traceback.print_exc()

        # Restore backup
        try:
            shutil.copy2(backup_path, excel_path)
            print("Restored original file from backup")
        except:
            pass

        return False

if __name__ == "__main__":
    populate_pcmsb_excel_sheets()