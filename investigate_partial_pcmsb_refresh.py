#!/usr/bin/env python3
"""
Intensive investigation: Why some PCMSB units remain stale
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pi_monitor.parquet_database import ParquetDatabase

def investigate_partial_pcmsb_refresh():
    """Investigate why only some PCMSB units are refreshing"""

    print("INTENSIVE INVESTIGATION: PARTIAL PCMSB REFRESH")
    print("=" * 70)

    # Get all units and their status
    db = ParquetDatabase()
    all_units = db.get_all_units()
    pcmsb_units = [unit for unit in all_units if unit.startswith('C-')]

    print(f"Total PCMSB units found: {len(pcmsb_units)}")
    print(f"PCMSB units: {pcmsb_units}")

    # Categorize by freshness
    fresh_units = []
    stale_units = []
    error_units = []

    print(f"\nDetailed PCMSB Unit Analysis:")
    print(f"{'Unit':<12} {'Age(h)':<8} {'Status':<8} {'Latest Time':<20} {'Records':<12}")
    print("-" * 80)

    for unit in pcmsb_units:
        try:
            info = db.get_data_freshness_info(unit)
            age_hours = info.get('data_age_hours', 999)
            latest_time = info.get('latest_timestamp')
            total_records = info.get('total_records', 0)

            if age_hours < 2.0:
                status = "FRESH"
                fresh_units.append(unit)
            else:
                status = "STALE"
                stale_units.append(unit)

            latest_str = latest_time.strftime('%Y-%m-%d %H:%M') if latest_time else "None"
            print(f"{unit:<12} {age_hours:<8.1f} {status:<8} {latest_str:<20} {total_records:<12,}")

        except Exception as e:
            print(f"{unit:<12} ERROR    Failed to get info: {e}")
            error_units.append(unit)

    print(f"\nSUMMARY:")
    print(f"Fresh PCMSB units: {len(fresh_units)} - {fresh_units}")
    print(f"Stale PCMSB units: {len(stale_units)} - {stale_units}")
    print(f"Error PCMSB units: {len(error_units)} - {error_units}")

    # Check if stale units have parquet files
    print(f"\nPARQUET FILE ANALYSIS FOR STALE UNITS:")
    print("-" * 50)

    data_dir = Path("data/processed")
    for unit in stale_units:
        unit_files = list(data_dir.glob(f"*{unit}*.parquet"))
        print(f"\n{unit}:")
        print(f"  Parquet files: {len(unit_files)}")

        for file_path in unit_files[:3]:  # Show first 3 files
            try:
                stat = file_path.stat()
                file_age = (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).total_seconds() / 3600
                print(f"    {file_path.name}: {file_age:.1f}h old")
            except Exception as e:
                print(f"    {file_path.name}: Error - {e}")

    # Check Excel data structure for stale units
    print(f"\nEXCEL DATA INVESTIGATION:")
    print("-" * 50)

    excel_path = Path("excel/PCMSB_Automation.xlsx")
    if excel_path.exists():
        try:
            # Read the current Excel structure
            df = pd.read_excel(excel_path, sheet_name='Sheet1')
            print(f"Excel columns: {list(df.columns)}")
            print(f"Excel shape: {df.shape}")

            # Check if all units are represented in the Excel file
            print(f"\nChecking if stale units are in Excel data...")

            # Look for unit references in column names or data
            excel_content = str(df.to_string()).upper()

            missing_in_excel = []
            found_in_excel = []

            for unit in stale_units:
                unit_patterns = [unit.upper(), unit.replace('-', ''), unit.replace('C-', '')]
                found = any(pattern in excel_content for pattern in unit_patterns)

                if found:
                    found_in_excel.append(unit)
                    print(f"  {unit}: FOUND in Excel")
                else:
                    missing_in_excel.append(unit)
                    print(f"  {unit}: NOT FOUND in Excel data")

            print(f"\nUnits found in Excel: {found_in_excel}")
            print(f"Units missing from Excel: {missing_in_excel}")

            if missing_in_excel:
                print(f"\nCRITICAL FINDING: {len(missing_in_excel)} stale units are missing from Excel!")
                print("This suggests the PCMSB Excel file only contains data for some units.")

        except Exception as e:
            print(f"Error reading Excel file: {e}")

    # Check if the issue is with the scanning logic
    print(f"\nSCANNING LOGIC INVESTIGATION:")
    print("-" * 50)

    try:
        from pi_monitor.parquet_auto_scan import ParquetAutoScanner

        scanner = ParquetAutoScanner()

        # Test scanning for a specific stale unit
        if stale_units:
            test_unit = stale_units[0]
            print(f"Testing scan logic for stale unit: {test_unit}")

            # Check if unit gets mapped to correct Excel file
            try:
                excel_file = scanner.get_excel_file_for_unit(test_unit)
                print(f"  Maps to Excel file: {excel_file}")

                if excel_file and excel_file.exists():
                    print(f"  Excel file exists: YES")
                else:
                    print(f"  Excel file exists: NO - This could be the issue!")

            except Exception as e:
                print(f"  Excel mapping error: {e}")

    except Exception as e:
        print(f"Error testing scan logic: {e}")

    # Root cause analysis
    print(f"\nROOT CAUSE ANALYSIS:")
    print("=" * 50)

    issues = []

    if len(fresh_units) > 0 and len(stale_units) > 0:
        issues.append("PARTIAL REFRESH: Some units refresh while others don't")

    if missing_in_excel:
        issues.append(f"MISSING DATA: {len(missing_in_excel)} units not in Excel file")

    # Time pattern analysis
    if stale_units:
        print(f"Stale unit timestamps:")
        for unit in stale_units[:3]:
            try:
                info = db.get_data_freshness_info(unit)
                latest_time = info.get('latest_timestamp')
                if latest_time:
                    print(f"  {unit}: {latest_time}")
            except:
                pass

    print(f"\nIDENTIFIED ISSUES:")
    for i, issue in enumerate(issues, 1):
        print(f"{i}. {issue}")

    print(f"\nRECOMMENDED ACTIONS:")
    print("1. Check if PCMSB Excel file contains data for ALL required units")
    print("2. Verify PI DataLink configuration for missing units")
    print("3. Check if stale units need different Excel sheets or files")
    print("4. Test manual refresh of stale units individually")

if __name__ == "__main__":
    investigate_partial_pcmsb_refresh()