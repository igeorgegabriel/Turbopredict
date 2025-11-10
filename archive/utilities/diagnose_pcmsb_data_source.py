#!/usr/bin/env python3
"""
Diagnose PCMSB data source issue and provide solutions
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime

def diagnose_pcmsb_data_source():
    """Diagnose why PCMSB units don't have real data"""

    print("PCMSB DATA SOURCE DIAGNOSIS")
    print("=" * 60)

    # Check what data we actually have for each unit in parquet files
    print("STEP 1: ANALYZING EXISTING PARQUET DATA")
    print("-" * 50)

    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']
    data_dir = Path("data/processed")

    unit_data_sources = {}

    for unit in pcmsb_units:
        print(f"\nUnit: {unit}")

        # Find parquet files for this unit
        unit_files = list(data_dir.glob(f"*{unit}*.parquet"))
        if unit_files:
            latest_file = max(unit_files, key=lambda x: x.stat().st_mtime)
            print(f"  Latest parquet: {latest_file.name}")

            try:
                df = pd.read_parquet(latest_file)
                if not df.empty and 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'])
                    latest_data = df['time'].max()
                    earliest_data = df['time'].min()
                    record_count = len(df)

                    print(f"  Records: {record_count:,}")
                    print(f"  Time range: {earliest_data.date()} to {latest_data.date()}")
                    print(f"  Latest data: {latest_data}")

                    # Check data freshness
                    age_hours = (datetime.now() - latest_data).total_seconds() / 3600
                    print(f"  Age: {age_hours:.1f} hours")

                    unit_data_sources[unit] = {
                        'has_parquet': True,
                        'latest_data': latest_data,
                        'age_hours': age_hours,
                        'records': record_count
                    }
                else:
                    print(f"  ERROR: Invalid parquet data")
                    unit_data_sources[unit] = {'has_parquet': False, 'error': 'Invalid data'}

            except Exception as e:
                print(f"  ERROR: Failed to read parquet: {e}")
                unit_data_sources[unit] = {'has_parquet': False, 'error': str(e)}
        else:
            print(f"  No parquet files found")
            unit_data_sources[unit] = {'has_parquet': False, 'error': 'No files'}

    # Check Excel data structure
    print(f"\n\nSTEP 2: ANALYZING EXCEL DATA STRUCTURE")
    print("-" * 50)

    excel_path = Path("excel/PCMSB_Automation.xlsx")
    if excel_path.exists():
        excel_file = pd.ExcelFile(excel_path)
        print(f"Excel sheets: {excel_file.sheet_names}")

        for unit in pcmsb_units:
            sheet_name = f"DL_{unit.replace('-', '_')}"
            print(f"\n{unit} -> {sheet_name}:")

            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                print(f"  Shape: {df.shape}")

                if 'TIME' in df.columns and len(df) > 0:
                    latest_excel = df['TIME'].max()
                    record_count = len(df)
                    print(f"  Latest Excel time: {latest_excel}")
                    print(f"  Records: {record_count:,}")

                    # Compare with parquet data
                    if unit in unit_data_sources and unit_data_sources[unit]['has_parquet']:
                        parquet_latest = unit_data_sources[unit]['latest_data']
                        time_diff = abs((latest_excel - parquet_latest).total_seconds() / 3600)
                        print(f"  vs Parquet: {time_diff:.1f}h difference")

                        if record_count < 1000:
                            print(f"  STATUS: PLACEHOLDER DATA (only {record_count} rows)")
                        elif time_diff > 24:
                            print(f"  STATUS: STALE EXCEL DATA ({time_diff:.1f}h behind parquet)")
                        else:
                            print(f"  STATUS: GOOD DATA")
                    else:
                        print(f"  STATUS: NO PARQUET COMPARISON")
                else:
                    print(f"  STATUS: NO TIME DATA")

            except Exception as e:
                print(f"  ERROR: {e}")

    # Root cause analysis
    print(f"\n\nSTEP 3: ROOT CAUSE ANALYSIS")
    print("-" * 50)

    issues = []
    units_with_real_data = []
    units_with_placeholder = []
    units_missing_data = []

    for unit in pcmsb_units:
        if unit in unit_data_sources and unit_data_sources[unit]['has_parquet']:
            # Check if Excel has real data for this unit
            sheet_name = f"DL_{unit.replace('-', '_')}"
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                if len(df) > 1000:
                    units_with_real_data.append(unit)
                else:
                    units_with_placeholder.append(unit)
            except:
                units_missing_data.append(unit)
        else:
            units_missing_data.append(unit)

    print(f"Units with real Excel data: {units_with_real_data}")
    print(f"Units with placeholder Excel data: {units_with_placeholder}")
    print(f"Units missing Excel data: {units_missing_data}")

    if units_with_placeholder:
        issues.append(f"PLACEHOLDER DATA: {len(units_with_placeholder)} units have placeholder Excel data")

    if units_missing_data:
        issues.append(f"MISSING DATA: {len(units_missing_data)} units missing from Excel")

    # Solutions
    print(f"\n\nSTEP 4: RECOMMENDED SOLUTIONS")
    print("-" * 50)

    if issues:
        for i, issue in enumerate(issues, 1):
            print(f"{i}. {issue}")

        print(f"\nSOLUTION OPTIONS:")
        print("A. COPY EXISTING DATA: Use parquet data to populate Excel sheets")
        print("B. PI DATALINK CONFIG: Configure PI DataLink to pull data for all units")
        print("C. HYBRID APPROACH: Use Sheet1 for all units (simpler structure)")

        print(f"\nRECOMMENDED: Option A (Copy existing data to Excel sheets)")
        print("This will immediately make all units fresh using existing parquet data.")

    else:
        print("No issues found - all units should be working correctly.")

    return {
        'units_with_real_data': units_with_real_data,
        'units_with_placeholder': units_with_placeholder,
        'units_missing_data': units_missing_data,
        'issues': issues
    }

if __name__ == "__main__":
    diagnose_pcmsb_data_source()