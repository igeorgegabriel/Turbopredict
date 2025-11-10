#!/usr/bin/env python3
"""
Test PCMSB multi-unit fix with intensive testing
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.parquet_database import ParquetDatabase
from datetime import datetime
import time

def test_pcmsb_multi_unit_fix():
    """Intensive test of PCMSB multi-unit fix"""

    print("INTENSIVE TEST: PCMSB MULTI-UNIT FIX")
    print("=" * 70)

    # Check initial status
    db = ParquetDatabase()
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']

    print("INITIAL STATUS:")
    print("-" * 50)
    print(f"{'Unit':<12} {'Age(h)':<8} {'Status':<8} {'Latest Time':<20}")
    print("-" * 50)

    initial_status = {}
    for unit in pcmsb_units:
        try:
            info = db.get_data_freshness_info(unit)
            age_hours = info.get('data_age_hours', 999)
            latest_time = info.get('latest_timestamp')
            status = "FRESH" if age_hours < 2.0 else "STALE"
            initial_status[unit] = {'age': age_hours, 'status': status, 'time': latest_time}

            latest_str = latest_time.strftime('%Y-%m-%d %H:%M') if latest_time else "None"
            print(f"{unit:<12} {age_hours:<8.1f} {status:<8} {latest_str:<20}")
        except Exception as e:
            print(f"{unit:<12} ERROR    Failed: {e}")
            initial_status[unit] = {'error': str(e)}

    initial_stale = [unit for unit, info in initial_status.items()
                    if 'age' in info and info['age'] > 2.0]

    print(f"\nInitial stale units: {len(initial_stale)} - {initial_stale}")

    if not initial_stale:
        print("All units already fresh! Test not needed.")
        return

    # Run refresh with the fix
    print(f"\nRUNNING REFRESH WITH MULTI-UNIT FIX...")
    print("-" * 50)

    try:
        scanner = ParquetAutoScanner()

        print(f"Starting refresh at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Run with aggressive freshness requirement to force refresh
        results = scanner.refresh_stale_units_with_progress(max_age_hours=0.5)

        print(f"Refresh completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Check results
        print(f"\nRefresh results: {results}")

    except Exception as e:
        print(f"ERROR during refresh: {e}")
        import traceback
        traceback.print_exc()
        return

    # Wait a moment for file system to settle
    print("\nWaiting for file system to settle...")
    time.sleep(5)

    # Check final status
    print(f"\nFINAL STATUS:")
    print("-" * 50)
    print(f"{'Unit':<12} {'Age(h)':<8} {'Status':<8} {'Change':<10} {'Latest Time':<20}")
    print("-" * 80)

    final_status = {}
    improvements = []
    still_stale = []

    for unit in pcmsb_units:
        try:
            info = db.get_data_freshness_info(unit)
            age_hours = info.get('data_age_hours', 999)
            latest_time = info.get('latest_timestamp')
            status = "FRESH" if age_hours < 2.0 else "STALE"
            final_status[unit] = {'age': age_hours, 'status': status, 'time': latest_time}

            # Compare with initial
            if unit in initial_status and 'age' in initial_status[unit]:
                initial_age = initial_status[unit]['age']
                improvement = initial_age - age_hours
                change = f"{improvement:+.1f}h" if improvement != 0 else "0.0h"

                if improvement > 1.0:  # Significant improvement
                    improvements.append(unit)
                elif age_hours > 2.0:  # Still stale
                    still_stale.append(unit)
            else:
                change = "N/A"

            latest_str = latest_time.strftime('%Y-%m-%d %H:%M') if latest_time else "None"
            print(f"{unit:<12} {age_hours:<8.1f} {status:<8} {change:<10} {latest_str:<20}")

        except Exception as e:
            print(f"{unit:<12} ERROR    Failed: {e}")

    # Analysis
    print(f"\nTEST RESULTS ANALYSIS:")
    print("=" * 50)
    print(f"Units improved: {len(improvements)} - {improvements}")
    print(f"Units still stale: {len(still_stale)} - {still_stale}")

    total_fresh = len([u for u, info in final_status.items()
                     if 'age' in info and info['age'] < 2.0])

    print(f"Total fresh units: {total_fresh}/{len(pcmsb_units)}")

    if len(still_stale) == 0:
        print("\nSUCCESS: ALL PCMSB UNITS ARE NOW FRESH!")
        return True
    elif len(improvements) > 0:
        print(f"\nPARTIAL SUCCESS: {len(improvements)} units improved, {len(still_stale)} still need work")

        # Additional analysis for remaining stale units
        print(f"\nAnalyzing remaining stale units...")
        for unit in still_stale[:3]:  # Check first 3
            print(f"\nDEBUG {unit}:")
            excel_path = Path("excel/PCMSB_Automation.xlsx")
            sheet_name = f"DL_{unit.replace('-', '_')}"

            try:
                import pandas as pd
                df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=5)
                print(f"  Sheet {sheet_name}: {df.shape[1]} columns, {df.shape[0]} rows preview")
                print(f"  Columns: {list(df.columns)}")
                if 'TIME' in df.columns:
                    latest_excel_time = df['TIME'].max()
                    print(f"  Latest Excel time: {latest_excel_time}")
                else:
                    print(f"  No TIME column found!")
            except Exception as e:
                print(f"  Error reading sheet {sheet_name}: {e}")

        return False
    else:
        print(f"\nFAILURE: No units improved")
        print(f"The fix may not be working correctly")
        return False

if __name__ == "__main__":
    test_pcmsb_multi_unit_fix()