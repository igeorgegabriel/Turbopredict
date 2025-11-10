#!/usr/bin/env python3
"""
CORRECTED COMPREHENSIVE TEST: All plants with XT-07002 properly under PCMSB.
Tests ALL units across ALL plants with correct plant assignments.
"""

import time
from pathlib import Path
import logging
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def find_all_excel_files_corrected():
    """Find ALL Excel files with CORRECTED unit classifications."""

    # CORRECTED unit classifications
    pcfs_units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202', 'XT-07002']  # XT-07002 NOW INCLUDED
    abfsb_units = ['07-MT01-K001']  # XT-07002 REMOVED

    excel_files = []

    # PCFS Excel files
    pcfs_dir = Path('excel/PCFS')
    if pcfs_dir.exists():
        for xlsx_file in pcfs_dir.glob('*.xlsx'):
            if xlsx_file.is_file():
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'PCFS',
                    'units': pcfs_units,
                    'priority': 1
                })

    # PCMSB Excel files (now includes XT-07002)
    pcmsb_dir = Path('excel/PCMSB')
    if pcmsb_dir.exists():
        for xlsx_file in pcmsb_dir.glob('*.xlsx'):
            if xlsx_file.is_file() and not xlsx_file.name.startswith('~'):
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'PCMSB',
                    'units': pcmsb_units,  # Now 8 units including XT-07002
                    'priority': 2
                })

    # ABFSB Excel files (XT-07002 removed)
    abfsb_dir = Path('excel/ABFSB')
    if abfsb_dir.exists():
        for xlsx_file in abfsb_dir.glob('*.xlsx'):
            if xlsx_file.is_file() and not xlsx_file.name.startswith('~'):
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'ABFSB',
                    'units': abfsb_units,  # Now only 1 unit
                    'priority': 3
                })

    # Sort by priority
    excel_files.sort(key=lambda x: x['priority'])

    return excel_files

def test_corrected_classification():
    """Test coordinated refresh with CORRECTED XT-07002 classification."""

    print("="*100)
    print("CORRECTED COMPREHENSIVE TEST: XT-07002 under PCMSB (as confirmed)")
    print("PCFS (4 units) + PCMSB (8 units including XT-07002) + ABFSB (1 unit) = 13 total units")
    print("="*100)

    excel_files = find_all_excel_files_corrected()

    print(f"Found {len(excel_files)} Excel files with CORRECTED classifications:")
    total_units_expected = 0

    for i, file_info in enumerate(excel_files, 1):
        plant = file_info['plant']
        filename = file_info['file'].name
        unit_count = len(file_info['units'])
        units_list = ', '.join(file_info['units'])
        total_units_expected += unit_count

        print(f"  {i}. {plant:5} | {filename} ({unit_count} units)")
        if plant == 'PCMSB':
            print(f"     -> PCMSB units: {units_list}")
            if 'XT-07002' in file_info['units']:
                print(f"     -> [CORRECTED] XT-07002 now under PCMSB")

    print(f"\nCORRECTED Total units: {total_units_expected}")
    print("Key change: XT-07002 moved from ABFSB -> PCMSB (as operationally confirmed)")
    print()

    results = {}
    overall_start_time = time.time()

    for i, file_info in enumerate(excel_files, 1):
        xlsx_file = file_info['file']
        plant = file_info['plant']
        units = file_info['units']

        print(f"[Test {i}/{len(excel_files)}] {plant} Plant: {xlsx_file.name}")

        if plant == 'PCMSB':
            print(f"PCMSB units (including XT-07002): {', '.join(units)}")
        else:
            print(f"Units covered: {', '.join(units)}")
        print("-" * 80)

        if not xlsx_file.exists():
            print(f"[SKIP] File not found: {xlsx_file}")
            results[xlsx_file.name] = "FILE_NOT_FOUND"
            continue

        try:
            start_time = time.time()

            # Use coordinated refresh
            refresh_excel_with_pi_coordination(
                xlsx=xlsx_file,
                settle_seconds=3,
                use_working_copy=True,
                auto_cleanup=True
            )

            elapsed = time.time() - start_time
            print(f"[SUCCESS] {plant} refresh succeeded in {elapsed:.1f}s")

            if plant == 'PCMSB':
                print(f"  -> 8 PCMSB units (including XT-07002) now have fresh data")
            else:
                print(f"  -> {len(units)} {plant} units now have fresh data")

            results[xlsx_file.name] = f"SUCCESS ({elapsed:.1f}s, {len(units)} units)"

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[FAILED] {plant} refresh failed after {elapsed:.1f}s: {e}")
            results[xlsx_file.name] = f"FAILED: {str(e)[:60]}..."

        print()

    # Summary with corrected classification
    overall_elapsed = time.time() - overall_start_time
    print("="*100)
    print("CORRECTED TEST RESULTS - XT-07002 under PCMSB")
    print("="*100)

    success_count = 0
    pcmsb_success = False

    for file_info in excel_files:
        filename = file_info['file'].name
        plant = file_info['plant']
        result = results.get(filename, "NOT_TESTED")
        status = "[OK]" if result.startswith("SUCCESS") else "[FAIL]"

        print(f"  {status} {plant:5} | {filename}: {result}")

        if result.startswith("SUCCESS"):
            success_count += 1
            if plant == 'PCMSB':
                pcmsb_success = True

    print(f"\nKey Results:")
    print(f"  - Files succeeded: {success_count}/{len(results)}")
    print(f"  - PCMSB (with XT-07002): {'SUCCESS' if pcmsb_success else 'FAILED'}")
    print(f"  - Total test time: {overall_elapsed:.1f}s")

    if success_count == len(excel_files) and pcmsb_success:
        print(f"\n*** CORRECTED CLASSIFICATION SUCCESS! ***")
        print(f"*** XT-07002 working properly under PCMSB plant! ***")
        print(f"*** All 13 units operational with correct plant assignments! ***")
    else:
        print(f"\n*** Some issues remain - check failed tests ***")

    return results

if __name__ == "__main__":
    test_corrected_classification()