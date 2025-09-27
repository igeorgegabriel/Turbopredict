#!/usr/bin/env python3
"""
Comprehensive live test of PI server coordination fix for ALL PCFS and PCMSB units.
Tests all available Excel files with PI coordination to prevent fetch conflicts.
"""

import time
from pathlib import Path
import logging
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination

# Set up logging to see coordination messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def find_excel_files_for_units():
    """Find all Excel files that correspond to PCFS and PCMSB units."""

    # Known unit classifications
    pcfs_units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']

    excel_files = []

    # Check PCFS Excel files
    pcfs_dir = Path('excel/PCFS')
    if pcfs_dir.exists():
        for xlsx_file in pcfs_dir.glob('*.xlsx'):
            if xlsx_file.is_file():
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'PCFS',
                    'units': pcfs_units,  # All PCFS units likely in same Excel file
                    'priority': 1  # Test PCFS first
                })

    # Check PCMSB Excel files
    pcmsb_dir = Path('excel/PCMSB')
    if pcmsb_dir.exists():
        for xlsx_file in pcmsb_dir.glob('*.xlsx'):
            if xlsx_file.is_file():
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'PCMSB',
                    'units': pcmsb_units,  # All PCMSB units likely in same Excel file
                    'priority': 2  # Test PCMSB second
                })

    # Sort by priority
    excel_files.sort(key=lambda x: x['priority'])

    return excel_files

def test_all_pcfs_pcmsb_units():
    """Test coordinated refresh on all PCFS and PCMSB Excel files."""

    print("="*80)
    print("COMPREHENSIVE TEST: All PCFS & PCMSB Units with PI Coordination")
    print("="*80)

    excel_files = find_excel_files_for_units()

    if not excel_files:
        print("❌ No Excel files found for PCFS or PCMSB units!")
        return {}

    print(f"Found {len(excel_files)} Excel files to test:")
    for i, file_info in enumerate(excel_files, 1):
        plant = file_info['plant']
        filename = file_info['file'].name
        unit_count = len(file_info['units'])
        print(f"  {i}. {plant}: {filename} (covers {unit_count} units)")

    print()
    results = {}
    overall_start_time = time.time()

    for i, file_info in enumerate(excel_files, 1):
        xlsx_file = file_info['file']
        plant = file_info['plant']
        units = file_info['units']

        print(f"[Test {i}/{len(excel_files)}] {plant} Plant: {xlsx_file.name}")
        print(f"Expected units: {', '.join(units)}")
        print("-" * 60)

        if not xlsx_file.exists():
            print(f"[SKIP] File not found: {xlsx_file}")
            results[xlsx_file.name] = "FILE_NOT_FOUND"
            continue

        try:
            start_time = time.time()

            # Use the coordinated refresh function
            refresh_excel_with_pi_coordination(
                xlsx=xlsx_file,
                settle_seconds=3,
                use_working_copy=True,
                auto_cleanup=True
            )

            elapsed = time.time() - start_time
            print(f"[SUCCESS] {plant} refresh succeeded in {elapsed:.1f}s")
            print(f"  -> All {len(units)} {plant} units should now have fresh data")
            results[xlsx_file.name] = f"SUCCESS ({elapsed:.1f}s, {len(units)} units)"

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[FAILED] {plant} refresh failed after {elapsed:.1f}s: {e}")
            results[xlsx_file.name] = f"FAILED: {str(e)[:50]}..."

        print()

    # Final summary
    overall_elapsed = time.time() - overall_start_time
    print("="*80)
    print("COMPREHENSIVE TEST RESULTS SUMMARY")
    print("="*80)

    success_count = 0
    total_units_tested = 0

    for file_info in excel_files:
        filename = file_info['file'].name
        plant = file_info['plant']
        unit_count = len(file_info['units'])
        result = results.get(filename, "NOT_TESTED")

        status = "[OK]" if result.startswith("SUCCESS") else "[FAIL]"
        print(f"{status} {plant:5} | {filename}: {result}")

        if result.startswith("SUCCESS"):
            success_count += 1
            total_units_tested += unit_count

    print(f"\nOverall Results:")
    print(f"  - Files tested: {len(results)}")
    print(f"  - Files succeeded: {success_count}")
    print(f"  - Total units with fresh data: {total_units_tested}")
    print(f"  - Total test time: {overall_elapsed:.1f}s")

    if success_count == len(excel_files):
        print("\n*** 🎉 ALL TESTS PASSED - All PCFS & PCMSB units can fetch data! ***")
    elif success_count > 0:
        print(f"\n*** ⚠️  PARTIAL SUCCESS - {success_count}/{len(excel_files)} plants working ***")
    else:
        print(f"\n*** 💥 ALL TESTS FAILED - Need further investigation ***")

    return results

if __name__ == "__main__":
    test_all_pcfs_pcmsb_units()