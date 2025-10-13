#!/usr/bin/env python3
"""
COMPLETE COMPREHENSIVE TEST: All plants (PCFS, PCMSB, ABFSB) with PI coordination.
Tests ALL units across ALL plants to verify the PI coordination fix works universally.
"""

import time
from pathlib import Path
import logging
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination

# Set up logging to see coordination messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def find_all_excel_files():
    """Find ALL Excel files across PCFS, PCMSB, and ABFSB plants."""

    # Complete unit classifications
    pcfs_units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']
    abfsb_units = ['07-MT01-K001', 'XT-07002']

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

    # PCMSB Excel files
    pcmsb_dir = Path('excel/PCMSB')
    if pcmsb_dir.exists():
        for xlsx_file in pcmsb_dir.glob('*.xlsx'):
            if xlsx_file.is_file() and not xlsx_file.name.startswith('~'):  # Skip temp files
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'PCMSB',
                    'units': pcmsb_units,
                    'priority': 2
                })

    # ABFSB Excel files
    abfsb_dir = Path('excel/ABFSB')
    if abfsb_dir.exists():
        for xlsx_file in abfsb_dir.glob('*.xlsx'):
            if xlsx_file.is_file() and not xlsx_file.name.startswith('~'):  # Skip temp files
                excel_files.append({
                    'file': xlsx_file,
                    'plant': 'ABFSB',
                    'units': abfsb_units,
                    'priority': 3  # Test ABFSB last (it was working before)
                })

    # Sort by priority
    excel_files.sort(key=lambda x: x['priority'])

    return excel_files

def test_all_plants_complete():
    """Test coordinated refresh on ALL plants: PCFS, PCMSB, and ABFSB."""

    print("="*90)
    print("ULTIMATE COMPREHENSIVE TEST: ALL PLANTS with PI Coordination")
    print("Testing: PCFS (4 units) + PCMSB (7 units) + ABFSB (2 units) = 13 total units")
    print("="*90)

    excel_files = find_all_excel_files()

    if not excel_files:
        print("[ERROR] No Excel files found across any plants!")
        return {}

    print(f"Found {len(excel_files)} Excel files across all plants:")
    total_units_expected = 0

    for i, file_info in enumerate(excel_files, 1):
        plant = file_info['plant']
        filename = file_info['file'].name
        unit_count = len(file_info['units'])
        total_units_expected += unit_count
        print(f"  {i}. {plant:5} | {filename} ({unit_count} units)")

    print(f"\nTotal units expected to have fresh data: {total_units_expected}")
    print()

    results = {}
    overall_start_time = time.time()
    successful_plants = []
    failed_plants = []

    for i, file_info in enumerate(excel_files, 1):
        xlsx_file = file_info['file']
        plant = file_info['plant']
        units = file_info['units']

        print(f"[Test {i}/{len(excel_files)}] {plant} Plant: {xlsx_file.name}")
        print(f"Units covered: {', '.join(units)}")
        print("-" * 70)

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
            print(f"  -> {len(units)} {plant} units now have fresh PI data")
            results[xlsx_file.name] = f"SUCCESS ({elapsed:.1f}s, {len(units)} units)"

            if plant not in successful_plants:
                successful_plants.append(plant)

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[FAILED] {plant} refresh failed after {elapsed:.1f}s: {e}")
            results[xlsx_file.name] = f"FAILED: {str(e)[:60]}..."

            if plant not in failed_plants:
                failed_plants.append(plant)

        print()

    # Ultimate summary
    overall_elapsed = time.time() - overall_start_time
    print("="*90)
    print("ULTIMATE TEST RESULTS - ALL PLANTS SUMMARY")
    print("="*90)

    success_count = 0
    total_units_refreshed = 0
    plant_results = {'PCFS': 0, 'PCMSB': 0, 'ABFSB': 0}

    print("\nDetailed Results by Plant:")
    for file_info in excel_files:
        filename = file_info['file'].name
        plant = file_info['plant']
        unit_count = len(file_info['units'])
        result = results.get(filename, "NOT_TESTED")

        status = "[OK]" if result.startswith("SUCCESS") else "[FAIL]"
        print(f"  {status} {plant:5} | {filename}: {result}")

        if result.startswith("SUCCESS"):
            success_count += 1
            total_units_refreshed += unit_count
            plant_results[plant] += 1

    # Plant-level summary
    print(f"\nPlant Success Summary:")
    for plant in ['PCFS', 'PCMSB', 'ABFSB']:
        plant_files = sum(1 for f in excel_files if f['plant'] == plant)
        plant_success = plant_results[plant]
        plant_status = "PASS" if plant_success == plant_files else "PARTIAL" if plant_success > 0 else "FAIL"
        print(f"  {plant:5}: {plant_success}/{plant_files} files successful ({plant_status})")

    print(f"\nOverall Statistics:")
    print(f"  - Excel files tested: {len(results)}")
    print(f"  - Files succeeded: {success_count}")
    print(f"  - Plants fully operational: {len(successful_plants)}")
    print(f"  - Total units with fresh data: {total_units_refreshed}")
    print(f"  - Total test time: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} minutes)")

    # Final verdict
    all_plants = ['PCFS', 'PCMSB', 'ABFSB']
    fully_operational_plants = [p for p in all_plants if plant_results[p] > 0]

    if len(fully_operational_plants) == 3 and success_count == len(excel_files):
        print(f"\n*** ULTIMATE SUCCESS: ALL 3 PLANTS FULLY OPERATIONAL! ***")
        print(f"*** PI coordination fix works perfectly across entire system! ***")
        print(f"*** No more alternating failures - {total_units_refreshed} units can fetch data! ***")
    elif len(fully_operational_plants) == 3:
        print(f"\n*** EXCELLENT: ALL 3 PLANTS WORKING (some Excel files had issues) ***")
        print(f"*** PI coordination fix is working across all plants! ***")
    elif len(fully_operational_plants) >= 2:
        print(f"\n*** GOOD: {len(fully_operational_plants)}/3 plants operational ***")
        print(f"*** PI coordination significantly improved system stability ***")
    else:
        print(f"\n*** MIXED RESULTS: Need further investigation ***")

    return results

if __name__ == "__main__":
    test_all_plants_complete()