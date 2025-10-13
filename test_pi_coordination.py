#!/usr/bin/env python3
"""
Live test of PI server coordination fix for alternating fetch failures.
Tests PCFS and PCMSB units sequentially with coordination.
"""

import time
from pathlib import Path
import logging
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination

# Set up logging to see coordination messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_coordinated_refresh():
    """Test the coordinated refresh on PCFS and PCMSB units."""

    # Test files
    test_files = [
        Path('excel/PCFS/PCFS_Automation_2.xlsx'),
        Path('excel/PCMSB/PCMSB_Automation.xlsx')
    ]

    print("="*60)
    print("LIVE TEST: PI Server Coordination Fix")
    print("="*60)
    print(f"Testing {len(test_files)} units with coordinated PI access...")
    print()

    results = {}

    for i, xlsx_file in enumerate(test_files, 1):
        if not xlsx_file.exists():
            print(f"[SKIP] File not found: {xlsx_file}")
            results[xlsx_file.name] = "FILE_NOT_FOUND"
            continue

        unit_name = "PCFS" if "PCFS" in str(xlsx_file) else "PCMSB"

        print(f"[Test {i}/{len(test_files)}] Testing {unit_name}: {xlsx_file.name}")
        print("-" * 40)

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
            print(f"[SUCCESS] {unit_name} refresh succeeded in {elapsed:.1f}s")
            results[xlsx_file.name] = f"SUCCESS ({elapsed:.1f}s)"

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[FAILED] {unit_name} refresh failed after {elapsed:.1f}s: {e}")
            results[xlsx_file.name] = f"FAILED: {str(e)[:50]}..."

        print()

    # Summary
    print("="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)

    success_count = 0
    for filename, result in results.items():
        status = "[OK]" if result.startswith("SUCCESS") else "[FAIL]"
        print(f"{status} {filename}: {result}")
        if result.startswith("SUCCESS"):
            success_count += 1

    print()
    print(f"Overall: {success_count}/{len(results)} units succeeded")

    if success_count == len(results):
        print("*** ALL TESTS PASSED - PI coordination fix is working! ***")
    elif success_count > 0:
        print("*** PARTIAL SUCCESS - Some units still having issues ***")
    else:
        print("*** ALL TESTS FAILED - Need further investigation ***")

    return results

if __name__ == "__main__":
    test_coordinated_refresh()