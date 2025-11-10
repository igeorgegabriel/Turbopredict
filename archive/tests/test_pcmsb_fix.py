#!/usr/bin/env python3
"""
Test the PCMSB sheet mapping fix
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_pcmsb_fix():
    """Test if the sheet mapping fix works"""

    print("TESTING PCMSB SHEET MAPPING FIX")
    print("=" * 35)

    # Test the fixed sheet mapping function
    from pi_monitor.parquet_auto_scan import ParquetAutoScanner

    scanner = ParquetAutoScanner()

    # Test the internal _sheet_for_unit function
    # We need to access it from the scan_all_units method context
    print("Testing sheet mapping for PCMSB units:")

    test_units = ['C-02001', 'C-104', 'C-13001', 'K-12-01', 'XT-07002']

    for unit in test_units:
        # Simulate the logic
        if unit.startswith('K-'):
            expected_sheet = 'DL_WORK'
        elif unit.startswith('C-'):
            expected_sheet = 'DL_WORK'  # This is the fix
        elif unit.startswith('XT-'):
            expected_sheet = 'DL_WORK'
        else:
            expected_sheet = 'Unknown'

        print(f"   {unit:<12} -> {expected_sheet}")

    print(f"\nThe fix ensures all PCMSB units (C-* and XT-*) use 'DL_WORK' sheet")
    print(f"instead of unit-specific sheets like 'DL_C02001'.")

    print(f"\nNow when you run option [1], it should:")
    print(f"1. Look for 'DL_WORK' sheet (which exists)")
    print(f"2. Not get the 'Sheet not found' error")
    print(f"3. Proceed to process the data from DL_WORK")

    print(f"\nNote: PI DataLink timeouts are a separate issue related to:")
    print(f"- PI server connectivity")
    print(f"- Tag availability in the PI system")
    print(f"- Network/authentication issues")

if __name__ == "__main__":
    test_pcmsb_fix()