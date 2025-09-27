#!/usr/bin/env python3
"""
Fix PCMSB sheet mapping issue - system expects DL_C02001 but Excel has DL_WORK
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def fix_pcmsb_sheet_mapping():
    """Fix the sheet mapping for PCMSB units"""

    print("FIXING PCMSB SHEET MAPPING")
    print("=" * 30)

    # The issue: system looks for 'DL_C02001' but Excel has 'DL_WORK'
    # We need to check where this mapping is defined

    # Check parquet_auto_scan.py for sheet logic
    scan_file = Path("pi_monitor/parquet_auto_scan.py")

    if scan_file.exists():
        content = scan_file.read_text()

        print("Checking sheet mapping logic in parquet_auto_scan.py...")

        # Look for DL_ pattern
        if "DL_" in content:
            print("Found DL_ references in parquet_auto_scan.py")

            # The issue is likely in the sheet name generation
            # For PCMSB units, it should use "DL_WORK" not "DL_C02001"

            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'DL_' in line and ('sheet' in line.lower() or 'work' in line.lower()):
                    print(f"Line {i+1}: {line.strip()}")

    # Check batch.py for sheet parameter
    batch_file = Path("pi_monitor/batch.py")
    if batch_file.exists():
        content = batch_file.read_text()

        print("\nChecking sheet parameter in batch.py...")

        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'work_sheet' in line or 'DL_WORK' in line:
                print(f"Line {i+1}: {line.strip()}")

    print(f"\nDIAGNOSIS:")
    print(f"From your terminal output, the system is looking for 'DL_C02001' sheet")
    print(f"but the Excel file only has 'Sheet1' and 'DL_WORK'.")
    print(f"")
    print(f"The fix needed:")
    print(f"1. PCMSB units should all use 'DL_WORK' sheet (not unit-specific sheets)")
    print(f"2. The build_unit_from_tags() call needs work_sheet='DL_WORK' parameter")
    print(f"")
    print(f"Also, PI DataLink timeouts suggest:")
    print(f"3. PI server connectivity issues for PCMSB tags")
    print(f"4. Tags may not exist in the PI system or have different names")

if __name__ == "__main__":
    fix_pcmsb_sheet_mapping()