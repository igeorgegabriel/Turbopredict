#!/usr/bin/env python3
"""
Test the Excel automation path fix for PCMSB
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.excel_refresh import refresh_excel_safe

def test_excel_path_fix():
    """Test if the Excel path resolution fix works"""

    print("TESTING EXCEL AUTOMATION PATH FIX")
    print("=" * 50)

    # Test with PCMSB file
    excel_path = Path("excel") / "PCMSB_Automation.xlsx"
    print(f"Testing Excel file: {excel_path}")
    print(f"Absolute path: {excel_path.resolve()}")
    print(f"File exists: {excel_path.exists()}")

    if not excel_path.exists():
        print("ERROR: PCMSB_Automation.xlsx not found!")
        return

    print("\nAttempting Excel refresh with path fix...")
    try:
        # Test with short settle time for quick test
        refresh_excel_safe(excel_path, settle_seconds=2)
        print("SUCCESS: Excel refresh completed without path errors!")

    except Exception as e:
        print(f"FAILED: Excel refresh failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_excel_path_fix()