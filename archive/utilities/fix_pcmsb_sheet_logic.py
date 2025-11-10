#!/usr/bin/env python3
"""
Fix PCMSB sheet logic to use unit-specific sheets
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def get_sheet_name_for_unit(unit: str) -> str:
    """Get the appropriate sheet name for a unit"""

    if unit.startswith('K-'):
        # PCFS units use DL_K pattern
        return f"DL_{unit.replace('-', '_')}"
    elif unit.startswith('C-'):
        # PCMSB units use DL_C pattern
        return f"DL_{unit.replace('-', '_')}"
    else:
        # Default to first sheet for other units
        return "Sheet1"

def test_sheet_mapping():
    """Test the sheet mapping logic"""

    print("TESTING SHEET MAPPING LOGIC")
    print("=" * 50)

    test_units = ['K-31-01', 'C-104', 'C-02001', 'C-1301', 'C-201']

    for unit in test_units:
        sheet_name = get_sheet_name_for_unit(unit)
        print(f"{unit:<10} -> {sheet_name}")

    # Test with actual PCMSB Excel file
    print("\nTesting with actual PCMSB Excel file:")
    excel_path = Path("excel/PCMSB_Automation.xlsx")

    if excel_path.exists():
        import pandas as pd
        excel_file = pd.ExcelFile(excel_path)
        available_sheets = excel_file.sheet_names
        print(f"Available sheets: {available_sheets}")

        for unit in ['C-104', 'C-02001', 'C-1301']:
            expected_sheet = get_sheet_name_for_unit(unit)
            sheet_exists = expected_sheet in available_sheets
            print(f"{unit}: expects {expected_sheet} - {'EXISTS' if sheet_exists else 'MISSING'}")

            if sheet_exists:
                try:
                    df = pd.read_excel(excel_path, sheet_name=expected_sheet, nrows=3)
                    print(f"  {expected_sheet}: {df.shape[1]} columns - {list(df.columns)}")
                except Exception as e:
                    print(f"  {expected_sheet}: Error reading - {e}")

def create_enhanced_load_latest_frame():
    """Create enhanced version that supports unit-specific sheets"""

    print("\nCREATING ENHANCED LOAD_LATEST_FRAME FUNCTION")
    print("=" * 60)

    enhanced_code = '''
def load_latest_frame_enhanced(xlsx, unit=None, plant=None, tag=None, sheet_name=None):
    """Enhanced version that auto-determines sheet for units"""

    from pathlib import Path
    import pandas as pd

    # If sheet_name not specified, try to determine from unit
    if sheet_name is None and unit:
        if unit.startswith('K-'):
            # PCFS units
            sheet_name = f"DL_{unit.replace('-', '_')}"
        elif unit.startswith('C-'):
            # PCMSB units
            sheet_name = f"DL_{unit.replace('-', '_')}"
        else:
            sheet_name = "Sheet1"  # Default

    # Check if the sheet exists
    try:
        excel_file = pd.ExcelFile(xlsx)
        available_sheets = excel_file.sheet_names

        if sheet_name and sheet_name not in available_sheets:
            print(f"Warning: Sheet '{sheet_name}' not found. Available: {available_sheets}")
            print(f"Falling back to first sheet: {available_sheets[0]}")
            sheet_name = available_sheets[0]
    except Exception as e:
        print(f"Error checking sheets: {e}")
        sheet_name = None  # Let pandas use default

    # Call original load_latest_frame with determined sheet_name
    from pi_monitor.ingest import load_latest_frame
    return load_latest_frame(xlsx, unit=unit, plant=plant, tag=tag, sheet_name=sheet_name)
'''

    print("Enhanced function created (conceptual).")
    print("This logic should be integrated into parquet_auto_scan.py")

    return enhanced_code

if __name__ == "__main__":
    test_sheet_mapping()
    create_enhanced_load_latest_frame()