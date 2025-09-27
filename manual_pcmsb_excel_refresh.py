#!/usr/bin/env python3
"""
Manual PCMSB Excel refresh without hanging
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import time
from datetime import datetime

def manual_excel_refresh():
    """Manually refresh PCMSB Excel file"""

    print("MANUAL PCMSB EXCEL REFRESH")
    print("=" * 30)

    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")

    if not excel_path.exists():
        print("ERROR: PCMSB_Automation.xlsx not found!")
        return False

    print(f"Excel file: {excel_path}")

    # Check current status
    stat = excel_path.stat()
    excel_modified = datetime.fromtimestamp(stat.st_mtime)
    excel_age_hours = (datetime.now() - excel_modified).total_seconds() / 3600

    print(f"Current status:")
    print(f"   Last modified: {excel_modified.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Age: {excel_age_hours:.1f} hours")

    if excel_age_hours < 1.0:
        print("Excel file is already fresh - no refresh needed")
        return True

    print(f"\nAttempting Excel refresh...")

    try:
        import xlwings as xw

        # Use invisible mode to avoid UI issues
        app = xw.App(visible=False, add_book=False)

        try:
            # Open the workbook
            wb = app.books.open(str(excel_path))

            print(f"   Workbook opened successfully")

            # Try to refresh all data connections
            wb.api.RefreshAll()

            print(f"   RefreshAll() called")

            # Wait for refresh to complete
            time.sleep(5)

            # Calculate and save
            wb.save()

            print(f"   Workbook saved")

            # Close workbook
            wb.close()

            print(f"   Workbook closed")

        finally:
            # Close Excel application
            app.quit()

        # Check if file was updated
        time.sleep(1)
        new_stat = excel_path.stat()
        new_modified = datetime.fromtimestamp(new_stat.st_mtime)
        new_age_minutes = (datetime.now() - new_modified).total_seconds() / 60

        print(f"\nPost-refresh status:")
        print(f"   Last modified: {new_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Age: {new_age_minutes:.1f} minutes")

        if new_age_minutes < 5:
            print("SUCCESS: Excel file was refreshed!")
            return True
        else:
            print("WARNING: Excel file may not have been refreshed")
            return False

    except Exception as e:
        print(f"ERROR during Excel refresh: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = manual_excel_refresh()
    if success:
        print("\nExcel refresh completed successfully!")
        print("Now you can run option [1] in turbopredict to update parquet files.")
    else:
        print("\nExcel refresh failed - check errors above")