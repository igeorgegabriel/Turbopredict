#!/usr/bin/env python3
"""
Debug PCMSB Excel file structure to understand TIME column issue
"""

import pandas as pd
import sys
from pathlib import Path

def debug_pcmsb_excel():
    """Debug the PCMSB Excel file structure"""

    print("DEBUGGING PCMSB EXCEL FILE STRUCTURE")
    print("=" * 60)

    excel_path = Path("excel/PCMSB_Automation.xlsx")

    if not excel_path.exists():
        print("ERROR: PCMSB_Automation.xlsx not found!")
        return

    print(f"Excel file: {excel_path}")
    print(f"File size: {excel_path.stat().st_size / (1024*1024):.1f} MB")

    try:
        # Read Excel file and examine its structure
        print("\nReading Excel file...")

        # Get all sheet names first
        excel_file = pd.ExcelFile(excel_path)
        sheet_names = excel_file.sheet_names
        print(f"Sheets found: {sheet_names}")

        for sheet_name in sheet_names[:3]:  # Check first 3 sheets
            print(f"\n--- SHEET: {sheet_name} ---")

            try:
                # Read first few rows to check structure
                df_preview = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=10)
                print(f"Shape: {df_preview.shape}")
                print(f"Columns: {list(df_preview.columns)}")

                # Look for time-related columns
                time_columns = [col for col in df_preview.columns
                              if any(keyword in str(col).upper() for keyword in ['TIME', 'TIMESTAMP', 'DATE'])]
                print(f"Time-related columns: {time_columns}")

                # Show first few rows
                print("First 3 rows:")
                print(df_preview.head(3))

                # Check if data starts from a different row
                if df_preview.empty or df_preview.isna().all().all():
                    print("Sheet appears empty or all NaN - trying to read from different starting rows...")

                    for skip_rows in [1, 2, 3, 4, 5]:
                        try:
                            df_skip = pd.read_excel(excel_path, sheet_name=sheet_name,
                                                  skiprows=skip_rows, nrows=5)
                            if not df_skip.empty and not df_skip.isna().all().all():
                                print(f"  Data found starting from row {skip_rows+1}:")
                                print(f"  Columns: {list(df_skip.columns)}")
                                time_cols = [col for col in df_skip.columns
                                           if any(keyword in str(col).upper() for keyword in ['TIME', 'TIMESTAMP', 'DATE'])]
                                print(f"  Time columns: {time_cols}")
                                break
                        except:
                            continue

            except Exception as e:
                print(f"Error reading sheet {sheet_name}: {e}")

    except Exception as e:
        print(f"Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()

def debug_expected_structure():
    """Debug what the system expects vs what it finds"""

    print(f"\n\nDEBUGGING EXPECTED DATA STRUCTURE")
    print("=" * 60)

    # Check how the system tries to read PCMSB data
    try:
        from pi_monitor.parquet_auto_scan import ParquetAutoScanner

        scanner = ParquetAutoScanner()

        # Try to get PCMSB Excel path
        excel_path = Path("excel/PCMSB_Automation.xlsx")
        print(f"Excel path used by system: {excel_path}")

        # Check what the scanner expects
        print("Checking what ParquetAutoScanner expects...")

        # Look at the error handling in the code
        test_unit = "C-104"
        print(f"Testing with unit: {test_unit}")

        # Try to trace the issue
        print("The error suggests the system is looking for a 'TIME' column in the first few rows")
        print("This usually happens when:")
        print("1. The Excel sheet has headers in a different row")
        print("2. The sheet structure has changed")
        print("3. The PI DataLink didn't populate data correctly")
        print("4. Wrong sheet is being read")

    except Exception as e:
        print(f"Error checking scanner: {e}")

if __name__ == "__main__":
    debug_pcmsb_excel()
    debug_expected_structure()