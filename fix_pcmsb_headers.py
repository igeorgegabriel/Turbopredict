#!/usr/bin/env python3
"""
Fix PCMSB Excel file by adding proper headers
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_pcmsb_excel_headers():
    """Fix PCMSB Excel file by adding proper column headers"""

    print("FIXING PCMSB EXCEL HEADERS")
    print("=" * 50)

    pcmsb_path = Path("excel/PCMSB_Automation.xlsx")

    if not pcmsb_path.exists():
        print("ERROR: PCMSB_Automation.xlsx not found!")
        return

    # Create backup first
    backup_path = pcmsb_path.with_name(f"PCMSB_Automation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    import shutil
    shutil.copy2(pcmsb_path, backup_path)
    print(f"Backup created: {backup_path.name}")

    try:
        # Read the raw data
        df_raw = pd.read_excel(pcmsb_path, sheet_name='Sheet1', header=None)
        print(f"Raw data shape: {df_raw.shape}")
        print("Raw data preview:")
        print(df_raw.head())

        # The data appears to be:
        # Column 0: Empty/NaN
        # Column 1: Timestamps
        # Column 2: Values (probably for a specific tag)

        # Create proper structured data
        clean_data = df_raw.dropna(axis=1, how='all')  # Remove empty columns
        print(f"After removing empty columns: {clean_data.shape}")

        if clean_data.shape[1] >= 2:
            # Assume first column is time, rest are values
            clean_data.columns = ['TIME'] + [f'C-104_Value_{i}' for i in range(1, clean_data.shape[1])]

            print("Fixed column headers:")
            print(list(clean_data.columns))

            # Validate time column
            time_col = clean_data['TIME']
            if pd.api.types.is_datetime64_any_dtype(time_col):
                print("SUCCESS: TIME column is properly formatted as datetime")
            else:
                print("Converting TIME column to datetime...")
                clean_data['TIME'] = pd.to_datetime(time_col, errors='coerce')

            # Show sample of fixed data
            print("\nFixed data preview:")
            print(clean_data.head())

            # Check for any remaining issues
            null_times = clean_data['TIME'].isnull().sum()
            if null_times > 0:
                print(f"WARNING: {null_times} rows have null timestamps")
                # Remove rows with null timestamps
                clean_data = clean_data.dropna(subset=['TIME'])
                print(f"Cleaned data shape: {clean_data.shape}")

            # Save the fixed Excel file
            with pd.ExcelWriter(pcmsb_path, engine='openpyxl') as writer:
                clean_data.to_excel(writer, sheet_name='Sheet1', index=False)

                # Also keep the original DL_WORK sheet if it exists
                try:
                    dl_work = pd.read_excel(backup_path, sheet_name='DL_WORK')
                    dl_work.to_excel(writer, sheet_name='DL_WORK', index=False)
                except:
                    pass  # DL_WORK sheet might not exist or be readable

            print(f"SUCCESS: Fixed Excel file saved: {pcmsb_path}")
            print("The file now has proper TIME column header")

            # Verify the fix
            print("\nVerifying fix...")
            test_df = pd.read_excel(pcmsb_path, sheet_name='Sheet1')
            print(f"Verification - columns: {list(test_df.columns)}")
            has_time = 'TIME' in test_df.columns
            print(f"TIME column present: {has_time}")

            if has_time:
                print("SUCCESS: PCMSB Excel file is now properly formatted!")
                return True
            else:
                print("FAILED: TIME column still missing after fix")
                return False

        else:
            print("ERROR: Not enough columns in data to fix")
            return False

    except Exception as e:
        print(f"ERROR: Failed to fix Excel file: {e}")
        import traceback
        traceback.print_exc()

        # Restore from backup if fix failed
        try:
            shutil.copy2(backup_path, pcmsb_path)
            print("Restored original file from backup")
        except:
            pass

        return False

if __name__ == "__main__":
    fix_pcmsb_excel_headers()