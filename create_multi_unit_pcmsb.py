#!/usr/bin/env python3
"""
Create multi-unit PCMSB Excel file based on PCFS pattern
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import shutil

def create_multi_unit_pcmsb():
    """Create PCMSB Excel file with sheets for all 7 units"""

    print("CREATING MULTI-UNIT PCMSB EXCEL FILE")
    print("=" * 60)

    pcmsb_path = Path("excel/PCMSB_Automation.xlsx")

    # Create backup first
    backup_path = pcmsb_path.with_name(f"PCMSB_Automation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    shutil.copy2(pcmsb_path, backup_path)
    print(f"Backup created: {backup_path.name}")

    # All PCMSB units that need data
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']

    print(f"Creating sheets for {len(pcmsb_units)} PCMSB units...")

    try:
        # Read current data (which only has C-104)
        current_df = pd.read_excel(pcmsb_path, sheet_name='Sheet1')
        print(f"Current data shape: {current_df.shape}")
        print(f"Current columns: {list(current_df.columns)}")

        # Create new Excel file with multiple sheets
        with pd.ExcelWriter(pcmsb_path, engine='openpyxl') as writer:

            # Main sheet (keep current C-104 data for compatibility)
            current_df.to_excel(writer, sheet_name='Sheet1', index=False)
            print("Created Sheet1 (main sheet)")

            # Create individual sheets for each unit
            for unit in pcmsb_units:
                sheet_name = f"DL_{unit.replace('-', '_')}"

                if unit == 'C-104':
                    # Use existing data for C-104
                    unit_data = current_df.copy()
                    unit_data.columns = ['TIME', f'{unit}_Value']
                else:
                    # Create template structure for other units
                    # In a real scenario, these would need to be configured in PI DataLink
                    template_data = pd.DataFrame({
                        'TIME': current_df['TIME'].head(10),  # Sample times
                        f'{unit}_Value': np.random.random(10) * 100  # Placeholder values
                    })
                    unit_data = template_data

                unit_data.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Created {sheet_name} ({len(unit_data)} rows)")

            # Keep DL_WORK sheet if it exists
            try:
                dl_work = pd.read_excel(backup_path, sheet_name='DL_WORK')
                dl_work.to_excel(writer, sheet_name='DL_WORK', index=False)
                print("Preserved DL_WORK sheet")
            except:
                print("No DL_WORK sheet to preserve")

        print(f"\nSUCCESS: Multi-unit PCMSB Excel file created!")

        # Verify the new structure
        print("\nVerifying new structure...")
        excel_file = pd.ExcelFile(pcmsb_path)
        print(f"Sheets created: {excel_file.sheet_names}")

        for sheet in excel_file.sheet_names:
            try:
                df = pd.read_excel(pcmsb_path, sheet_name=sheet, nrows=1)
                print(f"  {sheet}: {df.shape[1]} columns - {list(df.columns)}")
            except Exception as e:
                print(f"  {sheet}: Error - {e}")

        print(f"\nNOTE: The individual unit sheets need to be configured in PI DataLink")
        print("This structure provides the framework for multi-unit data refresh.")

        return True

    except Exception as e:
        print(f"ERROR: Failed to create multi-unit file: {e}")
        import traceback
        traceback.print_exc()

        # Restore backup
        try:
            shutil.copy2(backup_path, pcmsb_path)
            print("Restored original file from backup")
        except:
            pass

        return False

if __name__ == "__main__":
    create_multi_unit_pcmsb()