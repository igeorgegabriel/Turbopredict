#!/usr/bin/env python3
"""
Fix the Excel file with the correct PISampDat formula pattern based on the working example.
"""

import xlwings as xw
from pathlib import Path
from datetime import datetime, timedelta

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def fix_excel_with_correct_pi_formula(excel_path, tag_file_path):
    """Fix Excel file with the correct PISampDat formula pattern."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    print(f"Opening Excel file: {excel_path}")
    app = xw.App(visible=True)

    try:
        wb = app.books.open(excel_path)

        # Use the Data sheet or create new one
        if 'Data' in [sheet.name for sheet in wb.sheets]:
            ws = wb.sheets['Data']
        else:
            ws = wb.sheets.add('PI_Data_Fixed')

        print(f"Working with sheet: {ws.name}")

        # Clear existing data but keep structure
        print("Clearing existing sample data...")
        # Clear everything from row 8 onwards (keep headers and config)
        ws.range('A8:CC200000').clear()  # Clear large range to be sure

        # Set up correct PI DataLink formula pattern
        print("Setting up correct PISampDat formulas...")

        # Based on the working example:
        # =PISampDat("PCM.C-02001.020FI6203.PV","*-1y","*","0.1h",1,"\\PTSG-1MMPDPdb01")

        # Parameters from working formula
        pi_server = "\\\\PTSG-1MMPDPdb01"  # PI Server path
        start_time = "*-1.5y"  # 1.5 years ago (modified from original *-1y)
        end_time = "*"  # Now
        interval = "0.1h"  # 0.1 hour intervals
        param5 = 1  # Unknown parameter from original

        # Add headers if not present
        header_row = 7
        ws.range(f'A{header_row}').value = 'Time'

        # Add all PI tags as headers and formulas
        for i, tag in enumerate(tags):
            col = i + 2  # Start from column B
            col_letter = get_column_letter(col)

            # Set header
            ws.range(f'{col_letter}{header_row}').value = tag

            # Set PISampDat formula in first data row
            data_row = header_row + 1
            formula = f'=PISampDat("{tag}","{start_time}","{end_time}","{interval}",{param5},"{pi_server}")'

            print(f"Adding formula for tag {i+1}/{len(tags)}: {tag}")
            print(f"  Formula: {formula}")

            ws.range(f'{col_letter}{data_row}').value = formula

        # Format headers
        ws.range(f'A{header_row}:{get_column_letter(len(tags) + 1)}{header_row}').api.Font.Bold = True

        # Auto-fit columns
        ws.range('A:A').api.ColumnWidth = 20
        ws.range('B:CX').api.ColumnWidth = 12

        # Save the file
        print("Saving Excel file with correct PI formulas...")
        wb.save()

        print(f"\n" + "=" * 60)
        print("EXCEL FILE FIXED WITH CORRECT PI FORMULAS!")
        print("=" * 60)
        print(f"✓ Applied PISampDat formula to all {len(tags)} tags")
        print(f"✓ PI Server: {pi_server}")
        print(f"✓ Time range: {start_time} to {end_time}")
        print(f"✓ Interval: {interval}")
        print("\nNext steps:")
        print("1. Refresh the PI DataLink data (Ctrl+Alt+F9)")
        print("2. Wait for data to populate (may take several minutes)")
        print("3. Check that data appears in columns B onwards")
        print("4. Run parquet generation when data is ready")

        return True

    except Exception as e:
        print(f"Error fixing Excel file: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        print("Leaving Excel open for data refresh...")

def get_column_letter(col_num):
    """Convert column number to Excel column letter."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + ord('A')) + result
        col_num //= 26
    return result

def main():
    """Main entry point."""
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"

    # Check if files exist
    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    print("=" * 70)
    print("FIXING EXCEL FILE WITH CORRECT PISAMPDAT FORMULA")
    print("=" * 70)
    print("Based on working pattern:")
    print('=PISampDat("TAG","*-1.5y","*","0.1h",1,"\\\\PTSG-1MMPDPdb01")')
    print("=" * 70)

    success = fix_excel_with_correct_pi_formula(excel_file, tag_file)

    if success:
        print("\n🎉 Excel file fixed with correct PI formulas!")
        print("\nThe file now uses the same PISampDat formula pattern")
        print("that successfully fetches data in your working file.")
        print("\nRefresh the data and you should get the full 1.5-year dataset!")
    else:
        print("❌ Failed to fix Excel file")

if __name__ == "__main__":
    main()