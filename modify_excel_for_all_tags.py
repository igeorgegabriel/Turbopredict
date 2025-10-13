#!/usr/bin/env python3
"""
Modify the existing PCMSB_Automation.xlsx to include all 80 C-02001 PI tags.
This script will expand the existing structure to accommodate all tags.
"""

import xlwings as xw
from pathlib import Path
from datetime import datetime, timedelta
import time

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Remove any numbering prefix like "1→"
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def get_column_name(col_num):
    """Convert column number to Excel column name (A, B, C, ..., AA, AB, etc.)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + ord('A')) + result
        col_num //= 26
    return result

def modify_excel_file(excel_path, tag_file_path):
    """Modify the existing Excel file to include all 80 PI tags."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    print(f"Opening Excel file: {excel_path}")
    app = xw.App(visible=False)

    try:
        wb = app.books.open(excel_path)

        # Work with Sheet1 (the main data sheet)
        ws = wb.sheets['Sheet1']
        print("Modifying Sheet1 to include all PI tags...")

        # Clear existing content except row 1 (headers)
        print("Clearing existing data...")
        used_range = ws.used_range
        if used_range:
            # Clear everything except the header row
            if used_range.last_cell.row > 1:
                clear_range = f"A2:{used_range.last_cell.address}"
                ws.range(clear_range).clear()

        # Set up new headers with all tags
        print("Setting up headers for all 80 tags...")
        ws.range('A1').value = 'Timestamp'

        # Add all PI tags as column headers
        for i, tag in enumerate(tags):
            col_num = i + 2  # Start from column B (2)
            col_name = get_column_name(col_num)
            header_cell = f'{col_name}1'
            ws.range(header_cell).value = tag
            print(f"Added tag {i+1}/80: {tag} in column {col_name}")

        # Set up time configuration (add above headers)
        print("Adding configuration parameters...")
        ws.range('A1').offset(-3, 0).value = 'Start Time:'
        ws.range('B1').offset(-3, 0).value = (datetime.now() - timedelta(days=int(1.5 * 365))).strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A1').offset(-2, 0).value = 'End Time:'
        ws.range('B1').offset(-2, 0).value = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A1').offset(-1, 0).value = 'Interval:'
        ws.range('B1').offset(-1, 0).value = '0.1h'

        # Format headers
        print("Formatting headers...")
        header_range = f"A1:{get_column_name(len(tags) + 1)}1"
        ws.range(header_range).api.Font.Bold = True
        ws.range(header_range).api.Interior.Color = 0xD3D3D3  # Light gray background

        # Add sample PI DataLink formulas for the first few tags
        print("Adding sample PI DataLink formulas...")

        # Add a few sample rows with PI DataLink formulas
        sample_rows = 5
        start_time = datetime.now() - timedelta(days=int(1.5 * 365))

        for row in range(2, 2 + sample_rows):
            # Timestamp formula - incremental time
            time_offset = (row - 2) * 0.1 / 24  # 0.1 hours converted to days
            sample_time = start_time + timedelta(days=time_offset)
            ws.range(f'A{row}').value = sample_time

            # Add PI formulas for first 10 tags as examples
            for i, tag in enumerate(tags[:10]):
                col_num = i + 2
                col_name = get_column_name(col_num)
                cell_ref = f'{col_name}{row}'

                # Different PI DataLink formula patterns to try
                # You may need to adjust these based on your PI system
                formulas_to_try = [
                    f'=PIARCVAL("{tag}",A{row})',
                    f'=PI("{tag}",A{row})',
                    f'=PIVALUE("{tag}",A{row})',
                    f'=PIDATALINK("{tag}",A{row})'
                ]

                # Use the first formula pattern as default
                ws.range(cell_ref).value = formulas_to_try[0]

        # Add instructions for remaining tags
        instruction_row = 2 + sample_rows + 2
        ws.range(f'A{instruction_row}').value = 'SETUP INSTRUCTIONS:'
        ws.range(f'A{instruction_row + 1}').value = '1. Install PI DataLink add-in for Excel'
        ws.range(f'A{instruction_row + 2}').value = '2. Configure PI Server connection'
        ws.range(f'A{instruction_row + 3}').value = '3. Update PI formulas in columns B-CX with correct syntax'
        ws.range(f'A{instruction_row + 4}').value = '4. Set time range and refresh PI DataLink'
        ws.range(f'A{instruction_row + 5}').value = f'5. Copy formulas down for full 1.5 years of data'
        ws.range(f'A{instruction_row + 6}').value = f'6. Expected data points: ~87,601 rows'

        # Auto-fit columns
        print("Auto-fitting columns...")
        ws.range('A:A').api.ColumnWidth = 20
        ws.range('B:CX').api.ColumnWidth = 12

        # Save the file
        print("Saving modified Excel file...")
        wb.save()

        print(f"Successfully modified Excel file with {len(tags)} PI tag columns!")
        print("\nFile structure:")
        print(f"- Column A: Timestamp")
        print(f"- Columns B-{get_column_name(len(tags) + 1)}: PI tag data ({len(tags)} tags)")
        print(f"- Sample PI formulas added for first 10 tags")

        return True

    except Exception as e:
        print(f"Error modifying Excel file: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def main():
    """Main entry point."""
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"

    # Check if files exist
    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    print("=" * 70)
    print("MODIFYING EXCEL FILE TO INCLUDE ALL 80 C-02001 PI TAGS")
    print("=" * 70)

    success = modify_excel_file(excel_file, tag_file)

    if success:
        print("\n" + "=" * 50)
        print("EXCEL FILE SUCCESSFULLY MODIFIED!")
        print("=" * 50)
        print("\nNEXT STEPS:")
        print("1. Open Excel file and verify all 80 tag columns are present")
        print("2. Configure PI DataLink connection to your PI Server")
        print("3. Update the PI formulas with correct syntax for your system")
        print("4. Set the time range for 1.5 years of historical data")
        print("5. Refresh PI DataLink to populate all tag data")
        print("6. Run the parquet builder again to create full dataset")
        print(f"\nExpected final parquet size: ~15-20MB (vs current 1MB)")
    else:
        print("Failed to modify Excel file")

if __name__ == "__main__":
    main()