#!/usr/bin/env python3
"""
Create a comprehensive C-02001 sheet with all 80 PI tags properly configured for PI DataLink.
This replaces the existing sheet with a properly structured one.
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
                # Remove any numbering prefix like "1→"
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def create_full_c02001_sheet(excel_path, tag_file_path):
    """Create a comprehensive C-02001 sheet with all PI tags."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    # Calculate date range (1.5 years back from now)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=int(1.5 * 365))

    print(f"Opening Excel file: {excel_path}")
    app = xw.App(visible=False)

    try:
        wb = app.books.open(excel_path)

        # Delete existing C-02001 sheet if it exists and create new one
        sheet_names = [sheet.name for sheet in wb.sheets]
        if 'C-02001' in sheet_names:
            print("Deleting existing C-02001 sheet...")
            wb.sheets['C-02001'].delete()

        print("Creating new C-02001 sheet...")
        ws = wb.sheets.add('C-02001')

        # Configuration headers
        print("Setting up configuration...")
        ws.range('A1').value = 'PCMSB C-02001 Data Configuration'
        ws.range('A2').value = 'Start Time:'
        ws.range('B2').value = start_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A3').value = 'End Time:'
        ws.range('B3').value = end_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A4').value = 'Interval:'
        ws.range('B4').value = '0.1h'
        ws.range('A5').value = 'Total Tags:'
        ws.range('B5').value = len(tags)

        # Data headers starting from row 7
        print("Setting up data headers...")
        row_start = 7
        ws.range(f'A{row_start}').value = 'Timestamp'

        # Add all PI tags as column headers
        print(f"Adding {len(tags)} PI tag columns...")
        for i, tag in enumerate(tags):
            col = i + 2  # Start from column B
            if col <= 26:
                col_ref = chr(64 + col)
            else:
                # Handle columns beyond Z (AA, AB, etc.)
                first = chr(64 + ((col - 1) // 26))
                second = chr(64 + ((col - 1) % 26) + 1)
                col_ref = first + second

            cell_ref = f'{col_ref}{row_start}'
            ws.range(cell_ref).value = tag

        # Add sample PI DataLink formulas (template)
        print("Adding PI DataLink formula templates...")
        data_row = row_start + 1

        # Timestamp formula template
        ws.range(f'A{data_row}').value = '=NOW()-ROW()*$B$4/24'

        # PI tag formulas - these are templates and need to be adapted to actual PI DataLink syntax
        for i, tag in enumerate(tags[:10]):  # Add formulas for first 10 tags as examples
            col = i + 2
            if col <= 26:
                col_ref = chr(64 + col)
            else:
                first = chr(64 + ((col - 1) // 26))
                second = chr(64 + ((col - 1) % 26) + 1)
                col_ref = first + second

            cell_ref = f'{col_ref}{data_row}'
            # Template PI DataLink formula - adjust syntax based on your PI system
            ws.range(cell_ref).value = f'=PIARCVAL("{tag}",$A{data_row})'

        # Instructions
        print("Adding instructions...")
        instruction_row = row_start + 3
        ws.range(f'A{instruction_row}').value = 'INSTRUCTIONS:'
        ws.range(f'A{instruction_row + 1}').value = '1. Configure PI DataLink connection'
        ws.range(f'A{instruction_row + 2}').value = '2. Update PI formulas with correct syntax for your system'
        ws.range(f'A{instruction_row + 3}').value = '3. Refresh data using Ctrl+Alt+F9 or PI DataLink refresh'
        ws.range(f'A{instruction_row + 4}').value = '4. Copy formulas down for all time periods'

        # Format the sheet
        print("Formatting sheet...")
        ws.range('A1').api.Font.Bold = True
        ws.range('A1').api.Font.Size = 14
        ws.range('A2:A5').api.Font.Bold = True
        ws.range(f'A{row_start}:CV{row_start}').api.Font.Bold = True

        # Set column widths
        ws.range('A:A').api.ColumnWidth = 20
        ws.range('B:CV').api.ColumnWidth = 15

        # Add data validation and formatting
        ws.range('B2:B3').api.NumberFormat = 'mm/dd/yyyy hh:mm:ss'

        print("Saving Excel file...")
        wb.save()

        print(f"Successfully created C-02001 sheet with {len(tags)} PI tag columns")
        print("Manual steps required:")
        print("1. Configure PI DataLink connection in Excel")
        print("2. Update PI formulas with correct syntax")
        print("3. Refresh PI DataLink to fetch historical data")

        return True

    except Exception as e:
        print(f"Error creating comprehensive sheet: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def main():
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"

    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    print("=" * 60)
    print("CREATING COMPREHENSIVE C-02001 SHEET WITH ALL 80 PI TAGS")
    print("=" * 60)

    success = create_full_c02001_sheet(excel_file, tag_file)

    if success:
        print("\nSheet created successfully!")
        print("\nNEXT STEPS:")
        print("1. Open Excel and configure PI DataLink")
        print("2. Test the PI tag formulas")
        print("3. Refresh data to get full 1.5 years")
        print("4. Re-run the parquet builder")
    else:
        print("Failed to create comprehensive sheet")

if __name__ == "__main__":
    main()