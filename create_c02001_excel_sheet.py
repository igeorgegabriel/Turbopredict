#!/usr/bin/env python3
"""
Create a new C-02001 sheet in PCMSB_Automation.xlsx for data fetching.
This script adds a properly configured sheet for PI DataLink to fetch 1.5 years of data at 0.1h intervals.
"""

import xlwings as xw
from pathlib import Path
import sys
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

def create_c02001_sheet(excel_path, tag_file_path):
    """Create a new C-02001 sheet in the PCMSB Excel file."""

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
    app = xw.App(visible=True)  # Make visible for debugging

    try:
        wb = app.books.open(excel_path)

        # Check if C-02001 sheet already exists
        sheet_names = [sheet.name for sheet in wb.sheets]
        if 'C-02001' in sheet_names:
            print("C-02001 sheet already exists. Updating it...")
            ws = wb.sheets['C-02001']
            ws.clear()
        else:
            print("Creating new C-02001 sheet...")
            ws = wb.sheets.add('C-02001')

        # Set up the sheet with PI DataLink configuration
        print("Setting up PI DataLink configuration...")

        # Header row for time parameters
        ws.range('A1').value = 'Start Time:'
        ws.range('B1').value = start_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A2').value = 'End Time:'
        ws.range('B2').value = end_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A3').value = 'Interval:'
        ws.range('B3').value = '0.1h'
        ws.range('A4').value = 'Unit:'
        ws.range('B4').value = 'C-02001'

        # Add some spacing
        ws.range('A6').value = 'PI Tags for Data Fetch:'

        # Create headers starting from row 8
        headers = ['Timestamp'] + tags

        print(f"Setting up {len(headers)} columns...")
        for i, header in enumerate(headers):
            ws.range(8, i + 1).value = header

        # Set up PI DataLink formulas for data fetching
        # This is a template - actual PI DataLink formula structure depends on the system
        print("Setting up PI DataLink formulas...")

        # For each tag, create a placeholder for PI DataLink data
        for i, tag in enumerate(tags):
            col = i + 2  # Start from column B (timestamp is A)
            # Add PI DataLink formula placeholder - actual syntax depends on PI DataLink version
            if col <= 26:
                col_letter = chr(64 + col)
            else:
                # Handle columns beyond Z (AA, AB, etc.)
                first = chr(64 + ((col - 1) // 26))
                second = chr(64 + ((col - 1) % 26) + 1)
                col_letter = first + second

            formula_cell = ws.range(f'{col_letter}9')
            # This is a template - you may need to adjust based on your PI DataLink setup
            formula_cell.value = f'=PIData("{tag}",B1,B2,B3)'

        # Format the sheet
        ws.range('A1:B4').api.Font.Bold = True
        ws.range('A6').api.Font.Bold = True
        header_range = f"A8:{chr(64 + len(headers))}8"
        ws.range(header_range).api.Font.Bold = True

        # Auto-fit columns for first 26 columns
        ws.range('A:Z').api.EntireColumn.AutoFit()

        print("Saving Excel file...")
        wb.save()

        print(f"Successfully created C-02001 sheet with {len(tags)} tags")
        print("Sheet is ready for PI DataLink data fetching")

        return True

    except Exception as e:
        print(f"Error creating sheet: {e}")
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

    print("=" * 60)
    print("CREATING C-02001 SHEET IN PCMSB_AUTOMATION.XLSX")
    print("=" * 60)

    success = create_c02001_sheet(excel_file, tag_file)

    if success:
        print("\nNext steps:")
        print("1. Open the Excel file and verify the C-02001 sheet")
        print("2. Configure PI DataLink connections if needed")
        print("3. Run build_pcmsb_parquet.py to generate parquet file")
        print(f"   python build_pcmsb_parquet.py")
    else:
        print("Failed to create C-02001 sheet")

if __name__ == "__main__":
    main()