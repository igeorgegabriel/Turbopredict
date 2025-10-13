#!/usr/bin/env python3
"""
Simple script to create a new C-02001 sheet in PCMSB_Automation.xlsx.
Creates basic structure for manual PI DataLink configuration.
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

def create_c02001_simple_sheet(excel_path, tag_file_path):
    """Create a simple C-02001 sheet structure."""

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

        # Check if C-02001 sheet already exists
        sheet_names = [sheet.name for sheet in wb.sheets]
        if 'C-02001' in sheet_names:
            print("C-02001 sheet already exists. Updating it...")
            ws = wb.sheets['C-02001']
            ws.clear()
        else:
            print("Creating new C-02001 sheet...")
            ws = wb.sheets.add('C-02001')

        # Set up basic configuration
        print("Setting up basic configuration...")

        # Configuration section
        ws.range('A1').value = 'Start Time:'
        ws.range('B1').value = start_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A2').value = 'End Time:'
        ws.range('B2').value = end_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A3').value = 'Interval:'
        ws.range('B3').value = '0.1h'
        ws.range('A4').value = 'Unit:'
        ws.range('B4').value = 'C-02001'
        ws.range('A5').value = 'Tags Count:'
        ws.range('B5').value = len(tags)

        # Headers
        ws.range('A7').value = 'PI Tags List:'
        ws.range('A8').value = 'Timestamp'

        # Add tag names as headers
        print(f"Adding {len(tags)} tag headers...")
        for i, tag in enumerate(tags):
            col = i + 2  # Start from column B
            cell_ref = f'{chr(64 + col)}8' if col <= 26 else f'A{chr(64 + col - 26)}8'
            ws.range(cell_ref).value = tag

        # Add some sample data instructions
        ws.range('A10').value = 'Instructions:'
        ws.range('A11').value = '1. Configure PI DataLink for each tag column'
        ws.range('A12').value = '2. Set time range from B1 to B2'
        ws.range('A13').value = '3. Set interval to B3 (0.1h = 6 minutes)'
        ws.range('A14').value = '4. Refresh PI DataLink to fetch data'

        # Format headers
        ws.range('A1:B5').api.Font.Bold = True
        ws.range('A7').api.Font.Bold = True
        ws.range('A8:CV8').api.Font.Bold = True  # Header row

        print("Saving Excel file...")
        wb.save()

        print(f"Successfully created C-02001 sheet with {len(tags)} tag columns")
        print("Sheet is ready for manual PI DataLink configuration")

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
    print("CREATING SIMPLE C-02001 SHEET")
    print("=" * 60)

    success = create_c02001_simple_sheet(excel_file, tag_file)

    if success:
        print("\nSheet created successfully!")
        print("Next steps:")
        print("1. Open Excel and go to the C-02001 sheet")
        print("2. Configure PI DataLink for data fetching")
        print("3. Run build_pcmsb_parquet.py to generate parquet file")

if __name__ == "__main__":
    main()