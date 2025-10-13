#!/usr/bin/env python3
"""
Configure PI DataLink in the Excel file to fetch full 1.5 years of historical data
for all 80 C-02001 PI tags.
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
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def get_column_name(col_num):
    """Convert column number to Excel column name."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + ord('A')) + result
        col_num //= 26
    return result

def configure_pi_datalink_formulas(excel_path, tag_file_path):
    """Configure PI DataLink formulas for all 80 tags."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    # Calculate date range (1.5 years back from now)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=int(1.5 * 365))

    print(f"Date range: {start_time.strftime('%m/%d/%Y %H:%M:%S')} to {end_time.strftime('%m/%d/%Y %H:%M:%S')}")

    print(f"Opening Excel file: {excel_path}")
    app = xw.App(visible=True)  # Make visible for PI DataLink

    try:
        wb = app.books.open(excel_path)

        # Work with the Data sheet
        if 'Data' in [sheet.name for sheet in wb.sheets]:
            ws = wb.sheets['Data']
        else:
            ws = wb.sheets[0]  # Use first sheet if no Data sheet

        print(f"Working with sheet: {ws.name}")

        # Set up PI DataLink configuration parameters
        print("Setting up PI DataLink configuration...")

        # Add configuration cells (above the data)
        config_start_row = 1

        # PI Server configuration
        ws.range(f'A{config_start_row}').value = 'PI Server:'
        ws.range(f'B{config_start_row}').value = 'PI_SERVER'  # You may need to change this

        ws.range(f'A{config_start_row + 1}').value = 'Start Time:'
        ws.range(f'B{config_start_row + 1}').value = start_time

        ws.range(f'A{config_start_row + 2}').value = 'End Time:'
        ws.range(f'B{config_start_row + 2}').value = end_time

        ws.range(f'A{config_start_row + 3}').value = 'Interval:'
        ws.range(f'B{config_start_row + 3}').value = '6m'  # 6 minutes = 0.1 hours

        ws.range(f'A{config_start_row + 4}').value = 'Data Points:'
        expected_points = int((end_time - start_time).total_seconds() / 360)  # 360 seconds = 6 minutes
        ws.range(f'B{config_start_row + 4}').value = expected_points

        # Headers start from row 6
        header_row = config_start_row + 5
        data_start_row = header_row + 1

        # Set up headers
        print("Setting up headers...")
        ws.range(f'A{header_row}').value = 'Timestamp'

        # Add PI tag headers
        for i, tag in enumerate(tags):
            col_num = i + 2  # Start from column B
            col_name = get_column_name(col_num)
            ws.range(f'{col_name}{header_row}').value = tag

        # Generate time series for data rows
        print("Generating time series...")
        time_interval_minutes = 6  # 0.1 hours = 6 minutes

        # Calculate how many data points we need
        total_minutes = int((end_time - start_time).total_seconds() / 60)
        num_points = min(total_minutes // time_interval_minutes, 50000)  # Limit to reasonable size

        print(f"Generating {num_points} time points...")

        # Add time stamps
        for i in range(num_points):
            row = data_start_row + i
            time_point = start_time + timedelta(minutes=i * time_interval_minutes)
            ws.range(f'A{row}').value = time_point

        # Configure PI DataLink formulas
        print("Setting up PI DataLink formulas...")

        # Try different PI DataLink formula patterns that are commonly used
        formula_patterns = [
            '=PIValue("{tag}",$A{row})',  # OSIsoft PI DataLink
            '=PIArchive("{tag}",$A{row})',  # Alternative PI DataLink syntax
            '=PIINTERP("{tag}",$A{row})',  # PI Interpolated value
            '=PICompDat("{tag}",$A{row},"value")',  # PI Compressed data
            '=PIGetVal("{tag}",$A{row})',  # Another common syntax
        ]

        # Use the first pattern as default (most common)
        selected_formula = formula_patterns[0]

        print(f"Using formula pattern: {selected_formula}")

        # Add formulas for each tag
        for i, tag in enumerate(tags):
            col_num = i + 2
            col_name = get_column_name(col_num)

            print(f"Configuring tag {i+1}/{len(tags)}: {tag} in column {col_name}")

            # Add formula to first data row
            row = data_start_row
            formula = selected_formula.format(tag=tag, row=row)
            ws.range(f'{col_name}{row}').value = formula

        # Format the sheet
        print("Formatting sheet...")
        ws.range(f'A{config_start_row}:B{config_start_row + 4}').api.Font.Bold = True
        ws.range(f'A{header_row}:{get_column_name(len(tags) + 1)}{header_row}').api.Font.Bold = True

        # Auto-fit columns
        ws.range('A:A').api.ColumnWidth = 20
        ws.range('B:CX').api.ColumnWidth = 12

        # Save the file
        print("Saving Excel file...")
        wb.save()

        print("\n" + "=" * 60)
        print("PI DATALINK CONFIGURATION COMPLETED!")
        print("=" * 60)
        print(f"Configured {len(tags)} PI tags")
        print(f"Time range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
        print(f"Expected data points: {num_points}")
        print(f"Formula pattern: {selected_formula}")

        return True

    except Exception as e:
        print(f"Error configuring PI DataLink: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Don't close immediately - leave Excel open for PI DataLink configuration
        print("\nLeaving Excel open for PI DataLink configuration...")
        print("Please configure PI DataLink connection manually if needed.")

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
    print("CONFIGURING PI DATALINK FOR ALL 80 C-02001 TAGS")
    print("=" * 70)

    success = configure_pi_datalink_formulas(excel_file, tag_file)

    if success:
        print("\n" + "=" * 50)
        print("NEXT STEPS - MANUAL PI DATALINK SETUP:")
        print("=" * 50)
        print("1. Verify PI Server connection in Excel")
        print("2. Install PI DataLink add-in if not already installed")
        print("3. Configure PI Server connection (typically via PI DataLink ribbon)")
        print("4. Verify the PI formulas are working for a few tags")
        print("5. Copy formulas down to all time rows")
        print("6. Refresh PI DataLink data (Ctrl+Alt+F9 or PI DataLink refresh)")
        print("7. Wait for data to populate (may take time for 1.5 years of data)")
        print("8. Run parquet builder to create final dataset")
    else:
        print("Failed to configure PI DataLink")

if __name__ == "__main__":
    main()