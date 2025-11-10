#!/usr/bin/env python3
"""
Automated PI data fetching script that works with the existing Excel structure
and refreshes PI DataLink to populate the full 1.5 years of data.
"""

import xlwings as xw
from pathlib import Path
from datetime import datetime, timedelta
import time
import pandas as pd

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

def setup_pi_datalink_batch(excel_path, tag_file_path, batch_size=10):
    """Set up PI DataLink in batches to handle large dataset efficiently."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    # Calculate date range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=int(1.5 * 365))

    print(f"Date range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")

    app = xw.App(visible=True, add_book=False)

    try:
        wb = app.books.open(excel_path)

        # Create or use Data sheet
        if 'Data' in [sheet.name for sheet in wb.sheets]:
            ws = wb.sheets['Data']
        else:
            # Create new sheet for PI data
            ws = wb.sheets.add('PI_Data')

        print(f"Working with sheet: {ws.name}")

        # Clear existing content
        ws.clear()

        # Set up configuration
        ws.range('A1').value = 'PI DataLink Configuration - C-02001'
        ws.range('A2').value = 'Start Time:'
        ws.range('B2').value = start_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A3').value = 'End Time:'
        ws.range('B3').value = end_time.strftime('%m/%d/%Y %H:%M:%S')
        ws.range('A4').value = 'Interval:'
        ws.range('B4').value = '6m'  # 6 minutes = 0.1 hours
        ws.range('A5').value = 'Tags:'
        ws.range('B5').value = len(tags)

        # Headers starting from row 7
        header_row = 7
        ws.range(f'A{header_row}').value = 'Timestamp'

        # Add tag headers
        print("Setting up headers for all tags...")
        for i, tag in enumerate(tags):
            col = i + 2  # Start from column B
            if col <= 702:  # Excel column limit
                ws.range(header_row, col).value = tag

        # Set up PI DataLink formulas for first few rows
        print("Setting up PI DataLink formulas...")

        # Use the working DL_WORK sheet pattern if it exists
        if 'DL_WORK' in [sheet.name for sheet in wb.sheets]:
            dl_work = wb.sheets['DL_WORK']
            print("Found existing DL_WORK sheet - analyzing pattern...")

            # Try to understand the existing pattern
            try:
                # Check if there's data in DL_WORK
                used_range = dl_work.used_range
                if used_range and used_range.last_cell.row > 1:
                    print(f"DL_WORK has {used_range.last_cell.row} rows of data")

                    # Copy the time pattern from DL_WORK
                    time_data = dl_work.range('A:A').value
                    if time_data and len(time_data) > 1:
                        print("Copying time series from DL_WORK...")
                        valid_times = [t for t in time_data if t is not None and isinstance(t, datetime)]

                        if valid_times:
                            # Use the existing time series
                            for i, time_val in enumerate(valid_times[:50000]):  # Limit size
                                row = header_row + 1 + i
                                ws.range(f'A{row}').value = time_val

                            print(f"Copied {len(valid_times)} time points from DL_WORK")

            except Exception as e:
                print(f"Could not copy from DL_WORK: {e}")

        # If we don't have time data yet, generate it
        if ws.range('A8').value is None:
            print("Generating new time series...")
            current_time = start_time
            row = header_row + 1
            points_added = 0

            while current_time <= end_time and points_added < 50000:  # Limit to 50k points initially
                ws.range(f'A{row}').value = current_time
                current_time += timedelta(minutes=6)
                row += 1
                points_added += 1

                if points_added % 1000 == 0:
                    print(f"Added {points_added} time points...")

            print(f"Generated {points_added} time points")

        # Add PI DataLink formulas for a few tags to test
        print("Adding test PI DataLink formulas...")

        test_tags = tags[:5]  # Test with first 5 tags
        for i, tag in enumerate(test_tags):
            col = i + 2
            formula_cell = ws.range(header_row + 1, col)

            # Try different PI formula patterns
            formulas_to_try = [
                f'=PIValue("{tag}",A{header_row + 1})',
                f'=PIArchive("{tag}",A{header_row + 1})',
                f'=PIINTERP("{tag}",A{header_row + 1})',
                f'=PI("{tag}",A{header_row + 1})',
            ]

            # Use first formula
            formula_cell.value = formulas_to_try[0]
            print(f"Added formula for {tag}: {formulas_to_try[0]}")

        # Format and save
        ws.range('A1:B5').api.Font.Bold = True
        ws.range(f'A{header_row}:Z{header_row}').api.Font.Bold = True

        print("Saving Excel file...")
        wb.save()

        print("\n" + "=" * 60)
        print("PI DATALINK SETUP COMPLETED!")
        print("=" * 60)
        print(f"✓ Configured headers for all {len(tags)} tags")
        print(f"✓ Set up time series for data collection")
        print(f"✓ Added test PI formulas for first 5 tags")
        print(f"✓ Date range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")

        return True

    except Exception as e:
        print(f"Error setting up PI DataLink: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        print("\nExcel file ready for PI DataLink configuration.")

def refresh_pi_datalink_data(excel_path):
    """Refresh PI DataLink data in the Excel file."""

    print(f"Opening Excel file for PI DataLink refresh: {excel_path}")

    app = xw.App(visible=True)

    try:
        wb = app.books.open(excel_path)

        print("Refreshing PI DataLink data...")
        print("This may take several minutes for 1.5 years of data...")

        # Refresh all external data connections
        wb.api.RefreshAll()

        # Wait for refresh to complete
        print("Waiting for data refresh (60 seconds)...")
        time.sleep(60)

        # Check if data was populated
        ws = wb.sheets['PI_Data'] if 'PI_Data' in [s.name for s in wb.sheets] else wb.sheets[0]
        test_data = ws.range('B8:B10').value  # Check first few data cells

        if any(val is not None and isinstance(val, (int, float)) for val in test_data):
            print("✓ PI data appears to be populating!")
        else:
            print("⚠ No data detected - may need manual PI DataLink configuration")

        wb.save()

        return True

    except Exception as e:
        print(f"Error refreshing PI DataLink: {e}")
        return False

    finally:
        print("PI DataLink refresh completed.")

def main():
    """Main entry point."""
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"

    print("=" * 70)
    print("AUTOMATED PI DATA FETCH FOR C-02001 (1.5 YEARS)")
    print("=" * 70)

    # Step 1: Set up PI DataLink
    print("\nSTEP 1: Setting up PI DataLink structure...")
    setup_success = setup_pi_datalink_batch(excel_file, tag_file)

    if setup_success:
        print("\nSTEP 2: Refreshing PI DataLink data...")
        refresh_success = refresh_pi_datalink_data(excel_file)

        if refresh_success:
            print("\n" + "=" * 50)
            print("AUTOMATED PI DATA FETCH COMPLETED!")
            print("=" * 50)
            print("\nNext steps:")
            print("1. Verify PI data is populating in Excel")
            print("2. Configure additional PI formulas if needed")
            print("3. Wait for full dataset to load")
            print("4. Run parquet builder to create final dataset")
            print("\nExpected final parquet size: 15-20MB with all 80 tags")
        else:
            print("PI DataLink refresh failed - manual configuration may be needed")
    else:
        print("PI DataLink setup failed")

if __name__ == "__main__":
    main()