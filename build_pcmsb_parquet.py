#!/usr/bin/env python3
"""
Build PCMSB parquet file from Excel data using the C-02001 tags.
Uses the existing Excel automation framework from the project.
"""

import pandas as pd
import xlwings as xw
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[0]))

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:  # Just read each line as a tag
                tags.append(line)
    return tags

def refresh_excel_and_extract_data(excel_path, tags, timeframe='0.1h', duration_years=1.5):
    """Refresh Excel PI DataLink and extract data for specified tags."""
    print(f"Opening Excel file: {excel_path}")
    print(f"Timeframe: {timeframe}, Duration: {duration_years} years")

    # Calculate date range (1.5 years back from now)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=int(duration_years * 365))

    print(f"Date range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Open Excel file
    app = xw.App(visible=False)
    try:
        wb = app.books.open(excel_path)

        # Update PI DataLink time parameters if possible
        # This depends on how the Excel file is set up with PI DataLink
        print("Configuring PI DataLink time parameters...")
        try:
            # Try to find and update time range cells (common PI DataLink setup)
            ws = wb.sheets[0]

            # Look for common time parameter cells
            for row in range(1, 20):  # Check first 20 rows
                for col in range(1, 10):  # Check first 10 columns
                    cell_value = ws.cells(row, col).value
                    if cell_value and isinstance(cell_value, str):
                        if 'start' in cell_value.lower() or 'begin' in cell_value.lower():
                            # Found potential start time cell, update adjacent cell
                            ws.cells(row, col + 1).value = start_time.strftime('%m/%d/%Y %H:%M:%S')
                            print(f"Updated start time at row {row}, col {col + 1}")
                        elif 'end' in cell_value.lower() or 'stop' in cell_value.lower():
                            # Found potential end time cell
                            ws.cells(row, col + 1).value = end_time.strftime('%m/%d/%Y %H:%M:%S')
                            print(f"Updated end time at row {row}, col {col + 1}")
                        elif 'interval' in cell_value.lower() or 'step' in cell_value.lower():
                            # Found potential interval cell
                            ws.cells(row, col + 1).value = timeframe
                            print(f"Updated timeframe to {timeframe} at row {row}, col {col + 1}")
        except Exception as e:
            print(f"Note: Could not automatically configure time parameters: {e}")
            print("Using existing Excel configuration")

        # Refresh all PI DataLink connections
        print("Refreshing PI DataLink data...")
        wb.api.RefreshAll()

        # Wait longer for the larger dataset to refresh
        print("Waiting for data refresh to complete...")
        time.sleep(30)  # Longer wait for 1.5 years of data

        # Find the worksheet with data (assuming first sheet)
        ws = wb.sheets[0]

        # Read all data from the sheet
        print("Reading data from Excel...")
        used_range = ws.used_range
        if used_range:
            data = used_range.value
            # Convert to DataFrame
            if data and len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                print(f"Read {len(df)} rows from Excel")
                return df

        return pd.DataFrame()

    finally:
        wb.close()
        app.quit()

def build_pcmsb_parquet(tag_file_path, excel_path, output_path, timeframe='0.1h', duration_years=1.5):
    """Main function to build PCMSB parquet file."""

    # Read tags from configuration file
    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags")

    if not tags:
        print("No tags found in configuration file!")
        return

    # Check if Excel file exists
    excel_file = Path(excel_path)
    if not excel_file.exists():
        print(f"Excel file not found: {excel_path}")
        return

    # Extract data from Excel with specified timeframe and duration
    df = refresh_excel_and_extract_data(excel_path, tags, timeframe, duration_years)

    if df.empty:
        print("No data extracted from Excel!")
        return

    # Filter data for our tags if tag column exists
    tag_columns = [col for col in df.columns if any(tag in str(col) for tag in tags)]
    if tag_columns:
        print(f"Found {len(tag_columns)} tag columns matching our tags")
        df_filtered = df[['timestamp'] + tag_columns] if 'timestamp' in df.columns else df[tag_columns]
    else:
        print("Using all data from Excel (no tag filtering)")
        df_filtered = df

    # Add metadata columns for consistency with existing parquet files
    df_filtered['plant'] = 'PCMSB'
    df_filtered['unit'] = 'C-02001'

    # Ensure output directory exists
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Convert numeric columns and handle mixed types
    print("Processing data types...")
    for col in df_filtered.columns:
        if col not in ['plant', 'unit', 'timestamp']:
            # Convert to string first, then attempt numeric conversion
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

    # Save to parquet
    print(f"Saving to parquet: {output_path}")
    df_filtered.to_parquet(output_path, engine='pyarrow', compression='snappy')

    print(f"Successfully created PCMSB parquet file with {len(df_filtered)} rows")
    print(f"Output: {output_path}")

def main():
    """Main entry point."""
    # Configuration
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx"
    output_file = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_1p5y_0p1h.parquet"

    # Build the parquet file with 1.5 years of data at 0.1h intervals
    print("=" * 60)
    print("PCMSB PARQUET BUILDER - 1.5 YEARS @ 0.1H TIMEFRAME")
    print("=" * 60)
    build_pcmsb_parquet(tag_file, excel_file, output_file, timeframe='0.1h', duration_years=1.5)

if __name__ == "__main__":
    main()