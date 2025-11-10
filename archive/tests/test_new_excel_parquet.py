#!/usr/bin/env python3
"""
Test the parquet generation using the new Excel file with all 80 tags.
Modified version of build_pcmsb_parquet.py to use the new Excel file.
"""

import pandas as pd
import xlwings as xw
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:  # Just read each line as a tag
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def extract_data_from_new_excel(excel_path, sheet_name='Data'):
    """Extract data from the new Excel file structure."""
    print(f"Reading data from Excel: {excel_path}")
    print(f"Using sheet: {sheet_name}")

    try:
        # Read the data using pandas (faster than xlwings for large data)
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        print(f"Successfully read {len(df)} rows and {len(df.columns)} columns")

        # Show column info
        print(f"Columns: {list(df.columns[:5])}...{list(df.columns[-5:])}")

        return df

    except Exception as e:
        print(f"Error reading Excel with pandas: {e}")
        print("Trying with xlwings...")

        # Fallback to xlwings
        app = xw.App(visible=False)
        try:
            wb = app.books.open(excel_path)
            ws = wb.sheets[sheet_name]

            # Read all data
            used_range = ws.used_range
            if used_range:
                data = used_range.value
                if data and len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    print(f"Read {len(df)} rows from Excel using xlwings")
                    return df

            return pd.DataFrame()

        finally:
            if 'wb' in locals():
                wb.close()
            app.quit()

def build_parquet_from_new_excel(tag_file_path, excel_path, output_path):
    """Build parquet file from the new Excel structure."""

    # Read tags from configuration file
    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Expected {len(tags)} tags")

    if not tags:
        print("No tags found in configuration file!")
        return

    # Check if Excel file exists
    excel_file = Path(excel_path)
    if not excel_file.exists():
        print(f"Excel file not found: {excel_path}")
        return

    # Extract data from new Excel file
    df = extract_data_from_new_excel(excel_path)

    if df.empty:
        print("No data extracted from Excel!")
        return

    print(f"\n=== DATA ANALYSIS ===")
    print(f"Shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")

    # Check for timestamp column
    timestamp_col = None
    for col in df.columns:
        if 'time' in col.lower() or 'date' in col.lower():
            timestamp_col = col
            break

    if timestamp_col:
        print(f"Timestamp column: {timestamp_col}")
    else:
        print("No timestamp column found")

    # Check for PI tag columns
    tag_columns = []
    for col in df.columns:
        if any(tag in str(col) for tag in tags):
            tag_columns.append(col)

    print(f"Found {len(tag_columns)} matching tag columns")
    if tag_columns:
        print(f"Sample tag columns: {tag_columns[:5]}")

    # Process the data
    if tag_columns:
        # Select timestamp and tag columns
        if timestamp_col:
            df_filtered = df[[timestamp_col] + tag_columns].copy()
            df_filtered.rename(columns={timestamp_col: 'timestamp'}, inplace=True)
        else:
            df_filtered = df[tag_columns].copy()
    else:
        print("Using all data (no specific tag filtering)")
        df_filtered = df.copy()

    # Add metadata columns
    df_filtered['plant'] = 'PCMSB'
    df_filtered['unit'] = 'C-02001'

    # Process data types
    print("Processing data types...")
    for col in df_filtered.columns:
        if col not in ['plant', 'unit', 'timestamp']:
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

    # Remove rows that are completely null (except metadata)
    data_cols = [col for col in df_filtered.columns if col not in ['plant', 'unit', 'timestamp']]
    df_filtered = df_filtered.dropna(subset=data_cols, how='all')

    # Ensure output directory exists
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Save to parquet
    print(f"Saving to parquet: {output_path}")
    df_filtered.to_parquet(output_path, engine='pyarrow', compression='snappy')

    # File size analysis
    file_size = output_file.stat().st_size
    print(f"\n=== RESULTS ===")
    print(f"Parquet file created: {output_path}")
    print(f"Rows: {len(df_filtered)}")
    print(f"Columns: {len(df_filtered.columns)}")
    print(f"File size: {file_size / 1024 / 1024:.2f} MB")
    print(f"Tag columns with data: {len(tag_columns)}")

    # Analyze data quality
    print(f"\n=== DATA QUALITY ===")
    if data_cols:
        null_percentages = df_filtered[data_cols].isnull().mean() * 100
        non_null_cols = sum(null_percentages < 100)
        print(f"Columns with data: {non_null_cols}/{len(data_cols)}")
        if non_null_cols > 0:
            print(f"Average null percentage: {null_percentages.mean():.1f}%")

def main():
    """Main entry point."""
    # Configuration for new Excel file
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    output_file = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_Full_80tags.parquet"

    print("=" * 70)
    print("TESTING PARQUET GENERATION WITH NEW EXCEL FILE (80 TAGS)")
    print("=" * 70)

    build_parquet_from_new_excel(tag_file, excel_file, output_file)

if __name__ == "__main__":
    main()