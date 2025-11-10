#!/usr/bin/env python3
"""
SIMPLIFIED AUTOMATED PI FETCH - Version 2
Uses the existing Excel file and automates the PI data refresh and parquet generation.
"""

import xlwings as xw
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
import sys

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

def automated_pi_refresh_and_extract(excel_path, tag_file_path, output_parquet_path):
    """Fully automated PI data refresh and extraction."""

    print("=" * 70)
    print("AUTOMATED PI DATA FETCH AND EXTRACTION")
    print("=" * 70)

    # Read expected tags
    tags = read_tags_file(tag_file_path)
    print(f"Expected {len(tags)} PI tags")

    if not tags:
        print("No tags found!")
        return False

    print(f"Opening Excel file: {excel_path}")

    app = xw.App(visible=False)  # Hidden for automation

    try:
        wb = app.books.open(excel_path)

        # Find the sheet with PI data
        data_sheet = None
        for sheet in wb.sheets:
            if 'data' in sheet.name.lower() or 'pi' in sheet.name.lower():
                data_sheet = sheet
                break

        if not data_sheet:
            data_sheet = wb.sheets[0]  # Use first sheet as fallback

        print(f"Using sheet: {data_sheet.name}")

        # Check if PI formulas are present
        print("Checking for PI DataLink formulas...")

        # Look for PISampDat formulas in row 8 (first data row)
        formula_count = 0
        for col in range(2, min(82, 100)):  # Columns B through CC (or reasonable limit)
            try:
                cell = data_sheet.cells(8, col)
                if hasattr(cell, 'Formula') and cell.Formula:
                    formula = str(cell.Formula)
                    if 'PISampDat' in formula:
                        formula_count += 1
            except:
                continue

        print(f"Found {formula_count} PISampDat formulas")

        if formula_count == 0:
            print("No PISampDat formulas found. Please run fix_excel_with_correct_formula.py first")
            return False

        # Step 1: Refresh PI DataLink automatically
        print("Step 1: Refreshing PI DataLink data...")
        print("This may take several minutes for 1.5 years of data...")

        try:
            # Refresh all data connections
            wb.api.RefreshAll()
            print("PI DataLink refresh initiated")

            # Wait for refresh to complete
            print("Waiting for data refresh (checking every 30 seconds)...")

            max_wait_minutes = 20
            start_time = time.time()

            for attempt in range(max_wait_minutes * 2):  # Check every 30 seconds
                time.sleep(30)

                elapsed_minutes = (time.time() - start_time) / 60
                print(f"  Waiting... {elapsed_minutes:.1f} minutes elapsed")

                # Check if data is appearing
                try:
                    sample_data = data_sheet.range('B8:E12').value  # Sample a few cells
                    data_count = 0
                    if sample_data:
                        for row in sample_data:
                            if row:
                                for cell in row:
                                    if isinstance(cell, (int, float)) and cell != 0:
                                        data_count += 1

                    print(f"    Sample data points found: {data_count}")

                    if data_count >= 8:  # Reasonable amount of data
                        print("Data detected - proceeding to extraction")
                        break

                except Exception as e:
                    print(f"    Error checking data: {e}")

                if elapsed_minutes >= max_wait_minutes:
                    print(f"Timeout after {max_wait_minutes} minutes - proceeding anyway")
                    break

        except Exception as e:
            print(f"Error during refresh: {e}")
            print("Continuing with data extraction attempt...")

        # Step 2: Extract data regardless of refresh status
        print("\nStep 2: Extracting data from Excel...")

        try:
            # Read data using pandas (more reliable for large datasets)
            print("Using pandas to read Excel data...")

            # Try reading the Data sheet
            try:
                df = pd.read_excel(excel_path, sheet_name='Data', engine='openpyxl')
                print(f"Successfully read {df.shape[0]} rows, {df.shape[1]} columns from Data sheet")
            except:
                # Fallback to first sheet
                print("Fallback: Reading first sheet...")
                df = pd.read_excel(excel_path, sheet_name=0, engine='openpyxl')
                print(f"Successfully read {df.shape[0]} rows, {df.shape[1]} columns")

            if df.empty:
                print("No data found in Excel file")
                return False

            # Identify columns
            print("Analyzing columns...")

            # Find timestamp column
            timestamp_col = None
            for col in df.columns:
                if 'time' in str(col).lower():
                    timestamp_col = col
                    break

            # Find PI tag columns
            tag_columns = []
            for col in df.columns:
                col_str = str(col)
                if any(tag in col_str for tag in tags):
                    tag_columns.append(col)

            print(f"Timestamp column: {timestamp_col}")
            print(f"Found {len(tag_columns)} PI tag columns")

            if len(tag_columns) == 0:
                print("No PI tag columns found in data")
                return False

            # Prepare final dataset
            if timestamp_col:
                final_columns = [timestamp_col] + tag_columns
                df_final = df[final_columns].copy()
                df_final.rename(columns={timestamp_col: 'timestamp'}, inplace=True)
            else:
                df_final = df[tag_columns].copy()

            # Add metadata
            df_final['plant'] = 'PCMSB'
            df_final['unit'] = 'C-02001'

            # Clean data types
            print("Processing data types...")
            for col in tag_columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

            # Remove completely empty rows
            initial_rows = len(df_final)
            df_final = df_final.dropna(subset=tag_columns, how='all')
            final_rows = len(df_final)

            print(f"Data cleaning: {initial_rows} -> {final_rows} rows")

            if final_rows == 0:
                print("No valid data rows after cleaning")
                return False

            # Step 3: Save to parquet
            print(f"\nStep 3: Saving to parquet...")

            # Ensure output directory exists
            output_path = Path(output_parquet_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet
            df_final.to_parquet(output_parquet_path, engine='pyarrow', compression='snappy')

            # Report results
            file_size = output_path.stat().st_size

            print(f"\n" + "=" * 60)
            print("AUTOMATED EXTRACTION COMPLETED!")
            print("=" * 60)
            print(f"Output file: {output_parquet_path}")
            print(f"Rows: {len(df_final):,}")
            print(f"Columns: {len(df_final.columns)}")
            print(f"PI tags with data: {len(tag_columns)}")
            print(f"File size: {file_size / 1024 / 1024:.2f} MB")

            # Data quality analysis
            if tag_columns:
                null_percentages = df_final[tag_columns].isnull().mean() * 100
                avg_null_pct = null_percentages.mean()
                non_empty_tags = sum(null_percentages < 100)

                print(f"\nData Quality:")
                print(f"Tags with data: {non_empty_tags}/{len(tags)}")
                print(f"Average null percentage: {avg_null_pct:.1f}%")

                if file_size > 5 * 1024 * 1024:  # > 5MB
                    print("SUCCESS: Large dataset generated!")
                elif non_empty_tags >= 40:  # At least half the tags
                    print("PARTIAL SUCCESS: Significant data retrieved")
                else:
                    print("LIMITED SUCCESS: Some data retrieved")

            return True

        except Exception as e:
            print(f"Error extracting data: {e}")
            import traceback
            traceback.print_exc()
            return False

    except Exception as e:
        print(f"Error opening Excel file: {e}")
        return False

    finally:
        # Cleanup
        try:
            if 'wb' in locals():
                wb.close()
            if 'app' in locals():
                app.quit()
        except:
            pass

def main():
    """Main entry point."""

    # File paths
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    output_parquet = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_AUTOMATED_FULL.parquet"

    # Check if files exist
    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        print("Please ensure the Excel file exists with PISampDat formulas")
        return

    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    print("Starting fully automated PI data fetch...")
    print(f"Source: {Path(excel_file).name}")
    print(f"Target: {Path(output_parquet).name}")

    # Run automated process
    success = automated_pi_refresh_and_extract(excel_file, tag_file, output_parquet)

    if success:
        print("\nAUTOMATION COMPLETED SUCCESSFULLY!")
        print("Your parquet file is ready for analysis.")
    else:
        print("\nAutomation completed with issues.")
        print("Check the logs above for details.")

if __name__ == "__main__":
    main()