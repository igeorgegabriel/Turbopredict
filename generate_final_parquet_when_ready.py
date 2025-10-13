#!/usr/bin/env python3
"""
Generate the final parquet file once PI DataLink has populated the Excel with full data.
This script monitors the Excel file and creates the parquet when data is ready.
"""

import pandas as pd
import xlwings as xw
from pathlib import Path
from datetime import datetime
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

def check_excel_data_quality(excel_path, sheet_name='Data'):
    """Check if the Excel file has been populated with PI data."""

    print(f"Checking data quality in: {excel_path}")

    try:
        # Try reading with pandas first (faster)
        df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=1000)  # Sample first 1000 rows

        print(f"Sample data shape: {df.shape}")

        # Check for PI tag columns
        pi_columns = [col for col in df.columns if 'PCM.C-02001' in str(col)]
        print(f"Found {len(pi_columns)} PI tag columns")

        if len(pi_columns) == 0:
            print("No PI tag columns found")
            return False, 0, 0

        # Check data population
        non_null_counts = df[pi_columns].count()
        populated_tags = sum(non_null_counts > 0)
        avg_population = non_null_counts.mean()

        print(f"Tags with data: {populated_tags}/{len(pi_columns)}")
        print(f"Average data points per tag: {avg_population:.0f}")

        # Consider data ready if most tags have reasonable amount of data
        is_ready = (populated_tags >= len(pi_columns) * 0.8) and (avg_population > 100)

        return is_ready, populated_tags, len(pi_columns)

    except Exception as e:
        print(f"Error checking Excel data: {e}")
        return False, 0, 0

def generate_final_parquet_from_excel(excel_path, tag_file_path, output_path, sheet_name='Data'):
    """Generate the final parquet file from populated Excel data."""

    print(f"Generating final parquet from Excel data...")

    # Read expected tags
    tags = read_tags_file(tag_file_path)
    print(f"Expected {len(tags)} tags")

    try:
        # Read the full Excel data
        print("Reading Excel data (this may take a few minutes for large datasets)...")
        df = pd.read_excel(excel_path, sheet_name=sheet_name)

        print(f"Loaded data shape: {df.shape}")
        print(f"Columns: {len(df.columns)}")

        # Identify timestamp column
        timestamp_col = None
        for col in df.columns:
            if 'time' in col.lower() or 'date' in col.lower():
                timestamp_col = col
                break

        # Identify PI tag columns
        tag_columns = []
        for col in df.columns:
            if any(tag in str(col) for tag in tags):
                tag_columns.append(col)

        print(f"Found timestamp column: {timestamp_col}")
        print(f"Found {len(tag_columns)} matching tag columns")

        if len(tag_columns) == 0:
            print("No PI tag data found!")
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

        # Clean and process data
        print("Processing data types...")
        for col in tag_columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

        # Remove completely empty rows
        data_cols = tag_columns
        initial_rows = len(df_final)
        df_final = df_final.dropna(subset=data_cols, how='all')
        final_rows = len(df_final)

        print(f"Cleaned data: {initial_rows} → {final_rows} rows")

        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Save to parquet
        print(f"Saving to parquet: {output_path}")
        df_final.to_parquet(output_path, engine='pyarrow', compression='snappy')

        # Analyze results
        file_size = output_file.stat().st_size

        print(f"\n" + "=" * 60)
        print("FINAL PARQUET GENERATION COMPLETED!")
        print("=" * 60)
        print(f"✓ Output file: {output_path}")
        print(f"✓ Rows: {len(df_final):,}")
        print(f"✓ Columns: {len(df_final.columns)}")
        print(f"✓ PI tags with data: {len(tag_columns)}/80")
        print(f"✓ File size: {file_size / 1024 / 1024:.2f} MB")

        # Data quality summary
        print(f"\n=== DATA QUALITY SUMMARY ===")
        if tag_columns:
            null_percentages = df_final[tag_columns].isnull().mean() * 100
            non_empty_tags = sum(null_percentages < 100)
            avg_null_pct = null_percentages.mean()

            print(f"Tags with data: {non_empty_tags}/{len(tag_columns)}")
            print(f"Average null percentage: {avg_null_pct:.1f}%")

            if non_empty_tags == len(tags):
                print("🎉 SUCCESS: All 80 PI tags have data!")
            else:
                print(f"⚠ WARNING: Only {non_empty_tags}/80 tags have data")

        return True

    except Exception as e:
        print(f"Error generating parquet: {e}")
        import traceback
        traceback.print_exc()
        return False

def monitor_and_generate_when_ready(excel_path, tag_file_path, output_path, check_interval=300):
    """Monitor Excel file and generate parquet when data is ready."""

    print(f"Monitoring Excel file for data readiness...")
    print(f"Checking every {check_interval} seconds...")

    while True:
        is_ready, populated_tags, total_tags = check_excel_data_quality(excel_path)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Data check: {populated_tags}/{total_tags} tags populated")

        if is_ready:
            print("✓ Data appears ready! Generating final parquet...")
            success = generate_final_parquet_from_excel(excel_path, tag_file_path, output_path)
            if success:
                print("🎉 Final parquet generation completed!")
                break
            else:
                print("❌ Parquet generation failed, continuing to monitor...")
        else:
            print(f"⏳ Data not ready yet, waiting {check_interval} seconds...")

        time.sleep(check_interval)

def main():
    """Main entry point."""

    # File paths
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    output_file = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_FULL_1p5y_0p1h.parquet"

    print("=" * 70)
    print("FINAL PARQUET GENERATION - C-02001 FULL DATASET")
    print("=" * 70)

    # Check if files exist
    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    print("Options:")
    print("1. Check data quality and generate immediately")
    print("2. Monitor and auto-generate when ready")
    print("3. Force generate regardless of data quality")

    choice = input("Enter choice (1/2/3): ").strip()

    if choice == '1':
        # Check and generate immediately if ready
        is_ready, populated_tags, total_tags = check_excel_data_quality(excel_file)
        if is_ready:
            generate_final_parquet_from_excel(excel_file, tag_file, output_file)
        else:
            print(f"Data not ready: {populated_tags}/{total_tags} tags populated")

    elif choice == '2':
        # Monitor and auto-generate
        monitor_and_generate_when_ready(excel_file, tag_file, output_file)

    elif choice == '3':
        # Force generate
        print("Force generating parquet with current data...")
        generate_final_parquet_from_excel(excel_file, tag_file, output_file)

    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()