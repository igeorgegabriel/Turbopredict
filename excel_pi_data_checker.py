#!/usr/bin/env python3
"""
Excel PI Data Checker - Diagnose why Excel isn't fetching full PI data
"""

import pandas as pd
from pathlib import Path

def check_excel_pi_data():
    """Check the current state of PI data in Excel file."""

    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"

    print("=" * 60)
    print("EXCEL PI DATA DIAGNOSTIC")
    print("=" * 60)

    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    try:
        # Read all sheets
        excel_data = pd.ExcelFile(excel_file)
        print(f"Available sheets: {excel_data.sheet_names}")

        for sheet_name in excel_data.sheet_names:
            print(f"\n--- SHEET: {sheet_name} ---")

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            print(f"Shape: {df.shape}")

            if df.shape[0] > 0:
                print(f"Columns: {list(df.columns[:5])}..." if len(df.columns) > 5 else f"Columns: {list(df.columns)}")

                # Check for PI tag columns
                pi_columns = [col for col in df.columns if 'PCM.C-02001' in str(col)]
                print(f"PI tag columns: {len(pi_columns)}")

                if len(pi_columns) > 0:
                    print(f"Sample PI columns: {pi_columns[:3]}")

                    # Check data population
                    sample_data = df[pi_columns[:3]].head(10) if len(pi_columns) >= 3 else df[pi_columns].head(10)
                    print(f"Sample data:\n{sample_data}")

                    # Check for null values
                    null_counts = df[pi_columns].isnull().sum()
                    total_nulls = null_counts.sum()
                    total_cells = len(df) * len(pi_columns)

                    print(f"Data quality:")
                    print(f"  Total cells: {total_cells}")
                    print(f"  Null cells: {total_nulls}")
                    print(f"  Fill rate: {(1 - total_nulls/total_cells)*100:.1f}%")

    except Exception as e:
        print(f"Error reading Excel: {e}")

def main():
    check_excel_pi_data()

    print(f"\n" + "=" * 60)
    print("DIAGNOSIS COMPLETE")
    print("=" * 60)
    print("Key issues to check:")
    print("1. Are PISampDat formulas present in Excel?")
    print("2. Is PI DataLink properly connected?")
    print("3. Has the data been refreshed recently?")
    print("4. Are there any formula errors in Excel?")
    print("5. Is the time range appropriate for available data?")

if __name__ == "__main__":
    main()