#!/usr/bin/env python3
"""
Fix PCMSB Excel reading - read existing data instead of clearing and fetching
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime

def fix_pcmsb_excel_reading():
    """Fix the issue where PCMSB Excel data is cleared instead of read"""

    print("FIXING PCMSB EXCEL READING ISSUE")
    print("=" * 40)

    print("PROBLEM IDENTIFIED:")
    print("1. PCMSB DL_WORK sheet already has fresh data")
    print("2. But _fetch_single() clears the sheet and tries to fetch new data")
    print("3. This causes timeouts because it's trying to fetch individual tags")
    print("4. Instead, we should READ the existing aggregated data from DL_WORK")
    print()

    # Check the current DL_WORK data
    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")

    if not excel_path.exists():
        print("ERROR: PCMSB Excel file not found")
        return False

    print("ANALYZING CURRENT DL_WORK DATA:")

    try:
        # Read the existing data from DL_WORK
        df = pd.read_excel(excel_path, sheet_name='DL_WORK', header=None)

        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns) if len(df) > 0 else 0}")

        if len(df) > 0:
            print(f"   First few rows:")
            for i in range(min(5, len(df))):
                row_data = df.iloc[i].tolist()
                print(f"     Row {i}: {row_data}")

            # Check if we have time-value pairs
            time_values = []
            for i in range(len(df)):
                row = df.iloc[i]
                if len(row) >= 2:
                    time_col = row.iloc[0]
                    value_col = row.iloc[1]

                    if pd.isna(time_col) or pd.isna(value_col):
                        continue

                    # Check if time_col is a datetime
                    if isinstance(time_col, datetime):
                        time_values.append((time_col, value_col))

            print(f"   Valid time-value pairs: {len(time_values)}")

            if time_values:
                latest_time = max(time_values, key=lambda x: x[0])[0]
                oldest_time = min(time_values, key=lambda x: x[0])[0]

                print(f"   Time range: {oldest_time} to {latest_time}")

                # Check data freshness
                data_age_hours = (datetime.now() - latest_time).total_seconds() / 3600
                print(f"   Data age: {data_age_hours:.1f} hours")

                if data_age_hours < 2:
                    print("   STATUS: Fresh data available!")
                    return True
                else:
                    print("   STATUS: Data is stale")

    except Exception as e:
        print(f"   ERROR reading Excel: {e}")
        return False

    print(f"\nSOLUTION:")
    print(f"The PCMSB units should use a different approach:")
    print(f"1. Read existing time-value data from DL_WORK sheet")
    print(f"2. Convert it to the expected parquet format")
    print(f"3. Skip the individual tag fetching process")
    print(f"")
    print(f"This requires modifying the batch processing logic to:")
    print(f"- Detect PCMSB units")
    print(f"- Read DL_WORK sheet as-is (without clearing)")
    print(f"- Convert the time-value pairs to parquet format")

    return False

if __name__ == "__main__":
    fix_pcmsb_excel_reading()