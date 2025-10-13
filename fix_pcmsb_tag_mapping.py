#!/usr/bin/env python3
"""
Fix PCMSB tag mapping - need to handle multiple PI tags per unit correctly
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime

def analyze_pcmsb_tag_issue():
    """Analyze the PCMSB tag mapping issue"""

    print("ANALYZING PCMSB TAG MAPPING ISSUE")
    print("=" * 40)

    print("PROBLEM:")
    print("1. DL_WORK sheet contains aggregated time-value data (87,601 rows)")
    print("2. But each PCMSB unit should have multiple individual PI tags")
    print("3. Config files show 52-80 tags per unit")
    print("4. Current parquet files only have 1 synthetic tag per unit")
    print()

    # Check what's actually needed
    print("EXPECTED STRUCTURE:")

    # Sample unit
    unit = "C-02001"
    tags_file = Path(f"config/tags_pcmsb_{unit.lower().replace('-', '')}.txt")

    if tags_file.exists():
        tags = [t.strip() for t in tags_file.read_text().splitlines()
                if t.strip() and not t.strip().startswith('#')]

        print(f"Unit {unit} should have:")
        print(f"   {len(tags)} individual PI tags")
        print(f"   Each tag should have its own time series data")
        print(f"   Sample tags: {tags[:3]}")
        print()

        print("CURRENT vs EXPECTED:")
        print(f"   Current: 1 synthetic tag with 87,601 records")
        print(f"   Expected: {len(tags)} real tags with ~1,000-2,000 records each")
        print()

    print("ROOT CAUSE:")
    print("The DL_WORK sheet appears to contain data for a single aggregated metric,")
    print("but we need individual tag data. This suggests either:")
    print("1. The DL_WORK sheet is not the right data source")
    print("2. We need to use the original tag-by-tag fetching approach")
    print("3. The Excel file structure is different than expected")
    print()

    # Check if there are other sheets
    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")
    if excel_path.exists():
        try:
            excel_file = pd.ExcelFile(excel_path)
            print(f"Available sheets in PCMSB Excel file:")
            for sheet in excel_file.sheet_names:
                print(f"   - {sheet}")

                # Sample each sheet
                try:
                    df = pd.read_excel(excel_path, sheet_name=sheet, nrows=5)
                    print(f"     Rows: {len(df)}, Columns: {len(df.columns) if len(df) > 0 else 0}")
                    if len(df) > 0 and len(df.columns) > 0:
                        print(f"     Sample columns: {list(df.columns)[:3]}")
                except Exception as e:
                    print(f"     Error reading: {e}")
                print()

        except Exception as e:
            print(f"Error analyzing Excel file: {e}")

    print("SOLUTION:")
    print("We need to revert to the original tag-by-tag fetching approach")
    print("but fix the sheet mapping issue so it uses DL_WORK correctly.")
    print("The tag-by-tag approach should:")
    print("1. Use DL_WORK as the working sheet")
    print("2. Fetch each tag individually using PI formulas")
    print("3. Aggregate all tag data into a single parquet file per unit")

if __name__ == "__main__":
    analyze_pcmsb_tag_issue()