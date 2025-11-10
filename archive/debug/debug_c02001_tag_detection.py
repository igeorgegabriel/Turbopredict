#!/usr/bin/env python3
"""
Debug why C-02001 shows 0 tags in the fresh data scan.
Check tag detection and routing issues.
"""

import pandas as pd
from pathlib import Path
import sys

def debug_c02001_tag_detection():
    """Debug why C-02001 shows 0 tags in system scan."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print("=" * 70)
    print("DEBUGGING C-02001 TAG DETECTION ISSUE")
    print("=" * 70)

    # Check if C-02001 files exist
    c02001_files = list(processed_dir.glob("C-02001_*.parquet"))
    print(f"Found {len(c02001_files)} C-02001 files:")

    for file in c02001_files:
        size_mb = file.stat().st_size / 1024 / 1024
        print(f"  {file.name} ({size_mb:.1f} MB)")

    if not c02001_files:
        print("ERROR: No C-02001 files found!")
        return False

    # Analyze the main file
    main_file = None
    for file in c02001_files:
        if "1y_0p1h.parquet" in file.name and "dedup" not in file.name:
            main_file = file
            break

    if not main_file:
        main_file = c02001_files[0]  # Use first file as fallback

    print(f"\nAnalyzing main file: {main_file.name}")

    try:
        df = pd.read_parquet(main_file)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        # Look for PI tag columns specifically
        pi_tag_columns = []
        for col in df.columns:
            if 'PCM.C-02001' in str(col):
                pi_tag_columns.append(col)

        print(f"\nPI Tag Analysis:")
        print(f"Total PI tag columns found: {len(pi_tag_columns)}")

        if pi_tag_columns:
            print(f"Sample PI tags:")
            for tag in pi_tag_columns[:5]:
                print(f"  {tag}")
            if len(pi_tag_columns) > 5:
                print(f"  ... and {len(pi_tag_columns) - 5} more")

            # Check data in PI tag columns
            print(f"\nData Quality Check:")
            sample_tag = pi_tag_columns[0]
            sample_data = df[sample_tag].dropna()
            print(f"Sample tag '{sample_tag}':")
            print(f"  Non-null values: {len(sample_data)}")
            print(f"  Sample values: {sample_data.head().tolist()}")

        else:
            print("ERROR: No PI tag columns found!")
            print("This explains why the system shows 0 tags.")

    except Exception as e:
        print(f"Error reading file: {e}")
        return False

    # Compare with working unit (e.g., K-12-01)
    print(f"\n" + "=" * 50)
    print("COMPARISON WITH WORKING UNIT (K-12-01)")
    print("=" * 50)

    k12_file = processed_dir / "K-12-01_1y_0p1h.parquet"
    if k12_file.exists():
        try:
            df_k12 = pd.read_parquet(k12_file)
            print(f"K-12-01 shape: {df_k12.shape}")
            print(f"K-12-01 columns sample: {list(df_k12.columns[:5])}")

            # Look for tags in K-12-01
            k12_tags = [col for col in df_k12.columns if any(x in str(col) for x in ['K-12-01', 'PCFS', 'PI', 'TI', 'FI'])]
            print(f"K-12-01 tag-like columns: {len(k12_tags)}")
            if k12_tags:
                print(f"Sample K-12-01 tags: {k12_tags[:3]}")

        except Exception as e:
            print(f"Error reading K-12-01: {e}")

    return True

def check_system_tag_detection_pattern():
    """Check what pattern the system uses to detect tags."""

    print(f"\n" + "=" * 50)
    print("INVESTIGATING SYSTEM TAG DETECTION PATTERN")
    print("=" * 50)

    # Look for any system scripts that might do tag detection
    code_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX")

    # Search for files that might contain tag detection logic
    search_files = [
        "pi_monitor",
        "auto_scan",
        "parquet_auto_scan",
        "parquet_database",
        "turbopredict"
    ]

    for search_term in search_files:
        files = list(code_dir.glob(f"**/*{search_term}*.py"))
        if files:
            print(f"\nFound {search_term} related files:")
            for file in files[:3]:  # Show first 3
                print(f"  {file.relative_to(code_dir)}")

    # Look in pi_monitor directory specifically
    pi_monitor_dir = code_dir / "pi_monitor"
    if pi_monitor_dir.exists():
        print(f"\nFiles in pi_monitor directory:")
        for file in pi_monitor_dir.glob("*.py"):
            print(f"  {file.name}")

def suggest_fixes():
    """Suggest possible fixes for the tag detection issue."""

    print(f"\n" + "=" * 50)
    print("SUGGESTED FIXES")
    print("=" * 50)

    print("Possible reasons C-02001 shows 0 tags:")
    print("1. Column naming doesn't match expected pattern")
    print("2. Data values are all null/empty")
    print("3. System expects different file structure")
    print("4. Tag detection logic looks for specific patterns")
    print("5. File routing or unit mapping issue")

    print(f"\nRecommended actions:")
    print("1. Check how other units name their tag columns")
    print("2. Verify tag data has actual values (not all null)")
    print("3. Look at pi_monitor code for tag detection logic")
    print("4. Compare C-02001 file structure with working units")
    print("5. Check if system expects specific metadata")

def main():
    """Main entry point."""

    success = debug_c02001_tag_detection()

    if success:
        check_system_tag_detection_pattern()
        suggest_fixes()

        print(f"\n" + "=" * 70)
        print("DEBUG COMPLETED")
        print("=" * 70)
        print("Next step: Fix the tag detection issue based on findings above")

    else:
        print("Debug failed - check if C-02001 files exist")

if __name__ == "__main__":
    main()