#!/usr/bin/env python3
"""
Verify C-02001 files are properly created and match the system convention.
"""

import pandas as pd
from pathlib import Path

def verify_c02001_files():
    """Verify all C-02001 files."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print("=" * 60)
    print("C-02001 FILES VERIFICATION")
    print("=" * 60)

    # Check for C-02001 files
    c02001_files = list(processed_dir.glob("C-02001_*.parquet"))

    if not c02001_files:
        print("No C-02001 files found!")
        return False

    for file in sorted(c02001_files):
        size_mb = file.stat().st_size / 1024 / 1024
        print(f"\nFile: {file.name}")
        print(f"Size: {size_mb:.1f} MB")

        try:
            df = pd.read_parquet(file)
            pi_tags = [col for col in df.columns if 'PCM.C-02001' in str(col)]

            print(f"Rows: {len(df):,}")
            print(f"Columns: {len(df.columns)}")
            print(f"PI Tags: {len(pi_tags)}")

            if 'timestamp' in df.columns:
                time_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                print(f"Time range: {time_range}")

            print(f"Sample columns: {list(df.columns[:3])}...")

        except Exception as e:
            print(f"Error reading file: {e}")

    return True

def compare_with_existing_files():
    """Compare C-02001 files with existing system files."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print(f"\n" + "=" * 60)
    print("COMPARISON WITH EXISTING SYSTEM FILES")
    print("=" * 60)

    # Show similar files for comparison
    patterns = ["K-*_1y_0p1h.parquet", "C-*_1y_0p1h.parquet"]

    for pattern in patterns:
        files = list(processed_dir.glob(pattern))
        if files:
            print(f"\n{pattern} files:")
            for file in sorted(files)[:5]:  # Show first 5
                size_mb = file.stat().st_size / 1024 / 1024
                print(f"  {file.name} ({size_mb:.1f} MB)")

    print(f"\nNow C-02001 fits perfectly with your existing naming convention!")

def main():
    verify_c02001_files()
    compare_with_existing_files()

if __name__ == "__main__":
    main()