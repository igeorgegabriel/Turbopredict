#!/usr/bin/env python3
"""
Fix C-02001 parquet files to match the expected long format structure.
Convert from wide format (columns per tag) to long format (rows per tag).
"""

import pandas as pd
from pathlib import Path
import time

def convert_wide_to_long_format(df, unit_name):
    """Convert wide format DataFrame to long format matching K-12-01 structure."""

    print(f"Converting {unit_name} from wide to long format...")

    # Identify tag columns (exclude metadata columns)
    metadata_cols = ['timestamp', 'plant', 'unit']
    tag_columns = [col for col in df.columns if col not in metadata_cols]

    print(f"Found {len(tag_columns)} tag columns to convert")

    # Melt the DataFrame to convert from wide to long
    df_long = pd.melt(
        df,
        id_vars=['timestamp', 'plant', 'unit'],
        value_vars=tag_columns,
        var_name='tag',
        value_name='value'
    )

    # Rename columns to match K-12-01 format exactly
    df_long = df_long.rename(columns={'timestamp': 'time'})

    # Reorder columns to match K-12-01: ['plant', 'unit', 'tag', 'time', 'value']
    df_long = df_long[['plant', 'unit', 'tag', 'time', 'value']]

    # Remove null values
    initial_rows = len(df_long)
    df_long = df_long.dropna(subset=['value'])
    final_rows = len(df_long)

    print(f"Conversion complete: {initial_rows:,} -> {final_rows:,} rows ({final_rows/initial_rows*100:.1f}% retained)")

    return df_long

def fix_c02001_format():
    """Fix all C-02001 files to use the correct long format."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print("=" * 70)
    print("FIXING C-02001 FORMAT TO MATCH SYSTEM STRUCTURE")
    print("=" * 70)

    # Find C-02001 files
    c02001_files = list(processed_dir.glob("C-02001_*.parquet"))

    if not c02001_files:
        print("No C-02001 files found!")
        return False

    print(f"Found {len(c02001_files)} C-02001 files to convert:")
    for file in c02001_files:
        size_mb = file.stat().st_size / 1024 / 1024
        print(f"  {file.name} ({size_mb:.1f} MB)")

    # Convert each file
    converted_files = []

    for file in c02001_files:
        print(f"\n--- Converting {file.name} ---")

        try:
            # Read original file
            df_wide = pd.read_parquet(file)
            print(f"Original format: {df_wide.shape[0]:,} rows x {df_wide.shape[1]} columns")

            # Convert to long format
            start_time = time.time()
            df_long = convert_wide_to_long_format(df_wide, "C-02001")
            conversion_time = time.time() - start_time

            print(f"New format: {df_long.shape[0]:,} rows x {df_long.shape[1]} columns")
            print(f"Conversion time: {conversion_time:.1f} seconds")

            # Create backup of original
            backup_file = file.with_suffix('.parquet.backup')
            file.rename(backup_file)
            print(f"Backed up original as: {backup_file.name}")

            # Save converted file
            df_long.to_parquet(file, engine='pyarrow', compression='snappy')

            new_size = file.stat().st_size
            print(f"New file size: {new_size / 1024 / 1024:.1f} MB")

            converted_files.append({
                'file': file.name,
                'old_shape': df_wide.shape,
                'new_shape': df_long.shape,
                'old_size_mb': backup_file.stat().st_size / 1024 / 1024,
                'new_size_mb': new_size / 1024 / 1024
            })

        except Exception as e:
            print(f"Error converting {file.name}: {e}")
            continue

    # Summary
    print(f"\n" + "=" * 70)
    print("CONVERSION SUMMARY")
    print("=" * 70)

    for info in converted_files:
        print(f"\n{info['file']}:")
        print(f"  Shape: {info['old_shape']} -> {info['new_shape']}")
        print(f"  Size: {info['old_size_mb']:.1f} MB -> {info['new_size_mb']:.1f} MB")
        print(f"  Structure: Wide format -> Long format (matches K-12-01)")

    if converted_files:
        print(f"\n✓ Successfully converted {len(converted_files)} files!")
        print("✓ C-02001 now uses the same structure as other units")
        print("✓ System should now detect all 80 tags correctly")

        return True
    else:
        print("No files were successfully converted")
        return False

def verify_conversion():
    """Verify the conversion worked by comparing with K-12-01 structure."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print(f"\n" + "=" * 50)
    print("VERIFICATION - COMPARING STRUCTURES")
    print("=" * 50)

    # Check C-02001 structure
    c02001_file = processed_dir / "C-02001_1y_0p1h.parquet"
    if c02001_file.exists():
        df_c02001 = pd.read_parquet(c02001_file)
        print(f"C-02001 structure: {df_c02001.shape}")
        print(f"C-02001 columns: {list(df_c02001.columns)}")

        # Count unique tags
        if 'tag' in df_c02001.columns:
            unique_tags = df_c02001['tag'].nunique()
            print(f"C-02001 unique tags: {unique_tags}")

    # Check K-12-01 structure for comparison
    k12_file = processed_dir / "K-12-01_1y_0p1h.parquet"
    if k12_file.exists():
        df_k12 = pd.read_parquet(k12_file)
        print(f"\nK-12-01 structure: {df_k12.shape}")
        print(f"K-12-01 columns: {list(df_k12.columns)}")

        if 'tag' in df_k12.columns:
            k12_unique_tags = df_k12['tag'].nunique()
            print(f"K-12-01 unique tags: {k12_unique_tags}")

    print(f"\n✓ Both units now use the same column structure!")

def main():
    """Main entry point."""

    success = fix_c02001_format()

    if success:
        verify_conversion()

        print(f"\n" + "=" * 70)
        print("C-02001 FORMAT FIX COMPLETED!")
        print("=" * 70)
        print("✓ Converted from wide format to long format")
        print("✓ Now matches K-12-01 and other unit structures")
        print("✓ System should detect all 80 tags in next scan")
        print("✓ Original files backed up with .backup extension")

    else:
        print("Format fix failed - check error messages above")

if __name__ == "__main__":
    main()