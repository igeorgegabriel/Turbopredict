#!/usr/bin/env python3
"""
Analyze the generated PCMSB parquet file to understand data structure and quality.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_parquet_file(parquet_path):
    """Analyze the parquet file structure and data quality."""

    print(f"Analyzing parquet file: {parquet_path}")

    try:
        # Load the parquet file
        df = pd.read_parquet(parquet_path)

        print(f"\n=== BASIC INFO ===")
        print(f"Shape: {df.shape}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

        print(f"\n=== COLUMN INFO ===")
        print(f"Total columns: {len(df.columns)}")
        print(f"Column names (first 10): {list(df.columns[:10])}")
        if len(df.columns) > 10:
            print(f"... and {len(df.columns) - 10} more columns")

        print(f"\n=== DATA TYPES ===")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"{dtype}: {count} columns")

        print(f"\n=== NULL VALUES ===")
        null_counts = df.isnull().sum()
        non_zero_nulls = null_counts[null_counts > 0]
        if len(non_zero_nulls) > 0:
            print(f"Columns with null values:")
            for col, count in non_zero_nulls.items():
                percentage = (count / len(df)) * 100
                print(f"  {col}: {count} ({percentage:.1f}%)")
        else:
            print("No null values found")

        print(f"\n=== TAG COLUMNS ===")
        # Look for columns that look like PI tags
        tag_columns = [col for col in df.columns if 'PCM.C-02001' in str(col) or col.startswith('PCM.')]
        print(f"Found {len(tag_columns)} potential tag columns")
        if tag_columns:
            print(f"First few tag columns: {tag_columns[:5]}")

        # Check for completely empty columns
        empty_cols = []
        for col in df.columns:
            if df[col].isna().all():
                empty_cols.append(col)

        if empty_cols:
            print(f"\n=== EMPTY COLUMNS ===")
            print(f"Found {len(empty_cols)} completely empty columns:")
            for col in empty_cols[:10]:  # Show first 10
                print(f"  {col}")
            if len(empty_cols) > 10:
                print(f"  ... and {len(empty_cols) - 10} more")

        # Check for columns with very few values
        sparse_cols = []
        for col in df.columns:
            if col not in ['plant', 'unit', 'timestamp']:
                non_null_count = df[col].count()
                if non_null_count > 0 and non_null_count < len(df) * 0.01:  # Less than 1% data
                    sparse_cols.append((col, non_null_count))

        if sparse_cols:
            print(f"\n=== SPARSE COLUMNS (< 1% data) ===")
            for col, count in sparse_cols[:10]:
                print(f"  {col}: {count} values ({count/len(df)*100:.3f}%)")

        # Sample data
        print(f"\n=== SAMPLE DATA ===")
        print(df.head(3))

        # File size analysis
        file_size = Path(parquet_path).stat().st_size
        expected_size = len(df) * len(df.columns) * 8  # Rough estimate for float64
        compression_ratio = file_size / expected_size if expected_size > 0 else 0

        print(f"\n=== FILE SIZE ANALYSIS ===")
        print(f"File size: {file_size / 1024 / 1024:.2f} MB")
        print(f"Expected size (uncompressed): {expected_size / 1024 / 1024:.2f} MB")
        print(f"Compression ratio: {compression_ratio:.3f}")

        return df

    except Exception as e:
        print(f"Error analyzing parquet file: {e}")
        return None

def main():
    parquet_path = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_Full_80tags.parquet"

    if not Path(parquet_path).exists():
        print(f"Parquet file not found: {parquet_path}")
        return

    df = analyze_parquet_file(parquet_path)

if __name__ == "__main__":
    main()