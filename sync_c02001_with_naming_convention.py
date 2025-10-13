#!/usr/bin/env python3
"""
Sync C-02001 parquet file with existing naming convention and create options 1, 2, 3
"""

import pandas as pd
import shutil
from pathlib import Path
from datetime import datetime

def sync_c02001_parquet_files():
    """Sync C-02001 files with existing naming convention and create multiple options."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    # Source file (our generated file)
    source_file = processed_dir / "PCMSB_C-02001_FINAL_COMPLETE.parquet"

    if not source_file.exists():
        print(f"Source file not found: {source_file}")
        return False

    print("=" * 60)
    print("SYNCING C-02001 WITH NAMING CONVENTION")
    print("=" * 60)
    print(f"Source: {source_file.name}")
    print(f"Size: {source_file.stat().st_size / 1024 / 1024:.1f} MB")

    # Read the source data
    print("Loading source data...")
    df = pd.read_parquet(source_file)
    print(f"Source data: {len(df):,} rows × {len(df.columns)} columns")

    # Option 1: Standard 1.5y version (following pattern but with 1.5y)
    option1_file = processed_dir / "C-02001_1p5y_0p1h.parquet"
    print(f"\nOption 1: Creating {option1_file.name}")
    df.to_parquet(option1_file, engine='pyarrow', compression='snappy')
    option1_size = option1_file.stat().st_size
    print(f"  Created: {option1_size / 1024 / 1024:.1f} MB")

    # Option 2: Standard 1y version (to match existing convention exactly)
    option2_file = processed_dir / "C-02001_1y_0p1h.parquet"
    print(f"\nOption 2: Creating {option2_file.name} (1 year subset)")

    # Filter to last 1 year of data
    if 'timestamp' in df.columns:
        df_1y = df.copy()
        df_1y['timestamp'] = pd.to_datetime(df_1y['timestamp'])
        cutoff_date = df_1y['timestamp'].max() - pd.Timedelta(days=365)
        df_1y = df_1y[df_1y['timestamp'] >= cutoff_date]
        print(f"  Filtered to 1 year: {len(df_1y):,} rows")
    else:
        # If no timestamp, take last 1 year worth of rows (approximate)
        rows_per_year = int(len(df) * (365 / (365 * 1.5)))  # Approximate 1 year from 1.5 year dataset
        df_1y = df.tail(rows_per_year).copy()
        print(f"  Took last {rows_per_year:,} rows (approx 1 year)")

    df_1y.to_parquet(option2_file, engine='pyarrow', compression='snappy')
    option2_size = option2_file.stat().st_size
    print(f"  Created: {option2_size / 1024 / 1024:.1f} MB")

    # Option 3: Deduped versions (following the .dedup.parquet pattern)
    option3a_file = processed_dir / "C-02001_1p5y_0p1h.dedup.parquet"
    option3b_file = processed_dir / "C-02001_1y_0p1h.dedup.parquet"

    print(f"\nOption 3a: Creating deduped 1.5y version")
    # Remove duplicates based on timestamp (if available)
    if 'timestamp' in df.columns:
        df_dedup = df.drop_duplicates(subset=['timestamp']).copy()
        print(f"  Removed {len(df) - len(df_dedup):,} duplicate timestamps")
    else:
        df_dedup = df.drop_duplicates().copy()
        print(f"  Removed {len(df) - len(df_dedup):,} duplicate rows")

    df_dedup.to_parquet(option3a_file, engine='pyarrow', compression='snappy')
    option3a_size = option3a_file.stat().st_size
    print(f"  Created: {option3a_size / 1024 / 1024:.1f} MB")

    print(f"\nOption 3b: Creating deduped 1y version")
    if 'timestamp' in df_1y.columns:
        df_1y_dedup = df_1y.drop_duplicates(subset=['timestamp']).copy()
    else:
        df_1y_dedup = df_1y.drop_duplicates().copy()

    df_1y_dedup.to_parquet(option3b_file, engine='pyarrow', compression='snappy')
    option3b_size = option3b_file.stat().st_size
    print(f"  Created: {option3b_size / 1024 / 1024:.1f} MB")

    # Create backup of original with timestamp
    backup_file = processed_dir / f"PCMSB_C-02001_FINAL_COMPLETE_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    shutil.copy2(source_file, backup_file)
    print(f"\nBackup created: {backup_file.name}")

    # Summary
    print(f"\n" + "=" * 60)
    print("SYNC COMPLETED - FILES CREATED:")
    print("=" * 60)
    print(f"Option 1: {option1_file.name} ({option1_size / 1024 / 1024:.1f} MB) - 1.5y full dataset")
    print(f"Option 2: {option2_file.name} ({option2_size / 1024 / 1024:.1f} MB) - 1y standard")
    print(f"Option 3a: {option3a_file.name} ({option3a_size / 1024 / 1024:.1f} MB) - 1.5y deduped")
    print(f"Option 3b: {option3b_file.name} ({option3b_size / 1024 / 1024:.1f} MB) - 1y deduped")
    print(f"Backup: {backup_file.name}")

    print(f"\nNow C-02001 follows the same naming convention as:")
    print(f"  K-12-01_1y_0p1h.parquet")
    print(f"  K-16-01_1y_0p1h.parquet")
    print(f"  C-104_1y_0p1h.parquet")
    print(f"  etc.")

    return True

def verify_sync():
    """Verify the sync was successful."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print(f"\n" + "=" * 60)
    print("VERIFICATION - C-02001 FILES")
    print("=" * 60)

    # Check for C-02001 files
    c02001_files = list(processed_dir.glob("C-02001_*.parquet"))

    if not c02001_files:
        print("No C-02001 files found!")
        return False

    for file in sorted(c02001_files):
        size_mb = file.stat().st_size / 1024 / 1024
        print(f"✓ {file.name} ({size_mb:.1f} MB)")

        # Quick data check
        try:
            df = pd.read_parquet(file)
            pi_tags = [col for col in df.columns if 'PCM.C-02001' in str(col)]
            print(f"    Rows: {len(df):,}, PI Tags: {len(pi_tags)}, Columns: {len(df.columns)}")
        except Exception as e:
            print(f"    Error reading file: {e}")

    print(f"\n✓ C-02001 is now synced with your existing naming convention!")
    return True

def main():
    """Main entry point."""

    print("Syncing C-02001 parquet files with existing naming convention...")

    if sync_c02001_parquet_files():
        verify_sync()
        print(f"\nSUCCESS: C-02001 files are now properly named and synced!")
    else:
        print("FAILED: Could not sync C-02001 files")

if __name__ == "__main__":
    main()