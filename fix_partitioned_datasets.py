"""Fix partitioned dataset folders - convert RangeIndex to DatetimeIndex in-place"""
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq

def fix_partitioned_unit(plant, unit):
    """Fix a partitioned unit folder by rewriting with time index"""
    base_path = Path(f'data/processed/dataset/plant={plant}/unit={unit}')

    if not base_path.exists():
        return f"SKIP: Path not found"

    if not base_path.is_dir():
        return f"SKIP: Not a directory"

    try:
        print(f'  Reading partitioned data...')
        # Read the entire partitioned dataset
        df = pd.read_parquet(base_path)

        print(f'  Loaded {len(df):,} rows')
        print(f'  Current index: {type(df.index).__name__}')

        if type(df.index).__name__ == 'DatetimeIndex':
            return "OK - Already has DatetimeIndex"

        # Determine time column
        if 'time' in df.columns:
            time_col = 'time'
        elif 'timestamp' in df.columns:
            time_col = 'timestamp'
        else:
            return f"ERROR: No time/timestamp column found"

        # Fix index
        print(f'  Setting {time_col} as index...')
        df = df.set_index(time_col)

        # Backup old directory
        backup_path = Path(f'data/processed/dataset/plant={plant}/unit={unit}.BACKUP_RANGEINDEX')
        if base_path.exists() and not backup_path.exists():
            print(f'  Creating backup...')
            import shutil
            shutil.move(str(base_path), str(backup_path))

        # Write back with same partitioning (by tag, year, month if present)
        print(f'  Writing fixed partitioned data...')
        partition_cols = []
        if 'tag' in df.columns:
            partition_cols.append('tag')
        if 'year' in df.columns:
            partition_cols.append('year')
        if 'month' in df.columns:
            partition_cols.append('month')

        if partition_cols:
            df.to_parquet(base_path, partition_cols=partition_cols)
        else:
            df.to_parquet(base_path)

        # Verify
        df_check = pd.read_parquet(base_path)
        print(f'  Verified: {type(df_check.index).__name__}')
        print(f'  Date range: {df_check.index.min()} to {df_check.index.max()}')

        return "FIXED"

    except Exception as e:
        return f"ERROR: {str(e)[:60]}"

print('=' * 70)
print('FIX PARTITIONED DATASET FOLDERS')
print('=' * 70)

units_to_fix = [
    ('PCFS', 'K-12-01'),
    ('PCFS', 'K-16-01'),
    ('PCFS', 'K-19-01'),
    ('PCFS', 'K-31-01'),
    ('PCMSB', 'C-02001'),
]

for plant, unit in units_to_fix:
    print(f'\n[{plant}/{unit}]')
    result = fix_partitioned_unit(plant, unit)
    print(f'  Result: {result}')

print('\n' + '=' * 70)
print('COMPLETE - All partitioned datasets fixed')
print('=' * 70)
