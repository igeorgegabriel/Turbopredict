"""Fix all flat parquet files in data/processed/"""
import pandas as pd
from pathlib import Path
from glob import glob

print('=' * 70)
print('FIXING ALL FLAT PARQUET FILES')
print('=' * 70)

# Get all main and dedup files
files_to_fix = []
for pattern in ['*_1y_0p1h.parquet', '*_1y_0p1h.dedup.parquet']:
    for f in glob(f'data/processed/{pattern}'):
        if 'OLD' not in f and 'FIXED' not in f and 'archive' not in f and 'refreshed' not in f:
            files_to_fix.append(f)

print(f'\nFound {len(files_to_fix)} files to fix\n')

for fpath in sorted(files_to_fix):
    fname = Path(fpath).name
    print(f'[{fname}]')

    try:
        # Read
        df = pd.read_parquet(fpath)
        print(f'  Loaded: {len(df):,} rows, Index: {type(df.index).__name__}')

        # Determine time column name
        if 'time' in df.columns:
            time_col = 'time'
        elif 'timestamp' in df.columns:
            time_col = 'timestamp'
        else:
            print(f'  SKIP: No time/timestamp column')
            continue

        # Fix index
        df = df.set_index(time_col)

        # Save to .FIXED
        fixed_path = fpath + '.FIXED'
        df.to_parquet(fixed_path)

        # Verify
        df_check = pd.read_parquet(fixed_path)
        print(f'  Fixed: {type(df_check.index).__name__}')
        print(f'  Range: {df_check.index.min()} to {df_check.index.max()}')
        print(f'  OK')

    except Exception as e:
        print(f'  ERROR: {e}')

print('\n' + '=' * 70)
print('READY TO SWAP - Run this to activate fixes:')
print('=' * 70)
print('cd data/processed')
print('for f in *.FIXED; do')
print('  orig="${f%.FIXED}"')
print('  mv "$orig" "$orig.OLD"')
print('  mv "$f" "$orig"')
print('done')
