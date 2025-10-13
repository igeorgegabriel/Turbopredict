"""Fix all units with RangeIndex issue"""
import pandas as pd
from pathlib import Path

broken_units = [
    ('PCFS', 'K-16-01'),
    ('PCFS', 'K-19-01'),
    ('PCFS', 'K-31-01'),
    ('PCMSB', 'C-02001'),
]

print('=' * 60)
print('BATCH FIX: Convert RangeIndex to DatetimeIndex')
print('=' * 60)

for plant, unit in broken_units:
    print(f'\n[{plant}/{unit}]')

    parquet_path = Path(f'data/processed/dataset/plant={plant}/unit={unit}')
    fixed_path = Path(f'data/processed/dataset/plant={plant}/unit={unit}_FIXED')

    if not parquet_path.exists():
        print(f'  SKIP: Path does not exist')
        continue

    try:
        # Read
        print(f'  Reading parquet...')
        df = pd.read_parquet(parquet_path)
        print(f'  Loaded {len(df):,} rows')
        print(f'  Current index: {type(df.index).__name__}')

        # Fix
        print(f'  Setting time as index...')
        df = df.set_index('time')

        # Save to FIXED location
        print(f'  Saving to: {fixed_path}')
        df.to_parquet(fixed_path)

        # Verify
        df_check = pd.read_parquet(fixed_path)
        print(f'  Verified: {type(df_check.index).__name__}')
        print(f'  Date range: {df_check.index.min()} to {df_check.index.max()}')
        print(f'  OK - Ready to swap')

    except Exception as e:
        print(f'  ERROR: {e}')

print('\n' + '=' * 60)
print('MANUAL SWAP REQUIRED:')
print('=' * 60)
print('Stop all Python processes, then run:')
print('')
for plant, unit in broken_units:
    print(f'  cd data/processed/dataset/plant={plant}')
    print(f'  mv "unit={unit}" "unit={unit}.OLD"')
    print(f'  mv "unit={unit}_FIXED" "unit={unit}"')
    print('')
