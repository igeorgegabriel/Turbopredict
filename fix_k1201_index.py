"""Fix K-12-01 parquet index issue"""
import pandas as pd
import shutil
from pathlib import Path

parquet_path = Path('data/processed/dataset/plant=PCFS/unit=K-12-01')
fixed_path = Path('data/processed/dataset/plant=PCFS/unit=K-12-01_FIXED')

print('Reading K-12-01 parquet...')
df = pd.read_parquet(parquet_path)
print(f'Loaded {len(df):,} rows')
print(f'Current index type: {type(df.index)}')
print(f'Current date range: {df["time"].min()} to {df["time"].max()}')

# Fix index
print('\nSetting time as index...')
df = df.set_index('time')

# Save to new location (won't conflict with locked file)
print(f'Saving fixed parquet to: {fixed_path}')
df.to_parquet(fixed_path)

# Verify
print('\nVerifying fix...')
df_check = pd.read_parquet(fixed_path)
print(f'New index type: {type(df_check.index)}')
print(f'New date range: {df_check.index.min()} to {df_check.index.max()}')
print(f'\n✓ Fix complete!')
print(f'\nMANUAL STEPS:')
print(f'1. Stop any running Python processes')
print(f'2. Rename: unit=K-12-01 -> unit=K-12-01.OLD')
print(f'3. Rename: unit=K-12-01_FIXED -> unit=K-12-01')
print(f'4. Delete unit=K-12-01.OLD when confirmed working')
