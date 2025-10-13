"""Comprehensive check for all unit parquet files (dataset folders + flat files)"""
import pandas as pd
from pathlib import Path
from glob import glob

def check_index(file_path):
    """Check if parquet file has DatetimeIndex"""
    try:
        df = pd.read_parquet(file_path)
        idx_type = type(df.index).__name__
        if idx_type == 'RangeIndex':
            return 'BROKEN'
        elif idx_type == 'DatetimeIndex':
            return 'OK'
        else:
            return f'UNKNOWN ({idx_type})'
    except Exception as e:
        return f'ERROR: {str(e)[:30]}'

print('=' * 70)
print('COMPREHENSIVE PARQUET INDEX HEALTH CHECK')
print('=' * 70)

# Check dataset/ folder structure
print('\n[DATASET FOLDER STRUCTURE]')
for plant in ['PCFS', 'ABF', 'PCMSB', 'ABFSB']:
    plant_path = Path(f'data/processed/dataset/plant={plant}')
    if plant_path.exists():
        units = sorted([d.name.replace('unit=', '') for d in plant_path.iterdir()
                       if d.is_dir() and d.name.startswith('unit=') and 'OLD' not in d.name and 'backup' not in d.name])
        if units:
            print(f'\n  {plant}:')
            for unit in units:
                status = check_index(f'data/processed/dataset/plant={plant}/unit={unit}')
                symbol = 'X' if status == 'BROKEN' else ('OK' if status == 'OK' else '?')
                print(f'    [{symbol}] {unit:15s} : {status}')

# Check flat file structure in data/processed/
print('\n[FLAT FILE STRUCTURE - data/processed/]')

# Group by unit name
unit_files = {}
for f in glob('data/processed/*_1y_0p1h.parquet'):
    if 'OLD' not in f and 'FIXED' not in f and 'archive' not in f and 'refreshed' not in f and 'bak' not in f:
        fname = Path(f).name
        unit = fname.replace('_1y_0p1h.parquet', '')
        if unit not in unit_files:
            unit_files[unit] = []
        unit_files[unit].append(f)

for f in glob('data/processed/*_1y_0p1h.dedup.parquet'):
    if 'OLD' not in f and 'FIXED' not in f and 'archive' not in f and 'refreshed' not in f and 'bak' not in f:
        fname = Path(f).name
        unit = fname.replace('_1y_0p1h.dedup.parquet', '')
        if unit not in unit_files:
            unit_files[unit] = []
        unit_files[unit].append(f)

# Check each unit
for unit in sorted(unit_files.keys()):
    print(f'\n  {unit}:')
    for fpath in sorted(unit_files[unit]):
        fname = Path(fpath).name
        status = check_index(fpath)
        symbol = 'X' if status == 'BROKEN' else ('OK' if status == 'OK' else '?')
        ftype = 'dedup' if 'dedup' in fname else 'main'
        print(f'    [{symbol}] {ftype:10s} : {status}')

print('\n' + '=' * 70)
print('Legend: [OK] = DatetimeIndex, [X] = RangeIndex (needs fix)')
print('=' * 70)
