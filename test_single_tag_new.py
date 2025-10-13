"""Test single tag fetch with new dual-formula + copy-down strategy"""
from pi_monitor.batch import build_unit_from_tags
from pathlib import Path
import os

# Set environment
os.environ['PI_FETCH_TIMEOUT'] = '60'
os.environ['PI_FETCH_LINGER'] = '10'
os.environ['DEBUG_PI_FORMULA'] = '1'

excel_path = Path('excel/PCFS/PCFS_Automation.xlsx')
tag = 'PCFS.K-12-01.12PI-007.PV'
output = Path('test_single_tag.parquet')

print("=" * 70)
print(f"Testing single tag: {tag}")
print("=" * 70)

success = build_unit_from_tags(
    xlsx=excel_path,
    tags=[tag],
    out_parquet=output,
    plant='PCFS',
    unit='K-12-01',
    start='-2h',
    end='*',
    step='-0.1h',
    server='\\\\PTSG-1MMPDPdb01',
    visible=True
)

if success and output.exists():
    import pandas as pd
    df = pd.read_parquet(output)
    print(f"\n✓ SUCCESS: Fetched {len(df)} rows")
    print(f"Date range: {df.index.min()} to {df.index.max()}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))
    print(f"\nLast 3 rows:")
    print(df.tail(3))
    output.unlink()  # Clean up
else:
    print(f"\n✗ FAILED: No data returned")
