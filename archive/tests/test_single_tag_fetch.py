"""Test fetching ONE tag with extended warmup to diagnose the issue"""
import os
import sys
from pathlib import Path

# Set extended timeouts
os.environ['PI_DATALINK_WARMUP'] = '15'  # 15 seconds warmup
os.environ['PI_FETCH_TIMEOUT'] = '120'   # 2 minutes for first tag
os.environ['PI_FETCH_LINGER'] = '30'     # 30s linger
os.environ['DEBUG_PI_FETCH'] = '1'       # Enable debug

sys.path.insert(0, str(Path.cwd()))

from pi_monitor.batch import build_unit_from_tags
from pathlib import Path

print("=" * 70)
print("SINGLE TAG FETCH TEST - EXTENDED WARMUP")
print("=" * 70)
print(f"Warmup: {os.environ['PI_DATALINK_WARMUP']}s")
print(f"Timeout: {os.environ['PI_FETCH_TIMEOUT']}s")
print(f"Linger: {os.environ['PI_FETCH_LINGER']}s")
print("=" * 70 + "\n")

# Test with ONE tag that you confirmed has data
test_tag = "PCFS.K-12-01.12PI-007.PV"

try:
    build_unit_from_tags(
        xlsx=Path("excel/PCFS/PCFS_Automation.xlsx"),
        tags=[test_tag],
        out_parquet=Path("tmp/test_single_tag.parquet"),
        plant="PCFS",
        unit="K-12-01-TEST",
        server="\\\\PTSG-1MMPDPdb01",
        start="-2h",  # Just 2 hours to make it faster
        end="*",
        step="-0.1h",
        visible=True,
        settle_seconds=10,
    )

    print("\n" + "=" * 70)
    print("SUCCESS! Checking result...")
    print("=" * 70)

    import pandas as pd
    df = pd.read_parquet("tmp/test_single_tag.parquet")
    print(f"Rows fetched: {len(df):,}")
    if len(df) > 0:
        print(f"Date range: {df['time'].min()} to {df['time'].max()}")
        print(f"\nFirst 5 rows:")
        print(df.head())
        print("\n[SUCCESS] Tag fetch worked!")
    else:
        print("\n[FAILED] No data returned")

except Exception as e:
    print(f"\n[ERROR] {e}")
    import traceback
    traceback.print_exc()
