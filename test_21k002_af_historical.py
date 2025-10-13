#!/usr/bin/env python3
"""Test with AF historical paths from column M PISampDat formulas"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags

def main():
    xlsx = PROJECT_ROOT / "excel" / "ABFSB" / "ABF LIMIT REVIEW (CURRENT).xlsx"
    out = PROJECT_ROOT / "data" / "processed" / "21-K002_test_af_hist.parquet"

    # Use AF paths from column M (historical data)
    base = r"\\VCENPOCOEPTNAP01\Protean\ABF\Compressor\21-K002"
    tags = [
        f"{base}\\C2 - ST-C&M\\TURB_N_REFF|OperatingValue",
        f"{base}\\C2 - Comp-Comp\\TURB_TBRG_T_B|OperatingValue",
        f"{base}\\C2 - Comp-Comp\\TURB_TBRG_T_A|OperatingValue",
    ]

    print(f"[TEST] Fetching 3 AF historical paths from 21-K002...")
    print(f"[TEST] These are from column M PISampDat formulas")
    print(f"[TEST] Tags:")
    for t in tags:
        print(f"  - {t}")
    print(f"\n{'='*70}\n")

    result = build_unit_from_tags(
        xlsx,
        tags,
        out,
        plant="ABFSB",
        unit="21-K002",
        server="",  # Empty for AF paths - they contain full server path
        start="-7d",  # Test with 7 days first
        end="*",
        step="-1h",
        work_sheet="DL_WORK",
        settle_seconds=2.0,
        visible=True
    )

    print(f"\n[SUCCESS] {result}")
    print(f"[SUCCESS] Size: {result.stat().st_size / 1024:.1f} KB")

    # Show data
    import pandas as pd
    df = pd.read_parquet(result)
    print(f"\n[DATA] Shape: {df.shape}")
    print(f"[DATA] Columns: {list(df.columns)[:10]}")
    print(f"\n[DATA] Sample:")
    print(df.head())
    return 0

if __name__ == "__main__":
    sys.exit(main())
