#!/usr/bin/env python3
"""Quick test with correct PI tags from Column D"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags

def main():
    xlsx = PROJECT_ROOT / "excel" / "ABFSB" / "ABF LIMIT REVIEW (CURRENT).xlsx"
    out = PROJECT_ROOT / "data" / "processed" / "21-K002_test_correct.parquet"

    # Use actual PI tags from Column D
    tags = [
        "SI-21001-REF.PV",
        "TIAH-21023.PV",
        "TIAH-21022.PV",
    ]

    print(f"[TEST] Fetching {len(tags)} PI tags from 21-K002...")
    print(f"[TEST] Tags: {tags}")
    print(f"\n{'='*70}\n")

    result = build_unit_from_tags(
        xlsx,
        tags,
        out,
        plant="ABFSB",
        unit="21-K002",
        server=r"\\PTSG-1MMPDPdb01",  # PCFS/PCMSB server
        start="-7d",
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
    return 0

if __name__ == "__main__":
    sys.exit(main())
