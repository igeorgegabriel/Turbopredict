#!/usr/bin/env python3
"""Quick test: Fetch 3 tags from ABF 21-K002 to verify connectivity"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags

def main():
    xlsx = PROJECT_ROOT / "excel" / "ABFSB" / "ABF LIMIT REVIEW (CURRENT).xlsx"
    tags = ["SI-21001-REF.PV", "TIAH-21023.PV", "TIAH-21022.PV"]
    out = PROJECT_ROOT / "data" / "processed" / "21-K002_test_3tags.parquet"

    print(f"[TEST] Fetching {len(tags)} tags from ABF 21-K002...")
    print(f"[TEST] Excel: {xlsx}")
    print(f"[TEST] Output: {out}")
    print(f"[TEST] Tags: {tags}")
    print("\n" + "="*60)

    result = build_unit_from_tags(
        xlsx,
        tags,
        out,
        plant="ABFSB",
        unit="21-K002",
        server=r"\\PTSG-1MMPDPdb01",  # Try default PCFS server first
        start="-7d",  # Only 7 days for quick test
        end="*",
        step="-1h",  # 1 hour intervals for speed
        work_sheet="DL_WORK",
        settle_seconds=2.0,
        visible=True
    )

    print(f"\n[SUCCESS] Created: {result}")
    print(f"[SUCCESS] File size: {result.stat().st_size / 1024:.1f} KB")
    return 0

if __name__ == "__main__":
    sys.exit(main())
