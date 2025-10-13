#!/usr/bin/env python3
"""Test tag counting fix for all plants"""
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from simple_incremental_refresh import count_tags_in_parquet, PROJECT_ROOT

# All units to test
UNITS = {
    "PCFS": ["K-12-01", "K-16-01", "K-19-01", "K-31-01"],
    "ABFSB": ["07-MT01-K001"],
    "PCMSB": ["C-02001", "C-104", "C-13001", "C-1301", "C-1302", "C-201", "C-202", "XT-07002"],
}

print("="*80)
print("TAG COUNT VERIFICATION - ALL PLANTS")
print("="*80)
print()

for plant, units in UNITS.items():
    print(f"{plant}:")
    for unit in units:
        parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

        if not parquet_file.exists():
            print(f"  {unit:20s} - FILE NOT FOUND")
            continue

        total_tags, active_tags = count_tags_in_parquet(parquet_file, unit=unit, plant=plant)

        if total_tags == 0:
            status = "[BROKEN]"
        elif active_tags == total_tags:
            status = "[OK]"
        else:
            status = "[PARTIAL]"

        print(f"  {unit:20s} {status:12s} {active_tags}/{total_tags} tags")
    print()
