#!/usr/bin/env python3
"""
Check dedup file status after option [1] refresh
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime

def check_dedup_files():
    """Check status of dedup files in processed directory"""

    print("CHECKING DEDUP FILES STATUS")
    print("=" * 35)

    processed_dir = Path("data/processed")

    if not processed_dir.exists():
        print("ERROR: Processed directory doesn't exist!")
        return

    # Get all parquet files
    all_parquet = list(processed_dir.glob("*.parquet"))
    dedup_files = [f for f in all_parquet if ".dedup." in f.name]
    raw_files = [f for f in all_parquet if ".dedup." not in f.name and f.name.endswith("_1y_0p1h.parquet")]

    print(f"Total parquet files: {len(all_parquet)}")
    print(f"Dedup files: {len(dedup_files)}")
    print(f"Raw data files: {len(raw_files)}")
    print()

    # Check for missing dedup files
    print("DEDUP FILE ANALYSIS:")
    print("-" * 25)

    missing_dedup = []
    existing_dedup = []

    for raw_file in raw_files:
        # Expected dedup filename
        expected_dedup = raw_file.with_name(raw_file.name.replace("_1y_0p1h.parquet", "_1y_0p1h.dedup.parquet"))

        if expected_dedup.exists():
            existing_dedup.append((raw_file, expected_dedup))
        else:
            missing_dedup.append((raw_file, expected_dedup))

    print(f"Files WITH dedup versions: {len(existing_dedup)}")
    for raw_file, dedup_file in existing_dedup:
        raw_stat = raw_file.stat()
        dedup_stat = dedup_file.stat()

        raw_size = raw_stat.st_size / (1024 * 1024)
        dedup_size = dedup_stat.st_size / (1024 * 1024)
        raw_time = datetime.fromtimestamp(raw_stat.st_mtime)
        dedup_time = datetime.fromtimestamp(dedup_stat.st_mtime)

        # Check if dedup is newer than raw (good)
        dedup_newer = dedup_time >= raw_time
        status = "OK" if dedup_newer else "STALE"

        print(f"  {raw_file.name:<35} -> {dedup_file.name:<40} {status}")
        print(f"    Raw:   {raw_size:>8.1f}MB {raw_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    Dedup: {dedup_size:>8.1f}MB {dedup_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

    print(f"Files MISSING dedup versions: {len(missing_dedup)}")
    for raw_file, expected_dedup in missing_dedup:
        raw_stat = raw_file.stat()
        raw_size = raw_stat.st_size / (1024 * 1024)
        raw_time = datetime.fromtimestamp(raw_stat.st_mtime)

        print(f"  {raw_file.name:<35} -> MISSING: {expected_dedup.name}")
        print(f"    Raw: {raw_size:>8.1f}MB {raw_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

    # WHY DEDUP FILES MIGHT BE MISSING
    print("WHY DEDUP FILES MIGHT BE MISSING:")
    print("-" * 35)

    if missing_dedup:
        print("Possible reasons:")
        print("1. Option [1] refresh failed during dedup step")
        print("2. DuckDB not available for dedup processing")
        print("3. Memory/disk space issues during dedup")
        print("4. Dedup step was skipped due to errors")
        print("5. Files were created but dedup step failed silently")
        print()

        print("SOLUTIONS:")
        print("1. Run manual dedup for missing files:")
        for raw_file, expected_dedup in missing_dedup:
            print(f"   python -c \"from pi_monitor.clean import dedup_parquet; dedup_parquet('{raw_file}')\"")
        print()

        print("2. Or run option [1] again to retry the full refresh")

    else:
        print("OK All raw parquet files have corresponding dedup versions")

    # Check PCMSB specifically
    pcmsb_raw = [f for f in raw_files if any(unit in f.name for unit in ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202'])]
    pcmsb_dedup = [f for f in dedup_files if any(unit in f.name for unit in ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202'])]

    print(f"\nPCMSB FILES STATUS:")
    print(f"PCMSB raw files: {len(pcmsb_raw)}")
    print(f"PCMSB dedup files: {len(pcmsb_dedup)}")

    if len(pcmsb_raw) > len(pcmsb_dedup):
        print("WARNING: PCMSB missing some dedup files")
    elif len(pcmsb_raw) == 0:
        print("WARNING: No PCMSB files found - run option [1] to generate them")
    else:
        print("OK PCMSB dedup files look complete")

if __name__ == "__main__":
    check_dedup_files()