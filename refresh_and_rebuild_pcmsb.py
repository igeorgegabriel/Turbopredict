#!/usr/bin/env python3
"""
COMPLETE REFRESH SOLUTION: Excel refresh + Parquet rebuild for PCMSB units.
This solves the "Excel fresh but Parquet stale" issue.
"""

import time
import subprocess
from pathlib import Path
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination

def refresh_and_rebuild_pcmsb():
    """Complete refresh process: Excel refresh + Parquet rebuild for PCMSB units."""

    print("="*80)
    print("COMPLETE PCMSB REFRESH: Excel + Parquet Rebuild")
    print("Solving: Excel fresh but Parquet stale issue")
    print("="*80)

    # Step 1: Refresh all PCMSB Excel files
    pcmsb_excel_files = [
        Path("excel/PCMSB/PCMSB_Automation.xlsx"),
        Path("excel/PCMSB/9EFCCD10.xlsx"),
    ]

    print("Step 1: Refreshing PCMSB Excel files...")
    print("-" * 40)

    for i, excel_file in enumerate(pcmsb_excel_files, 1):
        if not excel_file.exists():
            print(f"[SKIP] {excel_file.name} not found")
            continue

        print(f"[{i}/{len(pcmsb_excel_files)}] Refreshing {excel_file.name}...")

        try:
            start_time = time.time()
            refresh_excel_with_pi_coordination(
                xlsx=excel_file,
                settle_seconds=3,
                use_working_copy=True,
                auto_cleanup=True
            )
            elapsed = time.time() - start_time
            print(f"[SUCCESS] Excel refresh completed in {elapsed:.1f}s")

        except Exception as e:
            print(f"[ERROR] Excel refresh failed: {e}")
            return False

    print()
    print("Step 2: Rebuilding Parquet files from refreshed Excel data...")
    print("-" * 60)

    # Step 2: Rebuild Parquet files for PCMSB units
    pcmsb_rebuild_scripts = [
        "scripts/build_pcmsb_c02001.py",
        "scripts/build_pcmsb_c104.py",
        "scripts/build_pcmsb_c13001.py",
        "scripts/build_pcmsb_c1301.py",
        "scripts/build_pcmsb_c1302.py",
        "scripts/build_pcmsb_c201.py",
        "scripts/build_pcmsb_c202.py",
        "scripts/build_pcmsb_xt07002.py",  # XT-07002 included in PCMSB
    ]

    successful_rebuilds = 0

    for i, script_path in enumerate(pcmsb_rebuild_scripts, 1):
        script = Path(script_path)
        unit_name = script.stem.replace("build_pcmsb_", "").upper().replace("_", "-")

        if not script.exists():
            print(f"[SKIP] {script.name} not found")
            continue

        print(f"[{i}/{len(pcmsb_rebuild_scripts)}] Rebuilding {unit_name} Parquet...")

        try:
            # Run the rebuild script
            result = subprocess.run(
                ["python", str(script)],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout per unit
                cwd=Path.cwd()
            )

            if result.returncode == 0:
                print(f"[SUCCESS] {unit_name} Parquet rebuilt")
                successful_rebuilds += 1
            else:
                print(f"[ERROR] {unit_name} rebuild failed:")
                print(f"  Error: {result.stderr[:100]}...")

        except subprocess.TimeoutExpired:
            print(f"[TIMEOUT] {unit_name} rebuild timed out after 5 minutes")
        except Exception as e:
            print(f"[ERROR] {unit_name} rebuild exception: {e}")

    # Summary
    print()
    print("="*80)
    print("COMPLETE REFRESH SUMMARY")
    print("="*80)

    print(f"Excel files refreshed: {len([f for f in pcmsb_excel_files if f.exists()])}")
    print(f"Parquet rebuilds successful: {successful_rebuilds}/{len(pcmsb_rebuild_scripts)}")

    if successful_rebuilds >= len(pcmsb_rebuild_scripts) * 0.8:  # 80% success rate
        print()
        print("*** SUCCESS: PCMSB units should now have fresh data! ***")
        print("*** Both Excel and Parquet data updated ***")
        return True
    else:
        print()
        print("*** PARTIAL SUCCESS: Some Parquet rebuilds failed ***")
        print("*** Check logs above for specific unit issues ***")
        return False

if __name__ == "__main__":
    success = refresh_and_rebuild_pcmsb()
    exit(0 if success else 1)