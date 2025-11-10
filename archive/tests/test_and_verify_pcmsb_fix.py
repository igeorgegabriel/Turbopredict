#!/usr/bin/env python3
"""
Test and Verify the PCMSB Multi-Unit Fix

This script automates the repair and verification process to ensure all
PCMSB units are marked as FRESH.

It performs the following steps:
1. Runs `populate_pcmsb_excel_sheets.py` to fix the Excel data source.
2. Runs the `ParquetAutoScanner` with a forced refresh for the PCMSB plant.
3. Verifies that all PCMSB units are now reported as FRESH.
"""

import sys
import subprocess
from pathlib import Path

# Add project root to path to find other modules
sys.path.insert(0, str(Path(__file__).parent))

try:
    from pi_monitor.parquet_database import ParquetDatabase
    PI_MONITOR_AVAILABLE = True
except ImportError:
    PI_MONITOR_AVAILABLE = False

def run_command(command: list[str], description: str):
    """Runs a command and prints its output."""
    print(f"\n--- {description} ---")
    try:
        # Use sys.executable to ensure the command runs with the same Python interpreter
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8' # Ensure consistent encoding
        )
        print(process.stdout)
        if process.stderr:
            print("--- STDERR ---")
            print(process.stderr)
        print(f"--- SUCCESS: {description} completed. ---\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"--- ERROR: {description} failed! ---")
        print(f"Return Code: {e.returncode}")
        print("--- STDOUT ---")
        print(e.stdout)
        print("--- STDERR ---")
        print(e.stderr)
        return False
    except Exception as e:
        print(f"--- UNEXPECTED ERROR: {e} ---")
        return False

def verify_freshness():
    """Checks the freshness of all PCMSB units."""
    print("\n--- STEP 3: VERIFYING DATA FRESHNESS ---")
    if not PI_MONITOR_AVAILABLE:
        print("ERROR: pi_monitor package not found. Cannot verify.")
        return False
    
    db = ParquetDatabase()
    all_units = db.get_all_units()
    pcmsb_units = [u for u in all_units if u.startswith('C-')]
    stale_units = []

    print(f"{'Unit':<12} {'Status':<8} {'Age(h)':<8}")
    print("-" * 30)
    for unit in pcmsb_units:
        try:
            info = db.get_data_freshness_info(unit)
            age_hours = info.get('data_age_hours', 999)
            if age_hours > 2.0: # Using a 2-hour threshold for freshness
                stale_units.append(unit)
                status = "STALE"
            else:
                status = "FRESH"
            print(f"{unit:<12} {status:<8} {age_hours:<8.1f}")
        except Exception as e:
            print(f"{unit:<12} ERROR: {e}")
            stale_units.append(unit)
    
    if not stale_units:
        print("\n✅ SUCCESS: All PCMSB units are now FRESH.")
        return True
    else:
        print(f"\n❌ FAILURE: {len(stale_units)} PCMSB units are still STALE: {stale_units}")
        return False

def test_and_verify_fix():
    """Main function to apply and test the fix."""
    print("=" * 70)
    print("  APPLYING AND VERIFYING PCMSB DATA FRESHNESS FIX")
    print("=" * 70)

    # The command to run the population script
    populate_command = [sys.executable, "populate_pcmsb_excel_sheets.py"]
    # Step 1: Populate Excel sheets with data from Parquet files
    if not run_command(populate_command, "STEP 1: POPULATING EXCEL SHEETS"):
        print("\nAborting fix process due to failure in Step 1.")
        return

    # The command to force the auto-scanner
    refresh_command = [sys.executable, "-m", "pi_monitor.cli", "auto-scan", "--plant", "PCMSB", "--force-refresh"]
    # Step 2: Force the auto-scanner to re-process the PCMSB plant
    if not run_command(refresh_command, "STEP 2: FORCING PARQUET REFRESH"):
        print("\nAborting fix process due to failure in Step 2.")
        return
    
    # Step 3: Verify the final freshness status
    verify_freshness()

if __name__ == "__main__":
    test_and_verify_fix()