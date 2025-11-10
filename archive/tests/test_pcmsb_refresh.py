#!/usr/bin/env python3
"""
Test PCMSB refresh after Excel automation fix

This script specifically tests if the fixes for Windows compatibility (`SIGALRM`)
and the incorrect PCMSB Excel path have resolved the refresh errors.
"""

import sys
from pathlib import Path

# Add project root to path to find other modules
sys.path.insert(0, str(Path(__file__).parent))

try:
    from pi_monitor.parquet_auto_scan import ParquetAutoScanner
    from pi_monitor.parquet_database import ParquetDatabase
    from datetime import datetime
    PI_MONITOR_AVAILABLE = True
except ImportError as e:
    print(f"Failed to import modules: {e}")
    PI_MONITOR_AVAILABLE = False

def test_pcmsb_refresh():
    """Test if PCMSB units can now refresh properly."""

    print("TESTING PCMSB REFRESH AFTER FIXES")
    print("=" * 50)

    if not PI_MONITOR_AVAILABLE:
        print("ERROR: pi_monitor package not found. Cannot run test.")
        return

    db = ParquetDatabase()
    # Focus on the units that were failing
    test_units = [u for u in db.get_all_units() if u.startswith('C-')]
    if not test_units:
        print("No PCMSB units found in the database to test.")
        return

    print(f"Testing with PCMSB units: {', '.join(test_units)}")

    # Run auto-refresh scan on the stale units
    print("\nRunning auto-refresh scan on stale units...")
    try:
        scanner = ParquetAutoScanner()
        # Directly call the method that was failing
        results = scanner.refresh_stale_units_with_progress(max_age_hours=0.1) # Force refresh

        print("\n--- SCAN RESULTS ---")
        print(f"Scan successful: {results.get('success', False)}")
        print(f"Units processed: {results.get('units_processed', [])}")
        print(f"Successful units: {results.get('successful_units', 0)}")
        print(f"Failed units: {results.get('failed_units', 0)}")
        print("--------------------\n")

        # Verify final status
        failed_count = results.get('failed_units', 0)

        if failed_count == 0:
            print("✅ SUCCESS: The fix is working. All units processed without critical errors.")
        else:
            print(f"❌ FAILURE: {failed_count} units still failed to process.")
            print("   Please review the error messages during the refresh process.")

    except Exception as e:
        print(f"\n❌ CRITICAL FAILURE: The test script crashed.")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pcmsb_refresh()