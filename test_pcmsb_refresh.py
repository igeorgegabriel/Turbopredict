#!/usr/bin/env python3
"""
Test PCMSB refresh after Excel automation fix
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.parquet_database import ParquetDatabase
from datetime import datetime

def test_pcmsb_refresh():
    """Test if PCMSB units can now refresh properly"""

    print("TESTING PCMSB REFRESH AFTER EXCEL FIX")
    print("=" * 50)

    # Check initial status
    db = ParquetDatabase()
    test_unit = "C-104"

    print(f"Testing with unit: {test_unit}")

    # Get initial freshness
    try:
        initial_info = db.get_data_freshness_info(test_unit)
        initial_age = initial_info.get('data_age_hours', 0)
        initial_time = initial_info.get('latest_timestamp')
        print(f"Initial data age: {initial_age:.1f} hours")
        print(f"Initial latest time: {initial_time}")
    except Exception as e:
        print(f"Error getting initial info: {e}")
        return

    # Run auto-refresh scan
    print("\nRunning auto-refresh scan...")
    try:
        scanner = ParquetAutoScanner()
        results = scanner.refresh_stale_units_with_progress(max_age_hours=1.0)

        print(f"Scan results: {results}")

        # Check final status
        final_info = db.get_data_freshness_info(test_unit)
        final_age = final_info.get('data_age_hours', 0)
        final_time = final_info.get('latest_timestamp')

        print(f"\nFinal data age: {final_age:.1f} hours")
        print(f"Final latest time: {final_time}")

        # Compare
        improvement = initial_age - final_age
        if improvement > 0.5:  # At least 30 minutes improvement
            print(f"SUCCESS: Data refreshed! Improvement: {improvement:.1f} hours")
        elif final_age < 1.0:
            print("SUCCESS: Data is now fresh (< 1 hour)")
        else:
            print("ISSUE: Data still stale after refresh attempt")

    except Exception as e:
        print(f"Error during refresh: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pcmsb_refresh()