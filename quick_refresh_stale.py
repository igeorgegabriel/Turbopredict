#!/usr/bin/env python3
"""
Quick refresh for stale units, skipping problematic ones
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pathlib import Path
import sys

def main():
    print("QUICK REFRESH - STALE UNITS ONLY")
    print("=" * 50)

    # Initialize scanner
    scanner = ParquetAutoScanner(data_dir=Path('data'))

    # Get scan results to identify stale units
    scan_results = scanner.scan_all_units(max_age_hours=1.0)
    stale_units = scan_results['stale_units']

    print(f"Found {len(stale_units)} stale units: {stale_units}")

    # Skip problematic units that timeout
    skip_units = ['07-MT01-K001']  # Skip this one - PI DataLink timeout issue
    refresh_units = [unit for unit in stale_units if unit not in skip_units]

    if not refresh_units:
        print("No units need refresh (excluding skipped ones)")
        return

    print(f"Refreshing {len(refresh_units)} units: {refresh_units}")
    print(f"Skipping: {skip_units}")
    print()

    # Refresh with progress tracking
    try:
        results = scanner.refresh_stale_units_with_progress(max_age_hours=1.0)

        print("\nREFRESH SUMMARY:")
        print(f"Total time: {results.get('total_time', 0)/60:.1f} minutes")
        print(f"Successful: {results.get('successful_units', 0)}")
        print(f"Failed: {results.get('failed_units', 0)}")

        if 'fresh_after_refresh' in results:
            print(f"Now fresh: {results['fresh_after_refresh']}")

    except Exception as e:
        print(f"Refresh failed: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())