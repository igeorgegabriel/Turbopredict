#!/usr/bin/env python3
"""
Test option [1] after fixing PCMSB Excel headers
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from datetime import datetime

def test_option1_after_fix():
    """Test option [1] AUTO-REFRESH SCAN after fixing PCMSB headers"""

    print("TESTING OPTION [1] AFTER PCMSB HEADER FIX")
    print("=" * 60)

    try:
        # Test the auto-scan system
        scanner = ParquetAutoScanner()

        print("Running auto-refresh scan (option [1])...")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Run with a short max age to trigger PCMSB refresh
        results = scanner.refresh_stale_units_with_progress(max_age_hours=1.0)

        print(f"Scan completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nResults: {results}")

        # Check if PCMSB units were processed successfully
        if 'units_processed' in results:
            processed_units = results['units_processed']
            pcmsb_units = [unit for unit in processed_units if unit.startswith('C-')]

            if pcmsb_units:
                print(f"\nSUCCESS: PCMSB units processed: {pcmsb_units}")
            else:
                print(f"\nNo PCMSB units in processed list: {processed_units}")

        if 'errors' in results and results['errors']:
            print(f"\nErrors encountered: {results['errors']}")
        else:
            print("\nNo errors reported")

    except Exception as e:
        print(f"ERROR during option [1] test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_option1_after_fix()