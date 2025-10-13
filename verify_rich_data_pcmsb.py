#!/usr/bin/env python3
"""
Verify which PCMSB units have rich data vs fallback data
"""

import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def verify_pcmsb_rich_data():
    """Check which PCMSB units have actual rich data vs minimal fallback data"""

    print("=== PCMSB UNITS DATA VERIFICATION ===")

    # PCMSB units from the scan results
    pcmsb_units = [
        'C-02001',  # Shown as 1 record in scan
        'C-104',    # Rich data expected
        'C-13001',  # Rich data expected
        'C-1301',   # Rich data expected
        'C-1302',   # Rich data expected
        'C-201',    # Rich data expected
        'C-202',    # Rich data expected
    ]

    from pi_monitor.parquet_database import ParquetDatabase
    db = ParquetDatabase(PROJECT_ROOT / "data")

    print("Unit Status Analysis:")
    print("=" * 60)

    rich_data_units = []
    minimal_data_units = []

    for unit in pcmsb_units:
        print(f"\nUnit: {unit}")
        print("-" * 30)

        try:
            # Check file sizes
            dedup_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

            if dedup_file.exists():
                size_mb = dedup_file.stat().st_size / (1024 * 1024)
                print(f"File size: {size_mb:.1f} MB")

                # Load data
                df = db.get_unit_data(unit)
                records = len(df)

                if records > 1000:  # Rich data threshold
                    print(f"Records: {records:,} - RICH DATA")

                    if 'tag' in df.columns:
                        unique_tags = df['tag'].nunique()
                        print(f"Unique tags: {unique_tags}")

                        # Check for real vs fallback tags
                        fallback_tags = df[df['tag'].str.contains('FALLBACK', na=False)]
                        real_tags = len(df) - len(fallback_tags)
                        print(f"Real data: {real_tags:,} records")
                        print(f"Fallback data: {len(fallback_tags):,} records")

                    if 'time' in df.columns:
                        print(f"Time range: {df['time'].min()} to {df['time'].max()}")

                    rich_data_units.append(unit)

                else:
                    print(f"Records: {records:,} - MINIMAL/FALLBACK DATA")
                    minimal_data_units.append(unit)

            else:
                print("No dedup file found")
                minimal_data_units.append(unit)

        except Exception as e:
            print(f"Error: {e}")
            minimal_data_units.append(unit)

    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print(f"\nUnits with RICH DATA ({len(rich_data_units)}):")
    for unit in rich_data_units:
        print(f"  - {unit}: Ready for extended analysis with plot stale fetch")

    print(f"\nUnits with MINIMAL DATA ({len(minimal_data_units)}):")
    for unit in minimal_data_units:
        print(f"  - {unit}: Needs fresh data build or has connectivity issues")

    print(f"\nExtended Analysis Status:")
    print(f"- Rich data units: Can demonstrate full extended plotting")
    print(f"- Minimal data units: Extended analysis will work but show limited data")
    print(f"- All units: Have plant-specific PCMSB configuration (90s timeout)")

    # Test extended analysis on a rich data unit
    if rich_data_units:
        print(f"\n=== TESTING EXTENDED ANALYSIS ON RICH DATA UNIT ===")
        test_unit = rich_data_units[0]
        print(f"Testing: {test_unit}")

        from pi_monitor.parquet_auto_scan import ParquetAutoScanner
        from pi_monitor.config import Config

        scanner = ParquetAutoScanner(Config())

        # Get plant-specific handling
        handling = scanner._get_plant_specific_handling(test_unit)
        print(f"Plant type: {handling.get('plant_type')}")
        print(f"Timeout: {handling.get('timeout_settings', {}).get('PI_FETCH_TIMEOUT')}s")
        print(f"Working tags: {len(handling.get('working_tags_identified', []))}")

        print(f"\nThis unit is ready for:")
        print(f"- Option [2] enhanced analysis")
        print(f"- Extended staleness plotting")
        print(f"- Plot stale fetch (data without cutoffs)")
        print(f"- Instrumentation anomaly detection")

if __name__ == "__main__":
    verify_pcmsb_rich_data()