#!/usr/bin/env python3
"""
Test the fixed primary detection counting
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def test_primary_detection_fix():
    """Test the fixed primary detection counting"""

    print("TESTING PRIMARY DETECTION FIX")
    print("=" * 40)

    # Load data
    db = ParquetDatabase()
    unit = "K-31-01"
    df = db.get_unit_data(unit)

    if df.empty:
        print("No data available")
        return

    # Filter to recent data
    cutoff = datetime.now() - timedelta(days=7)
    df = df[pd.to_datetime(df['time']) >= cutoff].copy()

    print(f"Unit: {unit}")
    print(f"Recent records: {len(df):,}")
    print(f"Unique tags: {df['tag'].nunique()}")

    # Run smart anomaly detection
    results = smart_anomaly_detection(df, unit)

    # Extract details directly (what should be passed to plotting)
    details = results.get('details', {})

    print(f"\nDETAILS STRUCTURE:")
    print(f"Details available: {bool(details)}")

    if details:
        sigma_by_tag = details.get('sigma_2p5_times_by_tag', {})
        verify_by_tag = details.get('verification_times_by_tag', {})
        ae_times = details.get('ae_times', [])

        print(f"2.5-sigma tags: {list(sigma_by_tag.keys())}")
        print(f"Verification tags: {list(verify_by_tag.keys())}")
        print(f"AE times: {len(ae_times)}")

        # Test the new counting logic for each tag
        for tag in sigma_by_tag.keys():
            print(f"\n--- TAG: {tag} ---")

            # 2.5-sigma count
            sigma_count = len(sigma_by_tag.get(tag, []))
            print(f"2.5-Sigma count: {sigma_count}")

            # AE count (using new logic)
            unique_tags_with_sigma = len(sigma_by_tag)
            if unique_tags_with_sigma == 1:
                ae_count = len(ae_times)
                print(f"AutoEncoder count (single tag): {ae_count}")
            else:
                total_anomalies = sum(len(times) for times in sigma_by_tag.values()) or 1
                tag_proportion = sigma_count / total_anomalies
                ae_count = int(len(ae_times) * tag_proportion)
                print(f"AutoEncoder count (proportional): {ae_count}")

            # Verification counts
            vdetail = verify_by_tag.get(tag, {})
            mtd_count = len(vdetail.get('mtd', []))
            if_count = len(vdetail.get('if', []))
            print(f"MTD Verified: {mtd_count}")
            print(f"Isolation Forest: {if_count}")

            print(f"\nSUMMARY FOR {tag}:")
            print(f"Primary Detection: 2.5σ={sigma_count}, AE={ae_count}")
            print(f"Verification: MTD={mtd_count}, IF={if_count}")

            # This should match what will be displayed in the plot
            expected_display = f"""Primary Detection:
├─ 2.5-Sigma: {sigma_count}
└─ AutoEncoder: {ae_count}

Verification:
├─ MTD Verified: {mtd_count}
└─ Isolation Forest: {if_count}"""

            print(f"\nEXPECTED PLOT DISPLAY:")
            print(expected_display)

if __name__ == "__main__":
    test_primary_detection_fix()