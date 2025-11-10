#!/usr/bin/env python3
"""
Test anomaly marks and tag state visualization
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.smart_anomaly_detection import smart_anomaly_detection
import pandas as pd
from datetime import datetime, timedelta

def test_anomaly_marks():
    """Test if anomaly detection returns proper details for marking"""

    print("TESTING ANOMALY MARKS AND TAG STATES")
    print("=" * 50)

    # Load some data
    db = ParquetDatabase()

    # Test with K-31-01 (should have fresh data)
    unit = "K-31-01"
    df = db.get_unit_data(unit)

    if df.empty:
        print(f"No data for {unit}")
        return

    # Filter to recent data (last 7 days)
    cutoff = datetime.now() - timedelta(days=7)
    df = df[pd.to_datetime(df['time']) >= cutoff].copy()

    print(f"Unit: {unit}")
    print(f"Recent records: {len(df):,}")
    print(f"Unique tags: {df['tag'].nunique()}")

    # Run anomaly detection
    print("\nRunning smart anomaly detection...")
    results = smart_anomaly_detection(df, unit)

    print(f"Unit Status: {results.get('unit_status', {}).get('status', 'UNKNOWN')}")
    print(f"Total Anomalies: {results.get('total_anomalies', 0):,}")
    print(f"Method: {results.get('method', 'Unknown')}")

    # Check details structure
    details = results.get('details', {})
    print(f"\nDetails available: {bool(details)}")

    if details:
        sigma_by_tag = details.get('sigma_2p5_times_by_tag', {})
        verify_by_tag = details.get('verification_times_by_tag', {})
        ae_times = details.get('ae_times', [])

        print(f"2.5-sigma tags: {len(sigma_by_tag)}")
        print(f"Verification tags: {len(verify_by_tag)}")
        print(f"AutoEncoder times: {len(ae_times)}")

        # Show details for first few tags
        if sigma_by_tag:
            print("\nFirst few 2.5-sigma tags:")
            for i, (tag, times) in enumerate(list(sigma_by_tag.items())[:3]):
                print(f"  {tag}: {len(times)} anomaly times")

        if verify_by_tag:
            print("\nFirst few verification tags:")
            for i, (tag, verif) in enumerate(list(verify_by_tag.items())[:3]):
                mtd_count = len(verif.get('mtd', []))
                if_count = len(verif.get('if', []))
                print(f"  {tag}: MTD={mtd_count}, IF={if_count}")
    else:
        print("No details found - this explains why marks don't show!")

    # Check by_tag structure
    by_tag = results.get('by_tag', {})
    print(f"\nProblematic tags: {len(by_tag)}")

    if by_tag:
        print("Top 3 problematic tags:")
        sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
        for tag, info in sorted_tags[:3]:
            count = info.get('count', 0)
            method = info.get('method', 'Unknown')
            print(f"  {tag}: {count} anomalies ({method})")

if __name__ == "__main__":
    test_anomaly_marks()