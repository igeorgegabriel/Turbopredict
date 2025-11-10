#!/usr/bin/env python3
"""
Debug script to investigate why verification layer produces 0 alerts
"""

import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def debug_verification_layer():
    """Debug why verification layer is producing 0 alerts"""

    print("=== DEBUGGING VERIFICATION LAYER ===")
    print("Investigating why 0 verified anomalies from 22K+ detections")

    # Test with one unit
    db = ParquetDatabase()
    units = db.get_all_units()
    test_unit = "K-31-01" if "K-31-01" in units else units[0]

    print(f"Testing with unit: {test_unit}")

    # Load data
    df = db.get_unit_data(test_unit)
    cutoff = datetime.now() - timedelta(days=30)
    df['time'] = pd.to_datetime(df['time'])
    recent_data = df[df['time'] >= cutoff].copy()

    print(f"Data: {len(recent_data):,} records, {recent_data['tag'].nunique()} tags")

    # Run detection with detailed analysis
    try:
        results = smart_anomaly_detection(recent_data, test_unit, auto_plot_anomalies=False)

        unit_status = results.get('unit_status', {})
        total_anomalies = results.get('total_anomalies', 0)
        by_tag = results.get('by_tag', {})
        method = results.get('method', 'unknown')

        print(f"\n--- DETECTION RESULTS ---")
        print(f"Method: {method}")
        print(f"Unit status: {unit_status.get('status', 'UNKNOWN')}")
        print(f"Total anomalies: {total_anomalies:,}")
        print(f"Problematic tags: {len(by_tag)}")

        # Analyze first few problematic tags in detail
        print(f"\n--- DETAILED TAG ANALYSIS ---")

        sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)

        for i, (tag, tag_info) in enumerate(sorted_tags[:5]):  # Top 5 problematic tags
            print(f"\n[{i+1}] Tag: {tag}")
            print(f"  Total count: {tag_info.get('count', 0)}")
            print(f"  Rate: {tag_info.get('rate', 0)*100:.2f}%")
            print(f"  Method: {tag_info.get('method', 'unknown')}")
            print(f"  Confidence: {tag_info.get('confidence', 'unknown')}")

            # Check individual detector counts
            sigma_count = tag_info.get('sigma_2_5_count', 0)
            ae_count = tag_info.get('autoencoder_count', 0)
            mtd_count = tag_info.get('mtd_count', 0)
            iso_count = tag_info.get('isolation_forest_count', 0)

            print(f"  Primary detectors:")
            print(f"    2.5-Sigma: {sigma_count}")
            print(f"    Autoencoder: {ae_count}")
            print(f"  Verification detectors:")
            print(f"    MTD: {mtd_count}")
            print(f"    Isolation Forest: {iso_count}")

            # Check verification logic
            primary_detected = sigma_count > 0 or ae_count > 0
            verification_detected = mtd_count > 0 or iso_count > 0
            high_confidence = tag_info.get('confidence', 'LOW') in ['HIGH', 'MEDIUM']

            print(f"  Pipeline status:")
            print(f"    Primary detected: {primary_detected}")
            print(f"    Verification detected: {verification_detected}")
            print(f"    High confidence: {high_confidence}")
            print(f"    Would verify: {primary_detected and verification_detected and high_confidence}")

            # Check if the detection method supports verification
            if 'details' in tag_info:
                details = tag_info['details']
                print(f"  Detection details available: {list(details.keys())}")

        # Check if hybrid detection is working
        print(f"\n--- DETECTION PIPELINE DIAGNOSIS ---")

        if method == 'smart_enhanced':
            print("✓ Smart enhanced detection active")
        elif method in ('hybrid_anomaly_detection', 'enhanced'):
            print("✓ Hybrid detection pipeline active")
        else:
            print(f"! Using fallback method: {method}")
            print("  This might explain why verification counts are 0")

        # Check if verification layer is properly imported
        try:
            from pi_monitor.hybrid_anomaly_detection import enhanced_anomaly_detection
            print("✓ Hybrid anomaly detection module importable")
        except ImportError as e:
            print(f"✗ Hybrid anomaly detection import failed: {e}")
            print("  This is likely why verification layer shows 0 results")

        # Check if we have the expected detection structure
        verification_fields = ['sigma_2_5_count', 'autoencoder_count', 'mtd_count', 'isolation_forest_count']
        sample_tag = list(by_tag.values())[0] if by_tag else {}

        missing_fields = [field for field in verification_fields if field not in sample_tag]
        if missing_fields:
            print(f"✗ Missing verification fields: {missing_fields}")
            print("  This indicates the hybrid detection pipeline is not running")
        else:
            print("✓ All verification fields present")

        # Summary
        total_primary_detections = sum(
            (tag_info.get('sigma_2_5_count', 0) + tag_info.get('autoencoder_count', 0))
            for tag_info in by_tag.values()
        )
        total_verification_detections = sum(
            (tag_info.get('mtd_count', 0) + tag_info.get('isolation_forest_count', 0))
            for tag_info in by_tag.values()
        )

        print(f"\n--- PIPELINE SUMMARY ---")
        print(f"Total primary detections (Sigma+AE): {total_primary_detections}")
        print(f"Total verification detections (MTD+IF): {total_verification_detections}")
        print(f"Verification success rate: {(total_verification_detections/max(1,total_primary_detections))*100:.1f}%")

        if total_verification_detections == 0:
            print("\n!!! ROOT CAUSE IDENTIFIED !!!")
            print("Verification layer (MTD + Isolation Forest) is not detecting anything")
            print("Possible causes:")
            print("1. Hybrid detection module not properly imported/working")
            print("2. MTD/IF thresholds too strict")
            print("3. Verification algorithms have bugs")
            print("4. Data format incompatibility with verification layer")

    except Exception as e:
        print(f"Error in detection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_verification_layer()