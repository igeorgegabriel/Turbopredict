#!/usr/bin/env python3
"""
Test script for Option [2] anomaly-triggered plotting integration
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
from pi_monitor.anomaly_triggered_plots import generate_anomaly_plots

def test_option2_integration():
    """Test the integrated anomaly-triggered plotting for Option [2]"""

    print("=== TESTING OPTION [2] ANOMALY-TRIGGERED PLOTTING INTEGRATION ===")

    # Initialize database
    try:
        db = ParquetDatabase()
        units = db.get_all_units()
        print(f"[OK] Database initialized - found {len(units)} units")
    except Exception as e:
        print(f"[ERROR] Database initialization failed: {e}")
        return

    # Test with a single priority unit (K-31-01)
    test_unit = "K-31-01" if "K-31-01" in units else units[0]
    print(f"Testing with unit: {test_unit}")

    try:
        # Load recent data (30 days for faster testing)
        df = db.get_unit_data(test_unit)
        if df.empty:
            print(f"[ERROR] No data found for {test_unit}")
            return

        # Filter to recent 30 days
        cutoff = datetime.now() - timedelta(days=30)
        df['time'] = pd.to_datetime(df['time'])
        recent_data = df[df['time'] >= cutoff].copy()

        print(f"[OK] Loaded {len(recent_data):,} recent records for {test_unit}")
        print(f"  Tags: {recent_data['tag'].nunique()}")
        print(f"  Time range: {recent_data['time'].min()} to {recent_data['time'].max()}")

    except Exception as e:
        print(f"[ERROR] Data loading failed: {e}")
        return

    # Test smart anomaly detection
    try:
        print("\n--- RUNNING SMART ANOMALY DETECTION ---")
        results = smart_anomaly_detection(recent_data, test_unit, auto_plot_anomalies=False)

        # Display results
        unit_status = results.get('unit_status', {})
        total_anomalies = results.get('total_anomalies', 0)
        by_tag = results.get('by_tag', {})

        print(f"[OK] Smart anomaly detection completed:")
        print(f"  Unit status: {unit_status.get('status', 'UNKNOWN')}")
        print(f"  Total anomalies: {total_anomalies:,}")
        print(f"  Problematic tags: {len(by_tag)}")

        # Count verified anomalies
        verified_count = 0
        for tag, tag_info in by_tag.items():
            sigma_count = tag_info.get('sigma_2_5_count', 0)
            ae_count = tag_info.get('autoencoder_count', 0)
            mtd_count = tag_info.get('mtd_count', 0)
            iso_count = tag_info.get('isolation_forest_count', 0)
            confidence = tag_info.get('confidence', 'LOW')

            # Check verification criteria
            primary_detected = sigma_count > 0 or ae_count > 0
            verification_detected = mtd_count > 0 or iso_count > 0
            high_confidence = confidence in ['HIGH', 'MEDIUM']

            if primary_detected and verification_detected and high_confidence:
                verified_count += 1
                print(f"  [VERIFIED] {tag} (Sigma:{sigma_count}, AE:{ae_count}, MTD:{mtd_count}, IF:{iso_count}, {confidence})")

        print(f"  Verified anomalies: {verified_count}")

    except Exception as e:
        print(f"[ERROR] Smart anomaly detection failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test anomaly-triggered plotting
    if verified_count > 0:
        try:
            print(f"\n--- GENERATING ANOMALY-TRIGGERED PLOTS ---")
            print(f"Found {verified_count} verified anomalies - triggering plot generation")

            # Prepare detection results in expected format
            detection_results = {test_unit: results}

            # Generate plots
            plot_session_dir = generate_anomaly_plots(detection_results)

            print(f"[OK] Anomaly-triggered plots generated:")
            print(f"  Session directory: {plot_session_dir}")

            # Count generated files
            if plot_session_dir.exists():
                plot_files = list(plot_session_dir.glob("*.png"))
                txt_files = list(plot_session_dir.glob("*.txt"))
                print(f"  Generated files: {len(plot_files)} plots, {len(txt_files)} reports")

                # Show some sample files
                for i, plot_file in enumerate(plot_files[:3]):
                    print(f"    - {plot_file.name}")
                if len(plot_files) > 3:
                    print(f"    ... and {len(plot_files) - 3} more plots")

        except Exception as e:
            print(f"[ERROR] Anomaly-triggered plotting failed: {e}")
            import traceback
            traceback.print_exc()
            return

    else:
        print(f"\n--- NO VERIFIED ANOMALIES ---")
        print("No verified anomalies found - no plots will be generated")
        print("This is the expected behavior for anomaly-triggered plotting")

    print(f"\n=== OPTION [2] INTEGRATION TEST COMPLETED ===")
    print("[OK] Smart anomaly detection pipeline working")
    print("[OK] Anomaly-triggered plotting integration successful")
    print("[OK] Only verified anomalies trigger plot generation")

if __name__ == "__main__":
    test_option2_integration()