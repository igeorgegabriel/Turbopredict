#!/usr/bin/env python3
"""
Test script to specifically check folder/plot count for Option [2]
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

def test_option2_folder_count():
    """Test and count exactly how many folders/plots Option [2] produces"""

    print("=== OPTION [2] FOLDER/PLOT COUNT ANALYSIS ===")
    print("Testing the new anomaly-triggered system vs old system")

    # Check reports directory before
    reports_dir = Path("reports")
    before_count = len([d for d in reports_dir.iterdir() if d.is_dir()]) if reports_dir.exists() else 0
    print(f"Report folders before Option [2]: {before_count}")

    # Initialize database
    db = ParquetDatabase()
    units = db.get_all_units()
    print(f"Units to analyze: {len(units)} ({', '.join(units[:5])}{'...' if len(units) > 5 else ''})")

    # Simulate what Option [2] now does
    detection_results = {}
    total_verified_anomalies = 0
    total_problematic_tags = 0

    cutoff = datetime.now() - timedelta(days=30)  # 30-day window for faster testing

    print(f"\n--- SIMULATING OPTION [2] ANALYSIS ---")

    for i, unit in enumerate(units[:3]):  # Test first 3 units for speed
        print(f"[{i+1}/3] Analyzing {unit}...")

        # Load unit data
        df = db.get_unit_data(unit)
        if df.empty:
            print(f"  No data for {unit}")
            continue

        # Filter recent data
        df['time'] = pd.to_datetime(df['time'])
        recent_data = df[df['time'] >= cutoff].copy()

        if recent_data.empty:
            print(f"  No recent data for {unit}")
            continue

        print(f"  Data: {len(recent_data):,} records, {recent_data['tag'].nunique()} tags")

        # Run smart anomaly detection
        try:
            results = smart_anomaly_detection(recent_data, unit, auto_plot_anomalies=False)
            detection_results[unit] = results

            # Count results
            unit_status = results.get('unit_status', {})
            total_anomalies = results.get('total_anomalies', 0)
            by_tag = results.get('by_tag', {})

            print(f"  Status: {unit_status.get('status', 'UNKNOWN')}")
            print(f"  Anomalies: {total_anomalies:,}")
            print(f"  Problematic tags: {len(by_tag)}")

            total_problematic_tags += len(by_tag)

            # Count verified anomalies
            unit_verified = 0
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
                    unit_verified += 1

            total_verified_anomalies += unit_verified
            print(f"  Verified anomalies: {unit_verified}")

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\n--- ANOMALY-TRIGGERED PLOTTING PHASE ---")
    print(f"Total verified anomalies across all units: {total_verified_anomalies}")
    print(f"Total problematic tags (before verification): {total_problematic_tags}")

    if total_verified_anomalies > 0:
        print(f"WOULD GENERATE PLOTS: {total_verified_anomalies} verified anomalies found")
        print("Triggering anomaly-triggered plotting...")

        try:
            plot_session_dir = generate_anomaly_plots(detection_results)
            print(f"Generated session directory: {plot_session_dir}")

            # Count actual files created
            if plot_session_dir.exists():
                png_files = list(plot_session_dir.glob("*.png"))
                txt_files = list(plot_session_dir.glob("*.txt"))
                all_files = list(plot_session_dir.glob("*"))

                print(f"Files created: {len(png_files)} plots + {len(txt_files)} reports = {len(all_files)} total")

                # Count folders after
                after_count = len([d for d in reports_dir.iterdir() if d.is_dir()])
                new_folders = after_count - before_count

                print(f"\n=== FINAL OPTION [2] OUTPUT COUNT ===")
                print(f"New report folders created: {new_folders}")
                print(f"Total plot files (.png): {len(png_files)}")
                print(f"Total report files (.txt): {len(txt_files)}")
                print(f"Total files in session: {len(all_files)}")

        except Exception as e:
            print(f"Plot generation error: {e}")

    else:
        print("NO PLOTS GENERATED: No verified anomalies found")
        print("This is the NEW behavior - prevents excessive plotting")

        # Count folders after (should be same)
        after_count = len([d for d in reports_dir.iterdir() if d.is_dir()]) if reports_dir.exists() else 0
        new_folders = after_count - before_count

        print(f"\n=== FINAL OPTION [2] OUTPUT COUNT ===")
        print(f"New report folders created: {new_folders}")
        print(f"Total plot files (.png): 0")
        print(f"Total report files (.txt): 0")
        print(f"Total files in session: 0")

    print(f"\n=== OLD vs NEW COMPARISON ===")
    print(f"OLD SYSTEM would have created:")
    print(f"  - 1 folder per unit = {len(units)} folders")
    print(f"  - ~10-20 plots per problematic tag")
    print(f"  - Total: {total_problematic_tags} tags × ~15 plots = ~{total_problematic_tags * 15:,} plots")
    print(f"")
    print(f"NEW SYSTEM created:")
    print(f"  - Folders: {new_folders if 'new_folders' in locals() else 0}")
    print(f"  - Plots: {len(png_files) if 'png_files' in locals() else 0}")
    print(f"  - Reduction: {((total_problematic_tags * 15) if total_problematic_tags > 0 else 1):,} -> {len(png_files) if 'png_files' in locals() else 0} plots")

if __name__ == "__main__":
    test_option2_folder_count()