#!/usr/bin/env python3
"""
Run extended analysis for XT-07002 with staleness as instrumentation anomaly
Integration of Option [2] analysis with extended data fetching
"""

import os
import sys
from pathlib import Path

# Set enhanced timeout for problematic units
os.environ['PI_FETCH_TIMEOUT'] = '90'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config

def run_extended_xt07002_analysis():
    """Run the extended analysis for XT-07002 following Option [2] approach"""

    print("=" * 60)
    print("EXTENDED XT-07002 ANALYSIS")
    print("Extending data to latest fetch regardless of staleness")
    print("Categorizing staleness as instrumentation anomaly")
    print("=" * 60)

    # Initialize with enhanced config
    config = Config()
    scanner = ParquetAutoScanner(config)

    unit = "XT-07002"

    print(f"\n[INFO] PI_FETCH_TIMEOUT set to: {os.environ.get('PI_FETCH_TIMEOUT')}s")
    print(f"[INFO] Analyzing unit: {unit}")
    print("[INFO] Using extended freshness analysis approach...")

    try:
        # Run the enhanced analysis with our new methods
        print("\n1. Running extended unit analysis...")
        result = scanner.analyze_unit_data(unit, run_anomaly_detection=True)

        print(f"\n=== ANALYSIS RESULTS FOR {unit} ===")

        # Basic metrics
        print(f"Records analyzed: {result.get('records', 0):,}")
        print(f"Status: {result.get('status', 'unknown')}")

        # Date range information
        date_range = result.get('date_range', {})
        if date_range:
            print(f"Data range: {date_range.get('start', 'N/A')}")
            print(f"          to {date_range.get('end', 'N/A')}")

        # Extended freshness analysis (our new feature)
        extended_freshness = result.get('extended_freshness', {})
        if extended_freshness:
            print(f"\n--- EXTENDED FRESHNESS ANALYSIS ---")

            hours_stale = extended_freshness.get('hours_since_latest', 0)
            staleness_cat = extended_freshness.get('staleness_category', {})

            print(f"Latest data time: {extended_freshness.get('latest_data_time', 'N/A')}")
            print(f"Hours since latest: {hours_stale:.1f}")
            print(f"Traditional stale threshold (1h): {'EXCEEDED' if extended_freshness.get('is_stale_traditional') else 'OK'}")

            print(f"\nStaleness Classification:")
            print(f"  Level: {staleness_cat.get('level', 'N/A')}")
            print(f"  Severity: {staleness_cat.get('severity', 'N/A')}")
            print(f"  Description: {staleness_cat.get('description', 'N/A')}")

            # Extended fetch information
            extended_fetch = extended_freshness.get('extended_fetch', {})
            if extended_fetch:
                print(f"\nExtended Fetch Status:")
                if extended_fetch.get('special_handling') == 'XT-07002_direct_fetch':
                    print("  ✓ Special XT-07002 handling enabled")
                    working_tags = extended_fetch.get('working_tags_identified', [])
                    if working_tags:
                        print(f"  ✓ Working tags identified: {', '.join(working_tags)}")

        # Enhanced anomaly detection (including staleness)
        anomalies = result.get('anomalies', {})
        if anomalies:
            print(f"\n--- ANOMALY DETECTION (WITH STALENESS) ---")

            total_anomalies = anomalies.get('total_anomalies', 0)
            print(f"Total anomalies detected: {total_anomalies}")
            print(f"Detection method: {anomalies.get('method', 'N/A')}")

            # Instrumentation anomalies (our staleness detection)
            inst_anomalies = anomalies.get('instrumentation_anomalies', {})
            if inst_anomalies:
                print(f"\n*** INSTRUMENTATION ANOMALIES DETECTED ***")
                print(f"Count: {anomalies.get('instrumentation_anomaly_count', len(inst_anomalies))}")

                for name, details in inst_anomalies.items():
                    print(f"\n  {name.upper()}:")
                    print(f"    Type: {details.get('type', 'N/A')}")
                    print(f"    Subtype: {details.get('subtype', 'N/A')}")
                    print(f"    Severity: {details.get('severity', 'N/A')}")
                    print(f"    Description: {details.get('description', 'N/A')}")
                    print(f"    Hours stale: {details.get('hours_stale', 0):.1f}")
                    print(f"    Detection method: {details.get('detection_method', 'N/A')}")

            # Traditional anomalies
            by_tag_anomalies = anomalies.get('by_tag', {})
            if by_tag_anomalies:
                print(f"\nTraditional anomalies by tag: {len(by_tag_anomalies)}")

            # Freshness metadata
            freshness_meta = anomalies.get('freshness_metadata', {})
            if freshness_meta:
                print(f"\nExtended Analysis Metadata:")
                print(f"  Extended analysis performed: {freshness_meta.get('extended_analysis_performed', False)}")
                print(f"  Extended fetch attempted: {freshness_meta.get('extended_fetch_attempted', False)}")

        # Value statistics
        value_stats = result.get('value_stats', {})
        if value_stats:
            print(f"\n--- VALUE STATISTICS ---")
            print(f"Count: {value_stats.get('count', 0):,}")
            print(f"Mean: {value_stats.get('mean', 0):.3f}")
            print(f"Std: {value_stats.get('std', 0):.3f}")
            print(f"Range: {value_stats.get('min', 0):.3f} to {value_stats.get('max', 0):.3f}")

        # Tags information
        tags = result.get('tags', [])
        if tags:
            print(f"\n--- TAG SUMMARY ---")
            print(f"Unique tags: {result.get('unique_tags', len(tags))}")
            print("Top 5 tags by record count:")
            for i, tag in enumerate(tags[:5]):
                tag_name = tag.get('tag', 'N/A')
                record_count = tag.get('records', 0)
                print(f"  {i+1}. {tag_name}: {record_count:,} records")

        print(f"\n=== EXTENDED ANALYSIS COMPLETE ===")
        print("Key insights:")
        print("- Data freshness analyzed regardless of traditional staleness thresholds")
        print("- Staleness categorized as instrumentation anomaly when appropriate")
        print("- XT-07002 special handling for timeout issues")
        print("- Ready for plotting current data without staleness cutoffs")

    except Exception as e:
        print(f"ERROR: Analysis failed - {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_extended_xt07002_analysis()