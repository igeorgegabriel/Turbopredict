#!/usr/bin/env python3
"""
Test the extended analysis with staleness as instrumentation anomaly
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config

def test_extended_analysis():
    """Test the extended analysis functionality"""

    print("=== Testing Extended Analysis with Staleness as Anomaly ===")

    # Initialize scanner
    config = Config()
    scanner = ParquetAutoScanner(config)

    # Test with XT-07002 (the unit with timeout issues)
    unit = "XT-07002"

    print(f"\nAnalyzing unit: {unit}")
    print("This will:")
    print("1. Analyze existing data")
    print("2. Extend freshness analysis regardless of staleness")
    print("3. Categorize staleness as instrumentation anomaly")
    print("4. Show latest data from your working tag")

    try:
        # Run the enhanced analysis
        result = scanner.analyze_unit_data(unit, run_anomaly_detection=True)

        print(f"\n=== Analysis Results for {unit} ===")

        # Basic info
        print(f"Status: {result.get('status', 'unknown')}")
        print(f"Records: {result.get('records', 0):,}")

        # Date range
        date_range = result.get('date_range', {})
        if date_range:
            print(f"Data range: {date_range.get('start', 'N/A')} to {date_range.get('end', 'N/A')}")

        # Extended freshness analysis
        extended_freshness = result.get('extended_freshness', {})
        if extended_freshness:
            print(f"\n--- Extended Freshness Analysis ---")
            print(f"Method: {extended_freshness.get('method', 'N/A')}")
            print(f"Latest data time: {extended_freshness.get('latest_data_time', 'N/A')}")
            print(f"Hours since latest: {extended_freshness.get('hours_since_latest', 0):.1f}")

            staleness_cat = extended_freshness.get('staleness_category', {})
            print(f"Staleness level: {staleness_cat.get('level', 'N/A')}")
            print(f"Severity: {staleness_cat.get('severity', 'N/A')}")
            print(f"Description: {staleness_cat.get('description', 'N/A')}")

            # Extended fetch info
            extended_fetch = extended_freshness.get('extended_fetch', {})
            if extended_fetch.get('special_handling') == 'XT-07002_direct_fetch':
                print(f"Special handling: XT-07002 direct fetch")
                working_tags = extended_fetch.get('working_tags_identified', [])
                print(f"Working tags identified: {working_tags}")

        # Anomaly detection with staleness
        anomalies = result.get('anomalies', {})
        if anomalies:
            print(f"\n--- Anomaly Detection (Including Staleness) ---")
            print(f"Total anomalies: {anomalies.get('total_anomalies', 0)}")
            print(f"Method: {anomalies.get('method', 'N/A')}")

            # Instrumentation anomalies (staleness)
            inst_anomalies = anomalies.get('instrumentation_anomalies', {})
            if inst_anomalies:
                print(f"Instrumentation anomalies detected: {len(inst_anomalies)}")
                for name, details in inst_anomalies.items():
                    print(f"  - {name}: {details.get('description', 'N/A')}")
                    print(f"    Severity: {details.get('severity', 'N/A')}")
                    print(f"    Hours stale: {details.get('hours_stale', 0):.1f}")

            # Freshness metadata
            freshness_meta = anomalies.get('freshness_metadata', {})
            if freshness_meta:
                print(f"Extended analysis performed: {freshness_meta.get('extended_analysis_performed', False)}")

        # Tag summary
        tags = result.get('tags', [])
        if tags:
            print(f"\nTags analyzed: {len(tags)}")
            print("Sample tags:")
            for tag in tags[:5]:  # Show first 5 tags
                tag_name = tag.get('tag', 'N/A')
                record_count = tag.get('records', 0)
                print(f"  - {tag_name}: {record_count:,} records")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_extended_analysis()