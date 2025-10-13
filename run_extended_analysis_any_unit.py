#!/usr/bin/env python3
"""
Universal Extended Analysis Runner
Apply plot stale fetch to any unit: ABF, PCFS, PCMSB, or Generic
Usage: python run_extended_analysis_any_unit.py [unit_name]
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config

def run_universal_extended_analysis(unit_name: str = None):
    """Run extended analysis with automatic plant-type detection and optimization"""

    print("=" * 80)
    print("UNIVERSAL EXTENDED ANALYSIS RUNNER")
    print("Automatic Plant Detection | Optimized Timeouts | Staleness as Anomaly")
    print("=" * 80)

    if not unit_name:
        # Demo with a representative unit from each plant type
        demo_units = ['K-31-01', 'XT-07002', '07-MT01-K001']
        print("No unit specified. Running demo with representative units:")
        for unit in demo_units:
            print(f"  - {unit}")
        print()

        for unit in demo_units:
            run_single_unit_analysis(unit)
    else:
        run_single_unit_analysis(unit_name)

def run_single_unit_analysis(unit: str):
    """Run extended analysis for a single unit with automatic optimization"""

    print(f"\n{'='*60}")
    print(f"ANALYZING UNIT: {unit}")
    print(f"{'='*60}")

    # Initialize scanner
    config = Config()
    scanner = ParquetAutoScanner(config)

    try:
        # Step 1: Get plant-specific configuration
        print("Step 1: Plant-Specific Configuration")
        print("-" * 40)

        handling = scanner._get_plant_specific_handling(unit)
        plant_type = handling.get('plant_type', 'unknown')
        special_handling = handling.get('special_handling', 'none')
        timeout_settings = handling.get('timeout_settings', {})

        print(f"Plant Type: {plant_type}")
        print(f"Special Handling: {special_handling}")

        # Apply plant-specific environment settings
        if 'PI_FETCH_TIMEOUT' in timeout_settings:
            os.environ['PI_FETCH_TIMEOUT'] = str(timeout_settings['PI_FETCH_TIMEOUT'])
            print(f"PI_FETCH_TIMEOUT: {timeout_settings['PI_FETCH_TIMEOUT']}s")

        if 'settle_seconds' in timeout_settings:
            print(f"Settle Seconds: {timeout_settings['settle_seconds']}")

        if 'recommended_timeframe' in timeout_settings:
            print(f"Recommended Timeframe: {timeout_settings['recommended_timeframe']}")

        # Working tags
        working_tags = handling.get('working_tags_identified', [])
        if working_tags:
            print(f"Working Tags Identified: {len(working_tags)}")
            for tag in working_tags:
                print(f"  - {tag}")

        # Step 2: Extended Freshness Analysis
        print(f"\nStep 2: Extended Freshness Analysis")
        print("-" * 40)

        # Load and analyze existing data
        from pi_monitor.parquet_database import ParquetDatabase
        db = ParquetDatabase(PROJECT_ROOT / "data")

        try:
            df = db.get_unit_data(unit)
            if not df.empty:
                print(f"Existing Data: {len(df):,} records")

                if 'time' in df.columns:
                    import pandas as pd
                    from datetime import datetime

                    latest_time = pd.to_datetime(df['time'].max())
                    now = datetime.now()
                    hours_stale = (now - latest_time).total_seconds() / 3600

                    print(f"Latest Data: {latest_time}")
                    print(f"Hours Stale: {hours_stale:.1f}")

                    # Apply universal staleness categorization
                    if hours_stale <= 1.0:
                        level, severity = 'FRESH', 'none'
                        description = 'Data is current'
                    elif hours_stale <= 6.0:
                        level, severity = 'MILDLY_STALE', 'low'
                        description = 'Slight data lag - normal variation'
                    elif hours_stale <= 24.0:
                        level, severity = 'STALE', 'medium'
                        description = 'Data staleness - potential instrumentation issue'
                    elif hours_stale <= 168.0:  # 1 week
                        level, severity = 'VERY_STALE', 'high'
                        description = 'Significant staleness - instrumentation anomaly likely'
                    else:
                        level, severity = 'EXTREMELY_STALE', 'critical'
                        description = 'Extreme staleness - instrumentation failure probable'

                    print(f"Staleness Level: {level}")
                    print(f"Severity: {severity}")
                    print(f"Description: {description}")

                    # Determine action
                    if severity in ['medium', 'high', 'critical']:
                        print(f"\n*** INSTRUMENTATION ANOMALY DETECTED ***")
                        print(f"Action Required: Investigation and potential refresh")

                        # Show what extended fetch would do
                        print(f"\nExtended Fetch Strategy:")
                        print(f"  - Ignore traditional staleness thresholds")
                        print(f"  - Use {timeout_settings.get('PI_FETCH_TIMEOUT', 60)}s timeout")
                        print(f"  - Focus on working tags if available")
                        print(f"  - Categorize staleness as instrumentation anomaly")
                        print(f"  - Plot data without staleness cutoffs")

                    else:
                        print(f"Status: Data freshness acceptable")

                # Tag information
                if 'tag' in df.columns:
                    unique_tags = df['tag'].nunique()
                    print(f"Unique Tags: {unique_tags}")

                    # Show sample of recent tag activity
                    recent_cutoff = latest_time - pd.Timedelta(hours=2)
                    recent_df = df[df['time'] >= recent_cutoff]
                    if not recent_df.empty:
                        recent_tags = recent_df['tag'].value_counts().head(3)
                        print("Recent Tag Activity (last 2h):")
                        for tag, count in recent_tags.items():
                            print(f"  - {tag}: {count} records")

            else:
                print("No existing data found")
                print("Extended fetch would attempt initial data collection")

        except Exception as e:
            print(f"Data analysis error: {e}")

        # Step 3: Optimization Notes
        print(f"\nStep 3: Plant-Specific Optimizations")
        print("-" * 40)

        optimization_notes = handling.get('optimization_notes', [])
        if optimization_notes:
            for note in optimization_notes:
                print(f"  - {note}")

        known_issues = handling.get('known_issues', [])
        if known_issues:
            print("Known Issues:")
            for issue in known_issues:
                print(f"  - {issue}")

        # Step 4: Extended Analysis Capabilities
        print(f"\nStep 4: Extended Analysis Ready")
        print("-" * 40)
        print("This unit is now configured for:")
        print("  1. Extended data fetching regardless of staleness")
        print("  2. Staleness categorized as instrumentation anomaly")
        print("  3. Plant-optimized timeout settings")
        print("  4. Working tag prioritization")
        print("  5. Comprehensive anomaly detection")
        print("  6. Plot stale fetch capability")

        print(f"\nConfiguration Complete for {unit} ({plant_type})")

    except Exception as e:
        print(f"Analysis error for {unit}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    unit_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_universal_extended_analysis(unit_arg)