#!/usr/bin/env python3
"""
Demo: Extended staleness analysis for XT-07002 showing instrumentation anomaly detection
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_database import ParquetDatabase

def demo_extended_analysis():
    """Demo the extended staleness analysis for XT-07002"""

    print("=== XT-07002 Extended Staleness Analysis Demo ===")

    # Load existing XT-07002 data
    data_dir = PROJECT_ROOT / "data"
    db = ParquetDatabase(data_dir)

    try:
        # Get XT-07002 data
        df = db.get_unit_data("XT-07002")

        if df.empty:
            print("No XT-07002 data found")
            return

        print(f"Loaded {len(df):,} records for XT-07002")

        # Get latest timestamp
        latest_time = pd.to_datetime(df['time'].max())
        now = datetime.now()
        hours_stale = (now - latest_time).total_seconds() / 3600

        print(f"Latest data time: {latest_time}")
        print(f"Current time: {now}")
        print(f"Hours since latest data: {hours_stale:.1f}")

        # Categorize staleness (simulate the new method)
        if hours_stale <= 1.0:
            level, severity = 'fresh', 'none'
            description = 'Data is current'
        elif hours_stale <= 6.0:
            level, severity = 'mildly_stale', 'low'
            description = 'Slight data lag - normal variation'
        elif hours_stale <= 24.0:
            level, severity = 'stale', 'medium'
            description = 'Data staleness - potential instrumentation issue'
        elif hours_stale <= 168.0:  # 1 week
            level, severity = 'very_stale', 'high'
            description = 'Significant staleness - instrumentation anomaly likely'
        else:
            level, severity = 'extremely_stale', 'critical'
            description = 'Extreme staleness - instrumentation failure probable'

        print(f"\n=== Staleness Classification ===")
        print(f"Level: {level}")
        print(f"Severity: {severity}")
        print(f"Description: {description}")

        # Show if this would be classified as instrumentation anomaly
        if severity in ['medium', 'high', 'critical']:
            print(f"\n*** INSTRUMENTATION ANOMALY DETECTED ***")
            print(f"Type: Data Staleness")
            print(f"Severity: {severity}")
            print(f"This unit would be flagged for investigation")

            if 'XT-07002' in 'XT-07002':
                print(f"\nSpecial handling for XT-07002:")
                print(f"- Known working tag: PCM.XT-07002.070GZI8402.PV")
                print(f"- Would attempt direct fetch with 90s timeout")
                print(f"- Skip problematic tags that timeout")

        # Show recent tag activity
        print(f"\n=== Recent Tag Activity ===")
        if 'tag' in df.columns:
            # Get latest data by tag
            recent_df = df[df['time'] >= latest_time - pd.Timedelta(hours=1)]
            if not recent_df.empty:
                tag_counts = recent_df['tag'].value_counts()
                print(f"Tags with data in last hour before staleness:")
                for tag, count in tag_counts.head(5).items():
                    print(f"  - {tag}: {count} records")

                # Check if the working tag from user's evidence is present
                working_tag = 'PCM.XT-07002.070GZI8402.PV'
                if working_tag in tag_counts.index:
                    print(f"\n✓ Working tag {working_tag} found with {tag_counts[working_tag]} recent records")
                    # Show its latest values
                    working_data = recent_df[recent_df['tag'] == working_tag].tail(3)
                    print("Latest values from working tag:")
                    for _, row in working_data.iterrows():
                        print(f"  {row['time']}: {row['value']:.5f}")
            else:
                print("No data in the hour before staleness occurred")

        # Extended fetch simulation
        print(f"\n=== Extended Fetch Simulation ===")
        print(f"In the extended system, this would:")
        print(f"1. Attempt to fetch latest data regardless of {hours_stale:.1f}h staleness")
        print(f"2. Use PI_FETCH_TIMEOUT=90 for better reliability")
        print(f"3. Focus on working tags like PCM.XT-07002.070GZI8402.PV")
        print(f"4. Plot current data without staleness cutoffs")
        print(f"5. Include staleness in anomaly detection as instrumentation issue")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    demo_extended_analysis()