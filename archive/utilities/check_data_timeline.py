#!/usr/bin/env python3
"""
Check data timeline and freshness issues
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase

def check_data_timeline():
    """Check data timeline for all units"""

    print("DATA TIMELINE ANALYSIS")
    print("=" * 50)
    print(f"Current date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Expected latest data: 22/09/2025")
    print()

    db = ParquetDatabase()
    units = db.get_all_units()

    print(f"Found {len(units)} units")
    print()

    for unit in units:
        print(f"--- UNIT: {unit} ---")

        try:
            # Get freshness info
            freshness_info = db.get_data_freshness_info(unit)

            latest_time = freshness_info.get('latest_timestamp')
            earliest_time = freshness_info.get('earliest_timestamp')
            data_age_hours = freshness_info.get('data_age_hours', 0)
            total_records = freshness_info.get('total_records', 0)

            print(f"Total records: {total_records:,}")
            print(f"Data age: {data_age_hours:.1f} hours")

            if latest_time:
                latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Latest timestamp: {latest_str}")

                # Check if data is from 15/09 (the cutoff date you mentioned)
                if latest_time.date().strftime('%d/%m') == "15/09":
                    print("❌ ISSUE: Data stops at 15/09 - missing recent data!")
                elif latest_time.date() >= datetime.now().date() - timedelta(days=1):
                    print("✅ Data is recent (within 24 hours)")
                else:
                    days_old = (datetime.now().date() - latest_time.date()).days
                    print(f"⚠️  Data is {days_old} days old")

            if earliest_time:
                earliest_str = earliest_time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Earliest timestamp: {earliest_str}")

            # Check recent data specifically
            df = db.get_unit_data(unit)
            if not df.empty and 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])

                # Check data distribution by date
                recent_cutoff = datetime.now() - timedelta(days=10)
                recent_data = df[df['time'] >= recent_cutoff]

                if not recent_data.empty:
                    print(f"Records in last 10 days: {len(recent_data):,}")

                    # Group by date to see data gaps
                    daily_counts = recent_data.groupby(recent_data['time'].dt.date).size()
                    print("Recent daily data counts:")
                    for date, count in daily_counts.tail(7).items():
                        date_str = date.strftime('%d/%m/%Y')
                        print(f"  {date_str}: {count:,} records")
                else:
                    print("❌ NO RECENT DATA in last 10 days!")

        except Exception as e:
            print(f"Error analyzing {unit}: {e}")

        print()

    # Check if PI DataLink refresh is needed
    print("RECOMMENDATION:")
    print("If data stops at 15/09/2025, run:")
    print("  python turbopredict.py --auto-refresh")
    print("  or")
    print("  python -m pi_monitor.cli auto-scan --refresh")

if __name__ == "__main__":
    check_data_timeline()