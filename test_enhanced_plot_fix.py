#!/usr/bin/env python3
"""
Test the enhanced plotting fix for file selection
"""

import sys
import os
import glob
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime, timedelta

def test_file_selection_fix():
    """Test the new file selection logic"""

    print("TESTING ENHANCED PLOTTING FIX")
    print("=" * 50)

    # Test the new logic for K-31-01
    unit = "K-31-01"
    print(f"Testing file selection for: {unit}")

    # Find files (same as enhanced plotting script)
    unit_files = glob.glob(f"data/processed/*{unit}*.parquet")
    print(f"Found {len(unit_files)} file(s):")
    for f in unit_files:
        mod_time = datetime.fromtimestamp(os.path.getmtime(f))
        size_mb = os.path.getsize(f) / (1024*1024)
        print(f"  {os.path.basename(f)}: {mod_time.strftime('%Y-%m-%d %H:%M:%S')} ({size_mb:.1f} MB)")

    # Apply new selection logic
    selected_file = None

    # Strategy 1: Prefer dedup files
    dedup_files = [f for f in unit_files if 'dedup' in f]
    if dedup_files:
        selected_file = max(dedup_files, key=lambda x: os.path.getmtime(x))
        print(f"\\nSELECTED: {os.path.basename(selected_file)} (most recent dedup)")
    else:
        selected_file = max(unit_files, key=lambda x: os.path.getmtime(x))
        print(f"\\nSELECTED: {os.path.basename(selected_file)} (most recent regular)")

    # Load and analyze the selected file
    try:
        df = pd.read_parquet(selected_file)
        print(f"\\nDATA ANALYSIS:")
        print(f"Records: {len(df):,}")

        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            latest_time = df['time'].max()
            earliest_time = df['time'].min()
            data_age = (datetime.now() - latest_time).total_seconds() / 3600

            print(f"Time range: {earliest_time.date()} to {latest_time.date()}")
            print(f"Latest timestamp: {latest_time}")
            print(f"Data age: {data_age:.1f} hours")

            # Check if data includes 22/09/2025
            today = datetime.now().date()
            if latest_time.date() >= today - timedelta(days=1):
                print("✅ SUCCESS: Data includes recent dates (22/09 or later)")
            else:
                print("❌ ISSUE: Data still missing recent dates")

            # Check last few days of data
            print(f"\\nRECENT DATA CHECK:")
            last_week = df[df['time'] >= datetime.now() - timedelta(days=7)]
            daily_counts = last_week.groupby(last_week['time'].dt.date).size()
            print("Daily record counts (last 7 days):")
            for date, count in daily_counts.tail(7).items():
                print(f"  {date.strftime('%d/%m/%Y')}: {count:,} records")

    except Exception as e:
        print(f"Error loading selected file: {e}")

if __name__ == "__main__":
    test_file_selection_fix()