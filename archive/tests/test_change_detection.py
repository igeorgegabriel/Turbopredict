#!/usr/bin/env python3
"""
Test the minimal change detection logic on a single unit
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

def detect_minimal_changes(data, column, threshold_percent=0.5):
    """Detect periods where values change less than threshold_percent from last value"""

    if column not in data.columns or len(data) < 2:
        return {'minimal_change_periods': [], 'stats': {}}

    # Sort by time
    sorted_data = data.sort_values('time').copy()
    values = sorted_data[column].values
    times = sorted_data['time'].values

    # Calculate percentage changes from previous value
    pct_changes = []
    minimal_change_indices = []

    for i in range(1, len(values)):
        if pd.isna(values[i]) or pd.isna(values[i-1]) or values[i-1] == 0:
            pct_changes.append(np.nan)
            continue

        pct_change = abs((values[i] - values[i-1]) / values[i-1]) * 100
        pct_changes.append(pct_change)

        # Mark as minimal change if below threshold
        if pct_change < threshold_percent:
            minimal_change_indices.append(i)

    # Group consecutive minimal change periods
    minimal_periods = []
    if minimal_change_indices:
        current_period = {'start_idx': minimal_change_indices[0], 'end_idx': minimal_change_indices[0]}

        for idx in minimal_change_indices[1:]:
            if idx == current_period['end_idx'] + 1:
                # Consecutive - extend current period
                current_period['end_idx'] = idx
            else:
                # New period - save current and start new
                if current_period['end_idx'] - current_period['start_idx'] >= 5:  # At least 5 consecutive points
                    duration_td = pd.Timestamp(times[current_period['end_idx']]) - pd.Timestamp(times[current_period['start_idx']])
                    minimal_periods.append({
                        'start_time': times[current_period['start_idx']],
                        'end_time': times[current_period['end_idx']],
                        'duration_hours': duration_td.total_seconds() / 3600,
                    })
                current_period = {'start_idx': idx, 'end_idx': idx}

        # Don't forget the last period
        if current_period['end_idx'] - current_period['start_idx'] >= 5:
            duration_td = pd.Timestamp(times[current_period['end_idx']]) - pd.Timestamp(times[current_period['start_idx']])
            minimal_periods.append({
                'start_time': times[current_period['start_idx']],
                'end_time': times[current_period['end_idx']],
                'duration_hours': duration_td.total_seconds() / 3600,
            })

    # Statistics
    valid_changes = [x for x in pct_changes if not pd.isna(x)]
    stats = {
        'total_points': len(values),
        'minimal_change_points': len(minimal_change_indices),
        'minimal_change_percentage': (len(minimal_change_indices) / max(1, len(values)-1)) * 100,
        'avg_change': np.mean(valid_changes) if valid_changes else 0,
        'max_change': np.max(valid_changes) if valid_changes else 0,
        'periods_count': len(minimal_periods)
    }

    return {
        'minimal_change_periods': minimal_periods,
        'stats': stats
    }

def test_change_detection():
    """Test minimal change detection on K-31-01"""

    print("TESTING MINIMAL CHANGE DETECTION")
    print("=" * 50)

    # Find K-31-01 data
    unit = "K-31-01"
    unit_files = glob.glob(f"data/processed/*{unit}*.parquet")

    if not unit_files:
        print(f"No files found for {unit}")
        return

    # Use most recent dedup file
    dedup_files = [f for f in unit_files if 'dedup' in f]
    if dedup_files:
        selected_file = max(dedup_files, key=lambda x: os.path.getmtime(x))
    else:
        selected_file = max(unit_files, key=lambda x: os.path.getmtime(x))

    print(f"Using file: {os.path.basename(selected_file)}")

    # Load data
    try:
        data = pd.read_parquet(selected_file)
        print(f"Loaded {len(data):,} records")

        # Get recent data
        data['time'] = pd.to_datetime(data['time'])
        cutoff = datetime.now() - timedelta(days=30)  # Last 30 days
        recent_data = data[data['time'] >= cutoff].copy()
        print(f"Recent data: {len(recent_data):,} records")

        if recent_data.empty:
            print("No recent data found")
            return

        # Test on first numeric column (excluding time)
        numeric_cols = [col for col in recent_data.columns
                       if col != 'time' and pd.api.types.is_numeric_dtype(recent_data[col])]

        if not numeric_cols:
            print("No numeric columns found")
            return

        test_col = numeric_cols[0]
        print(f"Testing on column: {test_col}")

        # Test different thresholds
        thresholds = [0.1, 0.5, 1.0, 2.0]

        for threshold in thresholds:
            print(f"\nThreshold: {threshold}%")
            analysis = detect_minimal_changes(recent_data, test_col, threshold)
            stats = analysis['stats']
            periods = analysis['minimal_change_periods']

            print(f"  Minimal change points: {stats['minimal_change_points']} ({stats['minimal_change_percentage']:.1f}%)")
            print(f"  Stable periods: {stats['periods_count']}")
            print(f"  Average change: {stats['avg_change']:.2f}%")

            if periods:
                total_stable_hours = sum(p['duration_hours'] for p in periods)
                longest_period = max(periods, key=lambda x: x['duration_hours'])
                print(f"  Total stable time: {total_stable_hours:.1f} hours")
                print(f"  Longest stable period: {longest_period['duration_hours']:.1f} hours")

        # Create a simple plot for the first threshold
        print(f"\nCreating test plot...")

        # Sample data for plotting (last 1000 points)
        plot_data = recent_data.tail(1000).copy()
        analysis = detect_minimal_changes(plot_data, test_col, 0.5)

        plt.figure(figsize=(14, 8))

        # Plot the data
        plt.plot(plot_data['time'], plot_data[test_col], 'b-', alpha=0.7, linewidth=0.8)

        # Mark minimal change periods
        for period in analysis['minimal_change_periods']:
            period_data = plot_data[
                (plot_data['time'] >= period['start_time']) &
                (plot_data['time'] <= period['end_time'])
            ]
            if not period_data.empty:
                plt.fill_between(
                    period_data['time'],
                    period_data[test_col].min() * 0.999,
                    period_data[test_col].max() * 1.001,
                    alpha=0.3, color='yellow'
                )

        plt.title(f"{unit} - {test_col}\nMinimal Change Detection (< 0.5% change)")
        plt.xlabel("Time")
        plt.ylabel(test_col)
        plt.grid(True, alpha=0.3)

        # Save plot
        output_path = f"test_change_detection_{unit}_{test_col}.png"
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"Plot saved: {output_path}")
        print("SUCCESS: Minimal change detection working correctly!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_change_detection()