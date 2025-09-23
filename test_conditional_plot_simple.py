#!/usr/bin/env python3
"""
Simple test of conditional plotting with minimal change markers
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

def detect_minimal_changes_simple(data, column, threshold_percent=0.5):
    """Simplified minimal change detection"""

    if column not in data.columns or len(data) < 2:
        return []

    # Sort by time
    sorted_data = data.sort_values('time').copy()
    values = sorted_data[column].values
    times = sorted_data['time'].values

    minimal_change_periods = []
    current_start = None
    consecutive_count = 0

    for i in range(1, len(values)):
        if pd.isna(values[i]) or pd.isna(values[i-1]) or values[i-1] == 0:
            # Reset on invalid data
            if consecutive_count >= 10:  # Save period if long enough
                minimal_change_periods.append({
                    'start_time': times[current_start],
                    'end_time': times[i-1],
                    'start_idx': current_start,
                    'end_idx': i-1
                })
            current_start = None
            consecutive_count = 0
            continue

        pct_change = abs((values[i] - values[i-1]) / values[i-1]) * 100

        if pct_change < threshold_percent:
            # Minimal change detected
            if current_start is None:
                current_start = i-1  # Start of period
                consecutive_count = 1
            consecutive_count += 1
        else:
            # Significant change - end current period
            if consecutive_count >= 10:  # Save if long enough
                minimal_change_periods.append({
                    'start_time': times[current_start],
                    'end_time': times[i-1],
                    'start_idx': current_start,
                    'end_idx': i-1
                })
            current_start = None
            consecutive_count = 0

    # Handle final period
    if consecutive_count >= 10:
        minimal_change_periods.append({
            'start_time': times[current_start],
            'end_time': times[-1],
            'start_idx': current_start,
            'end_idx': len(times)-1
        })

    return minimal_change_periods

def test_simple_conditional_plot():
    """Test simple conditional plotting"""

    print("TESTING SIMPLE CONDITIONAL PLOTTING")
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

    try:
        # Load and filter data
        data = pd.read_parquet(selected_file)
        data['time'] = pd.to_datetime(data['time'])

        # Get last 7 days for faster processing
        cutoff = datetime.now() - timedelta(days=7)
        recent_data = data[data['time'] >= cutoff].copy()
        print(f"Recent data (7 days): {len(recent_data):,} records")

        if recent_data.empty:
            print("No recent data found")
            return

        # Get first numeric tag
        numeric_cols = [col for col in recent_data.columns
                       if col != 'time' and pd.api.types.is_numeric_dtype(recent_data[col])
                       and recent_data[col].notna().sum() > 100]

        if not numeric_cols:
            print("No suitable numeric columns found")
            return

        test_tag = numeric_cols[0]
        print(f"Testing tag: {test_tag}")

        # Detect minimal changes
        change_threshold = 0.5
        minimal_periods = detect_minimal_changes_simple(recent_data, test_tag, change_threshold)
        print(f"Found {len(minimal_periods)} minimal change periods")

        # Create plot
        fig, ax = plt.subplots(figsize=(16, 8))

        # Plot the data
        tag_data = recent_data[['time', test_tag]].dropna()
        ax.plot(tag_data['time'], tag_data[test_tag], 'b-', alpha=0.7, linewidth=0.8, label=test_tag)

        # Mark minimal change periods
        for i, period in enumerate(minimal_periods):
            period_data = tag_data[
                (tag_data['time'] >= period['start_time']) &
                (tag_data['time'] <= period['end_time'])
            ]

            if not period_data.empty:
                # Highlight stable periods
                ax.fill_between(
                    period_data['time'],
                    period_data[test_tag].min() * 0.999,
                    period_data[test_tag].max() * 1.001,
                    alpha=0.3,
                    color='yellow',
                    label=f'Stable (<{change_threshold}% change)' if i == 0 else ""
                )

                # Add duration annotation for longer periods
                duration_td = pd.Timestamp(period['end_time']) - pd.Timestamp(period['start_time'])
                duration_hours = duration_td.total_seconds() / 3600
                if duration_hours > 2:  # Only annotate periods > 2 hours
                    mid_time = pd.Timestamp(period['start_time']) + duration_td / 2
                    mid_value = period_data[test_tag].mean()
                    ax.annotate(
                        f"{duration_hours:.1f}h\nstable",
                        xy=(mid_time, mid_value),
                        xytext=(10, 10),
                        textcoords='offset points',
                        fontsize=8,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.8),
                        ha='center'
                    )

        # Formatting
        ax.set_title(f'{unit} - {test_tag}\nConditional Plot with Minimal Change Markers (<{change_threshold}%)',
                    fontsize=14, fontweight='bold')
        ax.set_ylabel(test_tag, fontsize=12)
        ax.set_xlabel('Time', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.legend()

        # Save plot
        output_path = f"conditional_plot_{unit}_{test_tag}.png"
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"Plot saved: {output_path}")
        print("SUCCESS: Conditional plotting with change markers working!")

        # Summary
        total_stable_hours = sum(
            (pd.Timestamp(p['end_time']) - pd.Timestamp(p['start_time'])).total_seconds() / 3600
            for p in minimal_periods
        )
        print(f"Total stable time: {total_stable_hours:.1f} hours")
        if minimal_periods:
            longest = max(minimal_periods, key=lambda x: (pd.Timestamp(x['end_time']) - pd.Timestamp(x['start_time'])).total_seconds())
            longest_hours = (pd.Timestamp(longest['end_time']) - pd.Timestamp(longest['start_time'])).total_seconds() / 3600
            print(f"Longest stable period: {longest_hours:.1f} hours")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_conditional_plot()