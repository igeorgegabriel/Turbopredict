#!/usr/bin/env python3
"""
Enhanced conditional plotting: Only plot if data found, mark minimal changes
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

# Import required modules
from pi_monitor.stale_data_detector import StaleDataDetector, add_stale_data_warnings_to_plot

# Ensure AE is enabled
import os as _plot_env
_plot_env.environ.setdefault('ENABLE_AE_LIVE', _plot_env.getenv('ENABLE_AE_LIVE','1'))
_plot_env.environ.setdefault('REQUIRE_AE', _plot_env.getenv('REQUIRE_AE','1'))

from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def detect_minimal_changes(data, column, threshold_percent=0.5):
    """
    Detect periods where values change less than threshold_percent from last value

    Args:
        data: DataFrame with time series data
        column: Column name to analyze
        threshold_percent: Threshold percentage for minimal change detection

    Returns:
        Dict with minimal change periods and statistics
    """
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
                    minimal_periods.append({
                        'start_time': times[current_period['start_idx']],
                        'end_time': times[current_period['end_idx']],
                        'start_idx': current_period['start_idx'],
                        'end_idx': current_period['end_idx'],
                        'duration_hours': (pd.Timestamp(times[current_period['end_idx']]) - pd.Timestamp(times[current_period['start_idx']])).total_seconds() / 3600,
                        'avg_change': np.mean([pct_changes[i] for i in range(current_period['start_idx'], current_period['end_idx']+1) if not pd.isna(pct_changes[i])])
                    })
                current_period = {'start_idx': idx, 'end_idx': idx}

        # Don't forget the last period
        if current_period['end_idx'] - current_period['start_idx'] >= 5:
            minimal_periods.append({
                'start_time': times[current_period['start_idx']],
                'end_time': times[current_period['end_idx']],
                'start_idx': current_period['start_idx'],
                'end_idx': current_period['end_idx'],
                'duration_hours': (times[current_period['end_idx']] - times[current_period['start_idx']]).total_seconds() / 3600,
                'avg_change': np.mean([pct_changes[i] for i in range(current_period['start_idx'], current_period['end_idx']+1) if not pd.isna(pct_changes[i])])
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
        'stats': stats,
        'all_changes': pct_changes
    }

def plot_tag_with_change_markers(data, tag_name, unit, anomaly_details, output_path, change_threshold=0.5):
    """
    Plot individual tag with minimal change markers

    Args:
        data: DataFrame with tag data
        tag_name: Name of the tag to plot
        unit: Unit name
        anomaly_details: Anomaly detection results
        output_path: Path to save plot
        change_threshold: Threshold for minimal change detection (%)
    """

    if tag_name not in data.columns:
        print(f"    Tag {tag_name} not found in data")
        return False

    # Filter out invalid data
    tag_data = data[['time', tag_name]].dropna()

    if len(tag_data) < 10:  # Need at least 10 points for meaningful analysis
        print(f"    Tag {tag_name}: Insufficient data points ({len(tag_data)})")
        return False

    print(f"    Plotting tag: {tag_name} ({len(tag_data):,} points)")

    # Detect minimal changes
    change_analysis = detect_minimal_changes(tag_data, tag_name, change_threshold)
    minimal_periods = change_analysis['minimal_change_periods']
    stats = change_analysis['stats']

    print(f"      Minimal change periods: {stats['periods_count']}")
    print(f"      Minimal change points: {stats['minimal_change_percentage']:.1f}% of data")

    # Create plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), height_ratios=[3, 1])

    # Main plot
    ax1.plot(tag_data['time'], tag_data[tag_name], 'b-', alpha=0.7, linewidth=0.8, label=f'{tag_name}')

    # Mark minimal change periods
    for period in minimal_periods:
        period_data = tag_data[
            (tag_data['time'] >= period['start_time']) &
            (tag_data['time'] <= period['end_time'])
        ]
        if not period_data.empty:
            ax1.fill_between(
                period_data['time'],
                period_data[tag_name].min() * 0.999,
                period_data[tag_name].max() * 1.001,
                alpha=0.2,
                color='yellow',
                label=f'Minimal Change (<{change_threshold}%)' if period == minimal_periods[0] else ""
            )

            # Add text annotation for long periods
            if period['duration_hours'] > 6:  # Mark periods longer than 6 hours
                mid_time = period['start_time'] + (period['end_time'] - period['start_time']) / 2
                mid_value = period_data[tag_name].mean()
                ax1.annotate(
                    f"~{period['duration_hours']:.1f}h\nstable",
                    xy=(mid_time, mid_value),
                    xytext=(10, 10),
                    textcoords='offset points',
                    fontsize=8,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                    ha='center'
                )

    # Add anomaly markers if available
    if anomaly_details and tag_name in anomaly_details:
        tag_anomalies = anomaly_details[tag_name]
        if 'anomaly_times' in tag_anomalies and len(tag_anomalies['anomaly_times']) > 0:
            anomaly_times = pd.to_datetime(tag_anomalies['anomaly_times'])
            anomaly_values = tag_anomalies.get('anomaly_values', [])

            if len(anomaly_values) == len(anomaly_times):
                ax1.scatter(anomaly_times, anomaly_values,
                           color='red', s=30, marker='x', alpha=0.8,
                           label=f'Anomalies ({len(anomaly_times)})', zorder=5)

    # Formatting
    ax1.set_title(f'{unit} - {tag_name}\nMinimal Change Analysis (Threshold: {change_threshold}%)', fontsize=14, fontweight='bold')
    ax1.set_ylabel(f'{tag_name}', fontsize=12)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right')

    # Statistics subplot
    change_pcts = change_analysis['all_changes']
    valid_changes = [x for x in change_pcts if not pd.isna(x)]

    if valid_changes:
        ax2.hist(valid_changes, bins=50, alpha=0.7, color='lightblue', edgecolor='black')
        ax2.axvline(change_threshold, color='red', linestyle='--', linewidth=2,
                   label=f'Threshold: {change_threshold}%')
        ax2.set_xlabel('Percentage Change from Previous Value (%)', fontsize=12)
        ax2.set_ylabel('Frequency', fontsize=12)
        ax2.set_title('Distribution of Value Changes', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # Add statistics text
        stats_text = f"""Statistics:
        • Total periods with <{change_threshold}% change: {stats['periods_count']}
        • Minimal change points: {stats['minimal_change_percentage']:.1f}%
        • Average change: {stats['avg_change']:.2f}%
        • Maximum change: {stats['max_change']:.1f}%"""

        ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes, fontsize=10,
                verticalalignment='top', horizontalalignment='left',
                bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
    else:
        ax2.text(0.5, 0.5, 'Insufficient data for change analysis',
                ha='center', va='center', transform=ax2.transAxes, fontsize=12)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    return True

def create_conditional_enhanced_plots(change_threshold=0.5):
    """
    Create enhanced plots with conditional plotting and minimal change markers

    Args:
        change_threshold: Percentage threshold for minimal change detection
    """

    print(f"CREATING CONDITIONAL ENHANCED PLOTS")
    print(f"Change Threshold: {change_threshold}% from last value")
    print("=" * 70)

    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (16, 12)

    # Create output directory
    reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = reports_dir / f"conditional_plots_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}")

    # Process each unit
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    cutoff_date = datetime.now() - timedelta(days=90)

    total_plots_created = 0
    units_processed = 0

    for unit in units:
        print(f"\nProcessing Unit: {unit}")

        # Find parquet files for this unit
        unit_files = glob.glob(f"data/processed/*{unit}*.parquet")

        if not unit_files:
            print(f"  SKIPPED: No parquet files found for {unit}")
            continue

        # Intelligent file selection - prioritize most recent dedup file
        dedup_files = [f for f in unit_files if 'dedup' in f]
        if dedup_files:
            selected_file = max(dedup_files, key=lambda x: os.path.getmtime(x))
        else:
            selected_file = max(unit_files, key=lambda x: os.path.getmtime(x))

        print(f"  Using file: {os.path.basename(selected_file)}")

        # Load data
        try:
            all_unit_data = pd.read_parquet(selected_file)
            print(f"  Loaded {len(all_unit_data):,} records")

            if all_unit_data.empty:
                print(f"  SKIPPED: No data loaded for {unit}")
                continue

        except Exception as e:
            print(f"  ERROR: Failed to load data for {unit}: {e}")
            continue

        # Filter to recent data
        all_unit_data['time'] = pd.to_datetime(all_unit_data['time'])
        recent_data = all_unit_data[all_unit_data['time'] >= cutoff_date].copy()

        if recent_data.empty:
            print(f"  SKIPPED: No recent data for {unit}")
            continue

        print(f"  Recent data: {len(recent_data):,} records")
        units_processed += 1

        # Create unit directory
        unit_dir = output_dir / unit
        unit_dir.mkdir(exist_ok=True)

        # Run anomaly detection
        anomaly_details = {}
        try:
            anomaly_results = smart_anomaly_detection(recent_data, unit)
            anomaly_details = anomaly_results.get('details', {})
            print(f"  Anomaly detection completed: {anomaly_results.get('total_anomalies', 0)} anomalies")
        except Exception as e:
            print(f"  WARNING: Anomaly detection failed: {e}")

        # Plot only tags with sufficient data
        tag_columns = [col for col in recent_data.columns if col != 'time']
        plots_for_unit = 0

        for tag in tag_columns:
            if recent_data[tag].notna().sum() < 10:  # Skip tags with too little data
                continue

            output_path = unit_dir / f"{unit}_{tag}_conditional.png"

            try:
                success = plot_tag_with_change_markers(
                    recent_data, tag, unit, anomaly_details,
                    output_path, change_threshold
                )
                if success:
                    plots_for_unit += 1
                    total_plots_created += 1
            except Exception as e:
                print(f"    ERROR plotting {tag}: {e}")
                continue

        print(f"  Created {plots_for_unit} plots for {unit}")

    print(f"\nSUMMARY:")
    print(f"Units processed: {units_processed}")
    print(f"Total plots created: {total_plots_created}")
    print(f"Output directory: {output_dir}")

    return output_dir

if __name__ == "__main__":
    # Allow command line threshold specification
    import sys
    threshold = 0.5
    if len(sys.argv) > 1:
        try:
            threshold = float(sys.argv[1])
        except ValueError:
            print("Invalid threshold specified, using default 0.5%")

    create_conditional_enhanced_plots(change_threshold=threshold)