#!/usr/bin/env python3
"""
Controlled Anomaly Plotting with Smart Limits
Prevents excessive plot generation while maintaining analytical value
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from pi_monitor.smart_anomaly_detection import smart_anomaly_detection
from pi_monitor.plot_controls import (
    PlotController,
    create_controlled_report,
    build_scan_root_dir,
    ensure_unit_dir,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_controlled_plots(controller: PlotController = None) -> Path:
    """Create plots with intelligent controls and limits."""

    if controller is None:
        controller = PlotController()

    print("CREATING CONTROLLED ANOMALY PLOTS")
    print("=" * 60)
    print(f"Limits: {controller.max_plots_per_unit} plots/unit, {controller.max_units_per_report} units/report")
    print(f"Max total plots: {controller.max_total_plots}")

    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (14, 8)  # Smaller than before

    # Create controlled folder structure with day-of-scan master folder
    main_output_dir = build_scan_root_dir(Path("reports"))
    main_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {main_output_dir}")

    # Get available units and filter
    all_units = ['07-MT01-K001', 'C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302',
                 'C-201', 'C-202', 'K-12-01', 'K-16-01', 'K-19-01', 'K-31-01', 'XT-07002']

    units_to_process = controller.filter_units_for_analysis(all_units)

    print(f"\nProcessing {len(units_to_process)} priority units:")
    for unit in units_to_process:
        print(f"  - {unit}")

    # Analysis period - last 30 days (more reasonable than 90)
    cutoff_date = datetime.now() - timedelta(days=30)
    print(f"\nAnalysis period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")

    overall_results = {}
    total_plots_created = 0

    for unit_idx, unit in enumerate(units_to_process):
        print(f"\n[{unit_idx+1}/{len(units_to_process)}] Processing unit: {unit}")
        print("-" * 50)

        # Create unit directory
        unit_dir = ensure_unit_dir(main_output_dir, unit)

        # Find parquet files for this unit
        unit_files = glob.glob(f"data/processed/*{unit}*.parquet")

        if not unit_files:
            print(f"  No parquet files found for {unit}")
            continue

        print(f"  Data sources: {len(unit_files)} file(s)")

        # Load and combine data
        all_unit_data = pd.DataFrame()
        for file in unit_files:
            try:
                df = pd.read_parquet(file)
                all_unit_data = pd.concat([all_unit_data, df], ignore_index=True)
            except Exception as e:
                print(f"    Error loading {file}: {e}")
                continue

        if all_unit_data.empty:
            print(f"  No data loaded for {unit}")
            continue

        # Filter to analysis period
        all_unit_data['time'] = pd.to_datetime(all_unit_data['time'])
        recent_data = all_unit_data[all_unit_data['time'] >= cutoff_date].copy()

        print(f"  Records: {len(recent_data):,} | Tags: {recent_data['tag'].nunique()}")

        if recent_data.empty:
            print(f"  No data in analysis period")
            continue

        # Run smart anomaly detection
        try:
            print(f"  Running anomaly detection...")
            anomaly_results = smart_anomaly_detection(recent_data, unit)

            unit_status = anomaly_results.get('unit_status', {})
            total_anomalies = anomaly_results.get('total_anomalies', 0)
            by_tag = anomaly_results.get('by_tag', {})

            print(f"    Status: {unit_status.get('status', 'UNKNOWN')}")
            print(f"    Anomalies: {total_anomalies:,}")
            print(f"    Problematic tags: {len(by_tag)}")

            # Filter tags using controller
            tags_to_plot = controller.filter_tags_for_plotting(by_tag, unit)

            print(f"    Creating {len(tags_to_plot)} plots (filtered from {len(by_tag)} candidates)")

            # Create plots for filtered tags
            unit_plots_created = 0
            for i, (tag, tag_info) in enumerate(tags_to_plot):
                try:
                    create_controlled_tag_plot(unit, tag, tag_info, recent_data, unit_dir, cutoff_date)
                    unit_plots_created += 1
                    print(f"      [{i+1:2d}] ✓ {tag[:50]}...")
                except Exception as e:
                    print(f"      [{i+1:2d}] ✗ Plot failed: {e}")

            total_plots_created += unit_plots_created

            # Create unit summary
            create_unit_summary_report(unit, unit_dir, recent_data, anomaly_results, tags_to_plot, cutoff_date)

            # Store results
            overall_results[unit] = {
                'unit_status': unit_status,
                'total_anomalies': total_anomalies,
                'problematic_tags': len(by_tag),
                'plots_created': unit_plots_created,
                'total_records': len(recent_data),
                'unique_tags': recent_data['tag'].nunique()
            }

        except Exception as e:
            print(f"    Error in anomaly detection: {e}")
            continue

    # Create overall summary
    create_overall_summary(main_output_dir, overall_results, cutoff_date, total_plots_created, controller)

    print(f"\n{'='*60}")
    print(f"CONTROLLED ANALYSIS COMPLETED")
    print(f"{'='*60}")
    print(f"Location: {main_output_dir}")
    print(f"Units processed: {len(overall_results)}")
    print(f"Total plots created: {total_plots_created}")
    print(f"Average plots per unit: {total_plots_created/len(overall_results) if overall_results else 0:.1f}")

    return main_output_dir


def create_controlled_tag_plot(unit: str, tag: str, tag_info: dict, data: pd.DataFrame,
                              unit_dir: Path, cutoff_date: datetime):
    """Create a controlled, focused tag plot."""

    # Get tag data
    tag_data = data[data['tag'] == tag].copy()
    if tag_data.empty or len(tag_data) < 10:
        return

    tag_data = tag_data.sort_values('time')

    # Extract key info
    count = tag_info.get('count', 0)
    rate = tag_info.get('rate', 0) * 100
    method = tag_info.get('method', 'Unknown')
    confidence = tag_info.get('confidence', 'UNKNOWN')

    # Create focused plot (smaller than original)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), height_ratios=[3, 1])

    # Main time series plot
    ax1.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1, label='Value')

    # Highlight anomalies if present
    anomaly_data = tag_data[tag_data.get('is_anomaly', False)]
    if not anomaly_data.empty:
        ax1.scatter(anomaly_data['time'], anomaly_data['value'],
                   color='red', s=30, alpha=0.8, zorder=5, label='Anomalies')

    ax1.set_title(f"{unit} | {tag}\n"
                 f"Anomalies: {count} ({rate:.1f}%) | Method: {method} | Confidence: {confidence}")
    ax1.set_ylabel('Value')
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    # Simplified histogram
    ax2.hist(tag_data['value'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
    ax2.set_xlabel('Value')
    ax2.set_ylabel('Count')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save with controlled naming
    safe_tag = "".join(c for c in tag if c.isalnum() or c in "._-")[:50]
    filename = f"{unit}_{safe_tag}_ANALYSIS.png"
    filepath = unit_dir / filename

    plt.savefig(filepath, dpi=100, bbox_inches='tight')  # Lower DPI for smaller files
    plt.close()


def create_unit_summary_report(unit: str, unit_dir: Path, data: pd.DataFrame,
                              anomaly_results: dict, plots_created: list, cutoff_date: datetime):
    """Create a concise unit summary report."""

    summary_file = unit_dir / f"{unit}_SUMMARY.txt"

    with open(summary_file, 'w') as f:
        f.write(f"UNIT ANALYSIS SUMMARY: {unit}\n")
        f.write(f"=" * 50 + "\n\n")

        f.write(f"Analysis Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Unit status
        unit_status = anomaly_results.get('unit_status', {})
        f.write(f"Unit Status: {unit_status.get('status', 'UNKNOWN')}\n")
        f.write(f"Status Message: {unit_status.get('message', 'N/A')}\n\n")

        # Data summary
        f.write(f"Data Summary:\n")
        f.write(f"  Total Records: {len(data):,}\n")
        f.write(f"  Unique Tags: {data['tag'].nunique()}\n")
        f.write(f"  Time Range: {data['time'].min()} to {data['time'].max()}\n\n")

        # Anomaly summary
        total_anomalies = anomaly_results.get('total_anomalies', 0)
        by_tag = anomaly_results.get('by_tag', {})

        f.write(f"Anomaly Analysis:\n")
        f.write(f"  Total Anomalies: {total_anomalies:,}\n")
        f.write(f"  Problematic Tags: {len(by_tag)}\n")
        f.write(f"  Plots Created: {len(plots_created)}\n")
        f.write(f"  Detection Method: {anomaly_results.get('method', 'Unknown')}\n\n")

        # Top problematic tags
        if plots_created:
            f.write(f"Top Problematic Tags (Plotted):\n")
            for i, (tag, tag_info) in enumerate(plots_created):
                count = tag_info.get('count', 0)
                rate = tag_info.get('rate', 0) * 100
                f.write(f"  {i+1:2d}. {tag}\n")
                f.write(f"      Anomalies: {count} ({rate:.1f}%)\n")
        else:
            f.write("No problematic tags requiring plots.\n")


def create_overall_summary(output_dir: Path, results: dict, cutoff_date: datetime,
                          total_plots: int, controller: PlotController):
    """Create overall analysis summary."""

    summary_file = output_dir / "ANALYSIS_SUMMARY.txt"

    with open(summary_file, 'w') as f:
        f.write("CONTROLLED ANOMALY ANALYSIS SUMMARY\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write(f"Detection: Smart Anomaly Detection (MTD + Isolation Forest)\n\n")

        # Control settings
        f.write(f"Plot Controls Applied:\n")
        f.write(f"  Max plots per unit: {controller.max_plots_per_unit}\n")
        f.write(f"  Max units per report: {controller.max_units_per_report}\n")
        f.write(f"  Max total plots: {controller.max_total_plots}\n\n")

        # Results summary
        f.write(f"Analysis Results:\n")
        f.write(f"  Units processed: {len(results)}\n")
        f.write(f"  Total plots created: {total_plots}\n")
        f.write(f"  Average plots per unit: {total_plots/len(results) if results else 0:.1f}\n\n")

        # Unit breakdown
        f.write("Unit Breakdown:\n")
        for unit, data in results.items():
            status = data['unit_status'].get('status', 'UNKNOWN')
            f.write(f"  {unit:12} | {status:8} | {data['total_anomalies']:5,} anomalies | {data['plots_created']} plots\n")


if __name__ == "__main__":
    # Create controlled analysis
    reports_dir = Path("reports")

    def controlled_analysis(**kwargs):
        controller = kwargs.get('controller', PlotController())
        return create_controlled_plots(controller)

    report_path = create_controlled_report(controlled_analysis, reports_dir)
    print(f"\nControlled analysis completed: {report_path}")
