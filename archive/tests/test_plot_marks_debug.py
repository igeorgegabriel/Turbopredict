#!/usr/bin/env python3
"""
Debug why marks don't show on enhanced plots
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def debug_plot_marks():
    """Debug plot marking issues"""

    print("DEBUG: ENHANCED PLOT MARKS")
    print("=" * 40)

    # Load data
    db = ParquetDatabase()
    unit = "K-31-01"
    df = db.get_unit_data(unit)

    # Filter to recent data
    cutoff = datetime.now() - timedelta(days=7)
    df = df[pd.to_datetime(df['time']) >= cutoff].copy()

    if df.empty:
        print("No data available")
        return

    # Get the tag
    tag = df['tag'].iloc[0]
    tag_data = df[df['tag'] == tag].copy().sort_values('time')

    print(f"Tag: {tag}")
    print(f"Data points: {len(tag_data)}")

    # Run anomaly detection
    anomaly_results = smart_anomaly_detection(df, unit)

    # Extract details (same as enhanced_plot_anomalies.py)
    details = anomaly_results.get('details', {})
    sigma_by_tag = details.get('sigma_2p5_times_by_tag', {})
    verify_by_tag = details.get('verification_times_by_tag', {})
    ae_times = details.get('ae_times', [])

    print(f"Details keys: {list(details.keys())}")
    print(f"Sigma tags: {list(sigma_by_tag.keys())}")
    print(f"Verify tags: {list(verify_by_tag.keys())}")
    print(f"AE times count: {len(ae_times)}")

    # Create test plot
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot time series
    ax.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1.5, label='Values')

    # Test marking logic (same as enhanced_plot_anomalies.py)
    def _mask_times(ts_list):
        if not ts_list:
            return np.zeros(len(tag_data), dtype=bool)
        tset = set(pd.to_datetime(ts_list))
        series_t = pd.to_datetime(tag_data['time'])
        if getattr(series_t.dt, 'tz', None) is not None:
            series_t = series_t.dt.tz_convert(None)
        return series_t.isin(tset).values

    marks_added = 0

    # 2.5-sigma marks
    sigma_times = sigma_by_tag.get(tag, [])
    print(f"Sigma times for {tag}: {len(sigma_times)}")
    if sigma_times:
        print(f"First 3 sigma times: {sigma_times[:3]}")

    ms = _mask_times(sigma_times)
    sigma_matches = ms.sum()
    print(f"Sigma matches in tag data: {sigma_matches}")

    if ms.any():
        ax.scatter(tag_data['time'][ms], tag_data['value'][ms],
                  facecolors='none', edgecolors='purple', s=40,
                  label=f'2.5σ candidate ({sigma_matches})', zorder=6)
        marks_added += sigma_matches

    # MTD and IF marks
    vdetail = verify_by_tag.get(tag, {})
    mtd_times = vdetail.get('mtd', [])
    if_times = vdetail.get('if', [])

    print(f"MTD times: {len(mtd_times)}")
    print(f"IF times: {len(if_times)}")

    mm = _mask_times(mtd_times)
    mtd_matches = mm.sum()
    if mm.any():
        ax.scatter(tag_data['time'][mm], tag_data['value'][mm],
                  c='red', marker='x', s=50,
                  label=f'MTD confirmed ({mtd_matches})', zorder=7)
        marks_added += mtd_matches

    im = _mask_times(if_times)
    if_matches = im.sum()
    if im.any():
        ax.scatter(tag_data['time'][im], tag_data['value'][im],
                  c='orange', marker='s', s=30,
                  label=f'IF confirmed ({if_matches})', zorder=7)
        marks_added += if_matches

    # AE vertical lines
    ae_lines_added = 0
    for t in pd.to_datetime(ae_times):
        ax.axvline(t, color='gray', linestyle='--', alpha=0.3)
        ae_lines_added += 1

    ax.set_title(f'{unit} - {tag}\nDEBUG: Anomaly Marks Test')
    ax.set_ylabel('Value')
    ax.grid(True, alpha=0.3)
    ax.legend()

    print(f"\nMARKS SUMMARY:")
    print(f"Total scatter marks added: {marks_added}")
    print(f"AE vertical lines added: {ae_lines_added}")

    # Save plot
    plt.tight_layout()
    plt.savefig('debug_marks_test.png', dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Debug plot saved: debug_marks_test.png")

    return marks_added > 0 or ae_lines_added > 0

if __name__ == "__main__":
    has_marks = debug_plot_marks()
    if has_marks:
        print("SUCCESS: Marks should be visible!")
    else:
        print("ISSUE: No marks were added to plot")