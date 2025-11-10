#!/usr/bin/env python3
"""
Debug why primary detection (2.5-sigma + AE) returns 0 anomalies
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.hybrid_anomaly_detection import _sigma_2p5_candidates, _try_live_ae_anomaly_times, _load_autoencoder_anomaly_times

def debug_primary_detection():
    """Debug the primary detection pipeline"""

    print("DEBUG: PRIMARY DETECTION PIPELINE")
    print("=" * 50)

    # Load data
    db = ParquetDatabase()
    unit = "K-31-01"
    df = db.get_unit_data(unit)

    if df.empty:
        print("No data available")
        return

    # Filter to recent data (last 7 days)
    cutoff = datetime.now() - timedelta(days=7)
    df = df[pd.to_datetime(df['time']) >= cutoff].copy()

    print(f"Unit: {unit}")
    print(f"Recent records: {len(df):,}")
    print(f"Unique tags: {df['tag'].nunique()}")

    if df.empty:
        print("No recent data")
        return

    # Test each tag individually
    unique_tags = df['tag'].unique()

    for tag in unique_tags:
        print(f"\n--- TAG: {tag} ---")
        tag_data = df[df['tag'] == tag].copy()

        print(f"Tag data points: {len(tag_data)}")

        if 'value' in tag_data.columns:
            values = tag_data['value'].dropna()
            print(f"Valid values: {len(values)}")

            if len(values) > 0:
                mean_val = values.mean()
                std_val = values.std()
                print(f"Mean: {mean_val:.3f}")
                print(f"Std: {std_val:.3f}")

                if std_val > 0:
                    # Calculate z-scores manually
                    z_scores = np.abs((values - mean_val) / std_val)
                    candidates = z_scores > 2.5
                    candidate_count = candidates.sum()

                    print(f"Z-scores > 2.5: {candidate_count}")
                    print(f"Candidate rate: {candidate_count/len(values)*100:.2f}%")

                    if candidate_count > 0:
                        candidate_values = values[candidates]
                        print(f"Candidate value range: {candidate_values.min():.3f} to {candidate_values.max():.3f}")
                else:
                    print("WARNING: Standard deviation is 0 - no variation in data!")

    print("\n--- TESTING FULL 2.5-SIGMA PIPELINE ---")

    # Test the actual _sigma_2p5_candidates function
    try:
        candidate_times, by_tag, times_by_tag = _sigma_2p5_candidates(df)

        print(f"Total candidate times: {len(candidate_times)}")
        print(f"Tags with candidates: {len(by_tag)}")

        for tag, info in by_tag.items():
            count = info.get('count', 0)
            rate = info.get('rate', 0) * 100
            print(f"  {tag}: {count} candidates ({rate:.2f}%)")

        print(f"Times by tag: {len(times_by_tag)}")
        for tag, times in times_by_tag.items():
            print(f"  {tag}: {len(times)} timestamp(s)")

    except Exception as e:
        print(f"ERROR in _sigma_2p5_candidates: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- TESTING AUTOENCODER PIPELINE ---")

    # Test AutoEncoder pipeline
    project_root = Path(__file__).parent

    # Test live AE
    try:
        ae_times_live = _try_live_ae_anomaly_times(df, project_root)
        print(f"Live AE times: {len(ae_times_live)}")
    except Exception as e:
        print(f"Live AE failed: {e}")
        ae_times_live = []

    # Test file-based AE
    try:
        ae_times_file = _load_autoencoder_anomaly_times(project_root)
        print(f"File-based AE times: {len(ae_times_file)}")
    except Exception as e:
        print(f"File-based AE failed: {e}")
        ae_times_file = set()

    # Check if AutoEncoder directory exists
    ae_dir = project_root / 'AutoEncoder'
    print(f"AutoEncoder directory exists: {ae_dir.exists()}")
    if ae_dir.exists():
        csv_path = ae_dir / 'anomaly_timestamps_hybrid.csv'
        print(f"AE CSV file exists: {csv_path.exists()}")
        if csv_path.exists():
            print(f"AE CSV file size: {csv_path.stat().st_size} bytes")

if __name__ == "__main__":
    debug_primary_detection()