#!/usr/bin/env python3
"""
Option [2] Sensitivity Backtest

Evaluates whether the Option [2] "UNIT DEEP ANALYSIS" anomaly detection is
sensitively responding to injected abnormalities on 1-year Parquet data.

Method
- Load a unit's 1-year dataset via ParquetDatabase
- Run Option [2]'s detection pipeline to get baseline anomaly counts by tag
- Inject synthetic anomalies into a sample of tags (spikes beyond 5–6 sigma)
- Re-run the same detection pipeline on the injected DataFrame
- Report tag-level recall (fraction of injected tags flagged) and an estimate
  of false-positive rate on non-injected tags (delta > 0 without injection)

This script does not write any data back to disk.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from datetime import datetime
import json
import random
import numpy as np
import pandas as pd

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.parquet_auto_scan import ParquetAutoScanner


def run_option2_detection_on_df(scanner: ParquetAutoScanner, df: pd.DataFrame, unit: str) -> dict:
    """Run Option [2]-equivalent detection on a provided DataFrame.

    Tries smart enhanced detection; if it declines due to unit status,
    falls back to the simple detector for sensitivity testing purposes.
    """
    try:
        # Use the same enhanced path Option [2] uses internally
        res = scanner._detect_anomalies_enhanced(df, unit)  # type: ignore[attr-defined]
        # If analysis was skipped because unit offline, fall back to simple
        if not res or (isinstance(res, dict) and not res.get('analysis_performed', True)):
            res = scanner._detect_simple_anomalies(df)  # type: ignore[attr-defined]
        return res or {}
    except Exception:
        # Fallback if internals change
        return scanner._detect_simple_anomalies(df)  # type: ignore[attr-defined]


def inject_spike_anomalies(df: pd.DataFrame, target_tags: list[str], rate: float, seed: int = 42) -> pd.DataFrame:
    """Return a copy of df with spike anomalies injected into target tags.

    For each target tag, randomly select ~rate fraction of rows and add
    a large spike (±6 sigma) to the value to ensure exceedance of 4-sigma.
    """
    rng = np.random.default_rng(seed)
    out = df.copy()

    for tag in target_tags:
        tag_mask = out['tag'] == tag
        tag_df = out.loc[tag_mask]
        values = tag_df['value'].astype(float)
        if len(values) < 20:
            continue
        mean = values.mean()
        std = values.std() or 1.0

        # Choose indices to perturb
        n = len(values)
        k = max(1, int(n * rate))
        idx_positions = rng.choice(np.arange(n), size=k, replace=False)
        idx = values.iloc[idx_positions].index

        # Apply ±6 sigma spikes with random sign
        signs = rng.choice(np.array([1.0, -1.0]), size=k)
        spikes = mean + signs * (6.0 * std)
        out.loc[idx, 'value'] = spikes

    return out


def main():
    p = argparse.ArgumentParser(description="Option [2] sensitivity backtest with 1-year data")
    p.add_argument('--unit', default='K-31-01', help='Unit to test (default: K-31-01)')
    p.add_argument('--sample-tags', type=int, default=20, help='Number of tags to inject (default: 20)')
    p.add_argument('--inject-rate', type=float, default=0.005, help='Fraction of points per tag to spike (default: 0.005)')
    p.add_argument('--seed', type=int, default=42, help='Random seed (default: 42)')
    p.add_argument('--out', type=Path, default=Path('backtest_option2_sensitivity.json'), help='Output JSON summary')
    args = p.parse_args()

    db = ParquetDatabase()
    scanner = ParquetAutoScanner()

    print(f"Loading 1-year data for unit: {args.unit}")
    df = db.get_unit_data(args.unit)
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        print("ERROR: No suitable data available (need 'tag' and 'value' columns)")
        return 1

    df['time'] = pd.to_datetime(df['time']) if 'time' in df.columns else None
    print(f"Records: {len(df):,}; Tags: {df['tag'].nunique()}\n")

    # Baseline detection (no injection)
    print("Running baseline Option [2] detection...")
    base = run_option2_detection_on_df(scanner, df, args.unit)
    base_by_tag = base.get('by_tag', {}) if isinstance(base, dict) else {}

    # Choose tags to inject: prefer those with many observations
    tag_counts = df['tag'].value_counts()
    candidate_tags = [t for t, c in tag_counts.items() if c >= 200]  # ensure enough points
    if not candidate_tags:
        print("ERROR: Not enough data per tag to run backtest")
        return 1
    rng = random.Random(args.seed)
    inject_tags = rng.sample(candidate_tags, k=min(args.sample_tags, len(candidate_tags)))

    print(f"Injecting spikes into {len(inject_tags)} tags @ rate={args.inject_rate:.3%}")
    injected_df = inject_spike_anomalies(df, inject_tags, rate=args.inject_rate, seed=args.seed)

    # Detection after injection
    print("Running Option [2] detection on injected data...")
    after = run_option2_detection_on_df(scanner, injected_df, args.unit)
    after_by_tag = after.get('by_tag', {}) if isinstance(after, dict) else {}

    # Compute tag-level sensitivity
    detected_injected = 0
    for tag in inject_tags:
        base_count = base_by_tag.get(tag, {}).get('count', 0)
        after_count = after_by_tag.get(tag, {}).get('count', 0)
        if after_count > base_count:
            detected_injected += 1

    recall = detected_injected / len(inject_tags) if inject_tags else 0.0

    # Estimate false positives on non-injected tags by delta increase
    non_injected = set(df['tag'].unique()) - set(inject_tags)
    false_alarms = 0
    checked = 0
    for tag in non_injected:
        base_count = base_by_tag.get(tag, {}).get('count', 0)
        after_count = after_by_tag.get(tag, {}).get('count', 0)
        if base_count is None or after_count is None:
            continue
        checked += 1
        if after_count > base_count:
            false_alarms += 1
    fpr = (false_alarms / checked) if checked else 0.0

    summary = {
        'unit': args.unit,
        'records': int(len(df)),
        'unique_tags': int(df['tag'].nunique()),
        'injected_tags': inject_tags,
        'inject_rate': args.inject_rate,
        'baseline_total_anomalies': int(base.get('total_anomalies', 0)) if isinstance(base, dict) else 0,
        'after_total_anomalies': int(after.get('total_anomalies', 0)) if isinstance(after, dict) else 0,
        'tag_recall': recall,
        'tag_false_positive_rate': fpr,
        'timestamp': datetime.now().isoformat(),
    }

    print("\nSENSITIVITY SUMMARY")
    print("=" * 60)
    print(f"Injected tags: {len(inject_tags)}")
    print(f"Detected injected (tag-level): {detected_injected}/{len(inject_tags)} => Recall {recall*100:.1f}%")
    print(f"Non-injected tags showing new anomalies: {false_alarms}/{checked} => FPR {fpr*100:.2f}%")
    print(f"Total anomalies (baseline -> injected): {summary['baseline_total_anomalies']} -> {summary['after_total_anomalies']}")

    try:
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        print(f"\nSaved summary: {args.out}")
    except Exception as e:
        print(f"Could not save summary: {e}")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
