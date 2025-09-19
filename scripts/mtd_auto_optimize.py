#!/usr/bin/env python3
"""
MTD Auto-Optimization

Searches for good Mahalanobis-Taguchi Distance (MTD) parameters on a unit's
recent data by balancing low baseline alarm rate with high sensitivity to
synthetic injected spikes. Produces an `mtd_config_{UNIT}.json` file that
ParquetAutoScanner will pick up automatically.

Scoring
- Baseline anomaly rate should be small (target <= 2%)
- After injection, anomaly rate should increase substantially
- Score = (after_rate - baseline_rate) - lambda * baseline_rate
  with lambda=2.0 by default

Usage
  python scripts/mtd_auto_optimize.py --unit K-31-01
  python scripts/mtd_auto_optimize.py --unit K-31-01 --days 90 --max-iters 24
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Tuple
import json
import numpy as np
import pandas as pd

import sys
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase


@dataclass
class MTDParams:
    resample: str = 'h'
    baseline_fraction: float = 0.7
    threshold_quantile: float = 0.995
    support_fraction: float = 0.75
    max_features: int = 20


def _build_feature_matrix(df: pd.DataFrame, resample: str, max_features: int) -> Tuple[pd.DataFrame, List[str]]:
    # Build pivoted, resampled matrix; speed tag + features limited
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df_time_indexed = df.set_index('time')
    pivot_df = df_time_indexed.groupby(['tag']).resample(resample)['value'].mean().unstack(level=0).reset_index()
    pivot_df.columns.name = None

    # Identify speed tag heuristically
    tags = [c for c in pivot_df.columns if c != 'time']
    speed_tags = [t for t in tags if any(k in t.upper() for k in ['SI-', 'SPEED', 'RPM', 'FREQ', 'ROTATION'])]
    if not speed_tags and tags:
        speed_tags = [tags[0]]
    if not speed_tags:
        raise RuntimeError('No tags available for MTD')

    primary = speed_tags[0]
    other = [t for t in tags if t != primary]
    k = min(max_features, len(other))
    cols = [primary] + other[:k]
    X = pivot_df[cols].ffill().dropna()
    if len(cols) < 2:
        raise RuntimeError('Insufficient features for MTD')
    return X, cols


def _mtd_anomaly_rate(df: pd.DataFrame, params: MTDParams) -> float:
    """Return anomaly rate based on MTD distances using given params."""
    if df.empty:
        return 0.0

    X, cols = _build_feature_matrix(df, params.resample, params.max_features)
    if len(X) < 60:
        return 0.0

    # Split baseline/test
    bf = min(0.9, max(0.5, params.baseline_fraction))
    bsz = int(len(X) * bf)
    Xb = X.iloc[:bsz].copy()
    Xt = X.iloc[bsz:].copy()
    if len(Xt) < 1:
        return 0.0

    # Standardize by baseline
    eps = 1e-8
    mu = Xb.mean()
    sigma = Xb.std(ddof=0).replace(0, np.nan)
    keep = sigma[sigma > eps].index.tolist()
    if len(keep) < 2:
        return 0.0
    Xb = ((Xb[keep] - mu[keep]) / sigma[keep]).dropna()
    Xt = ((Xt[keep] - mu[keep]) / sigma[keep]).dropna()
    if len(Xb) < 50 or len(Xt) < 1:
        return 0.0

    # Robust covariance
    cov_inv = None
    mean_vec = Xb.mean().values
    try:
        from sklearn.covariance import MinCovDet
        sf = min(0.95, max(0.5, params.support_fraction))
        mcd = MinCovDet(support_fraction=sf, random_state=42).fit(Xb.values)
        mean_vec = mcd.location_
        cov = mcd.covariance_
        cov_inv = np.linalg.pinv(cov)
    except Exception:
        try:
            from sklearn.covariance import LedoitWolf
            lw = LedoitWolf().fit(Xb.values)
            cov = lw.covariance_
            cov_inv = np.linalg.pinv(cov)
        except Exception:
            cov = np.cov(Xb.T)
            cov_inv = np.linalg.pinv(cov)

    def md(x):
        d = x - mean_vec
        return float(np.sqrt(d @ cov_inv @ d))

    # Threshold from baseline distances
    bd = [md(v.values) for _, v in Xb.iterrows()]
    q = min(0.9999, max(0.9, params.threshold_quantile))
    thr = float(np.quantile(bd, q)) if bd else 3.0

    ad = [md(v.values) for _, v in Xt.iterrows()]
    anomalies = sum(1 for d in ad if d > thr)
    return anomalies / len(Xt) if len(Xt) else 0.0


def _inject_spikes(df: pd.DataFrame, tags: List[str], rate: float, seed: int = 42) -> pd.DataFrame:
    out = df.copy()
    rng = np.random.default_rng(seed)
    for tag in tags:
        mask = out['tag'] == tag
        vals = out.loc[mask, 'value'].astype(float)
        if len(vals) < 20:
            continue
        mu = vals.mean()
        sd = vals.std() or 1.0
        n = len(vals)
        k = max(1, int(n * rate))
        idx_pos = rng.choice(np.arange(n), size=k, replace=False)
        idx = vals.iloc[idx_pos].index
        signs = rng.choice(np.array([1.0, -1.0]), size=k)
        out.loc[idx, 'value'] = mu + signs * (6.0 * sd)
    return out


def main():
    ap = argparse.ArgumentParser(description='Auto-optimize MTD parameters by synthetic injection')
    ap.add_argument('--unit', required=True, help='Unit to optimize (e.g., K-31-01)')
    ap.add_argument('--days', type=int, default=90, help='Days of history to use (default 90)')
    ap.add_argument('--inject-rate', type=float, default=0.005, help='Fraction per tag to spike (default 0.005)')
    ap.add_argument('--sample-tags', type=int, default=20, help='Injected tags (default 20)')
    ap.add_argument('--lambda', dest='lam', type=float, default=2.0, help='Penalty weight for baseline alarms')
    ap.add_argument('--max-iters', type=int, default=16, help='Number of param combos to try (grid size)')
    ap.add_argument('--out', type=Path, default=None, help='Optional custom output path for config JSON')
    args = ap.parse_args()

    db = ParquetDatabase()
    df = db.get_unit_data(args.unit)
    if df.empty:
        print('ERROR: No data available')
        return 1

    # Focus on recent window
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        cutoff = df['time'].max() - pd.Timedelta(days=args.days)
        df = df[df['time'] >= cutoff].copy()
        if df.empty:
            print('ERROR: No data in requested window')
            return 1

    # Candidate param grid (kept small for speed)
    grid: List[MTDParams] = []
    for resample in ['h']:
        for bf in [0.6, 0.7, 0.8]:
            for tq in [0.99, 0.995, 0.999]:
                for sf in [0.6, 0.75, 0.9]:
                    for mf in [10, 15]:
                        grid.append(MTDParams(resample, bf, tq, sf, mf))

    # Limit grid size
    grid = grid[:max(1, args.max_iters)]

    # Pick injection tags
    counts = df['tag'].value_counts()
    candidates = [t for t, c in counts.items() if c >= 200]
    if not candidates:
        print('ERROR: Not enough dense tags for injection')
        return 1
    inject_tags = candidates[:args.sample_tags]

    best_score = -1e9
    best: Dict[str, Any] | None = None

    for i, p in enumerate(grid, 1):
        try:
            base_rate = _mtd_anomaly_rate(df, p)
            df_inj = _inject_spikes(df, inject_tags, args.inject_rate)
            after_rate = _mtd_anomaly_rate(df_inj, p)
            score = (after_rate - base_rate) - args.lam * base_rate
            print(f"[{i}/{len(grid)}] res={p.resample}, bf={p.baseline_fraction}, q={p.threshold_quantile}, sf={p.support_fraction}, mf={p.max_features} | base={base_rate:.4f}, after={after_rate:.4f}, score={score:.4f}")

            if score > best_score:
                best_score = score
                best = {
                    'unit': args.unit,
                    'params': p.__dict__,
                    'baseline_rate': base_rate,
                    'after_rate': after_rate,
                    'score': score
                }
        except Exception as e:
            print(f"  Skipped combo due to error: {e}")
            continue

    if not best:
        print('No viable parameter set found')
        return 1

    # Write config file that scanner will consume
    cfg = dict(best['params'])
    out_path = args.out or Path(f"mtd_config_{args.unit}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, indent=2)

    print('\nBEST CONFIG')
    print(json.dumps(best, indent=2))
    print(f"Saved MTD config to: {out_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
