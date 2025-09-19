#!/usr/bin/env python3
"""
Isolation Forest Auto-Optimization

Tunes Isolation Forest parameters (contamination, n_estimators) to maximize
delta between injected vs baseline anomaly rate on recent data while keeping
baseline low. Produces `if_config_{UNIT}.json` consumed at runtime.
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
from sklearn.ensemble import IsolationForest


@dataclass
class IFParams:
    contamination: float
    n_estimators: int


def _prep_matrix(df: pd.DataFrame, resample: str = 'h', max_features: int = 25) -> Tuple[pd.DataFrame, List[str]]:
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    X = df.set_index('time').groupby('tag').resample(resample)['value'].mean().unstack(level=0)
    X.columns.name = None
    X = X.ffill().dropna()
    tags = [c for c in X.columns]
    if len(tags) > max_features:
        tags = tags[:max_features]
        X = X[tags]
    return X, tags


def _if_rate(X: pd.DataFrame, params: IFParams) -> float:
    if X.empty:
        return 0.0
    # Split baseline/test
    bsz = int(len(X) * 0.7)
    Xb = X.iloc[:bsz]
    Xt = X.iloc[bsz:]
    if len(Xt) < 1:
        return 0.0
    iso = IsolationForest(contamination=params.contamination, n_estimators=params.n_estimators, random_state=42, n_jobs=-1)
    iso.fit(Xb)
    labels = iso.predict(Xt)
    return float((labels == -1).sum() / len(labels))


def _inject(X: pd.DataFrame, rate: float, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    Y = X.copy()
    # inject into 5 random columns
    cols = list(Y.columns)
    if not cols:
        return Y
    k = min(5, len(cols))
    inj_cols = rng.choice(np.array(cols), size=k, replace=False)
    for c in inj_cols:
        n = len(Y)
        m = max(1, int(n * rate))
        idx = rng.choice(np.arange(n), size=m, replace=False)
        col = Y[c]
        mu = col.mean()
        sd = col.std() or 1.0
        spikes = mu + rng.choice(np.array([1.0, -1.0]), size=m) * 6.0 * sd
        Y.loc[Y.index[idx], c] = spikes
    return Y


def main():
    ap = argparse.ArgumentParser(description='Auto-optimize Isolation Forest params for a unit')
    ap.add_argument('--unit', required=True)
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--grid', type=str, default='0.01,0.02,0.03;100,200')
    ap.add_argument('--inject-rate', type=float, default=0.005)
    ap.add_argument('--lambda', dest='lam', type=float, default=2.0)
    ap.add_argument('--out', type=Path, default=None)
    args = ap.parse_args()

    db = ParquetDatabase()
    df = db.get_unit_data(args.unit)
    if df.empty:
        print('ERROR: No data available')
        return 1
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        cutoff = df['time'].max() - pd.Timedelta(days=args.days)
        df = df[df['time'] >= cutoff].copy()
        if df.empty:
            print('ERROR: No data in window')
            return 1
    X, tags = _prep_matrix(df, 'h', 25)
    if X.empty:
        print('ERROR: matrix empty')
        return 1

    cont_str, n_est_str = args.grid.split(';')
    cont_vals = [float(s) for s in cont_str.split(',') if s]
    n_est_vals = [int(s) for s in n_est_str.split(',') if s]

    best = None
    best_score = -1e9
    for c in cont_vals:
        for ne in n_est_vals:
            p = IFParams(c, ne)
            base = _if_rate(X, p)
            Xinj = _inject(X, rate=args.inject_rate)
            aft = _if_rate(Xinj, p)
            score = (aft - base) - args.lam * base
            print(f"c={c:.3f}, n={ne}: base={base:.4f}, after={aft:.4f}, score={score:.4f}")
            if score > best_score:
                best_score = score
                best = {'contamination': c, 'n_estimators': ne, 'baseline_rate': base, 'after_rate': aft, 'score': score}

    if not best:
        print('No best params found')
        return 1

    out_path = args.out or Path(f"if_config_{args.unit}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({'contamination': best['contamination'], 'n_estimators': best['n_estimators']}, f, indent=2)
    print('\nBEST IF CONFIG')
    print(json.dumps(best, indent=2))
    print(f"Saved IF config to: {out_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
