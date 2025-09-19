#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
import json
import pandas as pd

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.breakout import detect_breakouts
import argparse


def _auto_speed_window(pivot: pd.DataFrame, speed_col: str, recent_days: int = 90) -> list[float] | None:
    try:
        if 'time' in pivot.columns:
            cutoff = pivot['time'].max() - pd.Timedelta(days=recent_days)
            s = pivot.loc[pivot['time'] >= cutoff, speed_col].dropna()
        else:
            s = pivot[speed_col].dropna()
        if len(s) < 50:
            return None
        binned = (s/10.0).round().astype(int)
        mode_bin = binned.value_counts().idxmax()
        center = float(mode_bin * 10)
        half = max(100.0, center * 0.03)
        return [center - half, center + half]
    except Exception:
        return None


def _load_unit_cfg(unit: str) -> dict:
    p = project_root / f"mtd_config_{unit}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def backtest_unit(unit: str, chunk_days: int = 30) -> dict:
    db = ParquetDatabase()
    df = db.get_unit_data(unit)
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return {"unit": unit, "status": "no_data"}
    df['time'] = pd.to_datetime(df['time'])

    # Load unit config if present
    cfg = _load_unit_cfg(unit)

    # Build speed-only resample (memory light) to pick speed column and auto window
    speed_series = (
        df[df['tag'].str.contains('SI-|KI-|SPEED|RPM', regex=True, na=False)]
        .pivot_table(index='time', columns='tag', values='value')
        .resample('10min').mean().ffill()
    )
    if speed_series.empty:
        # Fallback: build full pivot but only first 2k rows (avoid OOM)
        speed_col = None
    else:
        speed_col = speed_series.columns[0]

    # If we couldn't find speed column, try again from a small full pivot slice
    if speed_col is None:
        head_times = df['time'].sort_values().unique()[:2000]
        small = df[df['time'].isin(head_times)]
        small_pivot = small.pivot_table(index='time', columns='tag', values='value').resample('10min').mean().ffill()
        cand = [c for c in small_pivot.columns if any(k in str(c) for k in ('SI-','KI-','SPEED','RPM'))]
        speed_col = cand[0] if cand else (small_pivot.columns[0] if len(small_pivot.columns) else None)

    # Compose chunk boundaries over recent horizon
    horizon_days = 30
    end = df['time'].max()
    start = max(df['time'].min(), end - pd.Timedelta(days=horizon_days))
    # Build auto window from speed-only series if available
    if cfg.get('speed_window_rpm'):
        bw = cfg['speed_window_rpm']
    elif not speed_series.empty and speed_col in speed_series.columns:
        speed_pivot = speed_series.reset_index()
        bw = _auto_speed_window(speed_pivot, speed_col, recent_days=90)
    else:
        bw = None

    # Iterate chunks
    current = start
    counts: dict[str, float] = {}
    total_points = 0
    while current < end:
        chunk_end = min(current + pd.Timedelta(days=chunk_days), end)
        chunk = df[(df['time'] >= current) & (df['time'] < chunk_end)]
        if not chunk.empty:
            # Pivot this chunk only
            pivot = chunk.pivot_table(index='time', columns='tag', values='value').resample('10min').mean().ffill().reset_index()
            tags = [c for c in pivot.columns if c not in ('time', speed_col) and isinstance(c, str)]
            if len(tags) > 0:
                recent_mask = (pivot['time'] >= (chunk_end - pd.Timedelta(days=chunk_days)))
                ql, qh = 0.10, 0.90
                if 'band_percentiles' in cfg and isinstance(cfg['band_percentiles'], list) and len(cfg['band_percentiles']) == 2:
                    ql = float(cfg['band_percentiles'][0]) / 100.0
                    qh = float(cfg['band_percentiles'][1]) / 100.0
                br = detect_breakouts(
                    pivot, speed_col=speed_col if speed_col in pivot.columns else tags[0], tag_cols=tags,
                    window=int(cfg.get('break_window', 20) or 20), q_low=ql, q_high=qh,
                    persist=int(cfg.get('break_persist', 2) or 2), persist_window=int(cfg.get('break_persist_window', 3) or 3), cooldown=int(cfg.get('break_cooldown', 5) or 5),
                    speed_window=bw, recent_mask=recent_mask
                )
                for k, info in br.items():
                    counts[k] = counts.get(k, 0.0) + float(info.get('count', 0.0))
                total_points += int(recent_mask.sum())
        current = chunk_end

    total_tags = len(set(k for k in counts.keys()))
    flagged = sum(1 for v in counts.values() if v > 0)
    return {
        "unit": unit,
        "total_tags": int(total_tags),
        "flagged_tags": int(flagged),
        "flag_rate": (flagged/total_tags) if total_tags else 0.0,
        "speed_col": str(speed_col) if speed_col is not None else None,
        "speed_window": bw,
    }


def main():
    ap = argparse.ArgumentParser(description='Breakout backtest (chunked, speed-gated)')
    ap.add_argument('--units', nargs='*', default=['K-12-01','K-16-01','K-19-01','K-31-01'])
    ap.add_argument('--horizon-days', type=int, default=30)
    ap.add_argument('--cooldown', type=int, default=5)
    args = ap.parse_args()

    units = args.units
    results = []
    for u in units:
        print(f"Backtesting breakout on {u}...")
        # Temporarily patch cooldown and horizon via env-like toggles
        global DEFAULT_HORIZON, DEFAULT_COOLDOWN
        DEFAULT_HORIZON = int(args.horizon_days)
        DEFAULT_COOLDOWN = int(args.cooldown)
        r = backtest_unit(u)
        results.append(r)
        print(json.dumps(r, indent=2))
    summary = {
        "units": results,
        "test": f"breakout_20_window_10min_speed_gated_h{args.horizon_days}_cd{args.cooldown}"
    }
    out = project_root / 'reports' / f'breakout_backtest_summary.json'
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(f"Saved summary: {out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
