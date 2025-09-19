from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, List, Optional


def _rolling_quantile_bands(series: pd.Series, window: int, q_low: float, q_high: float) -> pd.DataFrame:
    ql = series.rolling(window, min_periods=max(3, window // 2)).quantile(q_low)
    qh = series.rolling(window, min_periods=max(3, window // 2)).quantile(q_high)
    return pd.DataFrame({"low": ql, "high": qh})


def _event_count(mask: pd.Series, cooldown: int = 5) -> int:
    if mask.empty:
        return 0
    m = mask.fillna(False).to_numpy()
    count = 0
    i = 0
    n = len(m)
    while i < n:
        if m[i]:
            count += 1
            i += max(1, cooldown)
        else:
            i += 1
    return int(count)


def detect_breakouts(
    pivot_df: pd.DataFrame,
    speed_col: str,
    tag_cols: List[str],
    *,
    window: int = 20,
    q_low: float = 0.10,
    q_high: float = 0.90,
    persist: int = 2,
    persist_window: int = 3,
    cooldown: int = 5,
    speed_window: Optional[List[float]] = None,
    recent_mask: Optional[pd.Series] = None,
) -> Dict[str, Dict[str, float]]:
    """Detect breakout events per tag using rolling quantile bands.

    Returns a dict[tag] -> {count, rate}.
    """
    results: Dict[str, Dict[str, float]] = {}

    if 'time' in pivot_df.columns:
        df = pivot_df.copy()
    else:
        # Assume index is time-like
        df = pivot_df.copy().reset_index()
        df.rename(columns={df.columns[0]: 'time'}, inplace=True)

    # Build evaluation mask: within recent_mask and speed_window
    mask = pd.Series(True, index=df.index)
    if recent_mask is not None and len(recent_mask) == len(df):
        mask &= recent_mask
    if speed_window is not None and speed_col in df.columns:
        lo, hi = float(speed_window[0]), float(speed_window[1])
        mask &= df[speed_col].between(lo, hi, inclusive='both')

    # Evaluate breakouts per tag
    for col in tag_cols:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors='coerce')
        bands = _rolling_quantile_bands(s, window, q_low, q_high)
        out = (s < bands['low']) | (s > bands['high'])
        # Apply persistence: at least `persist` of last `persist_window` points are out of band
        if persist_window > 1 and persist > 1:
            rolling_hits = out.rolling(persist_window, min_periods=1).sum()
            out = rolling_hits >= persist
        # Apply mask
        out = out & mask
        c = _event_count(out, cooldown=cooldown)
        if c > 0:
            results[col] = {"count": float(c), "rate": float(c / max(1, mask.sum()))}

    return results

