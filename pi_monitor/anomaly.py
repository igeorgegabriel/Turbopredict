from __future__ import annotations

import pandas as pd


def add_anomalies(df: pd.DataFrame, roll: int = 5, drop_pct: float = 0.10) -> pd.DataFrame:
    """Add rolling mean and simple drop-based anomaly flag.

    An alert is true when value < (1 - drop_pct) * roll_mean.
    """
    out = df.copy()
    out["roll_mean"] = out["value"].rolling(roll, min_periods=2).mean()
    out["alert"] = out["value"] < (1 - drop_pct) * out["roll_mean"]
    return out


class AnomalyModel:
    """Placeholder for a future ML-based anomaly model."""

    def fit(self, df: pd.DataFrame) -> "AnomalyModel":  # noqa: D401
        self._fitted = True
        return self

    def predict(self, df: pd.DataFrame) -> pd.Series:
        if not getattr(self, "_fitted", False):
            raise RuntimeError("Model not fitted. Call fit() first.")
        # Placeholder: no-op baseline
        return pd.Series(False, index=df.index, name="ml_alert")

