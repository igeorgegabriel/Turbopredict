from __future__ import annotations

from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import re

from .anomaly import add_anomalies


def plot_anomalies(df: pd.DataFrame, *, title: str | None = None, save_to: Path | None = None, show: bool = False) -> Path | None:
    """Plot value and rolling mean; highlight anomalies.

    - If `save_to` is provided, saves a PNG there (parents created).
    - If `show` is True, shows the plot interactively.
    Returns the saved path if any.
    """
    if title is None:
        title = "Automation — Value vs Rolling Mean (Anomalies)"

    plt.figure(figsize=(12, 6))
    plt.plot(df["time"], df["value"], label="Value")
    if "roll_mean" in df.columns:
        plt.plot(df["time"], df["roll_mean"], label="Rolling Mean")
    anom = df[df.get("alert", pd.Series(False, index=df.index))]
    if not anom.empty:
        plt.scatter(anom["time"], anom["value"], label="Anomaly", zorder=5)
    plt.legend()
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Value")
    plt.grid(alpha=0.3)
    plt.tight_layout()

    out: Path | None = None
    if save_to is not None:
        out = Path(save_to)
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out)
    if show:
        plt.show()
    plt.close()
    return out


def _slug(s: str) -> str:
    s = re.sub(r"\s+", "_", str(s).strip())
    s = re.sub(r"[^A-Za-z0-9_\-]", "_", s)
    return s


def plot_series(df: pd.DataFrame, *, title: str | None = None, save_to: Path | None = None, show: bool = False) -> Path | None:
    """Simple time vs value plot without rolling mean or anomalies."""
    plt.figure(figsize=(12, 6))
    plt.plot(df["time"], df["value"], linewidth=1.2)
    if title:
        plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Value")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    out: Path | None = None
    if save_to is not None:
        out = Path(save_to)
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out)
    if show:
        plt.show()
    plt.close()
    return out


def plot_all_from_parquet(parquet_path: Path, out_dir: Path, *, roll: int = 5, drop_pct: float = 0.10, filter_plant: str | None = None, filter_unit: str | None = None, filter_tag: str | None = None, limit: int | None = None, plain: bool = False) -> list[Path]:
    """Plot each tag's time series + rolling mean from a Parquet table.

    Saves one PNG per (plant, unit, tag) into `out_dir` and returns saved paths.
    """
    parquet_path = Path(parquet_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(parquet_path)
    if "time" not in df.columns or "value" not in df.columns:
        raise RuntimeError("Parquet must contain 'time' and 'value' columns")
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")

    if filter_plant and "plant" in df.columns:
        df = df[df["plant"] == filter_plant]
    if filter_unit and "unit" in df.columns:
        df = df[df["unit"] == filter_unit]
    if filter_tag and "tag" in df.columns:
        df = df[df["tag"].astype(str) == filter_tag]

    group_cols = [c for c in ["plant", "unit", "tag"] if c in df.columns]
    groups = [ ((), df) ] if not group_cols else df.groupby(group_cols)

    paths: list[Path] = []
    count = 0
    for key, g in groups:
        if limit is not None and count >= limit:
            break
        gs = g.sort_values("time")
        if plain:
            gg = gs
        else:
            gg = add_anomalies(gs, roll=roll, drop_pct=drop_pct)
        name_parts = [str(k) for k in (key if isinstance(key, tuple) else (key,)) if k is not None and k != ()]
        if not name_parts:
            name_parts = ["series"]
        fname = "_".join(_slug(p) for p in name_parts) + ".png"
        save_path = out_dir / fname
        plot_title = " - ".join(name_parts) or "Time Series"
        if plain:
            plot_series(gg, title=plot_title, save_to=save_path, show=False)
        else:
            plot_anomalies(gg, title=plot_title, save_to=save_path, show=False)
        paths.append(save_path)
        count += 1

    return paths
