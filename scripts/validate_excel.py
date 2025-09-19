import argparse
from pathlib import Path
import sys
from pathlib import Path as _Path
import pandas as pd

# Ensure package import when running from scripts/
sys.path.append(str(_Path(__file__).resolve().parents[1]))

from pi_monitor.ingest import load_latest_frame  # noqa: E402
from pi_monitor.anomaly import add_anomalies  # noqa: E402
from pi_monitor.plotting import plot_anomalies  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Validate Excel -> tidy DF and anomalies (no Parquet write)")
    ap.add_argument("xlsx", type=Path, help="Path to Excel file")
    ap.add_argument("--save-plot", type=Path, default=None)
    args = ap.parse_args()

    df = load_latest_frame(args.xlsx)
    df = add_anomalies(df)

    print("Rows:", len(df))
    print("Columns:", list(df.columns))
    print("Dtypes:\n", df.dtypes)
    print("Time range:", df["time"].min(), "->", df["time"].max())
    print("Monotonic increasing time:", df["time"].is_monotonic_increasing)
    print("Duplicate timestamps:", int(df["time"].duplicated().sum()))
    print("NaNs per column:\n", df.isna().sum())
    print("Tail(5):\n", df.tail(5))

    if args.save_plot:
        out = plot_anomalies(df, save_to=args.save_plot, show=False)
        print("Saved plot:", out)


if __name__ == "__main__":
    main()
