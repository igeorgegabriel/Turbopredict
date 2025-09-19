from pathlib import Path
import argparse
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from pi_monitor.dataset import write_dataset  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Build partitioned Parquet dataset (plant/unit/tag/year/month)")
    ap.add_argument("--out-dir", type=Path, default=Path("data/processed/dataset"))
    ap.add_argument("--inputs", type=Path, nargs="+", default=[
        Path("data/processed/K-12-01_1y_0p1h.dedup.parquet"),
        Path("data/processed/K-16-01_1y_0p1h.dedup.parquet"),
    ])
    args = ap.parse_args()

    paths = []
    for p in args.inputs:
        if not p.exists():
            # fallback to non-dedup file
            alt = Path(str(p).replace(".dedup", ""))
            if alt.exists():
                paths.append(alt)
        else:
            paths.append(p)

    if not paths:
        raise SystemExit("No input Parquet files found.")

    for p in paths:
        df = pd.read_parquet(p)
        write_dataset(df, args.out_dir)
        print(f"Appended {len(df):,} rows from {p}")
    print(f"Dataset ready at {args.out_dir}")


if __name__ == "__main__":
    main()
