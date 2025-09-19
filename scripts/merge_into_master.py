#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def merge_and_dedup(master_path: Path, new_path: Path, keys=None, *, cleanup: bool = True) -> Path:
    keys = keys or ["plant", "unit", "tag", "time"]
    master_path = Path(master_path)
    new_path = Path(new_path)

    if not new_path.exists():
        raise FileNotFoundError(f"New parquet not found: {new_path}")

    new_df = pd.read_parquet(new_path)
    if "time" in new_df.columns:
        new_df["time"] = pd.to_datetime(new_df["time"])  # normalize

    if master_path.exists():
        base_df = pd.read_parquet(master_path)
        if "time" in base_df.columns:
            base_df["time"] = pd.to_datetime(base_df["time"])  # normalize
        combined = pd.concat([base_df, new_df], ignore_index=True)
    else:
        master_path.parent.mkdir(parents=True, exist_ok=True)
        combined = new_df

    # Ensure required columns exist
    for col in ["plant", "unit", "tag", "time", "value"]:
        if col not in combined.columns:
            combined[col] = pd.NA

    # Deduplicate and sort
    combined = combined.drop_duplicates(subset=keys, keep="last")
    if "time" in combined.columns:
        combined = combined.sort_values("time")

    # Write master
    combined.to_parquet(master_path, index=False)

    # Also write a dedup variant (explicitly named)
    dedup_path = master_path.with_suffix("")
    dedup_path = master_path.parent / (master_path.stem + ".dedup.parquet")
    combined.to_parquet(dedup_path, index=False)

    # Optional cleanup: remove temporary artifact (e.g., *.updated.parquet)
    try:
        if cleanup:
            name = new_path.name.lower()
            if name.endswith(".updated.parquet") or "updated" in name:
                new_path.unlink(missing_ok=True)
                print(f"Removed temporary artifact: {new_path}")
    except Exception as e:
        print(f"Warning: could not remove temporary file {new_path}: {e}")

    return master_path


def main():
    ap = argparse.ArgumentParser(description="Merge a new unit parquet into master and deduplicate")
    ap.add_argument("--unit", required=True)
    ap.add_argument("--new", dest="new_path", required=True, type=Path)
    ap.add_argument("--master", dest="master_path", required=True, type=Path)
    ap.add_argument("--no-cleanup", action="store_true",
                    help="Do not remove the new parquet after merge (keeps updated/refreshed artifacts)")
    args = ap.parse_args()

    out = merge_and_dedup(args.master_path, args.new_path, cleanup=(not args.no_cleanup))
    print(f"Merged into master: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
