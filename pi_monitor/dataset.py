from __future__ import annotations

from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ensure_partition_cols(df: pd.DataFrame, time_col: str = "time") -> pd.DataFrame:
    out = df.copy()
    if time_col in out.columns:
        t = pd.to_datetime(out[time_col])
        out["year"] = t.dt.year
        out["month"] = t.dt.month
    for col in ("plant", "unit", "tag"):
        if col in out.columns:
            out[col] = out[col].astype("string")
    return out


def write_dataset(
    df: pd.DataFrame,
    out_dir: Path,
    *,
    partition_cols: list[str] | None = None,
    compression: str = "zstd",
    max_rows_per_file: int | None = None,
) -> Path:
    """Append/write a Parquet dataset partitioned by keys.

    Default partitioning: plant/unit/tag/year/month (where present).
    Creates a directory dataset instead of a single file, enabling scalable
    storage for many tags and long histories.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = ensure_partition_cols(df)

    if partition_cols is None:
        partition_cols = [c for c in ["plant", "unit", "tag", "year", "month"] if c in df.columns]

    if max_rows_per_file and len(df) > max_rows_per_file:
        # Write in chunks to avoid oversized files
        for i in range(0, len(df), max_rows_per_file):
            table = pa.Table.from_pandas(df.iloc[i:i + max_rows_per_file], preserve_index=False)
            pq.write_to_dataset(table, root_path=str(out_dir), partition_cols=partition_cols, compression=compression)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=str(out_dir), partition_cols=partition_cols, compression=compression)

    return out_dir
