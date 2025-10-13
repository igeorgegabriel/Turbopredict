from __future__ import annotations

from pathlib import Path
from typing import Sequence
import os


def _ensure_duckdb():
    try:
        import duckdb  # noqa: F401
        return True
    except Exception:
        return False


def dedup_parquet(
    in_path: Path,
    out_path: Path | None = None,
    *,
    keys: Sequence[str] = ("plant", "unit", "tag", "time"),
    compression: str = "ZSTD",
) -> Path:
    """Remove duplicate rows based on `keys` and write a clean Parquet.

    Prefer DuckDB for streaming performance. Falls back to pandas if DuckDB
    is not available.
    """
    in_path = Path(in_path)
    if out_path is None:
        out_path = in_path.with_suffix("")
        out_path = out_path.with_name(out_path.name + ".dedup.parquet")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if _ensure_duckdb():
        import duckdb

        # Configure DuckDB to spill to disk and cap memory usage for large files
        mem_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")  # Increased for large dedup operations
        tmp_dir = Path(os.getenv("DUCKDB_TEMP_DIR", str(out_path.parent / "_duckdb_tmp")))
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            # Fall back to system temp if custom dir cannot be created
            import tempfile as _tempfile
            tmp_dir = Path(_tempfile.gettempdir())

        con = duckdb.connect()
        # Best-effort PRAGMAs (ignore if not supported)
        try:
            con.execute(f"PRAGMA memory_limit='{mem_limit}'")
        except Exception:
            pass
        try:
            con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")
        except Exception:
            pass
        try:
            threads = os.getenv("DUCKDB_THREADS")
            if threads:
                con.execute(f"PRAGMA threads={int(threads)}")
        except Exception:
            pass

        key_list = ", ".join(keys)
        # any_value picks a representative value when duplicates exist.
        sql = f"""
COPY (
  SELECT {key_list}, any_value(value) AS value
  FROM read_parquet('{in_path.as_posix()}')
  GROUP BY {key_list}
  ORDER BY {key_list}
) TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION '{compression}');
"""
        con.execute(sql)
        con.close()
        return out_path

    # Fallback to pandas (may be memory heavy for very large files)
    import pandas as pd

    df = pd.read_parquet(in_path)
    df = df.sort_values(list(keys)).drop_duplicates(subset=list(keys), keep="first")
    df.to_parquet(out_path, index=False)
    return out_path
