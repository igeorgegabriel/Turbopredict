from __future__ import annotations

from pathlib import Path
import time
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import xlwings as xw
except Exception:
    xw = None  # type: ignore


def _slug(s: str) -> str:
    import re
    s = re.sub(r"\s+", "_", s.strip())
    s = re.sub(r"[^A-Za-z0-9_\-]", "_", s)
    return s


def _fetch_single(
    wb, sheet_name: str, tag: str, server: str, start: str, end: str, step: str, *, settle_seconds: float = 1.5
) -> pd.DataFrame:
    """Write a PISampDat formula for `tag` and read its spilled (time,value) array."""
    sht = None
    try:
        sht = wb.sheets[sheet_name]
        sht.clear()
    except Exception:
        sht = wb.sheets.add(sheet_name, after=wb.sheets[-1])

    # Write PISampDat as an array formula over a large target range to support
    # legacy Excel without dynamic arrays. DataLink will fill rows up to the
    # requested window; remaining rows may be blank or errors.
    tag_escaped = tag.replace('"', '""')
    formula = f'=PISampDat("{tag_escaped}","{start}","{end}","{step}",1,"{server}")'
    max_rows = 100000  # covers ~1y at 6-min (≈87.6k)
    target = sht.range((2, 1), (max_rows + 1, 2))
    target.clear()
    try:
        target.formula_array = formula
    except Exception:
        # Fallback: put in A2 and hope dynamic arrays are available
        sht.range("A2").formula = formula

    # Recalculate and allow time for DataLink
    try:
        sht.book.app.api.CalculateFull()
    except Exception:
        sht.api.Calculate()
    time.sleep(settle_seconds)

    values = target.value
    if not values:
        values = sht.range("A2").expand().value
        if not values or not isinstance(values, list):
            return pd.DataFrame(columns=["time", "value"])  # empty

    df = pd.DataFrame(values, columns=["time", "value"])  # may include blanks
    # PI DataLink returns Excel serial date numbers when 'Show time stamps' is on.
    # Convert Excel serial to pandas datetime using Excel's epoch.
    try:
        ser = pd.to_numeric(df["time"], errors="coerce")
        df["time"] = pd.to_datetime(ser, unit="d", origin="1899-12-30")
    except Exception:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["time", "value"]).reset_index(drop=True)
    return df


def build_unit_from_tags(
    xlsx: Path,
    tags: Iterable[str],
    out_parquet: Path,
    *,
    plant: str,
    unit: str,
    server: str = "\\\\PTSG-1MMPDPdb01",
    start: str = "-1y",
    end: str = "*",
    step: str = "-0.1h",
    work_sheet: str = "DL_WORK",
    settle_seconds: float = 1.5,
    visible: bool = True,
) -> Path:
    """Loop over tags, pull 1y/0.1h via DataLink, and write a single Parquet.

    Parquet schema columns: time (timestamp[ns]), value (float64), plant, unit, tag.
    Uses a streaming ParquetWriter to avoid holding all tags in memory.
    """
    if xw is None:
        raise RuntimeError("xlwings is required for DataLink-driven fetches.")

    xlsx = Path(xlsx)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    app = xw.App(visible=visible, add_book=False)
    writer: pq.ParquetWriter | None = None
    try:
        app.display_alerts = False
        app.screen_updating = False
        wb = app.books.open(str(xlsx))

        for tag in tags:
            tag = tag.strip()
            if not tag or tag.startswith("#"):
                continue
            df = _fetch_single(wb, work_sheet, tag, server, start, end, step, settle_seconds=settle_seconds)
            if df.empty:
                print(f"[warn] No data for tag: {tag}")
                continue
            df["plant"] = plant
            df["unit"] = unit
            df["tag"] = _slug(tag)

            # Reorder columns
            df = df[["time", "value", "plant", "unit", "tag"]]

            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(str(out_parquet), table.schema, compression="zstd")
            writer.write_table(table)

        wb.save()
        wb.close()
    finally:
        if writer is not None:
            writer.close()
        app.quit()

    return out_parquet


def read_tags_from_sheet(xlsx: Path, sheet_name: str = "DL_K12_01", row: int = 2, max_cols: int = 256) -> list[str]:
    """Read tags from a single row in an Excel sheet (A<Row>.. up to blank).

    This is useful when you already maintain a tag list in the workbook.
    """
    import openpyxl

    wb = openpyxl.load_workbook(xlsx, data_only=True, read_only=True)
    ws = wb[sheet_name]
    out: list[str] = []
    for c in range(1, max_cols + 1):
        v = ws.cell(row=row, column=c).value
        if v is None or str(v).strip() == "":
            if c > 1 and (ws.cell(row=row, column=c + 1).value is None):
                break
            continue
        s = str(v).strip()
        # Heuristic: PI tags often contain dots, no spaces, not starting with '-' and not a UNC path
        if "." in s and " " not in s and not s.startswith("-") and not s.startswith("\\\\") and any(ch.isalpha() for ch in s):
            out.append(s)
    wb.close()
    return out
