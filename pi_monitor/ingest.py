from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


def _detect_header_row(raw: pd.DataFrame, search_rows: int = 10) -> int | None:
    """Return index of header row containing 'TIME' within first search_rows."""
    for i in range(min(search_rows, len(raw))):
        vals = [str(v).strip().upper() if isinstance(v, str) else v for v in raw.iloc[i].tolist()]
        if "TIME" in vals:
            return i
    return None


def _infer_unit_from_header(header: str) -> str | None:
    """Infer unit code from a header like 'PCFS K-31-01 ST_PERFORMANCE' or 'PCFS K-31 ...'."""
    m = re.search(r"\b([A-Z]+-\d{2}(?:-\d{2})?)\b", header.upper())
    return m.group(1) if m else None


def _infer_plant_unit_tag(header: str) -> tuple[str | None, str | None, str | None]:
    """Attempt to infer plant, unit, tag from a column header.

    Example: 'PCFS K-31-01 ST_PERFORMANCE' -> (PCFS, K-31-01, ST_PERFORMANCE)
    """
    s = header.strip()
    m = re.match(r"^([A-Za-z0-9_\-]+)\s+([A-Za-z]+-\d{2}(?:-\d{2})?)\s+(.+)$", s)
    if m:
        plant = m.group(1)
        unit = m.group(2)
        tag = m.group(3).strip()
        return plant, unit, tag
    # fallback: unit only via previous helper; tag as header
    unit = _infer_unit_from_header(header)
    return None, unit, header.strip()


def load_latest_frame(
    xlsx: Path,
    time_label: str = "TIME",
    value_col_hint: str = "ST_PERFORMANCE",
    unit: str | None = None,
    plant: str | None = None,
    tag: str | None = None,
    sheet_name: str | int | None = None,
) -> pd.DataFrame:
    """Load a tidy time/value DataFrame from an Excel file.

    - Finds the header row that contains the time label (default 'TIME').
    - Chooses a value column ending with value_col_hint; falls back to first non-time column.
    - Returns DataFrame with columns ['time', 'value'] sorted by time.
    """
    xlsx = Path(xlsx)

    raw = pd.read_excel(xlsx, header=None, engine="openpyxl", sheet_name=sheet_name)
    
    # Handle case where sheet_name=None returns dict of sheets
    if isinstance(raw, dict):
        # Use first sheet if multiple sheets returned
        sheet_names = list(raw.keys())
        raw = raw[sheet_names[0]]
        if sheet_name is None:
            sheet_name = sheet_names[0]  # Use the first sheet for subsequent read
    
    hdr_idx = _detect_header_row(raw)
    if hdr_idx is None:
        raise RuntimeError("Header row with 'TIME' not found in first rows.")

    df = pd.read_excel(xlsx, header=hdr_idx, engine="openpyxl", sheet_name=sheet_name)

    # map columns
    time_col = next(c for c in df.columns if str(c).strip().upper() == time_label)

    val_col = next((c for c in df.columns if str(c).strip().upper().endswith(value_col_hint)), None)
    if val_col is None:
        cand = [c for c in df.columns if c != time_col]
        if not cand:
            raise RuntimeError("No value column found.")
        val_col = cand[0]

    original_value_name = str(val_col)
    df = df.rename(columns={time_col: "time", val_col: "value"})
    df = df[[c for c in df.columns if not str(c).startswith("Unnamed")]]
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["time", "value"]).sort_values("time").reset_index(drop=True)

    # Attach markers for plant/unit/tag so many tags can live in one dataset
    infer_plant, infer_unit, infer_tag = _infer_plant_unit_tag(original_value_name)
    if unit or plant or tag or infer_unit or infer_plant or infer_tag:
        if plant or infer_plant:
            df["plant"] = (plant or infer_plant)
        if unit or infer_unit:
            df["unit"] = (unit or infer_unit)
        if tag or infer_tag:
            df["tag"] = _slugify_tag(tag or infer_tag)
    return df


def write_parquet(df: pd.DataFrame, out_path: Path, *, engine: str | None = None) -> Path:
    """Write DataFrame to Parquet with a robust engine fallback.

    Prefers 'pyarrow' for best type fidelity; falls back to 'fastparquet' if
    unavailable. Raises a clear error if neither is installed.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    engines = [engine] if engine else ["pyarrow", "fastparquet"]
    last_err: Exception | None = None
    for eng in engines:
        try:
            df.to_parquet(out_path, index=False, engine=eng)
            return out_path
        except Exception as e:  # keep trying
            last_err = e
            continue
    raise RuntimeError(
        "Parquet write failed. Install 'pyarrow' (preferred) or 'fastparquet'.\n"
        f"Last error: {last_err}"
    )
def _slugify_tag(name: str) -> str:
    t = re.sub(r"\s+", "_", name.strip())
    t = re.sub(r"[^A-Za-z0-9_\-]", "_", t)
    return t
