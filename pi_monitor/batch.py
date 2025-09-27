from __future__ import annotations

from pathlib import Path
import os
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
    wb, sheet_name: str, tag: str, server: str, start: str, end: str, step: str, *, settle_seconds: float = 1.0
) -> pd.DataFrame:
    """Write a PISampDat formula for `tag` and read its spilled (time,value) array faster.

    Optimizations:
    - Prefer dynamic-array spill with `expand()` (tiny reads) and only fallback to large
      array formulas when required (older Excel).
    - Adapt target array row count to requested window/step to avoid 100k-cell reads.
    - Allow lighter calculation via EXCEL_CALC_MODE env (sheet|full), default 'sheet'.
    """
    sht = None
    try:
        sht = wb.sheets[sheet_name]
        sht.clear()
    except Exception:
        sht = wb.sheets.add(sheet_name, after=wb.sheets[-1])

    # Helper: estimate expected rows to limit large array targets when we must use them
    def _estimate_rows(start_s: str, end_s: str, step_s: str) -> int:
        import re
        from datetime import datetime, timedelta
        now = datetime.now()
        # parse end
        try:
            end_dt = now if end_s.strip() == '*' else pd.to_datetime(end_s)
        except Exception:
            end_dt = now
        # parse start (supports -Nd, -Nh, -Nm, -Ny)
        m = re.match(r"^-\s*(\d*\.?\d+)([ydhm])$", start_s.replace(" ", ""), re.IGNORECASE)
        if m:
            val = float(m.group(1))
            unit = m.group(2).lower()
            if unit == 'y':
                start_dt = end_dt - timedelta(days=365 * val)
            elif unit == 'd':
                start_dt = end_dt - timedelta(days=val)
            elif unit == 'h':
                start_dt = end_dt - timedelta(hours=val)
            else:
                start_dt = end_dt - timedelta(minutes=val)
        else:
            try:
                start_dt = pd.to_datetime(start_s)
            except Exception:
                start_dt = end_dt - timedelta(days=365)
        total_minutes = max(1.0, (end_dt - start_dt).total_seconds() / 60.0)
        # parse step (-0.1h, -5m)
        step_s = step_s.strip()
        sm = re.match(r"^-\s*(\d*\.?\d+)([hm])$", step_s, re.IGNORECASE)
        if sm:
            sval = float(sm.group(1))
            sunit = sm.group(2).lower()
            step_minutes = sval * (60.0 if sunit == 'h' else 1.0)
        else:
            # default 6 min
            step_minutes = 6.0
        rows = int((total_minutes / max(0.1, step_minutes)) + 10)  # small buffer
        # clamp
        return max(2000, min(rows, 100000))

    # Write PISampDat to A2 as a dynamic formula and let results spill.
    # Only if dynamic arrays not available, fallback to legacy array formula over a bounded target.
    tag_escaped = tag.replace('"', '""')
    formula = f'=PISampDat("{tag_escaped}","{start}","{end}","{step}",1,"{server}")'
    max_rows = 100000  # covers ~1y at 6-min (≈87.6k)
    # Prefer dynamic spill first (fast path)
    resolved_rows = _estimate_rows(start, end, step)
    target = sht.range((2, 1), (resolved_rows + 1, 2))
    target.clear()
    try:
        # Try dynamic single-cell formula
        sht.range("A2").formula = formula
    except Exception:
        # Fallback to array formula over a reasonably sized target
        try:
            target.formula_array = formula
        except Exception:
            # As a last resort, put scalar formula
            sht.range("A2").formula = formula

    # Recalculate and robustly wait for PI DataLink to finish
    import os as _os
    calc_mode = _os.getenv('EXCEL_CALC_MODE', 'sheet').lower()
    # Optional overall fetch timeout (seconds). If unset, scale with settle_seconds.
    # Default increased to better tolerate slower PI servers (e.g., remote/VPN).
    try:
        _fetch_timeout = float(_os.getenv('PI_FETCH_TIMEOUT', '0').strip())
    except Exception:
        _fetch_timeout = 0.0
    if _fetch_timeout <= 0:
        _fetch_timeout = max(20.0, settle_seconds * 12.0)

    def _wait_for_datalink_completion(timeout: float) -> bool:
        t0 = time.monotonic()
        app = sht.book.app
        done_cycles = 0
        while (time.monotonic() - t0) < timeout:
            try:
                # Encourage async queries to complete if available
                try:
                    app.api.CalculateUntilAsyncQueriesDone()
                except Exception:
                    pass
                # Trigger a calculation cycle according to mode
                try:
                    if calc_mode == 'full':
                        app.api.CalculateFull()
                    else:
                        try:
                            sht.api.Calculate()
                        except Exception:
                            app.api.CalculateFull()
                except Exception:
                    pass
                # Check Excel calculation state (XlCalculationState: 0=Done, 1=Calculating, 2=Pending)
                try:
                    state = int(app.api.CalculationState)
                except Exception:
                    state = None

                # Inspect early rows of the spill for a numeric value in second column
                vals = None
                try:
                    vals = sht.range("A2").expand().value
                except Exception:
                    vals = None
                if vals:
                    if isinstance(vals, tuple):
                        vals = list(vals)
                    if isinstance(vals, list) and vals and not isinstance(vals[0], (list, tuple)):
                        vals = [list(vals)]
                    probe = vals[: min(10, len(vals))]
                    for row in probe:
                        if isinstance(row, (list, tuple)) and len(row) >= 2:
                            v = row[1]
                            if isinstance(v, (int, float)) and pd.notna(v):
                                return True
                if state == 0:
                    done_cycles += 1
                    if done_cycles >= 3:
                        return True
                else:
                    done_cycles = 0
            except Exception:
                pass
            time.sleep(0.5)
        # Timed out
        return False

    _completed = _wait_for_datalink_completion(_fetch_timeout)
    if not _completed:
        try:
            print(f"[warn] PI DataLink completion timed out after {_fetch_timeout:.1f}s for tag '{tag}'. Proceeding to read spill...")
        except Exception:
            pass

    # Read spill first (usually fastest/smallest)
    values = None
    try:
        values = sht.range("A2").expand().value
    except Exception:
        values = None
    if not values:
        # Fallback to reading the bounded target range
        values = target.value

    # If still empty, linger briefly to allow slow async completion then retry once.
    # Controlled by PI_FETCH_LINGER (seconds, default 10). Set to 0 to disable.
    def _is_empty(v) -> bool:
        if v is None:
            return True
        if isinstance(v, (list, tuple)):
            return len(v) == 0
        return False

    if _is_empty(values):
        try:
            _linger = float(_os.getenv('PI_FETCH_LINGER', '10').strip())
        except Exception:
            _linger = 10.0
        if _linger > 0:
            _wait_for_datalink_completion(_linger)
            # Retry spill then target
            try:
                values = sht.range("A2").expand().value
            except Exception:
                values = None
            if not values:
                values = target.value
    # Normalize to 2D list-of-lists
    if values is None:
        return pd.DataFrame(columns=["time", "value"])  # empty
    if isinstance(values, tuple):
        values = list(values)
    if isinstance(values, list) and values and not isinstance(values[0], (list, tuple)):
        # Single row spill
        values = [list(values)]
    if not isinstance(values, list):
        return pd.DataFrame(columns=["time", "value"])  # empty

    try:
        df = pd.DataFrame(values, columns=["time", "value"])  # may include blanks
    except Exception:
        return pd.DataFrame(columns=["time", "value"])  # treat as no data
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
    settle_seconds: float = 1.0,
    visible: bool = False,
    use_working_copy: bool = True,
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

    # We'll create a fresh working copy per attempt

    attempts = 0
    wrote_any_rows = False
    # Try headless first; if ABF and no rows, retry with Excel visible to ensure PI DataLink loads like your manual session
    while True:
        # Create fresh working copy for this attempt (if requested)
        open_path = Path(xlsx)
        working_path = None
        if use_working_copy:
            from datetime import datetime
            import shutil
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            working_path = open_path.parent / f"{open_path.stem}_working_{ts}{open_path.suffix}"
            try:
                shutil.copy2(str(open_path), str(working_path))
                open_path = working_path
            except Exception:
                working_path = None

        app = xw.App(visible=visible, add_book=False)
        writer: pq.ParquetWriter | None = None
        rows_this_try = 0
        try:
            app.display_alerts = False
            app.screen_updating = False
            # Best-effort: ensure PI DataLink is connected in this Excel instance
            try:
                for c in app.api.COMAddIns:
                    try:
                        desc = str(getattr(c, 'Description', ''))
                        prog = str(getattr(c, 'ProgId', ''))
                        name = (desc or prog).lower()
                        if ('pi' in name and 'datalink' in name) or ('pitime' in name):
                            c.Connect = True
                    except Exception:
                        pass
            except Exception:
                pass
            wb = app.books.open(str(open_path))

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
                rows_this_try += len(df)

            try:
                wb.save()
            except Exception:
                pass
            wb.close()
        finally:
            if writer is not None:
                writer.close()
            app.quit()
            # Clean up the working copy if used
            if working_path is not None:
                try:
                    working_path.unlink(missing_ok=True)
                except Exception:
                    pass

        wrote_any_rows = wrote_any_rows or (rows_this_try > 0)
        attempts += 1
        # Retry rule: if ABF or PCMSB and initial headless attempt wrote 0 rows.
        # Allow skipping the visible Excel fallback via env NO_VISIBLE_FALLBACK=1|true|yes|on
        if ((plant.upper().startswith('ABF') or plant.upper().startswith('PCMSB')) and (not visible) and rows_this_try == 0 and attempts == 1):
            no_visible = str(os.getenv('NO_VISIBLE_FALLBACK', '')).strip().lower() in {"1", "true", "yes", "on"}
            if no_visible:
                try:
                    print("[info] Headless fetch returned no rows; skipping visible Excel fallback due to NO_VISIBLE_FALLBACK.")
                except Exception:
                    pass
                break
            try:
                print("[info] Headless fetch returned no rows; retrying with Excel visible to ensure PI DataLink loads...")
            except Exception:
                pass
            visible = True
            continue
        break

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
