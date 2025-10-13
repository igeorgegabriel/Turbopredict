#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import os
import sys

# Ensure project root on sys.path when invoked directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags, read_tags_from_sheet  # type: ignore
from pi_monitor.ingest import append_parquet  # type: ignore


def fmt_abs(dt: datetime) -> str:
    # PI DataLink understands ISOish local timestamps, e.g. 2025-10-06 13:45
    return dt.strftime("%Y-%m-%d %H:%M")


def chunked_fetch(
    xlsx: Path,
    tags_file: Path,
    out_master: Path,
    *,
    plant: str,
    unit: str,
    server: str | None = None,
    days_total: int = 365,
    window_days: int = 30,
    step: str = "-0.1h",
    visible: bool = True,
    settle_seconds: float = 5.0,
) -> Path:
    raw = Path(tags_file).read_text(encoding="utf-8", errors="ignore").splitlines()
    tags = []
    for t in raw:
        if not t:
            continue
        tt = t.lstrip('\ufeff').strip()
        if not tt or tt.startswith('#'):
            continue
        tags.append(tt)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    # Conservative defaults for Excel reliability
    os.environ.setdefault('EXCEL_VISIBLE', '1' if visible else '0')
    os.environ.setdefault('PI_FETCH_TIMEOUT', '600')
    os.environ.setdefault('PI_FETCH_LINGER', '30')

    now = datetime.now()
    start_total = now - timedelta(days=days_total)
    windows: list[tuple[datetime, datetime]] = []
    w = timedelta(days=window_days)
    cursor = start_total
    while cursor < now:
        end = min(cursor + w, now)
        windows.append((cursor, end))
        cursor = end

    print(f"Chunked Excel fetch for {unit}: {len(windows)} window(s) of ~{window_days}d")
    out_master.parent.mkdir(parents=True, exist_ok=True)

    for i, (ws, we) in enumerate(windows, 1):
        print(f"\n[{i}/{len(windows)}] Window: {fmt_abs(ws)} -> {fmt_abs(we)}")
        temp_out = out_master.with_name(out_master.stem + f".part{i}" + out_master.suffix)
        # Use absolute window for PI DataLink
        part = build_unit_from_tags(
            xlsx=xlsx,
            tags=tags,
            out_parquet=temp_out,
            plant=plant,
            unit=unit,
            server=(server or ""),
            start=fmt_abs(ws),
            end=fmt_abs(we),
            step=step,
            settle_seconds=settle_seconds,
            visible=visible,
        )
        if part.exists() and part.stat().st_size > 0:
            try:
                append_parquet(__import__('pandas').read_parquet(part), out_master)
            except Exception:
                # Fall back to replacing if master missing
                if not out_master.exists():
                    part.replace(out_master)
                else:
                    # If append failed, concatenate in-memory as last resort
                    import pandas as pd
                    master_df = pd.read_parquet(out_master)
                    df = pd.read_parquet(part)
                    combined = pd.concat([master_df, df], ignore_index=True)
                    from pi_monitor.ingest import write_parquet  # type: ignore
                    write_parquet(combined, out_master)
            try:
                part.unlink()
            except Exception:
                pass
        else:
            print(f"[warn] No data for window {fmt_abs(ws)} -> {fmt_abs(we)}")

    print(f"\n[OK] Chunked fetch complete. Master Parquet: {out_master}")
    return out_master


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Chunked Excel PI DataLink fetch to build long windows efficiently")
    ap.add_argument("--xlsx", type=Path, required=True)
    ap.add_argument("--tags", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--plant", type=str, required=True)
    ap.add_argument("--unit", type=str, required=True)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--step", type=str, default="-0.1h")
    ap.add_argument("--visible", action="store_true")
    ap.add_argument("--server", type=str, default=None, help="Optional PI Data Archive server for PI tag fetches")
    args = ap.parse_args()

    chunked_fetch(
        args.xlsx,
        args.tags,
        args.out,
        plant=args.plant,
        unit=args.unit,
        days_total=args.days,
        window_days=args.window_days,
        step=args.step,
        visible=args.visible,
        server=args.server,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
