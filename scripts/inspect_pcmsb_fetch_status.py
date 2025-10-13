#!/usr/bin/env python3
"""
Inspect PCMSB fetch status per unit.

Reports for each unit:
- Parquet file presence
- Latest data timestamp and age (hours)
- Row count and unique tag count

Classifies units as: FRESH (<6h), OLD (6–24h), or STALE (>24h), or MISSING.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import importlib.util
import sys

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None  # type: ignore


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _get_pcmsb_units() -> list[str]:
    """Load unit list from scripts/build_pcmsb.py if available, else fallback."""
    build_script = PROJECT_ROOT / "scripts" / "build_pcmsb.py"
    if build_script.exists():
        try:
            spec = importlib.util.spec_from_file_location("_build_pcmsb", str(build_script))
            mod = importlib.util.module_from_spec(spec)  # type: ignore
            assert spec and spec.loader
            spec.loader.exec_module(mod)  # type: ignore
            units = list(getattr(mod, "PCMSB_UNITS", {}).keys())
            if units:
                return units
        except Exception:
            pass
    # Fallback list
    return ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202', 'XT-07002']


def inspect_unit(unit: str) -> dict:
    data_dir = PROJECT_ROOT / "data" / "processed"
    file_path = data_dir / f"{unit}_1y_0p1h.parquet"
    info = {
        "unit": unit,
        "exists": file_path.exists(),
        "file": str(file_path.relative_to(PROJECT_ROOT)) if file_path.exists() else None,
        "file_mtime": None,
        "file_age_h": None,
        "latest_time": None,
        "data_age_h": None,
        "rows": None,
        "tags": None,
        "status": "MISSING",
    }
    now = datetime.now()

    if not file_path.exists():
        return info

    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    info["file_mtime"] = mtime
    info["file_age_h"] = (now - mtime).total_seconds() / 3600.0

    if pq is None:
        # Can't read internals; classify by file mtime only
        age_h = info["file_age_h"] or 1e9
        if age_h < 6:
            info["status"] = "FRESH"
        elif age_h < 24:
            info["status"] = "OLD"
        else:
            info["status"] = "STALE"
        return info

    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        info["rows"] = len(df)
        if len(df) > 0 and "time" in df.columns:
            latest_time = df["time"].max()
            info["latest_time"] = latest_time
            info["data_age_h"] = (now - latest_time).total_seconds() / 3600.0
        if "tag" in df.columns:
            info["tags"] = int(df["tag"].nunique())
    except Exception as e:
        info["error"] = str(e)

    age_h = info.get("data_age_h")
    if age_h is None:
        info["status"] = "ERROR"
    elif age_h < 6:
        info["status"] = "FRESH"
    elif age_h < 24:
        info["status"] = "OLD"
    else:
        info["status"] = "STALE"
    return info


def main() -> int:
    print("PCMSB FETCH STATUS")
    print("=" * 70)

    # Check Excel and script presence
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    build_script = PROJECT_ROOT / "scripts" / "build_pcmsb.py"
    print(f"Excel: {'OK' if xlsx.exists() else 'MISSING'} - {xlsx.relative_to(PROJECT_ROOT)}")
    print(f"Builder: {'OK' if build_script.exists() else 'MISSING'} - {build_script.relative_to(PROJECT_ROOT)}")
    print("-" * 70)

    units = _get_pcmsb_units()
    results: list[dict] = []
    for u in units:
        results.append(inspect_unit(u))

    fresh = sum(1 for r in results if r["status"] == "FRESH")
    old = sum(1 for r in results if r["status"] == "OLD")
    stale = sum(1 for r in results if r["status"] == "STALE")
    missing = sum(1 for r in results if r["status"] == "MISSING")
    errors = sum(1 for r in results if r["status"] == "ERROR")

    for r in results:
        unit = r["unit"]
        status = r["status"]
        latest = r["latest_time"].strftime('%Y-%m-%d %H:%M') if r.get("latest_time") else "-"
        age = f"{r['data_age_h']:.1f}h" if r.get("data_age_h") is not None else "-"
        rows = f"{r['rows']:,}" if r.get("rows") is not None else "-"
        tags = f"{r['tags']}" if r.get("tags") is not None else "-"
        print(f"{unit:<8} | {status:<6} | latest: {latest:<16} | age: {age:<6} | rows: {rows:<10} | tags: {tags}")

    print("-" * 70)
    print(f"Summary: FRESH={fresh}, OLD={old}, STALE={stale}, MISSING={missing}, ERROR={errors}")
    if stale or old or missing or errors:
        print("Action: Run 'python fetch_all_plants_latest.py' or 'python scripts/build_pcmsb.py <UNIT>' to refresh.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

