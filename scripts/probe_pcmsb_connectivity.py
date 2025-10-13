#!/usr/bin/env python3
"""
Quick probe to verify PCMSB units can fetch live data via PI DataLink.

- Uses the unified PCMSB builder config to locate tags per unit.
- For each unit, picks the first valid tag from its tag file.
- Fetches only the last 5 minutes at 1-minute step using the existing
  build pipeline (xlwings + DataLink), writing to a temporary parquet.
- Classifies PASS if at least one fresh row (<15m old) exists.

This avoids full 1y rebuilds and finishes quickly.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import importlib.util
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from typing import List

try:
    # Import builder for fetch function
    from pi_monitor.batch import build_unit_from_tags
except Exception as e:
    print(f"ERROR: Cannot import build pipeline: {e}")
    sys.exit(2)

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None  # optional; we can still check file mtime


def _load_pcmsb_units_mapping() -> dict:
    build_script = PROJECT_ROOT / "scripts" / "build_pcmsb.py"
    spec = importlib.util.spec_from_file_location("_build_pcmsb", str(build_script))
    if not spec or not spec.loader:
        raise RuntimeError("Failed to load PCMSB builder mapping")
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(mod)  # type: ignore
    return getattr(mod, "PCMSB_UNITS", {})


def _read_tags(path: Path) -> List[str]:
    tags: List[str] = []
    try:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            tags.append(s)
    except Exception:
        pass
    return tags


def main() -> int:
    units_map = _load_pcmsb_units_mapping()
    if not units_map:
        print("ERROR: No PCMSB units mapping found.")
        return 2

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    if not xlsx.exists():
        print(f"ERROR: Excel workbook missing: {xlsx}")
        return 2

    # Tame DataLink wait per tag
    os.environ.setdefault("PI_FETCH_TIMEOUT", "45")
    os.environ.setdefault("EXCEL_CALC_MODE", "sheet")

    probe_dir = PROJECT_ROOT / "tmp" / "pcmsb_probe"
    probe_dir.mkdir(parents=True, exist_ok=True)

    print("PCMSB LIVE CONNECTIVITY PROBE (last 5 minutes, 1 tag/unit)")
    print("=" * 70)

    passed = 0
    failed = 0

    for unit, tag_file in units_map.items():
        tags_path = PROJECT_ROOT / "config" / tag_file
        tags = _read_tags(tags_path)
        tag = next((t for t in tags if t and not t.startswith('#')), None)
        if not tag:
            print(f"{unit:<8} | FAIL   | No tags in {tags_path.name}")
            failed += 1
            continue

        out_parquet = probe_dir / f"{unit}_probe.parquet"
        # Clean previous probe
        try:
            if out_parquet.exists():
                out_parquet.unlink()
        except Exception:
            pass

        try:
            build_unit_from_tags(
                xlsx,
                [tag],
                out_parquet,
                plant="PCMSB",
                unit=unit,
                start="-5m",
                end="*",
                step="-1m",
                work_sheet="DL_WORK",
                settle_seconds=2.0,
                visible=False,
                use_working_copy=True,
            )
        except Exception as e:
            print(f"{unit:<8} | FAIL   | Exception during fetch: {e}")
            failed += 1
            continue

        # Inspect result
        if not out_parquet.exists():
            print(f"{unit:<8} | FAIL   | No output parquet created")
            failed += 1
            continue

        latest_str = "-"
        age_min_str = "-"
        ok = False
        try:
            if pq is not None:
                table = pq.read_table(out_parquet)
                df = table.to_pandas()
                if not df.empty and "time" in df.columns:
                    latest = df["time"].max()
                    age_min = (datetime.now() - latest).total_seconds() / 60.0
                    latest_str = latest.strftime('%Y-%m-%d %H:%M')
                    age_min_str = f"{age_min:.1f}m"
                    ok = age_min <= 15.0
                else:
                    ok = False
            else:
                # Fallback to file mtime if pyarrow unavailable
                mtime = datetime.fromtimestamp(out_parquet.stat().st_mtime)
                age_min = (datetime.now() - mtime).total_seconds() / 60.0
                latest_str = mtime.strftime('%Y-%m-%d %H:%M')
                age_min_str = f"{age_min:.1f}m"
                ok = age_min <= 15.0
        except Exception as e:
            print(f"{unit:<8} | FAIL   | Error reading probe: {e}")
            failed += 1
            continue

        if ok:
            print(f"{unit:<8} | PASS   | latest: {latest_str:<16} | age: {age_min_str}")
            passed += 1
        else:
            print(f"{unit:<8} | FAIL   | latest: {latest_str:<16} | age: {age_min_str}")
            failed += 1

        # Clean up probe file to avoid clutter
        try:
            out_parquet.unlink()
        except Exception:
            pass

    print("-" * 70)
    print(f"Result: PASS={passed}, FAIL={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

