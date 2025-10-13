#!/usr/bin/env python3
"""
ABF 21-K002 (C2 Compressor) - Using VSARMNGPIMDB01 server like 07-MT01-K001
WARNING: Tags may not exist on this server - previous tests all failed
"""
from __future__ import annotations

from pathlib import Path
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags  # noqa: E402
from pi_monitor.clean import dedup_parquet  # noqa: E402


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def main() -> int:
    xlsx = PROJECT_ROOT / "excel" / "ABFSB" / "ABF LIMIT REVIEW (CURRENT).xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_abf_21k002_column_d.txt"
    safe_unit = "21-K002"
    out_parquet = PROJECT_ROOT / "data" / "processed" / f"{safe_unit}_1y_0p1h.parquet"

    plant = "ABFSB"
    unit = "21-K002"

    # Use VSARMNGPIMDB01 server (same as 07-MT01-K001)
    server = r"\\VSARMNGPIMDB01"
    start = "-1y"
    end = "*"
    step = "-0.1h"
    work_sheet = "DL_WORK"
    settle_seconds = 2.0
    visible = True

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    print(f"Building Parquet for {plant} {unit} with {len(tags)} PI tags...")
    print(f"Using PI server: {server}")
    print(f"Sample tags: {tags[:5]}")
    print(f"WARNING: Previous tests show these tags may not exist on this server")
    print(f"")

    try:
        out = build_unit_from_tags(
            xlsx,
            tags,
            out_parquet,
            plant=plant,
            unit=unit,
            server=server,
            start=start,
            end=end,
            step=step,
            work_sheet=work_sheet,
            settle_seconds=settle_seconds,
            visible=visible,
        )
        print(f"Wrote: {out}")

        dedup = dedup_parquet(out)
        print(f"Master (dedup) ready: {dedup}")
        return 0
    except Exception as e:
        print(f"\nERROR: {e}")
        print(f"\nFailed to fetch data for 21-K002")
        print(f"These PI tags may not exist in PI Data Archive.")
        print(f"They only exist as AF (Asset Framework) paths.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
