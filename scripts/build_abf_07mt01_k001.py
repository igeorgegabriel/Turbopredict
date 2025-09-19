#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

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
    xlsx = PROJECT_ROOT / "excel" / "ABF_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_abf_07mt01_k001.txt"
    safe_unit = "07-MT01-K001"  # canonical and filename-safe
    out_parquet = PROJECT_ROOT / "data" / "processed" / f"{safe_unit}_1y_0p1h.parquet"

    plant = "ABF"
    unit = "07-MT01-K001"  # canonical unit identifier
    server = r"\\PTSG-1MMPDPdb01"  # adjust if ABF uses a different PI server
    start = "-1y"
    end = "*"
    step = "-0.1h"
    work_sheet = "DL_WORK"
    settle_seconds = 1.5
    visible = True

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    print(f"Building Parquet for {plant} {unit} with {len(tags)} tags...")
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


if __name__ == "__main__":
    raise SystemExit(main())
