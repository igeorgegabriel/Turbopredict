#!/usr/bin/env python3
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
    # Updated path to use ABFSB directory
    xlsx = PROJECT_ROOT / "excel" / "ABFSB" / "ABFSB_Automation_Master.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_abf_07mt01_k001.txt"
    safe_unit = "07-MT01-K001"  # canonical and filename-safe
    out_parquet = PROJECT_ROOT / "data" / "processed" / f"{safe_unit}_1y_0p1h.parquet"

    plant = "ABF"
    unit = "07-MT01-K001"  # canonical unit identifier
    
    # Flexible PI server configuration - check environment variable first
    server = os.getenv('ABF_PI_SERVER', r"\\PTSG-1MMPDPdb01")  # default to PCFS server if not set
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
    print(f"Using PI server: {server}")
    
    # Test alternative server configurations if default fails
    alternative_servers = [
        r"\\ABF-PI-SERVER",
        r"\\ABF-PI-DB01", 
        r"\\ABFSB-PI-SERVER",
        r"\\PTSG-ABF-DB01"
    ]
    
    for attempt_server in [server] + alternative_servers:
        try:
            print(f"Attempting with server: {attempt_server}")
            out = build_unit_from_tags(
                xlsx,
                tags,
                out_parquet,
                plant=plant,
                unit=unit,
                server=attempt_server,
                start=start,
                end=end,
                step=step,
                work_sheet=work_sheet,
                settle_seconds=settle_seconds,
                visible=visible,
            )
            print(f"Wrote: {out}")
            break
        except Exception as e:
            print(f"Failed with server {attempt_server}: {e}")
            if attempt_server == alternative_servers[-1]:  # Last attempt
                raise SystemExit(f"All PI server attempts failed for ABF unit")
            continue

    dedup = dedup_parquet(out)
    print(f"Master (dedup) ready: {dedup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
