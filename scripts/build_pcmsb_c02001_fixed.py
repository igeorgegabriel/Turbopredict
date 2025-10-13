#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys
import os
import time

# Ensure project root on sys.path
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


def safe_build_with_retry(xlsx_path, tags, out_parquet, plant, unit, max_retries=3):
    """Safe build function with retry logic for Excel automation issues."""
    
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {unit}")
            
            # Add delay between attempts
            if attempt > 0:
                print(f"Waiting 10 seconds before retry...")
                time.sleep(10)
            
            # Kill any stray Excel processes before retry
            if attempt > 0:
                try:
                    import psutil
                    for proc in psutil.process_iter(['name']):
                        if 'excel' in proc.info['name'].lower():
                            print(f"Terminating Excel process: {proc.info['name']} (PID: {proc.pid})")
                            proc.terminate()
                            time.sleep(2)
                except (ImportError, Exception):
                    pass
            
            out = build_unit_from_tags(
                xlsx_path,
                tags,
                out_parquet,
                plant=plant,
                unit=unit,
                server=r"\\PTSG-1MMPDPdb01",
                start="-1y",
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=2.0,  # Increased settle time
                visible=True,  # Always show Excel for PCMSB
                use_working_copy=True  # Use working copy to avoid file locks
            )
            return out
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise  # Re-raise on final attempt
    
    return None


def main() -> int:
    # Configuration for PCMSB C-02001 with correct path
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_c02001.txt"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "C-02001_1y_0p1h.parquet"

    plant = "PCMSB"
    unit = "C-02001"
    
    # Verify files exist
    if not xlsx.exists():
        print(f"ERROR: Excel file not found: {xlsx}")
        return 1
        
    if not tags_file.exists():
        print(f"ERROR: Tags file not found: {tags_file}")
        return 1

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    print(f"Building Parquet for {plant} {unit} with {len(tags)} tags...")
    print(f"Using Excel file: {xlsx}")
    
    try:
        # Use safe build with retry logic
        out = safe_build_with_retry(xlsx, tags, out_parquet, plant, unit)
        
        if out:
            print(f"Successfully wrote: {out}")
            
            # Deduplicate into master .dedup.parquet
            dedup = dedup_parquet(out)
            print(f"Master (dedup) ready: {dedup}")
            return 0
        else:
            print("All retry attempts failed")
            return 1
            
    except Exception as e:
        print(f"Fatal error building {unit}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
