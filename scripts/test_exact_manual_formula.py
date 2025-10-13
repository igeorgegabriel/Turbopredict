#!/usr/bin/env python3
"""Test the EXACT formula from the user's successful Excel screenshot: -1y format."""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags


def test_exact_manual_formula() -> int:
    """Use the exact same parameters that worked in manual Excel: -1y."""

    # Use exact parameters from screenshot:
    # =PISampDat("PCFS.K-12-01.12SI-401B.PV","-1y","*","0.11h",1,"\\PTSG-1MMPDPdb01")
    test_tags = [
        "PCFS.K-12-01.12SI-401B.PV",  # Exact tag from screenshot
    ]

    unit = "K-12-01"
    plant = "PCFS"
    excel_file = "PCFS/PCFS_Automation.xlsx"
    server = r"\\PTSG-1MMPDPdb01"

    xlsx_path = PROJECT_ROOT / "excel" / excel_file
    temp_file = PROJECT_ROOT / "tmp" / f"{unit}_exact_manual_test.parquet"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    print("="*80)
    print("TEST: Exact formula from manual Excel screenshot")
    print("="*80)
    print("\nFrom screenshot:")
    print('  =PISampDat("PCFS.K-12-01.12SI-401B.PV","-1y","*","0.11h",1,"\\\\PTSG-1MMPDPdb01")')
    print("\nUsing:")
    print('  start: "-1y"')
    print('  end: "*"')
    print('  step: "-0.1h"')
    print('  visible: True (to see Excel)')

    try:
        import pandas as pd

        build_unit_from_tags(
            xlsx_path,
            test_tags,
            temp_file,
            plant=plant,
            unit=unit,
            server=server,
            start="-1y",  # Exact format from screenshot
            end="*",      # Exact format from screenshot
            step="-0.1h", # Close to screenshot's 0.11h
            work_sheet="DL_WORK",
            settle_seconds=3.0,  # Longer settle time
            visible=True,  # Make Excel visible
        )

        if temp_file.exists() and temp_file.stat().st_size > 0:
            df_new = pd.read_parquet(temp_file)
            print(f"\n[OK] SUCCESS!")
            print(f"  Fetched: {len(df_new):,} records")
            if len(df_new) > 0:
                print(f"  Time range: {df_new['time'].min()} to {df_new['time'].max()}")
                print(f"  Tags: {df_new['tag'].nunique()}")
            temp_file.unlink()
            return 0
        else:
            print(f"\n[X] FAILED - No data fetched")
            print(f"  Check Excel window to see if PI DataLink formula calculated")
            return 1

    except Exception as e:
        print(f"\n[X] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(test_exact_manual_formula())
