#!/usr/bin/env python3
"""
Test XT-07002 with just a few tags to verify timeout fix
"""

import os
import sys
from pathlib import Path

# Set higher timeout
os.environ['PI_FETCH_TIMEOUT'] = '60'

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags

def test_small_xt07002():
    # Test with just 3 tags
    test_tags = [
        'PCM.XT-07002.070AI8001.PV',
        'PCM.XT-07002.070FC8004.PV',
        'PCM.XT-07002.070FC8005.PV'
    ]

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "XT-07002_test.parquet"

    print(f"PI_FETCH_TIMEOUT: {os.environ.get('PI_FETCH_TIMEOUT')}")
    print(f"Testing {len(test_tags)} tags with 60-second timeout...")

    try:
        result = build_unit_from_tags(
            xlsx,
            test_tags,
            out_parquet,
            plant="PCMSB",
            unit="XT-07002",
            server=r"\\PTSG-1MMPDPdb01",
            start="-1d",  # Just 1 day for testing
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=2.0,
            visible=False,
            use_working_copy=True
        )
        print(f"SUCCESS: {result}")
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    success = test_small_xt07002()
    print(f"Test result: {'PASSED' if success else 'FAILED'}")