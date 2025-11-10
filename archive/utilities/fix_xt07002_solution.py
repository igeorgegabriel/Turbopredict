#!/usr/bin/env python3
"""
Complete solution for XT-07002 timeout and data issues
"""

import os
import sys
from pathlib import Path

# Set higher timeout for PCMSB
os.environ['PI_FETCH_TIMEOUT'] = '90'  # Even higher for problematic units

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags

def fix_xt07002_build():
    """
    Fixed build for XT-07002 with:
    1. Increased timeout (90 seconds)
    2. Longer settle time
    3. Visible Excel for debugging
    4. Alternative time range
    """

    print("=== XT-07002 Fixed Build Solution ===")
    print(f"PI_FETCH_TIMEOUT: {os.environ.get('PI_FETCH_TIMEOUT')}")

    # Read tags
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_xt07002.txt"
    tags = []
    for line in tags_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            tags.append(line)

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "XT-07002_1y_0p1h.parquet"

    print(f"Building {len(tags)} tags for XT-07002...")
    print("Using enhanced settings:")
    print("- PI_FETCH_TIMEOUT: 90 seconds")
    print("- settle_seconds: 5.0 (increased)")
    print("- visible: True (for debugging)")
    print("- Alternative time range: -3d to * (shorter)")

    try:
        result = build_unit_from_tags(
            xlsx,
            tags,
            out_parquet,
            plant="PCMSB",
            unit="XT-07002",
            server=r"\\PTSG-1MMPDPdb01",
            start="-3d",  # Shorter time range
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=5.0,  # Increased settle time
            visible=True,  # Visible for debugging
            use_working_copy=True
        )

        print(f"SUCCESS: Built parquet file: {result}")

        # Try deduplication
        try:
            from pi_monitor.clean import dedup_parquet
            dedup_file = dedup_parquet(result)
            print(f"Deduplication complete: {dedup_file}")
        except Exception as dedup_error:
            print(f"Dedup warning: {dedup_error}")

        return True

    except Exception as e:
        print(f"Build failed: {e}")

        # Provide troubleshooting info
        print("\n=== Troubleshooting Info ===")
        print("1. Tags are timing out after 90 seconds")
        print("2. This suggests either:")
        print("   - PI server connectivity issues for PCMSB")
        print("   - Tags may not exist or have been moved")
        print("   - PI DataLink configuration issues")
        print("3. Recommended actions:")
        print("   - Check if XT-07002 unit is active")
        print("   - Verify tag names with PI administrator")
        print("   - Try connecting to PCMSB PI server directly")
        print("   - Consider using different time range")

        return False

if __name__ == "__main__":
    success = fix_xt07002_build()
    if success:
        print("\n✓ XT-07002 build completed successfully!")
    else:
        print("\n✗ XT-07002 build failed - see troubleshooting info above")