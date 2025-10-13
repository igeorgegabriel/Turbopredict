#!/usr/bin/env python3
"""
Quick XT-07002 build - use proven approach based on user's evidence
"""

import os
import sys
from pathlib import Path

# Set high timeout
os.environ['PI_FETCH_TIMEOUT'] = '90'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def build_xt07002_quick():
    """Build XT-07002 with optimized settings"""

    print("=== Quick XT-07002 Build ===")
    print(f"PI_FETCH_TIMEOUT: {os.environ.get('PI_FETCH_TIMEOUT')}")

    # Since you showed that PCM.XT-07002.070GZI8402.PV has data,
    # let's try building with all tags but with very conservative settings

    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_xt07002.txt"
    all_tags = []
    for line in tags_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            all_tags.append(line)

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "XT-07002_1y_0p1h.parquet"

    print(f"Building {len(all_tags)} XT-07002 tags...")
    print("Optimized settings:")
    print("- PI_FETCH_TIMEOUT: 90s")
    print("- settle_seconds: 3.0")
    print("- Short timeframe: -3d (3 days)")
    print("- visible: True for debugging")

    from pi_monitor.batch import build_unit_from_tags

    try:
        result = build_unit_from_tags(
            xlsx,
            all_tags,
            out_parquet,
            plant="PCMSB",
            unit="XT-07002",
            server=r"\\PTSG-1MMPDPdb01",
            start="-3d",  # Only 3 days to reduce load
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=3.0,  # Increased settle time
            visible=True,  # For debugging
            use_working_copy=True
        )

        print(f"SUCCESS: Built {result}")

        # Check file size
        if result.exists():
            size_mb = result.stat().st_size / (1024 * 1024)
            print(f"File size: {size_mb:.1f} MB")

        # Try deduplication
        try:
            from pi_monitor.clean import dedup_parquet
            dedup_file = dedup_parquet(result)
            print(f"Deduplicated: {dedup_file}")

            if dedup_file.exists():
                dedup_size_mb = dedup_file.stat().st_size / (1024 * 1024)
                print(f"Dedup file size: {dedup_size_mb:.1f} MB")

        except Exception as e:
            print(f"Dedup warning: {e}")

        return True

    except Exception as e:
        print(f"Build failed: {e}")

        # Fallback: try with shorter timeframe
        print("\nTrying fallback with 1 day timeframe...")
        try:
            result_fallback = build_unit_from_tags(
                xlsx,
                all_tags,
                PROJECT_ROOT / "data" / "processed" / "XT-07002_1d_0p1h.parquet",
                plant="PCMSB",
                unit="XT-07002",
                server=r"\\PTSG-1MMPDPdb01",
                start="-1d",  # Just 1 day
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=3.0,
                visible=True,
                use_working_copy=True
            )

            print(f"Fallback SUCCESS: {result_fallback}")
            return True

        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return False

if __name__ == "__main__":
    success = build_xt07002_quick()
    if success:
        print("\nXT-07002 build completed!")
    else:
        print("\nXT-07002 build failed - may need manual tag verification")