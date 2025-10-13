#!/usr/bin/env python3
"""
Diagnose PCMSB XT-07002 connectivity and timeout issues
"""

import os
import sys
from pathlib import Path

# Set environment
os.environ['PI_FETCH_TIMEOUT'] = '60'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def diagnose_pcmsb():
    print("=== PCMSB XT-07002 Diagnosis ===")
    print(f"PI_FETCH_TIMEOUT: {os.environ.get('PI_FETCH_TIMEOUT')}")

    # Check files exist
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_xt07002.txt"

    print(f"Excel file exists: {xlsx.exists()}")
    print(f"Tags file exists: {tags_file.exists()}")

    if tags_file.exists():
        tags = []
        for line in tags_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                tags.append(line)
        print(f"Total tags: {len(tags)}")
        print(f"First 5 tags: {tags[:5]}")

    # Import after environment is set
    try:
        from pi_monitor.batch import _fetch_single
        import xlwings as xw

        print("Testing single tag fetch...")

        # Test with one tag only
        test_tag = 'PCM.XT-07002.070AI8001.PV'

        with xw.App(visible=False) as app:
            wb = app.books.open(xlsx)

            print(f"Testing tag: {test_tag}")
            print("This should timeout after 60 seconds if there are issues...")

            try:
                df = _fetch_single(
                    wb,
                    'DL_WORK',
                    test_tag,
                    r"\\PTSG-1MMPDPdb01",
                    '-2h',  # Very short timeframe
                    '*',
                    '-0.1h',
                    settle_seconds=2.0
                )

                print(f"SUCCESS: Retrieved {len(df)} rows")
                print(f"Data preview:\n{df.head()}")

            except Exception as e:
                print(f"FETCH ERROR: {e}")

            wb.close()

    except Exception as e:
        print(f"IMPORT/SETUP ERROR: {e}")

    print("=== End Diagnosis ===")

if __name__ == "__main__":
    diagnose_pcmsb()