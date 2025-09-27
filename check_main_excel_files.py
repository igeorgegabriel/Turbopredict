#!/usr/bin/env python3
"""
Check which main Excel files need to be copied or created
"""
import os
from pathlib import Path

def check_main_excel_files():
    """Check which main Excel files exist and what needs to be done"""

    excel_dir = Path("excel")

    # Required main files for each plant
    required_files = {
        'PCFS': ['PCFS_Automation_2.xlsx', 'PCFS_Automation.xlsx'],
        'PCMSB': ['PCMSB_Automation.xlsx'],
        'ABFSB': ['ABF_Automation.xlsx', 'ABFSB_Automation.xlsx'],
        'MLNG': ['MLNG_Automation.xlsx'],
        'PFLNG1': ['PFLNG1_Automation.xlsx'],
        'PFLNG2': ['PFLNG2_Automation.xlsx']
    }

    print("CHECKING MAIN EXCEL FILES")
    print("=" * 50)

    # Check each plant directory
    for plant, files in required_files.items():
        print(f"\n{plant} Plant:")
        plant_dir = excel_dir / plant

        if not plant_dir.exists():
            print(f"  ERROR: Directory {plant_dir} does not exist!")
            continue

        for filename in files:
            file_path = plant_dir / filename
            if file_path.exists():
                print(f"  OK: {filename}")
            else:
                print(f"  MISSING: {filename}")

                # Check if file exists in main excel directory (might need copying)
                main_file = excel_dir / filename
                if main_file.exists():
                    print(f"    -> Found in main directory, can copy")
                else:
                    print(f"    -> Not found anywhere, needs creation")

    # Check main excel directory for orphaned files
    print(f"\n\nMAIN EXCEL DIRECTORY:")
    main_files = [f for f in excel_dir.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']
    if main_files:
        print("  Files still in main directory:")
        for f in main_files:
            print(f"    {f.name}")
    else:
        print("  No files in main directory - clean!")

if __name__ == "__main__":
    check_main_excel_files()