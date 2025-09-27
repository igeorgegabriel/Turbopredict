#!/usr/bin/env python3
"""Quick status check for data fetching capability"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def check_build_scripts():
    """Check if build scripts exist."""
    scripts_to_check = [
        'scripts/build_pcfs_k1201.py',
        'scripts/build_pcfs_k1601.py',
        'scripts/build_pcfs_k1901.py',
        'scripts/build_pcfs_k3101.py',
        'scripts/build_pcmsb.py',
        'scripts/build_abf_07mt01_k001.py'
    ]

    print("=== BUILD SCRIPTS STATUS ===")
    for script in scripts_to_check:
        path = PROJECT_ROOT / script
        status = "EXISTS" if path.exists() else "MISSING"
        print(f"{script}: {status}")

def check_excel_files():
    """Check if Excel files exist."""
    excel_files = [
        'excel/PCFS/PCFS_Automation.xlsx',
        'excel/PCMSB/PCMSB_Automation.xlsx',
        'excel/ABFSB/ABFSB_Automation_Master.xlsx'
    ]

    print("\n=== EXCEL FILES STATUS ===")
    for excel_file in excel_files:
        path = PROJECT_ROOT / excel_file
        status = "EXISTS" if path.exists() else "MISSING"
        print(f"{excel_file}: {status}")

def check_processed_data():
    """Check current processed data."""
    data_dir = PROJECT_ROOT / "data" / "processed"

    print("\n=== PROCESSED DATA STATUS ===")
    if not data_dir.exists():
        print("No processed data directory")
        return

    parquet_files = list(data_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files:")

    for file in sorted(parquet_files)[:10]:  # Show first 10
        size_mb = file.stat().st_size / (1024*1024)
        print(f"  {file.name}: {size_mb:.1f}MB")

def test_single_unit():
    """Test building a single unit quickly."""
    print("\n=== TESTING PCMSB C-02001 (QUICK TEST) ===")

    # Check if we can import the build function
    try:
        from pi_monitor.batch import build_unit_from_tags
        print("Build function imported successfully")

        # Check if basic files exist
        xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
        tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_c02001.txt"

        if xlsx.exists():
            print("PCMSB Excel file exists")
        else:
            print("PCMSB Excel file missing")

        if tags_file.exists():
            print("Tags file exists")
            # Count tags
            tags = []
            for line in tags_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    tags.append(line)
            print(f"  Found {len(tags)} tags")
        else:
            print("Tags file missing")

    except Exception as e:
        print(f"Import error: {e}")

if __name__ == "__main__":
    check_build_scripts()
    check_excel_files()
    check_processed_data()
    test_single_unit()

    print("\n=== RECOMMENDATION ===")
    print("To fetch all latest data, run: python fetch_all_plants_latest.py")
    print("To test single unit: python scripts/build_pcmsb.py C-02001")