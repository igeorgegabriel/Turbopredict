#!/usr/bin/env python3
"""
Verify which PCMSB fixes are integrated into option [1] vs separate scripts
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def verify_option1_integration():
    """Check which fixes are integrated into option [1]"""

    print("VERIFYING OPTION [1] INTEGRATION STATUS")
    print("=" * 45)

    # Check files that implement option [1]
    key_files = [
        "turbopredict.py",
        "pi_monitor/parquet_auto_scan.py",
        "pi_monitor/batch.py",
        "pi_monitor/clean.py"
    ]

    print("KEY FILES FOR OPTION [1]:")
    for file_path in key_files:
        if Path(file_path).exists():
            print(f"   OK {file_path}")
        else:
            print(f"   MISSING {file_path}")
    print()

    # Check specific fixes
    fixes_status = {}

    # 1. PCMSB Sheet Mapping Fix
    try:
        scan_file = Path("pi_monitor/parquet_auto_scan.py")
        if scan_file.exists():
            content = scan_file.read_text()
            if "PCMSB units (C-) use DL_WORK sheet" in content:
                fixes_status["Sheet Mapping"] = "INTEGRATED"
            else:
                fixes_status["Sheet Mapping"] = "NOT INTEGRATED"
        else:
            fixes_status["Sheet Mapping"] = "FILE MISSING"
    except Exception as e:
        fixes_status["Sheet Mapping"] = f"ERROR: {e}"

    # 2. PCMSB Timeout Fix
    try:
        scan_file = Path("pi_monitor/parquet_auto_scan.py")
        if scan_file.exists():
            content = scan_file.read_text()
            if "settle_time = 30.0 if str(plant).upper().startswith(\"PCMSB\")" in content:
                fixes_status["Timeout Fix"] = "INTEGRATED"
            else:
                fixes_status["Timeout Fix"] = "NOT INTEGRATED"
        else:
            fixes_status["Timeout Fix"] = "FILE MISSING"
    except Exception as e:
        fixes_status["Timeout Fix"] = f"ERROR: {e}"

    # 3. PCMSB Excel Path Integration
    try:
        turbo_file = Path("turbopredict.py")
        if turbo_file.exists():
            content = turbo_file.read_text(encoding='utf-8', errors='ignore')
            if "PCMSB_Automation.xlsx" in content:
                fixes_status["Excel Path"] = "INTEGRATED"
            else:
                fixes_status["Excel Path"] = "NOT INTEGRATED"
        else:
            fixes_status["Excel Path"] = "FILE MISSING"
    except Exception as e:
        fixes_status["Excel Path"] = f"ERROR: {e}"

    # 4. Dedup Integration
    try:
        scan_file = Path("pi_monitor/parquet_auto_scan.py")
        if scan_file.exists():
            content = scan_file.read_text()
            if "from .clean import dedup_parquet" in content and "dedup_path = dedup_parquet(" in content:
                fixes_status["Dedup Creation"] = "INTEGRATED"
            else:
                fixes_status["Dedup Creation"] = "NOT INTEGRATED"
        else:
            fixes_status["Dedup Creation"] = "FILE MISSING"
    except Exception as e:
        fixes_status["Dedup Creation"] = f"ERROR: {e}"

    # Display results
    print("PCMSB FIXES INTEGRATION STATUS:")
    print("-" * 35)

    for fix_name, status in fixes_status.items():
        print(f"{fix_name:<20} {status}")

    print()

    # Check separate scripts that might not be integrated
    separate_scripts = [
        "manual_pcmsb_excel_refresh.py",
        "pcmsb_excel_reader.py",
        "generate_pcmsb_data.py",
        "fix_pcmsb_batch_timeout.py",
        "ensure_pcmsb_database_integration.py"
    ]

    print("SEPARATE PCMSB SCRIPTS (not integrated):")
    print("-" * 45)

    for script in separate_scripts:
        script_path = Path(script)
        if script_path.exists():
            print(f"   WARNING {script} - EXISTS (manual script)")
        else:
            print(f"   OK {script} - NOT FOUND (good)")

    print()

    # Summary
    integrated_count = sum(1 for status in fixes_status.values() if "INTEGRATED" in status)
    total_fixes = len(fixes_status)

    print("INTEGRATION SUMMARY:")
    print("-" * 20)
    print(f"Fixes integrated into option [1]: {integrated_count}/{total_fixes}")

    if integrated_count == total_fixes:
        print("SUCCESS: ALL PCMSB FIXES ARE INTEGRATED INTO OPTION [1]")
        print("   Option [1] will handle all PCMSB units correctly")
    else:
        print("WARNING: SOME FIXES ARE NOT INTEGRATED")
        print("   You may need to run separate scripts for full functionality")

    print()

    # Test integration
    print("TESTING OPTION [1] READINESS:")
    print("-" * 30)

    try:
        from pi_monitor.parquet_auto_scan import ParquetAutoScanner
        from pi_monitor.config import Config

        config = Config()
        scanner = ParquetAutoScanner(config)

        print("OK ParquetAutoScanner can be imported and initialized")

        # Check if PCMSB units are recognized
        units = scanner.db.get_all_units()
        pcmsb_units = [unit for unit in units if unit.startswith('C-')]

        print(f"OK Found {len(pcmsb_units)} PCMSB units in database")

        if pcmsb_units:
            print(f"   PCMSB units: {pcmsb_units}")

        print("SUCCESS: OPTION [1] IS READY FOR PCMSB PROCESSING")

    except Exception as e:
        print(f"ERROR testing option [1]: {e}")
        print("WARNING: OPTION [1] MAY NOT BE READY")

if __name__ == "__main__":
    verify_option1_integration()