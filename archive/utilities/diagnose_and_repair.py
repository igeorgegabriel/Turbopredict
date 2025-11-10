#!/usr/bin/env python3
"""
Comprehensive Diagnostic and Repair Tool for Data Freshness Issues

This script combines insights from all investigation scripts to provide a
holistic view of the data pipeline and suggest concrete repair actions.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import shutil

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from pi_monitor.parquet_database import ParquetDatabase
    from pi_monitor.parquet_auto_scan import ParquetAutoScanner
    PI_MONITOR_AVAILABLE = True
except ImportError:
    PI_MONITOR_AVAILABLE = False

def diagnose_and_repair():
    """Run a full diagnostic on the data pipeline and suggest repairs."""
    print("TURBOPREDICT X PROTEAN - DATA PIPELINE DIAGNOSTICS")
    print("=" * 70)

    if not PI_MONITOR_AVAILABLE:
        print("ERROR: pi_monitor package not found. Cannot run diagnostics.")
        print("Please run this script from the correct environment.")
        return

    db = ParquetDatabase()
    scanner = ParquetAutoScanner()
    all_units = db.get_all_units()
    pcmsb_units = [u for u in all_units if u.startswith('C-')]

    # --- Phase 1: Unit Status Analysis ---
    print("\nPHASE 1: UNIT STATUS ANALYSIS")
    print("-" * 50)
    stale_units = []
    fresh_units = []
    for unit in pcmsb_units:
        try:
            info = db.get_data_freshness_info(unit)
            if info.get('data_age_hours', 999) > 8:
                stale_units.append(unit)
                print(f"  - {unit:<12} | STALE ({info.get('data_age_hours', 999):.1f}h old)")
            else:
                fresh_units.append(unit)
                print(f"  - {unit:<12} | FRESH ({info.get('data_age_hours', 0):.1f}h old)")
        except Exception as e:
            stale_units.append(unit)
            print(f"  - {unit:<12} | ERROR ({e})")

    print(f"\nSummary: {len(fresh_units)} FRESH, {len(stale_units)} STALE/ERROR")
    if not stale_units:
        print("\n✅ All PCMSB units appear to be fresh. No issues detected.")
        return

    # --- Phase 2: Excel Data Source Forensics ---
    print("\nPHASE 2: EXCEL DATA SOURCE FORENSICS")
    print("-" * 50)
    excel_path = Path("excel/PCMSB_Automation.xlsx")
    units_missing_from_excel = []
    units_with_placeholder_data = []

    if not excel_path.exists():
        print(f"CRITICAL: Excel file not found at {excel_path}")
        print("This is the primary reason for data staleness.")
        return

    try:
        excel_file = pd.ExcelFile(excel_path)
        available_sheets = excel_file.sheet_names
        print(f"Found Excel file with sheets: {available_sheets}")

        for unit in stale_units:
            sheet_name = f"DL_{unit.replace('-', '_')}"
            if sheet_name not in available_sheets:
                units_missing_from_excel.append(unit)
                print(f"  - {unit}: Sheet '{sheet_name}' is MISSING.")
            else:
                # Check if the sheet has real data
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                if len(df) < 100: # Arbitrary threshold for "real" data
                    units_with_placeholder_data.append(unit)
                    print(f"  - {unit}: Sheet '{sheet_name}' exists but has only {len(df)} rows (likely placeholder).")

    except Exception as e:
        print(f"Error analyzing Excel file: {e}")

    # --- Phase 3: Parquet File System vs. Database Disconnect ---
    print("\nPHASE 3: PARQUET FILE SYSTEM ANALYSIS")
    print("-" * 50)
    data_dir = Path("data/processed")
    latest_parquet_mtime = None
    latest_parquet_file = None

    pcmsb_parquet_files = list(data_dir.glob("*C-*.parquet"))
    if pcmsb_parquet_files:
        latest_parquet_file = max(pcmsb_parquet_files, key=lambda p: p.stat().st_mtime)
        latest_parquet_mtime = datetime.fromtimestamp(latest_parquet_file.stat().st_mtime)
        age_hours = (datetime.now() - latest_parquet_mtime).total_seconds() / 3600
        print(f"Latest PCMSB parquet file modification: {latest_parquet_file.name} ({age_hours:.1f}h ago)")
        if age_hours > 8:
            print("  WARNING: The Parquet files themselves are stale. The ingestion pipeline is not running.")
    else:
        print("  CRITICAL: No PCMSB parquet files found in data/processed/.")

    # --- Phase 4: Root Cause Analysis and Repair Plan ---
    print("\n\nPHASE 4: ROOT CAUSE ANALYSIS & REPAIR PLAN")
    print("=" * 70)

    # Finding 1: Missing data in Excel
    if units_missing_from_excel or units_with_placeholder_data:
        print("🔴 FINDING 1: Incomplete PI DataLink Configuration")
        print("   The PCMSB Excel file is not configured to pull data for all required units.")
        print(f"   - Units with MISSING sheets: {units_missing_from_excel}")
        print(f"   - Units with PLACEHOLDER data: {units_with_placeholder_data}")
        print("\n   REPAIR ACTION 1: Populate Excel with existing data.")
        print("   This will use the historical data from Parquet files to fill the Excel sheets,")
        print("   making the units appear fresh temporarily and allowing the pipeline to run.")
        print("   Run this command:")
        print("   python populate_pcmsb_excel_sheets.py")
        print("\n   REPAIR ACTION 2 (Permanent Fix): Configure PI DataLink.")
        print("   You must open `PCMSB_Automation.xlsx` and configure PI DataLink to")
        print(f"   populate the sheets for these units: {sorted(list(set(units_missing_from_excel + units_with_placeholder_data)))}")

    # Finding 2: Stale Parquet files
    if latest_parquet_mtime and (datetime.now() - latest_parquet_mtime).total_seconds() / 3600 > 8:
        print("\n🔴 FINDING 2: Broken Data Ingestion Pipeline")
        print("   The system is not converting the (potentially fresh) Excel data into Parquet files.")
        print("   This means the `auto-scan` process for PCMSB units is failing or not running.")
        print("\n   REPAIR ACTION: Manually trigger the auto-scan for PCMSB units.")
        print("   This will force the system to re-read the Excel file and create new Parquet files.")
        print("   Run this command:")
        print("   python -m pi_monitor.cli auto-scan --plant PCMSB --force-refresh")

    # Finding 3: General Staleness
    if not (units_missing_from_excel or units_with_placeholder_data) and stale_units:
         print("\n🔴 FINDING 3: General System Staleness")
         print("   The Excel file seems correctly structured, but the data is still stale.")
         print("   This could be due to the PI server connection or the master refresh script failing.")
         print("\n   REPAIR ACTION 1: Verify PI Server Connection.")
         print("   Open `PCMSB_Automation.xlsx` and manually refresh all data connections.")
         print("\n   REPAIR ACTION 2: Run the master auto-refresh.")
         print("   This will attempt to refresh all configured Excel files.")
         print("   Run this command:")
         print("   python turbopredict.py --auto-refresh")

    print("\n" + "=" * 70)
    print("DIAGNOSTICS COMPLETE.")

if __name__ == "__main__":
    diagnose_and_repair()