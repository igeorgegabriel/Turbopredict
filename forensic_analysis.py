#!/usr/bin/env python3
"""
ULTRA DEEP INVESTIGATION: Why database staleness persists despite fixes
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase

def forensic_analysis():
    """Forensic analysis of the entire data pipeline"""

    print("ULTRA DEEP INVESTIGATION: DATABASE STALENESS")
    print("=" * 80)
    print(f"Investigation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Phase 1: Parquet File System Analysis
    print("PHASE 1: PARQUET FILE SYSTEM FORENSICS")
    print("-" * 50)

    data_dir = Path("data/processed")
    if not data_dir.exists():
        print("CRITICAL: data/processed directory doesn't exist!")
        return

    parquet_files = list(data_dir.glob("*.parquet"))
    print(f"Total Parquet files found: {len(parquet_files)}")

    # Analyze each parquet file
    file_analysis = {}
    for file_path in parquet_files:
        try:
            stat = file_path.stat()

            # Read file headers only for speed
            df = pd.read_parquet(file_path, columns=['time'] if 'time' in pd.read_parquet(file_path, nrows=1).columns else None)

            if not df.empty and 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                latest_data = df['time'].max()
                earliest_data = df['time'].min()
                record_count = len(df)

                # Extract unit from filename
                unit = None
                if 'K-' in file_path.name:
                    parts = file_path.name.split('_')
                    for part in parts:
                        if part.startswith('K-'):
                            unit = part
                            break

                file_analysis[file_path.name] = {
                    'file_modified': datetime.fromtimestamp(stat.st_mtime),
                    'file_size_mb': stat.st_size / (1024*1024),
                    'record_count': record_count,
                    'latest_data_timestamp': latest_data,
                    'earliest_data_timestamp': earliest_data,
                    'data_age_hours': (datetime.now() - latest_data).total_seconds() / 3600,
                    'unit': unit,
                    'file_path': str(file_path)
                }

        except Exception as e:
            print(f"Error analyzing {file_path.name}: {e}")

    # Sort by latest data timestamp
    sorted_files = sorted(file_analysis.items(), key=lambda x: x[1]['latest_data_timestamp'], reverse=True)

    print("\\nPARQUET FILE TIMELINE ANALYSIS:")
    print(f"{'File':<40} {'File Modified':<20} {'Latest Data':<20} {'Age(h)':<8} {'Records':<10}")
    print("-" * 110)

    stale_files = []
    fresh_files = []
    critical_stale = []

    for filename, info in sorted_files[:20]:  # Top 20 files
        file_mod = info['file_modified'].strftime('%Y-%m-%d %H:%M')
        latest_data = info['latest_data_timestamp'].strftime('%Y-%m-%d %H:%M')
        age_hours = info['data_age_hours']
        records = info['record_count']

        display_name = filename[:39] if len(filename) <= 39 else filename[:36] + "..."

        print(f"{display_name:<40} {file_mod:<20} {latest_data:<20} {age_hours:<8.1f} {records:<10,}")

        if age_hours > 8:  # Very stale
            critical_stale.append((filename, info))
        elif age_hours > 2:  # Somewhat stale
            stale_files.append((filename, info))
        else:
            fresh_files.append((filename, info))

    print(f"\\nSUMMARY:")
    print(f"Fresh files (< 2h old): {len(fresh_files)}")
    print(f"Stale files (2-8h old): {len(stale_files)}")
    print(f"Critical stale files (> 8h old): {len(critical_stale)}")

    # Critical finding: Check if K-31-01 files are stale
    k31_files = [f for f in sorted_files if 'K-31-01' in f[0]]
    if k31_files:
        print(f"\\nK-31-01 SPECIFIC ANALYSIS:")
        for filename, info in k31_files[:3]:
            age = info['data_age_hours']
            status = "CRITICAL STALE" if age > 8 else "STALE" if age > 2 else "FRESH"
            print(f"  {filename}: {age:.1f}h old - {status}")

    # Phase 2: Database vs File System Disconnect
    print(f"\\n\\nPHASE 2: DATABASE VS FILE SYSTEM DISCONNECT")
    print("-" * 50)

    try:
        db = ParquetDatabase()

        # Check what the database thinks vs what files actually contain
        test_unit = "K-31-01"
        print(f"Testing database loading for: {test_unit}")

        # Database freshness check
        freshness_info = db.get_data_freshness_info(test_unit)
        db_latest = freshness_info.get('latest_timestamp')
        db_age = freshness_info.get('data_age_hours', 0)

        print(f"Database reports:")
        print(f"  Latest timestamp: {db_latest}")
        print(f"  Data age: {db_age:.1f} hours")

        # File system reality check
        if k31_files:
            file_latest = k31_files[0][1]['latest_data_timestamp']
            file_age = k31_files[0][1]['data_age_hours']

            print(f"\\nFile system reality:")
            print(f"  Latest timestamp: {file_latest}")
            print(f"  Data age: {file_age:.1f} hours")

            # Check for disconnect
            time_diff = abs((db_latest - file_latest).total_seconds() / 3600) if db_latest and file_latest else 999
            if time_diff > 1:  # More than 1 hour difference
                print(f"\\nCRITICAL DISCONNECT FOUND!")
                print(f"  Database and file system timestamps differ by {time_diff:.1f} hours")
                print(f"  This indicates database is not loading the latest files!")

    except Exception as e:
        print(f"Database analysis error: {e}")

    # Phase 3: Excel Refresh Evidence
    print(f"\\n\\nPHASE 3: EXCEL REFRESH EXECUTION EVIDENCE")
    print("-" * 50)

    excel_dir = Path("excel")
    if not excel_dir.exists():
        print("CRITICAL: Excel directory not found!")
        return

    # Check for recent refresh activity
    now = datetime.now()
    recent_threshold = now - timedelta(hours=6)  # Last 6 hours

    backup_files = list(excel_dir.glob("*backup*"))
    dummy_files = list(excel_dir.glob("*dummy*"))

    recent_backups = []
    recent_dummies = []

    for backup in backup_files:
        backup_time = datetime.fromtimestamp(backup.stat().st_mtime)
        if backup_time > recent_threshold:
            recent_backups.append((backup.name, backup_time))

    for dummy in dummy_files:
        dummy_time = datetime.fromtimestamp(dummy.stat().st_mtime)
        if dummy_time > recent_threshold:
            recent_dummies.append((dummy.name, dummy_time))

    print(f"Recent backup files (last 6h): {len(recent_backups)}")
    for name, time in sorted(recent_backups, key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {name}: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"\\nRecent dummy files (last 6h): {len(recent_dummies)}")
    for name, time in sorted(recent_dummies, key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {name}: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Check main Excel files
    main_excel = ['PCFS_Automation_2.xlsx', 'ABF_Automation.xlsx', 'PCMSB_Automation.xlsx']
    print(f"\\nMain Excel file status:")
    for excel_name in main_excel:
        excel_path = excel_dir / excel_name
        if excel_path.exists():
            excel_time = datetime.fromtimestamp(excel_path.stat().st_mtime)
            excel_age = (now - excel_time).total_seconds() / 3600
            print(f"  {excel_name}: {excel_time.strftime('%Y-%m-%d %H:%M:%S')} ({excel_age:.1f}h ago)")
        else:
            print(f"  {excel_name}: MISSING!")

    # Phase 4: Root Cause Determination
    print(f"\\n\\nPHASE 4: ROOT CAUSE DETERMINATION")
    print("-" * 50)

    root_causes = []

    # Check 1: Are files actually stale?
    if len(critical_stale) > 0:
        root_causes.append(f"CONFIRMED: {len(critical_stale)} files are critically stale (>8h)")

    # Check 2: Is refresh actually running?
    if not recent_backups and not recent_dummies:
        root_causes.append("CONFIRMED: No evidence of Excel refresh execution in last 6h")

    # Check 3: Database loading wrong files?
    if k31_files and freshness_info:
        file_age = k31_files[0][1]['data_age_hours']
        db_age = freshness_info.get('data_age_hours', 0)
        if abs(file_age - db_age) > 2:
            root_causes.append(f"CONFIRMED: Database loading disconnect (file: {file_age:.1f}h vs db: {db_age:.1f}h)")

    # Check 4: Parquet update pipeline broken?
    if recent_backups and len(critical_stale) > 0:
        root_causes.append("CONFIRMED: Excel refreshes but Parquet files not updating")

    print("ROOT CAUSES IDENTIFIED:")
    for i, cause in enumerate(root_causes, 1):
        print(f"{i}. {cause}")

    if not root_causes:
        print("No definitive root cause found - may need deeper investigation")

    print(f"\\nCRITICAL ACTIONS REQUIRED:")
    if "No evidence of Excel refresh execution" in str(root_causes):
        print("1. IMMEDIATE: Verify option [1] is actually executing refresh logic")
        print("2. Check if Excel automation is blocked/failing silently")

    if "Parquet files not updating" in str(root_causes):
        print("3. IMMEDIATE: Check parquet file write pipeline")
        print("4. Verify file permissions and disk space")

    if "Database loading disconnect" in str(root_causes):
        print("5. IMMEDIATE: Check database file discovery logic")
        print("6. Verify database is scanning correct directory")

if __name__ == "__main__":
    forensic_analysis()