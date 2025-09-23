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
import json

from pi_monitor.parquet_database import ParquetDatabase

def ultra_deep_investigation():
    """Forensic analysis of the entire data pipeline"""

    print("🔍 ULTRA DEEP INVESTIGATION: DATABASE STALENESS")
    print("=" * 80)
    print(f"Investigation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Phase 1: Parquet File System Analysis
    print("📁 PHASE 1: PARQUET FILE SYSTEM FORENSICS")
    print("-" * 50)

    data_dir = Path("data/processed")
    if not data_dir.exists():
        print("❌ CRITICAL: data/processed directory doesn't exist!")
        return

    parquet_files = list(data_dir.glob("*.parquet"))
    print(f"Total Parquet files found: {len(parquet_files)}")

    # Analyze each parquet file
    file_analysis = {}
    for file_path in parquet_files:
        try:
            stat = file_path.stat()

            # Read file to get data timestamps
            df = pd.read_parquet(file_path)

            if not df.empty and 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                latest_data = df['time'].max()
                earliest_data = df['time'].min()
                record_count = len(df)

                # Check if it's a unit file
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
            print(f"❌ Error analyzing {file_path.name}: {e}")

    # Sort by latest data timestamp
    sorted_files = sorted(file_analysis.items(), key=lambda x: x[1]['latest_data_timestamp'], reverse=True)

    print("\n🕒 PARQUET FILE TIMELINE ANALYSIS:")
    print(f"{'File':<40} {'Modified':<20} {'Latest Data':<20} {'Age(h)':<8} {'Records':<10}")
    print("-" * 110)

    stale_files = []
    fresh_files = []

    for filename, info in sorted_files[:15]:  # Top 15 most recent
        file_mod = info['file_modified'].strftime('%Y-%m-%d %H:%M')
        latest_data = info['latest_data_timestamp'].strftime('%Y-%m-%d %H:%M')
        age_hours = info['data_age_hours']
        records = info['record_count']

        display_name = filename[:39] if len(filename) <= 39 else filename[:36] + "..."

        status = "🔴 STALE" if age_hours > 2 else "🟢 FRESH"

        print(f"{display_name:<40} {file_mod:<20} {latest_data:<20} {age_hours:<8.1f} {records:<10,}")

        if age_hours > 2:
            stale_files.append((filename, info))
        else:
            fresh_files.append((filename, info))

    print(f"\n📊 SUMMARY:")
    print(f"Fresh files (< 2h old): {len(fresh_files)}")
    print(f"Stale files (> 2h old): {len(stale_files)}")

    # Phase 2: Database Loading Mechanism Analysis
    print(f"\n\n🗄️ PHASE 2: DATABASE LOADING MECHANISM")
    print("-" * 50)

    try:
        db = ParquetDatabase()

        # Check which files the database is actually loading
        print("Database initialization analysis:")
        print(f"Database data directory: {db.processed_dir}")
        print(f"Database files found: {len(list(db.processed_dir.glob('*.parquet')))}")

        # Test specific unit loading
        test_unit = "K-31-01"
        print(f"\nTesting unit loading for: {test_unit}")

        unit_data = db.get_unit_data(test_unit)
        if not unit_data.empty:
            unit_latest = pd.to_datetime(unit_data['time']).max()
            unit_age = (datetime.now() - unit_latest).total_seconds() / 3600
            print(f"Unit data latest timestamp: {unit_latest}")
            print(f"Unit data age: {unit_age:.1f} hours")
            print(f"Unit records loaded: {len(unit_data):,}")

            # Check which files were actually used
            unique_tags = unit_data['tag'].nunique() if 'tag' in unit_data.columns else 0
            print(f"Unique tags in loaded data: {unique_tags}")

        else:
            print("❌ CRITICAL: No data loaded for test unit!")

        # Get freshness info
        freshness_info = db.get_data_freshness_info(test_unit)
        print(f"\nFreshness info:")
        for key, value in freshness_info.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"❌ Database loading error: {e}")
        import traceback
        traceback.print_exc()

    # Phase 3: Excel vs Parquet Synchronization Check
    print(f"\n\n📊 PHASE 3: EXCEL VS PARQUET SYNCHRONIZATION")
    print("-" * 50)

    excel_dir = Path("excel")
    if excel_dir.exists():
        excel_files = list(excel_dir.glob("*.xlsx"))
        main_excel_files = [f for f in excel_files if not any(x in f.name for x in ['backup', 'dummy', 'working'])]

        print(f"Main Excel files found: {len(main_excel_files)}")

        for excel_file in main_excel_files:
            try:
                stat = excel_file.stat()
                excel_modified = datetime.fromtimestamp(stat.st_mtime)
                excel_age = (datetime.now() - excel_modified).total_seconds() / 3600

                print(f"\n📄 {excel_file.name}:")
                print(f"  Last modified: {excel_modified.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Age: {excel_age:.1f} hours")
                print(f"  Size: {stat.st_size / (1024*1024):.1f} MB")

                # Check if there are corresponding parquet files that should be newer
                if 'K-' in excel_file.name or 'PCFS' in excel_file.name:
                    k_unit_files = [f for f in sorted_files if f[1]['unit'] and f[1]['unit'].startswith('K-')]
                    if k_unit_files:
                        latest_k_file = k_unit_files[0]
                        parquet_age = latest_k_file[1]['data_age_hours']

                        print(f"  Corresponding K-unit Parquet age: {parquet_age:.1f} hours")

                        if excel_age < parquet_age:
                            print(f"  ⚠️  SYNC ISSUE: Excel is newer than Parquet data!")
                        elif parquet_age > 2:
                            print(f"  ❌ STALE ISSUE: Both Excel and Parquet are stale!")
                        else:
                            print(f"  ✅ Sync appears normal")

            except Exception as e:
                print(f"  ❌ Error analyzing {excel_file.name}: {e}")

    # Phase 4: Option [1] Execution Trace
    print(f"\n\n🔄 PHASE 4: OPTION [1] EXECUTION EFFECTIVENESS")
    print("-" * 50)

    print("Checking if option [1] is actually working...")

    # Check for recent refresh logs/evidence
    recent_backups = list(excel_dir.glob("*backup*")) if excel_dir.exists() else []
    recent_dummies = list(excel_dir.glob("*dummy*")) if excel_dir.exists() else []

    now = datetime.now()
    recent_threshold = now - timedelta(hours=2)

    recent_backup_activity = []
    for backup in recent_backups:
        backup_time = datetime.fromtimestamp(backup.stat().st_mtime)
        if backup_time > recent_threshold:
            recent_backup_activity.append((backup.name, backup_time))

    print(f"Recent backup activity (last 2h): {len(recent_backup_activity)}")
    for name, time in recent_backup_activity:
        print(f"  {name}: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    if not recent_backup_activity:
        print("❌ CRITICAL: No recent Excel refresh activity detected!")
        print("   This suggests option [1] may not be executing properly")

    # Phase 5: Root Cause Hypothesis
    print(f"\n\n🎯 PHASE 5: ROOT CAUSE ANALYSIS")
    print("-" * 50)

    issues_found = []

    # Check for stale parquet files
    if len(stale_files) > len(fresh_files):
        issues_found.append("MAJOR: More stale parquet files than fresh ones")

    # Check for sync issues
    if not recent_backup_activity:
        issues_found.append("CRITICAL: No evidence of recent Excel refresh execution")

    # Check data age vs file modification
    for filename, info in stale_files[:5]:
        file_age = (datetime.now() - info['file_modified']).total_seconds() / 3600
        data_age = info['data_age_hours']

        if abs(file_age - data_age) < 1:  # File and data age are similar
            issues_found.append(f"SYNC: {filename} - file and data both stale (no refresh)")
        elif file_age < data_age:
            issues_found.append(f"PIPELINE: {filename} - file newer but data still stale (refresh failed)")

    print("🚨 ISSUES IDENTIFIED:")
    for i, issue in enumerate(issues_found, 1):
        print(f"{i}. {issue}")

    if not issues_found:
        print("✅ No obvious issues found - data staleness may be expected")

    print(f"\n🔧 RECOMMENDED ACTIONS:")
    print("1. Run option [1] with verbose logging to trace execution")
    print("2. Check PI DataLink connectivity and permissions")
    print("3. Verify Excel automation is not blocked by security/antivirus")
    print("4. Check if parquet files are being written to correct location")
    print("5. Verify database reload is picking up updated files")

if __name__ == "__main__":
    ultra_deep_investigation()