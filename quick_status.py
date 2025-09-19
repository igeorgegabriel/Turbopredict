#!/usr/bin/env python3
"""
Quick Status Checker - Check Excel and Unit Progress
"""

import time
import psutil
from pathlib import Path
from datetime import datetime
import sys

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("TURBOPREDICT X PROTEAN - QUICK STATUS CHECK")
    print("=" * 50)
    print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
    
    # Check Excel processes
    print("\n1. EXCEL PROCESSES:")
    print("-" * 20)
    
    excel_found = False
    for proc in psutil.process_iter(['pid', 'name', 'create_time']):
        try:
            if 'excel' in proc.info['name'].lower():
                create_time = datetime.fromtimestamp(proc.info['create_time'])
                runtime = (datetime.now() - create_time).total_seconds() / 60
                print(f"   PID {proc.info['pid']}: Running {runtime:.1f} minutes")
                excel_found = True
        except:
            continue
    
    if not excel_found:
        print("   No Excel processes found")
    
    # Check recent file changes
    print("\n2. RECENT FILE ACTIVITY:")
    print("-" * 25)
    
    recent_files = []
    for pattern in ["data/**/*.xlsx", "data/**/*.parquet", "data/**/*.sqlite"]:
        for file_path in Path(".").glob(pattern):
            try:
                stat = file_path.stat()
                modified_time = datetime.fromtimestamp(stat.st_mtime)
                minutes_ago = (datetime.now() - modified_time).total_seconds() / 60
                
                if minutes_ago < 60:  # Last hour
                    recent_files.append({
                        'path': file_path,
                        'minutes_ago': minutes_ago,
                        'size_mb': stat.st_size / (1024 * 1024)
                    })
            except:
                continue
    
    if recent_files:
        recent_files.sort(key=lambda x: x['minutes_ago'])
        for file_info in recent_files[:5]:  # Show 5 most recent
            print(f"   {file_info['path'].name}: {file_info['minutes_ago']:.1f}m ago ({file_info['size_mb']:.1f}MB)")
    else:
        print("   No recent file activity")
    
    # Try to check unit status from cache
    print("\n3. UNIT STATUS (from cache):")
    print("-" * 30)
    
    try:
        from pi_monitor.instant_cache import get_instant_cache
        cache = get_instant_cache()
        
        units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        for unit in units:
            try:
                status = cache.get_instant_unit_status(unit)
                if status["success"]:
                    records = status["analytics"]["total_records"]
                    response_ms = status["response_time_ms"]
                    print(f"   {unit}: {records:,} records ({response_ms:.0f}ms)")
                else:
                    print(f"   {unit}: Not in cache")
            except Exception as e:
                print(f"   {unit}: Error - {str(e)[:30]}...")
    except:
        print("   Cache not available")
    
    # Recommendations
    print("\n4. STATUS ASSESSMENT:")
    print("-" * 20)
    
    if excel_found and recent_files:
        print("   [ACTIVE] Excel is running and files are being updated")
        print("   [WAIT] Continue current process")
    elif excel_found and not recent_files:
        print("   [STUCK?] Excel running but no recent file changes")
        print("   [ACTION] Consider restarting if >2 hours")
    elif not excel_found and recent_files:
        print("   [COMPLETED?] No Excel process but recent file activity")
        print("   [CHECK] Run main system to verify completion")
    else:
        print("   [IDLE] No Excel process or recent activity")
        print("   [START] Begin new refresh cycle if needed")
    
    print(f"\nTo run incremental processing:")
    print(f"python incremental_processor.py")
    print(f"\nTo check full system:")
    print(f"python turbopredict.py")

if __name__ == "__main__":
    main()