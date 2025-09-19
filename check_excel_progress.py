#!/usr/bin/env python3
"""
IMMEDIATE EXCEL PROGRESS CHECKER
Check if Excel is actually processing and show unit-by-unit status
"""

import time
import psutil
from pathlib import Path
from datetime import datetime
import sys

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def check_excel_processes():
    """Check if Excel processes are running"""
    excel_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'create_time', 'cpu_percent']):
        try:
            if 'excel' in proc.info['name'].lower():
                create_time = datetime.fromtimestamp(proc.info['create_time'])
                runtime = (datetime.now() - create_time).total_seconds()
                
                excel_processes.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'runtime_minutes': runtime / 60,
                    'cpu_percent': proc.info['cpu_percent'],
                    'create_time': create_time
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return excel_processes

def check_file_activity():
    """Check if data files are being modified"""
    
    data_dir = Path("data")
    file_activity = {}
    
    # Check for recent file modifications
    for pattern in ["**/*.xlsx", "**/*.parquet", "**/*.sqlite", "**/*.duckdb"]:
        for file_path in data_dir.glob(pattern):
            try:
                stat = file_path.stat()
                modified_time = datetime.fromtimestamp(stat.st_mtime)
                minutes_ago = (datetime.now() - modified_time).total_seconds() / 60
                
                if minutes_ago < 30:  # Files modified in last 30 minutes
                    file_activity[str(file_path)] = {
                        'modified': modified_time.strftime('%H:%M:%S'),
                        'minutes_ago': minutes_ago,
                        'size_mb': stat.st_size / (1024 * 1024)
                    }
            except:
                continue
    
    return file_activity

def quick_unit_status_check():
    """Quick check of unit data freshness"""
    
    try:
        from pi_monitor.parquet_database import ParquetDatabase
        from pi_monitor.instant_cache import get_instant_cache
        
        db = ParquetDatabase()
        units = db.get_all_units()
        
        # Check instant cache status
        cache = get_instant_cache()
        cache_stats = cache.get_cache_stats()
        
        unit_status = {}
        for unit in units:
            try:
                status = cache.get_instant_unit_status(unit)
                if status["success"]:
                    analytics = status["analytics"]
                    unit_status[unit] = {
                        "cache_available": True,
                        "records": analytics.get("total_records", 0),
                        "response_time_ms": status["response_time_ms"],
                        "latest_data": analytics.get("data_timespan", {}).get("latest", "Unknown")
                    }
                else:
                    unit_status[unit] = {
                        "cache_available": False,
                        "error": status.get("error", "Unknown")
                    }
            except Exception as e:
                unit_status[unit] = {
                    "cache_available": False,
                    "error": str(e)
                }
        
        return unit_status, cache_stats
        
    except Exception as e:
        return {}, {"error": str(e)}

def main():
    """Main progress checker"""
    
    print("TURBOPREDICT X PROTEAN - EXCEL PROGRESS CHECKER")
    print("=" * 60)
    print(f"⏰ Current time: {datetime.now().strftime('%H:%M:%S')}")
    
    # Check Excel processes
    print("\n1. EXCEL PROCESS STATUS:")
    print("-" * 30)
    
    excel_procs = check_excel_processes()
    if excel_procs:
        for proc in excel_procs:
            print(f"   📊 PID {proc['pid']}: {proc['name']}")
            print(f"      Runtime: {proc['runtime_minutes']:.1f} minutes")
            print(f"      CPU: {proc['cpu_percent']:.1f}%")
            print(f"      Started: {proc['create_time'].strftime('%H:%M:%S')}")
    else:
        print("   ❌ No Excel processes found")
    
    # Check file activity
    print("\n2. FILE ACTIVITY (Last 30 minutes):")
    print("-" * 40)
    
    file_activity = check_file_activity()
    if file_activity:
        for file_path, info in file_activity.items():
            print(f"   📁 {Path(file_path).name}")
            print(f"      Modified: {info['modified']} ({info['minutes_ago']:.1f}m ago)")
            print(f"      Size: {info['size_mb']:.1f} MB")
    else:
        print("   ❌ No recent file activity detected")
    
    # Check unit status
    print("\n3. UNIT DATA STATUS:")
    print("-" * 25)
    
    unit_status, cache_stats = quick_unit_status_check()
    
    if unit_status:
        for unit, status in unit_status.items():
            if status["cache_available"]:
                print(f"   ✅ {unit}: {status['records']:,} records ({status['response_time_ms']:.0f}ms)")
                print(f"      Latest data: {status['latest_data']}")
            else:
                print(f"   ❌ {unit}: {status.get('error', 'Not available')}")
    else:
        print("   ⚠️  Unable to check unit status")
    
    # Cache status
    if cache_stats and "error" not in cache_stats:
        print(f"\n4. CACHE STATUS:")
        print(f"-" * 20)
        print(f"   📊 Cached items: {cache_stats.get('total_cached_items', 0)}")
        print(f"   💾 Memory usage: {cache_stats.get('total_memory_usage_mb', 0):.0f} MB")
        print(f"   ✅ Fresh items: {cache_stats.get('fresh_items', 0)}")
        print(f"   ⏰ Stale items: {cache_stats.get('stale_items', 0)}")
    
    # Recommendations
    print(f"\n5. RECOMMENDATIONS:")
    print(f"-" * 20)
    
    if excel_procs:
        longest_runtime = max(proc['runtime_minutes'] for proc in excel_procs)
        if longest_runtime > 60:
            print("   ⚠️  Excel has been running for over 1 hour")
            print("   💡 Consider running incremental processing instead")
        elif longest_runtime > 30:
            print("   ⏳ Excel processing taking longer than expected")
            print("   💡 This is normal for large datasets")
        else:
            print("   ✅ Excel processing time is reasonable")
    else:
        print("   ❓ No Excel processes - refresh may have completed or failed")
    
    if not file_activity:
        print("   ⚠️  No recent file changes detected")
        print("   💡 Excel may be stuck - consider restarting")
    else:
        print("   ✅ Recent file activity suggests processing is active")
    
    # Show next steps
    print(f"\n6. NEXT STEPS:")
    print(f"-" * 15)
    print("   1. If Excel is stuck (no file activity + long runtime):")
    print("      - Kill Excel processes and restart")
    print("      - Run: python incremental_processor.py")
    print("")  
    print("   2. If Excel is active (recent file changes):")
    print("      - Wait for current process to complete")
    print("      - Check again in 10-15 minutes")
    print("")
    print("   3. For immediate unit status:")
    print("      - Use cached diagnostics (sub-second response)")
    print("      - Run unit analysis on current data")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()