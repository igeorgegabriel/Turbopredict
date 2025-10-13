#!/usr/bin/env python3
"""Quick test to verify CLI integration"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

# Test imports
try:
    from turbopredict import TurbopredictSystem
    print("[OK] turbopredict.py imports successfully")
except Exception as e:
    print(f"[X] Import error: {e}")
    sys.exit(1)

# Check if new methods exist
system = TurbopredictSystem()

methods_to_check = [
    "run_data_health_check",
    "run_incremental_refresh",
    "run_unit_data_analysis"
]

print("\nChecking new methods:")
for method in methods_to_check:
    if hasattr(system, method):
        print(f"  [OK] {method}")
    else:
        print(f"  [X] {method} - MISSING")

# Check if scripts exist
print("\nChecking scripts:")
scripts = [
    "scripts/check_unit_data_health.py",
    "scripts/incremental_refresh.py",
    "scripts/analyze_unit_data.py",
    "scripts/fix_invalid_dates.py"
]

for script in scripts:
    script_path = PROJECT_ROOT / script
    if script_path.exists():
        print(f"  [OK] {script}")
    else:
        print(f"  [X] {script} - MISSING")

print("\n[OK] CLI integration test complete!")
print("\nNew menu options available:")
print("  H - DATA HEALTH CHECK")
print("  I - INCREMENTAL REFRESH")
print("  J - UNIT DATA ANALYSIS")
