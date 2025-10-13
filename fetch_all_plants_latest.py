#!/usr/bin/env python3
"""
Fetch All Plants Latest Data
Unified script to fetch the latest data for all plants in the system.
"""

from pathlib import Path
import sys
import subprocess
import time
from datetime import datetime
import os

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Plant configuration - maps plant to their units and build scripts
PLANT_CONFIG = {
    'PCFS': {
        'units': ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01'],
        'scripts': [
            'scripts/build_pcfs_k1201.py',
            'scripts/build_pcfs_k1601.py',
            'scripts/build_pcfs_k1901.py',
            'scripts/build_pcfs_k3101.py'
        ]
    },
    'PCMSB': {
        'units': ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202', 'XT-07002'],
        'script': 'scripts/build_pcmsb.py'
    },
    'ABFSB': {
        'units': ['07-MT-01-K-001'],
        'scripts': ['scripts/build_abf_07mt01_k001.py']
    },
    'MLNG': {
        'units': [],  # Add units when available
        'scripts': []
    },
    'PFLNG1': {
        'units': [],  # Add units when available
        'scripts': []
    },
    'PFLNG2': {
        'units': [],  # Add units when available
        'scripts': []
    }
}

def run_script(script_path, args=None):
    """Run a Python script and return success status."""
    cmd = ['python', str(script_path)]
    if args:
        cmd.extend(args)

    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
        if result.returncode == 0:
            print(f"✓ Success: {script_path}")
            if result.stdout.strip():
                print(result.stdout)
            return True
        else:
            print(f"✗ Failed: {script_path}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            if result.stdout:
                print(f"Output: {result.stdout}")
            return False
    except Exception as e:
        print(f"✗ Exception running {script_path}: {e}")
        return False

def fetch_pcfs_data():
    """Fetch all PCFS units data."""
    print("\n=== FETCHING PCFS DATA ===")
    config = PLANT_CONFIG['PCFS']
    success_count = 0

    for script in config['scripts']:
        script_path = PROJECT_ROOT / script
        if script_path.exists():
            if run_script(script_path):
                success_count += 1
        else:
            print(f"Script not found: {script}")

    print(f"PCFS: {success_count}/{len(config['scripts'])} units successful")
    return success_count

def fetch_pcmsb_data():
    """Fetch all PCMSB units data."""
    print("\n=== FETCHING PCMSB DATA ===")
    config = PLANT_CONFIG['PCMSB']
    script_path = PROJECT_ROOT / config['script']

    if not script_path.exists():
        print(f"PCMSB script not found: {config['script']}")
        return 0

    success_count = 0
    for unit in config['units']:
        print(f"\nFetching PCMSB {unit}...")
        if run_script(script_path, [unit]):
            success_count += 1

    print(f"PCMSB: {success_count}/{len(config['units'])} units successful")
    return success_count

def fetch_abfsb_data():
    """Fetch ABFSB units data."""
    print("\n=== FETCHING ABFSB DATA ===")
    config = PLANT_CONFIG['ABFSB']
    success_count = 0

    for script in config['scripts']:
        script_path = PROJECT_ROOT / script
        if script_path.exists():
            if run_script(script_path):
                success_count += 1
        else:
            print(f"Script not found: {script}")

    print(f"ABFSB: {success_count}/{len(config['scripts'])} units successful")
    return success_count

def check_data_freshness():
    """Check freshness of processed data files."""
    print("\n=== CHECKING DATA FRESHNESS ===")
    data_dir = PROJECT_ROOT / "data" / "processed"

    if not data_dir.exists():
        print("No processed data directory found")
        return

    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        print("No parquet files found")
        return

    print(f"Found {len(parquet_files)} parquet files:")
    for file in sorted(parquet_files):
        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        age = datetime.now() - mtime
        age_str = f"{age.total_seconds() / 3600:.1f}h ago"
        print(f"  {file.name}: {mtime.strftime('%Y-%m-%d %H:%M')} ({age_str})")

def main():
    """Main function to fetch all plants data."""
    start_time = time.time()
    print(f"Starting all plants data fetch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    total_success = 0
    total_units = 0

    # Fetch data for each plant
    try:
        # PCFS
        pcfs_success = fetch_pcfs_data()
        total_success += pcfs_success
        total_units += len(PLANT_CONFIG['PCFS']['scripts'])

        # PCMSB
        pcmsb_success = fetch_pcmsb_data()
        total_success += pcmsb_success
        total_units += len(PLANT_CONFIG['PCMSB']['units'])

        # ABFSB
        abfsb_success = fetch_abfsb_data()
        total_success += abfsb_success
        total_units += len(PLANT_CONFIG['ABFSB']['scripts'])

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 1

    # Summary
    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"DATA FETCH COMPLETE")
    print(f"{'='*50}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Success rate: {total_success}/{total_units} units ({total_success/total_units*100:.1f}%)")

    # Check final data state
    check_data_freshness()

    if total_success == total_units:
        print("\n🎉 ALL PLANTS DATA FETCH SUCCESSFUL!")
        return 0
    else:
        print(f"\n⚠️  Some units failed ({total_units - total_success} failed)")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())