"""Test if increased timeouts resolve the 'No data' issue for confirmed tags"""
import os
import sys
from pathlib import Path

# Set debug and timeout environment variables
os.environ['PI_FETCH_TIMEOUT'] = '240'  # 4 minutes
os.environ['PI_FETCH_LINGER'] = '90'    # 90 seconds
os.environ['DEBUG_PI_FETCH'] = '1'      # Enable debug logging

# Run incremental refresh for K-12-01
print("=" * 70)
print("TESTING TIMEOUT FIX FOR K-12-01")
print("=" * 70)
print(f"PI_FETCH_TIMEOUT: {os.environ['PI_FETCH_TIMEOUT']}s")
print(f"PI_FETCH_LINGER: {os.environ['PI_FETCH_LINGER']}s")
print(f"DEBUG_PI_FETCH: {os.environ['DEBUG_PI_FETCH']}")
print("=" * 70)

# Import after setting env vars
sys.path.insert(0, str(Path(__file__).parent))
from scripts.incremental_refresh_safe import incremental_refresh_unit, read_tags
from pathlib import Path

# Test with K-12-01
unit = "K-12-01"
plant = "PCFS"
server = "\\\\PTSG-1MMPDPdb01"
xlsx = Path("excel/PCFS_Automation.xlsx")
tags_file = Path(f"config/tags/{plant}_{unit}.txt")

if tags_file.exists():
    tags = read_tags(tags_file)
else:
    print(f"Warning: Tags file not found at {tags_file}")
    tags = []

if tags:
    print(f"\nStarting incremental refresh for {unit} with {len(tags)} tags...")
    print("Watch for '[debug] Read X rows from Excel spill' messages")
    print("=" * 70 + "\n")

    result = incremental_refresh_unit(
        unit=unit,
        tags=tags,
        xlsx=xlsx,
        plant=plant,
        server=server
    )

    print("\n" + "=" * 70)
    print(f"RESULT: {'SUCCESS' if result else 'FAILED'}")
    print("=" * 70)
else:
    print("No tags found - cannot test")
