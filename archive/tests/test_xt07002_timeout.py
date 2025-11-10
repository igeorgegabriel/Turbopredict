#!/usr/bin/env python3
"""
Test XT-07002 build with increased PI_FETCH_TIMEOUT
"""

import os
import subprocess
import sys
from pathlib import Path

# Set the environment variable for increased timeout
os.environ['PI_FETCH_TIMEOUT'] = '60'

print(f"PI_FETCH_TIMEOUT set to: {os.environ.get('PI_FETCH_TIMEOUT', 'Not set')}")

# Change to project directory
project_root = Path(__file__).parent
os.chdir(project_root)

print(f"Working directory: {os.getcwd()}")
print("Starting XT-07002 build with 60-second timeout...")

try:
    # Run the build script with increased timeout
    result = subprocess.run([
        sys.executable,
        "scripts/build_pcmsb.py",
        "XT-07002"
    ], capture_output=True, text=True, timeout=1800)  # 30 minute timeout for the whole process

    print("STDOUT:")
    print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    print(f"Return code: {result.returncode}")

except subprocess.TimeoutExpired:
    print("ERROR: Build process timed out after 30 minutes")
except Exception as e:
    print(f"ERROR: {e}")