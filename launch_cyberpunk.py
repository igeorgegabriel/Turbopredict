#!/usr/bin/env python3
"""
TURBOPREDICT X PROTEAN - Cyberpunk CLI Launcher
Beautiful cyberpunk-themed terminal interface
"""

import sys
import os
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def check_dependencies():
    """Check and install required dependencies"""
    missing_deps = []
    
    try:
        import rich
    except ImportError:
        missing_deps.append("rich")
    
    try:
        import colorama
    except ImportError:
        missing_deps.append("colorama")
    
    if missing_deps:
        print("Installing required dependencies for cyberpunk interface...")
        import subprocess
        for dep in missing_deps:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
                print(f"Installed {dep}")
            except subprocess.CalledProcessError:
                print(f"Failed to install {dep}")
                print(f"Please run: pip install {dep}")
                return False
    
    return True

def main():
    """Launch the cyberpunk CLI"""
    print("TURBOPREDICT X PROTEAN - Initializing Neural Interface...")
    
    # Check dependencies
    if not check_dependencies():
        print("Dependency check failed. Please install missing packages.")
        sys.exit(1)
    
    try:
        from pi_monitor.cyberpunk_cli import CyberpunkCLI
        cli = CyberpunkCLI()
        cli.run()
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure you're running from the CodeX directory")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()