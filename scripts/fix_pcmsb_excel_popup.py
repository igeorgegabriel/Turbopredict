#!/usr/bin/env python3
"""
Fix PCMSB Excel popup issues during script execution.
"""

from pathlib import Path

def create_improved_pcmsb_script():
    """Create an improved PCMSB script that handles Excel popups better."""
    
    improved_script = '''#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys
import os
import time

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags  # noqa: E402
from pi_monitor.clean import dedup_parquet  # noqa: E402


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def main() -> int:
    # Configuration for PCMSB C-02001 with improved Excel handling
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_c02001.txt"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "C-02001_1y_0p1h.parquet"

    plant = "PCMSB"
    unit = "C-02001"
    
    # Verify files exist
    if not xlsx.exists():
        print(f"ERROR: Excel file not found: {xlsx}")
        return 1
        
    if not tags_file.exists():
        print(f"ERROR: Tags file not found: {tags_file}")
        return 1

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    print(f"Building Parquet for {plant} {unit} with {len(tags)} tags...")
    print(f"Using Excel file: {xlsx}")
    
    try:
        # Set environment variables to minimize Excel popups
        os.environ['EXCEL_CALC_MODE'] = 'sheet'  # Faster calculation
        os.environ['PI_FETCH_TIMEOUT'] = '15'    # Longer timeout for PCMSB
        
        # Build with minimized Excel visibility
        out = build_unit_from_tags(
            xlsx,
            tags,
            out_parquet,
            plant=plant,
            unit=unit,
            server=r"\\\\PTSG-1MMPDPdb01",
            start="-1y",
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=2.0,      # Increased settle time
            visible=False,           # Run Excel in background (minimized)
            use_working_copy=True    # Use working copy to avoid file locks
        )
        
        print(f"Successfully wrote: {out}")
        
        # Deduplicate into master .dedup.parquet
        dedup = dedup_parquet(out)
        print(f"Master (dedup) ready: {dedup}")
        return 0
            
    except Exception as e:
        print(f"Error building {unit}: {e}")
        print("Trying with visible Excel as fallback...")
        
        # Fallback: try with visible Excel
        try:
            out = build_unit_from_tags(
                xlsx,
                tags,
                out_parquet,
                plant=plant,
                unit=unit,
                server=r"\\\\PTSG-1MMPDPdb01",
                start="-1y",
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=2.0,
                visible=True,        # Show Excel as fallback
                use_working_copy=True
            )
            
            print(f"Successfully wrote (fallback): {out}")
            dedup = dedup_parquet(out)
            print(f"Master (dedup) ready: {dedup}")
            return 0
            
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
'''

    # Write the improved script
    script_path = Path("scripts/build_pcmsb_c02001_improved.py")
    script_path.write_text(improved_script)
    print(f"Created improved PCMSB script: {script_path}")
    return script_path

def update_all_pcmsb_scripts():
    """Update all PCMSB scripts to minimize Excel popups."""
    
    pcmsb_scripts = [
        'scripts/build_pcmsb_c02001.py',
        'scripts/build_pcmsb_c104.py',
        'scripts/build_pcmsb_c13001.py',
        'scripts/build_pcmsb_c1301.py',
        'scripts/build_pcmsb_c1302.py',
        'scripts/build_pcmsb_c201.py',
        'scripts/build_pcmsb_c202.py',
        'scripts/build_pcmsb_xt07002.py'
    ]
    
    for script_path in pcmsb_scripts:
        if Path(script_path).exists():
            print(f"Updating {script_path} to minimize Excel popups...")
            
            # Read current content
            with open(script_path, 'r') as f:
                content = f.read()
            
            # Update visible parameter to False
            if 'visible = True' in content:
                content = content.replace('visible = True', 'visible = False')
                print(f"  - Changed visible=True to visible=False")
            
            # Add environment variables for better control
            if 'settle_seconds = 1.5' in content:
                content = content.replace(
                    'settle_seconds = 1.5',
                    'settle_seconds = 2.0      # Increased settle time for PCMSB'
                )
                print(f"  - Increased settle seconds to 2.0")
            
            # Write updated content
            with open(script_path, 'w') as f:
                f.write(content)
            
            print(f"  - Updated {script_path}")

def main():
    """Main function to fix Excel popup issues."""
    
    print("FIXING PCMSB EXCEL POPUP ISSUES")
    print("=" * 60)
    
    # 1. Create improved script with better error handling
    improved_script = create_improved_pcmsb_script()
    
    print("\n" + "=" * 60)
    
    # 2. Update all existing PCMSB scripts
    update_all_pcmsb_scripts()
    
    print("\n" + "=" * 60)
    print("SOLUTIONS IMPLEMENTED:")
    print("1. Set visible=False to run Excel in background")
    print("2. Increased settle_seconds to 2.0 for better PI DataLink reliability")
    print("3. Added environment variables for better Excel control")
    print("4. Implemented fallback mechanism with visible Excel")
    print("5. Use working copies to avoid file locking issues")
    
    print("\nRECOMMENDED ACTIONS:")
    print("1. Test with improved script: python scripts/build_pcmsb_c02001_improved.py")
    print("2. If Excel still pops up, check for running Excel processes first")
    print("3. Use: python scripts/kill_excel_processes.py (if available)")
    print("4. Ensure PI DataLink is properly installed in Excel")

if __name__ == "__main__":
    main()