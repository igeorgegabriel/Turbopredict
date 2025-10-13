#!/usr/bin/env python3
"""
Unified PCMSB build script - handles all PCMSB units with a single script.
Usage: python scripts/build_pcmsb.py [unit_name]
Example: python scripts/build_pcmsb.py C-02001
"""

from __future__ import annotations

from pathlib import Path
import sys
import argparse

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags  # noqa: E402
from pi_monitor.clean import dedup_parquet  # noqa: E402

# PCMSB unit configuration mapping
PCMSB_UNITS = {
    'C-02001': 'tags_pcmsb_c02001.txt',
    'C-104': 'tags_pcmsb_c104.txt',
    'C-13001': 'tags_pcmsb_c13001.txt',
    'C-1301': 'tags_pcmsb_c1301.txt',
    'C-1302': 'tags_pcmsb_c1302.txt',
    'C-201': 'tags_pcmsb_c201.txt',
    'C-202': 'tags_pcmsb_c202.txt',
    'XT-07002': 'tags_pcmsb_xt07002.txt'
}

def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags

def build_pcmsb_unit(unit: str, *, force_visible: bool = False) -> int:
    """Build data for a specific PCMSB unit."""
    
    if unit not in PCMSB_UNITS:
        print(f"ERROR: Unknown PCMSB unit '{unit}'")
        print(f"Available units: {', '.join(PCMSB_UNITS.keys())}")
        return 1
    
    # Configuration for the specified PCMSB unit
    tags_file_name = PCMSB_UNITS[unit]
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / tags_file_name
    out_parquet = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.parquet"

    plant = "PCMSB"
    server = r"\\PTSG-1MMPDPdb01"
    
    # Verify files exist
    if not xlsx.exists():
        print(f"ERROR: Excel file not found: {xlsx}")
        return 1
        
    if not tags_file.exists():
        print(f"ERROR: Tags file not found: {tags_file}")
        return 1

    tags = read_tags(tags_file)
    if not tags:
        print(f"ERROR: No tags found in {tags_file}")
        return 1

    print(f"Building Parquet for {plant} {unit} with {len(tags)} tags...")
    print(f"Using Excel file: {xlsx}")
    
    try:
        out = build_unit_from_tags(
            xlsx,
            tags,
            out_parquet,
            plant=plant,
            unit=unit,
            server=server,
            start="-1y",
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            # PCMSB servers are slower; allow longer settle/timeout
            settle_seconds=30.0,
            visible=bool(force_visible),
            use_working_copy=True
        )
        
        print(f"Successfully wrote: {out}")
        
        # Deduplicate into master .dedup.parquet
        dedup = dedup_parquet(out)
        print(f"Master (dedup) ready: {dedup}")
        return 0
            
    except Exception as e:
        print(f"Error building {unit}: {e}")
        
        # Fallback: try with visible Excel
        try:
            print("Trying with visible Excel as fallback...")
            out = build_unit_from_tags(
                xlsx,
                tags,
                out_parquet,
                plant=plant,
                unit=unit,
                server=server,
                start="-1y",
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=30.0,
                visible=True,
                use_working_copy=True
            )
            
            print(f"Successfully wrote (fallback): {out}")
            dedup = dedup_parquet(out)
            print(f"Master (dedup) ready: {dedup}")
            return 0
            
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return 1

def main() -> int:
    parser = argparse.ArgumentParser(description='Build PCMSB unit data')
    parser.add_argument('unit', nargs='?', help='PCMSB unit name (e.g., C-02001)')
    parser.add_argument('--list-units', action='store_true', help='List available PCMSB units')
    parser.add_argument('--visible', action='store_true', help='Open Excel visible (useful if headless returns no data)')
    
    args = parser.parse_args()
    
    if args.list_units:
        print("Available PCMSB units:")
        for unit in PCMSB_UNITS.keys():
            print(f"  - {unit}")
        return 0
    
    if not args.unit:
        print("ERROR: Please specify a PCMSB unit")
        print("Usage: python scripts/build_pcmsb.py [unit_name]")
        print("Example: python scripts/build_pcmsb.py C-02001")
        print("Use --list-units to see available units")
        return 1
    
    return build_pcmsb_unit(args.unit, force_visible=args.visible)

if __name__ == "__main__":
    raise SystemExit(main())
