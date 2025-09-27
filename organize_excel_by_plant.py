#!/usr/bin/env python3
"""
Organize Excel files into plant-specific directories
"""
import os
import shutil
from pathlib import Path

def organize_excel_files():
    """Move Excel files to their respective plant directories"""

    excel_dir = Path("excel")

    # Plant mappings - check ABF before ABFSB since ABF is a subset
    plant_mappings = [
        ('ABF_', 'ABFSB'),    # ABF files go to ABFSB directory
        ('ABFSB', 'ABFSB'),
        ('MLNG', 'MLNG'),
        ('PCFS', 'PCFS'),
        ('PCMSB', 'PCMSB'),
        ('PFLNG1', 'PFLNG1'),
        ('PFLNG2', 'PFLNG2')
    ]

    print("ORGANIZING EXCEL FILES BY PLANT")
    print("=" * 50)

    moved_count = 0
    skipped_count = 0

    # Get all Excel files in the main directory
    excel_files = [f for f in excel_dir.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']

    print(f"Found {len(excel_files)} Excel files to organize")

    for excel_file in excel_files:
        filename = excel_file.name
        moved = False

        # Check which plant this file belongs to
        for plant_prefix, plant_dir in plant_mappings:
            if filename.startswith(plant_prefix):
                target_dir = excel_dir / plant_dir
                target_path = target_dir / filename

                try:
                    # Move the file
                    shutil.move(str(excel_file), str(target_path))
                    print(f"  MOVED: {filename} -> {plant_dir}/")
                    moved_count += 1
                    moved = True
                    break
                except Exception as e:
                    print(f"  ERROR moving {filename}: {e}")

        if not moved:
            print(f"  SKIPPED: {filename} (no matching plant)")
            skipped_count += 1

    print(f"\nSUMMARY:")
    print(f"  Files moved: {moved_count}")
    print(f"  Files skipped: {skipped_count}")

    # Verify the organization
    print(f"\nVERIFICATION:")
    plant_dirs = set([plant_dir for _, plant_dir in plant_mappings])
    for plant_dir in plant_dirs:
        plant_path = excel_dir / plant_dir
        if plant_path.exists():
            files_in_plant = list(plant_path.glob("*.xlsx"))
            print(f"  {plant_dir}: {len(files_in_plant)} files")
        else:
            print(f"  {plant_dir}: Directory not found")

    return moved_count, skipped_count

if __name__ == "__main__":
    organize_excel_files()