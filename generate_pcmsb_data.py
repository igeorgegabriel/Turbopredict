#!/usr/bin/env python3
"""
Generate PCMSB parquet data using proper tag-by-tag approach
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from datetime import datetime
import time

def generate_pcmsb_unit_data(unit="C-02001"):
    """Generate parquet data for a single PCMSB unit using proper tag fetching"""

    print(f"GENERATING PCMSB DATA FOR {unit}")
    print("=" * 40)

    # Check if config file exists
    config_file = Path(f"config/tags_pcmsb_{unit.lower().replace('-', '')}.txt")
    if not config_file.exists():
        print(f"ERROR: Config file not found: {config_file}")
        return False

    # Read tags
    try:
        tags = [t.strip() for t in config_file.read_text(encoding="utf-8").splitlines()
               if t.strip() and not t.strip().startswith('#')]
        print(f"Found {len(tags)} tags for {unit}")
        print(f"Sample tags: {tags[:3]}")
    except Exception as e:
        print(f"ERROR reading config file: {e}")
        return False

    # Set up paths
    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")
    output_path = Path(f"data/processed/{unit}_1y_0p1h.parquet")

    if not excel_path.exists():
        print(f"ERROR: Excel file not found: {excel_path}")
        return False

    print(f"Using Excel file: {excel_path}")
    print(f"Output path: {output_path}")

    # Generate the data using the fixed batch processing
    try:
        from pi_monitor.batch import build_unit_from_tags

        print(f"\nStarting tag-by-tag data fetch...")
        print(f"This will take approximately {len(tags) * 11} seconds ({len(tags) * 11 / 60:.1f} minutes)")

        start_time = time.time()

        # Use the corrected parameters
        build_unit_from_tags(
            xlsx=excel_path,
            tags=tags,
            out_parquet=output_path,
            plant="PCMSB",
            unit=unit,
            work_sheet="DL_WORK",  # Use the corrected sheet
            settle_seconds=30.0,   # Increased timeout
            visible=False,         # Keep hidden for speed
        )

        elapsed = time.time() - start_time
        print(f"\nData generation completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")

        # Check if file was created
        if output_path.exists():
            # Get file info
            stat = output_path.stat()
            size_mb = stat.st_size / (1024 * 1024)

            # Load and analyze data
            df = pd.read_parquet(output_path)
            unique_tags = df['tag'].nunique() if 'tag' in df.columns else 0
            total_records = len(df)
            latest_time = df['time'].max() if 'time' in df.columns and len(df) > 0 else None

            print(f"\nSUCCESS: Parquet file created")
            print(f"   File: {output_path.name}")
            print(f"   Size: {size_mb:.1f} MB")
            print(f"   Records: {total_records:,}")
            print(f"   Unique tags: {unique_tags}")
            print(f"   Latest data: {latest_time}")

            # Create dedup version
            try:
                from pi_monitor.clean import dedup_parquet
                dedup_path = dedup_parquet(output_path)
                dedup_size_mb = dedup_path.stat().st_size / (1024 * 1024)
                print(f"   Dedup file: {dedup_path.name} ({dedup_size_mb:.1f} MB)")
            except Exception as e:
                print(f"   WARNING: Dedup failed: {e}")

            return True

        else:
            print(f"\nERROR: Parquet file was not created")
            return False

    except Exception as e:
        print(f"\nERROR during data generation: {e}")
        import traceback
        traceback.print_exc()
        return False

def generate_all_pcmsb_data():
    """Generate data for all PCMSB units"""

    print("GENERATING DATA FOR ALL PCMSB UNITS")
    print("=" * 40)

    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']
    success_count = 0

    for i, unit in enumerate(pcmsb_units, 1):
        print(f"\n{'='*60}")
        print(f"PROCESSING UNIT {i}/{len(pcmsb_units)}: {unit}")
        print(f"{'='*60}")

        if generate_pcmsb_unit_data(unit):
            success_count += 1
            print(f"✓ {unit} completed successfully")
        else:
            print(f"✗ {unit} failed")

    print(f"\n{'='*60}")
    print(f"GENERATION COMPLETE: {success_count}/{len(pcmsb_units)} units successful")
    print(f"{'='*60}")

    # Final verification
    if success_count > 0:
        print(f"\nVerifying database integration...")
        try:
            from pi_monitor.parquet_database import ParquetDatabase
            db = ParquetDatabase()

            for unit in pcmsb_units:
                try:
                    freshness_info = db.get_data_freshness_info(unit)
                    records = freshness_info.get('total_records', 0)
                    age_hours = freshness_info.get('data_age_hours', 0)
                    unique_tags = len(freshness_info.get('unique_tags', []))

                    status = "FRESH" if age_hours <= 1.0 else "STALE"
                    print(f"   {unit}: {status} - {records:,} records, {unique_tags} tags, {age_hours:.1f}h old")

                except Exception as e:
                    print(f"   {unit}: ERROR - {e}")

        except Exception as e:
            print(f"Database verification failed: {e}")

    return success_count == len(pcmsb_units)

if __name__ == "__main__":
    # Generate all PCMSB data
    success = generate_all_pcmsb_data()
    if success:
        print("\n🎉 ALL PCMSB DATA GENERATED SUCCESSFULLY!")
        print("All PCMSB units are now integrated with the master database.")
    else:
        print("\n⚠️ SOME PCMSB UNITS FAILED - check errors above")