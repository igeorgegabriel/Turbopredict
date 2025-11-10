"""
Quick check for gaps in ABFSB unit 07-MT01/K001 September 2025 data.
Uses existing parquet_database system.
"""

import sys
from pathlib import Path
from pi_monitor.parquet_database import ParquetDatabase
from datetime import datetime
import pandas as pd

def check_abf_gaps():
    """Check for data gaps in ABFSB September 2025."""

    print("="*80)
    print("CHECKING ABFSB 07-MT01/K001 - SEPTEMBER 2025 DATA GAPS")
    print("="*80)

    # Initialize database
    db = ParquetDatabase()

    # Unit name (try different formats)
    possible_units = [
        "07-MT01/K001",
        "07-MT01-K001",
        "ABF_07-MT01-K001",
        "ABFSB_07-MT01-K001"
    ]

    unit = None
    for u in possible_units:
        try:
            print(f"\n[INFO] Trying unit name: {u}")
            data = db.get_unit_data(
                unit=u,
                start_time=datetime(2025, 9, 1),
                end_time=datetime(2025, 9, 30)
            )
            if not data.empty:
                unit = u
                print(f"[SUCCESS] Found data for unit: {unit}")
                break
        except Exception as e:
            print(f"[SKIP] {u}: {str(e)[:100]}")

    if unit is None:
        print("\n[ERROR] Could not find ABFSB 07-MT01/K001 data")
        print("[INFO] Available Parquet files:")
        files = db.get_available_parquet_files()
        for f in files[:10]:
            print(f"  - {f}")
        return

    # Get data for September 2025
    print(f"\n[FETCH] Loading September 2025 data for {unit}...")

    df = db.get_unit_data(
        unit=unit,
        start_time=datetime(2025, 9, 1),
        end_time=datetime(2025, 9, 30)
    )

    if df.empty:
        print("[ERROR] No data found for September 2025")
        return

    print(f"[SUCCESS] Loaded {len(df)} records")

    # Check specific tags from your plots
    tags_to_check = [
        "ABF_07-MT001_FI-07004_PV",
        "ABF_07-MT001_SI-07002D_new_PV"
    ]

    available_tags = df['tag'].unique()
    # Filter out None values
    available_tags = [t for t in available_tags if t is not None and isinstance(t, str)]
    print(f"\n[INFO] Available tags in September data: {len(available_tags)}")

    # Find matching tags (partial match)
    matching_tags = [t for t in available_tags if any(check in t for check in ['FI-07004', 'SI-07002'])]

    if matching_tags:
        print(f"[FOUND] Matching tags:")
        for t in matching_tags:
            print(f"  - {t}")
        tags_to_check = matching_tags
    else:
        print(f"[WARNING] Exact tags not found, showing sample of available tags:")
        for t in list(available_tags)[:5]:
            print(f"  - {t}")
        tags_to_check = list(available_tags)[:2]

    # Analyze gaps for each tag
    all_gaps = {}

    for tag in tags_to_check:
        print(f"\n{'='*80}")
        print(f"TAG: {tag}")
        print(f"{'='*80}")

        tag_data = df[df['tag'] == tag].copy()

        if tag_data.empty:
            print(f"[SKIP] No data for this tag")
            continue

        # Check what columns exist
        print(f"[DEBUG] Columns: {list(tag_data.columns)}")

        # Sort by time column (could be 'timestamp' or 'time')
        time_col = 'timestamp' if 'timestamp' in tag_data.columns else 'time'
        tag_data = tag_data.sort_values(time_col)

        # Calculate time gaps
        tag_data['time_diff'] = tag_data[time_col].diff()

        # Find gaps > 1 hour
        gaps = tag_data[tag_data['time_diff'] > pd.Timedelta(hours=1)]

        print(f"[INFO] Records: {len(tag_data)}")
        print(f"[INFO] Date range: {tag_data[time_col].min()} to {tag_data[time_col].max()}")
        print(f"[INFO] Value range: {tag_data['value'].min():.2f} to {tag_data['value'].max():.2f}")

        if not gaps.empty:
            print(f"\n[FOUND] {len(gaps)} gap(s) > 1 hour:")

            tag_gaps = []

            for idx in gaps.index:
                prev_idx = tag_data.index[tag_data.index.get_loc(idx) - 1]

                gap_start = tag_data.loc[prev_idx, time_col]
                gap_end = tag_data.loc[idx, time_col]
                duration = tag_data.loc[idx, 'time_diff']
                value_before = tag_data.loc[prev_idx, 'value']
                value_after = tag_data.loc[idx, 'value']

                gap_info = {
                    'start': str(gap_start),
                    'end': str(gap_end),
                    'duration_hours': duration.total_seconds() / 3600,
                    'value_before': value_before,
                    'value_after': value_after,
                    'value_change': value_after - value_before
                }

                tag_gaps.append(gap_info)

                print(f"\n  Gap #{len(tag_gaps)}:")
                print(f"    Start: {gap_start}")
                print(f"    End: {gap_end}")
                print(f"    Duration: {gap_info['duration_hours']:.2f} hours")
                print(f"    Value: {value_before:.2f} -> {value_after:.2f} (change: {gap_info['value_change']:.2f})")

                # Diagnosis
                if gap_info['duration_hours'] > 24:
                    print(f"    >> LIKELY: Unit shutdown/maintenance")
                elif gap_info['duration_hours'] > 4:
                    print(f"    >> POSSIBLE: Data collection interruption")
                else:
                    print(f"    >> Minor gap")

            all_gaps[tag] = tag_gaps
        else:
            print(f"\n[OK] No gaps > 1 hour - data is continuous")

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Tags analyzed: {len(tags_to_check)}")
    print(f"Tags with gaps: {len(all_gaps)}")

    if all_gaps:
        total_gaps = sum(len(gaps) for gaps in all_gaps.values())
        print(f"Total gaps: {total_gaps}")

        print(f"\n[CONCLUSION]")
        print(f"The September 2025 drops you see in the plots are due to:")
        for tag, gaps in all_gaps.items():
            print(f"\n  {tag}:")
            for gap in gaps:
                if gap['duration_hours'] > 24:
                    print(f"    - Likely PLANT SHUTDOWN around {gap['start'][:10]}")
                    print(f"      (Data missing for {gap['duration_hours']:.1f} hours)")
    else:
        print(f"\n[RESULT] No significant gaps detected")
        print(f"The drops you see may be ACTUAL PROCESS VALUES, not missing data")

if __name__ == '__main__':
    try:
        check_abf_gaps()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
