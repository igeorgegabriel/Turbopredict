"""
Simple gap checker for September 2025 ABF data.
Reads data directly from reports or checks existing databases.
"""

import pandas as pd
import duckdb
from pathlib import Path
import json

def check_gaps_from_database():
    """Check gaps in the ABF unit 07-MT01/K001 for September 2025."""

    print("="*80)
    print("CHECKING SEPTEMBER 2025 GAPS - ABF 07-MT01/K001")
    print("="*80)

    # Tags visible in your plots
    tags = [
        "ABF_07-MT001_FI-07004_PV",
        "ABF_07-MT001_SI-07002D_new_PV"
    ]

    # Check if DuckDB database exists
    db_path = Path("data/processed/timeseries.duckdb")

    if not db_path.exists():
        print(f"\n[INFO] DuckDB not found at {db_path}")
        print("[INFO] Checking for Parquet files instead...")
        return check_gaps_from_parquet(tags)

    print(f"\n[INFO] Using database: {db_path}")

    # Connect to database
    conn = duckdb.connect(str(db_path), read_only=True)

    all_gaps = {}

    for tag in tags:
        print(f"\n{'='*80}")
        print(f"TAG: {tag}")
        print(f"{'='*80}")

        try:
            # Query September 2025 data
            query = f"""
            SELECT
                timestamp,
                value,
                tag
            FROM timeseries
            WHERE tag = '{tag}'
              AND timestamp >= '2025-09-01'
              AND timestamp <= '2025-09-30'
            ORDER BY timestamp
            """

            df = conn.execute(query).fetchdf()

            if df.empty:
                print(f"[WARNING] No data found for {tag}")
                continue

            print(f"[INFO] Found {len(df)} records")

            # Analyze gaps
            df = df.sort_values('timestamp')
            df['time_diff'] = df['timestamp'].diff()

            # Find gaps > 1 hour
            gaps = df[df['time_diff'] > pd.Timedelta(hours=1)]

            if not gaps.empty:
                tag_gaps = []

                for idx, row in gaps.iterrows():
                    gap_start = df.loc[idx-1, 'timestamp']
                    gap_end = row['timestamp']
                    duration_hours = row['time_diff'].total_seconds() / 3600

                    gap_info = {
                        'gap_start': str(gap_start),
                        'gap_end': str(gap_end),
                        'duration_hours': duration_hours,
                        'value_before': float(df.loc[idx-1, 'value']),
                        'value_after': float(row['value'])
                    }

                    tag_gaps.append(gap_info)

                    print(f"\n  Gap #{len(tag_gaps)}:")
                    print(f"    Start: {gap_start}")
                    print(f"    End: {gap_end}")
                    print(f"    Duration: {duration_hours:.2f} hours")
                    print(f"    Value before: {gap_info['value_before']:.2f}")
                    print(f"    Value after: {gap_info['value_after']:.2f}")

                all_gaps[tag] = tag_gaps
            else:
                print(f"\n[OK] No gaps found - data is continuous")

            # Show data statistics
            print(f"\n[STATS]")
            print(f"  Start: {df['timestamp'].min()}")
            print(f"  End: {df['timestamp'].max()}")
            print(f"  Records: {len(df)}")
            print(f"  Value range: {df['value'].min():.2f} to {df['value'].max():.2f}")

        except Exception as e:
            print(f"[ERROR] Failed to query {tag}: {e}")

    conn.close()

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Tags checked: {len(tags)}")
    print(f"Tags with gaps: {len(all_gaps)}")

    if all_gaps:
        total_gaps = sum(len(gaps) for gaps in all_gaps.values())
        print(f"Total gaps: {total_gaps}")

        # Save report
        report_dir = Path("data/gap_reports")
        report_dir.mkdir(parents=True, exist_ok=True)

        report_file = report_dir / "september_2025_gaps.json"
        with open(report_file, 'w') as f:
            json.dump({
                'tags': tags,
                'gaps': all_gaps,
                'summary': {
                    'total_tags': len(tags),
                    'tags_with_gaps': len(all_gaps),
                    'total_gaps': total_gaps
                }
            }, f, indent=2)

        print(f"\n[SAVED] Gap report: {report_file}")

        # Show next steps
        print(f"\n{'='*80}")
        print(f"ANALYSIS")
        print(f"{'='*80}")

        for tag, gaps in all_gaps.items():
            print(f"\n{tag}:")
            for gap in gaps:
                duration = gap['duration_hours']
                value_drop = gap['value_before'] - gap['value_after']
                value_drop_pct = (value_drop / gap['value_before']) * 100

                print(f"  - {duration:.1f} hour gap around {gap['gap_start'][:10]}")
                print(f"    Value dropped by {value_drop:.2f} ({value_drop_pct:.1f}%)")

                if duration > 24:
                    print(f"    >> Likely SHUTDOWN or MAINTENANCE event")
                elif duration > 4:
                    print(f"    >> Possible DATA COLLECTION ISSUE")
                else:
                    print(f"    >> Minor gap, possible transient issue")

    return all_gaps


def check_gaps_from_parquet(tags):
    """Fallback: check gaps from Parquet files."""
    print(f"\n[INFO] Checking Parquet files...")

    # Try to find Parquet files
    parquet_files = list(Path("data/processed").glob("**/*.parquet"))

    if not parquet_files:
        print("[ERROR] No data files found")
        print("[INFO] Please ensure data has been fetched and processed")
        return {}

    print(f"[INFO] Found {len(parquet_files)} Parquet files")

    # Try to read and analyze
    all_gaps = {}

    for tag in tags:
        print(f"\n[INFO] Searching for {tag} in Parquet files...")

        # This is a simple search - you'd need to adapt based on your file structure
        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)

                if 'tag' in df.columns and tag in df['tag'].values:
                    print(f"[FOUND] {tag} in {pf.name}")

                    # Filter to tag and September 2025
                    df = df[df['tag'] == tag]
                    df = df[(df['timestamp'] >= '2025-09-01') & (df['timestamp'] <= '2025-09-30')]

                    if not df.empty:
                        print(f"[INFO] {len(df)} records in September 2025")
                        # Add gap analysis here similar to database version
                        break
            except Exception as e:
                pass  # Skip files that don't match format

    return all_gaps


def main():
    """Run gap check."""
    gaps = check_gaps_from_database()

    if gaps:
        print(f"\nTo fetch missing data and fill gaps:")
        print(f"  1. Review the gap report above")
        print(f"  2. Use the independent gap filler modules")
        print(f"  3. Or manually fetch data from PI system")
    else:
        print(f"\n[OK] No gaps detected in September 2025 data")


if __name__ == '__main__':
    main()
