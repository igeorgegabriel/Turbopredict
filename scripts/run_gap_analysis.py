"""
Gap Analysis Runner - Simple interface to check and fill missing data.

Safe, user-friendly interface for gap detection and filling.
"""

from gap_filler_independent import IndependentGapFiller
from safe_pi_fetcher import SafePIFetcher
import json
from pathlib import Path


def analyze_september_gaps():
    """
    Analyze the September 2025 gaps visible in the plots.
    """
    print("=" * 80)
    print("GAP ANALYSIS FOR SEPTEMBER 2025 - ABF UNIT 07-MT01-K001")
    print("=" * 80)

    # Initialize gap filler in preview mode (safe)
    filler = IndependentGapFiller(
        plant="ABF",
        unit="07-MT01-K001",
        preview_only=True  # Safe mode - no writes
    )

    # Analyze specific tags from the plots
    tags_to_check = [
        "ABF_07-MT001_FI-07004_PV",  # Flow measurement
        "ABF_07-MT001_SI-07002D_new_PV"  # Speed measurement
    ]

    print(f"\nAnalyzing {len(tags_to_check)} tags for gaps...")

    all_gaps = {}

    for tag in tags_to_check:
        print(f"\n{'='*80}")
        print(f"TAG: {tag}")
        print(f"{'='*80}")

        # Detect gaps in September 2025
        gaps = filler.detect_gaps(
            tag=tag,
            start_date='2025-09-01',
            end_date='2025-09-30',
            max_gap_hours=1.0  # Any gap > 1 hour
        )

        if gaps:
            all_gaps[tag] = gaps

            # Show detailed gap info
            print(f"\nFound {len(gaps)} gap(s):")
            for i, gap in enumerate(gaps, 1):
                print(f"\n  Gap #{i}:")
                print(f"    Start: {gap['gap_start']}")
                print(f"    End: {gap['gap_end']}")
                print(f"    Duration: {gap['duration_hours']:.2f} hours")
                print(f"    Value before: {gap['value_before']:.2f}")
                print(f"    Value after: {gap['value_after']:.2f}")
                print(f"    Expected missing points: {gap['expected_points']}")
        else:
            print(f"\n✓ No gaps found (data is continuous)")

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Tags checked: {len(tags_to_check)}")
    print(f"Tags with gaps: {len(all_gaps)}")

    if all_gaps:
        total_gaps = sum(len(gaps) for gaps in all_gaps.values())
        print(f"Total gaps found: {total_gaps}")

        # Save report
        report_dir = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\data\gap_reports")
        report_dir.mkdir(parents=True, exist_ok=True)

        report_file = report_dir / "september_2025_gap_analysis.json"
        with open(report_file, 'w') as f:
            json.dump({
                'tags_analyzed': tags_to_check,
                'gaps_found': all_gaps,
                'summary': {
                    'total_tags': len(tags_to_check),
                    'tags_with_gaps': len(all_gaps),
                    'total_gaps': total_gaps
                }
            }, f, indent=2)

        print(f"\n✓ Report saved: {report_file}")

        # Ask user if they want to fill gaps
        print(f"\n{'='*80}")
        print(f"NEXT STEPS")
        print(f"{'='*80}")
        print(f"To fill these gaps, run:")
        print(f"  python scripts/run_gap_analysis.py --fill-gaps")
        print(f"\nThis will:")
        print(f"  1. Fetch missing data from PI system")
        print(f"  2. Validate data quality")
        print(f"  3. Save to separate gap_filled directory (safe)")
        print(f"  4. Never modify original Parquet files")

    else:
        print(f"\n✓ No gaps detected - data appears continuous")

    return all_gaps


def fill_detected_gaps():
    """
    Fill gaps detected in the analysis.
    This is still safe - writes to separate directory.
    """
    print("=" * 80)
    print("GAP FILLING FOR SEPTEMBER 2025 - ABF UNIT 07-MT01-K001")
    print("=" * 80)

    # Load gap report
    report_file = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\data\gap_reports\september_2025_gap_analysis.json")

    if not report_file.exists():
        print("\n[ERROR] No gap analysis report found.")
        print("Run this script without --fill-gaps first to analyze gaps.")
        return

    with open(report_file) as f:
        report = json.load(f)

    gaps_found = report['gaps_found']

    if not gaps_found:
        print("\n✓ No gaps to fill")
        return

    # Show what will be filled
    total_gaps = sum(len(gaps) for gaps in gaps_found.values())
    print(f"\nGaps to fill: {total_gaps}")
    print(f"Tags affected: {len(gaps_found)}")

    # Confirm
    response = input("\nProceed with gap filling? This will fetch data from PI system. (yes/no): ")

    if response.lower() != 'yes':
        print("\n[CANCELLED] Gap filling cancelled")
        return

    # Initialize filler in write mode
    filler = IndependentGapFiller(
        plant="ABF",
        unit="07-MT01-K001",
        preview_only=False  # Enable writes (to separate directory)
    )

    # Initialize fetcher
    fetcher = SafePIFetcher(plant="ABF")

    # Fill each gap
    results = []

    for tag, gaps in gaps_found.items():
        print(f"\n{'='*80}")
        print(f"FILLING GAPS FOR: {tag}")
        print(f"{'='*80}")

        for gap in gaps:
            print(f"\nGap: {gap['gap_start']} to {gap['gap_end']}")

            # Fetch missing data
            df = fetcher.fetch_with_retry(
                tag=tag,
                start_time=gap['gap_start'],
                end_time=gap['gap_end'],
                interval='1h'
            )

            if df is not None and not df.empty:
                # Validate
                validation = fetcher.validate_fetched_data(
                    df, tag,
                    gap['gap_start'],
                    gap['gap_end']
                )

                # Save
                output_path = fetcher.save_fetched_data(df, tag, gap['gap_start'])

                results.append({
                    'tag': tag,
                    'gap_start': gap['gap_start'],
                    'gap_end': gap['gap_end'],
                    'status': 'success',
                    'records_fetched': len(df),
                    'output_file': str(output_path),
                    'validation': validation
                })

                print(f"✓ Gap filled successfully")
            else:
                results.append({
                    'tag': tag,
                    'gap_start': gap['gap_start'],
                    'gap_end': gap['gap_end'],
                    'status': 'failed',
                    'records_fetched': 0
                })

                print(f"✗ Failed to fetch data")

    # Summary
    print(f"\n{'='*80}")
    print(f"GAP FILLING COMPLETE")
    print(f"{'='*80}")

    success_count = sum(1 for r in results if r['status'] == 'success')
    failed_count = sum(1 for r in results if r['status'] == 'failed')

    print(f"Successful: {success_count}/{len(results)}")
    print(f"Failed: {failed_count}/{len(results)}")

    # Save results
    results_file = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\data\gap_reports\gap_fill_results.json")
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': pd.Timestamp.now().isoformat(),
            'summary': {
                'total_gaps': len(results),
                'successful': success_count,
                'failed': failed_count
            },
            'results': results
        }, f, indent=2)

    print(f"\n✓ Results saved: {results_file}")

    if success_count > 0:
        print(f"\nFilled data saved to:")
        print(f"  c:\\Users\\george.gabrielujai\\Documents\\CodeX\\data\\gap_filled\\plant=ABF\\")
        print(f"\nOriginal Parquet files remain unchanged (safe operation)")


def main():
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="Gap Analysis and Filling Runner")
    parser.add_argument('--fill-gaps', action='store_true',
                       help='Fill detected gaps (after running analysis)')

    args = parser.parse_args()

    if args.fill_gaps:
        fill_detected_gaps()
    else:
        analyze_september_gaps()


if __name__ == '__main__':
    main()
