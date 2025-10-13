"""
Independent Gap Filler Module - Safe Data Recovery
Detects and fills missing data gaps without modifying main system.

Features:
- Read-only analysis of existing data
- Safe gap detection with validation
- Preview mode before any writes
- Separate output directory for filled data
- No modification of original Parquet files
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple, Optional
import sys

class IndependentGapFiller:
    """Safe, independent gap detection and filling system."""

    def __init__(self, plant: str = "ABF", unit: str = "07-MT01-K001", preview_only: bool = True):
        self.plant = plant
        self.unit = unit
        self.preview_only = preview_only

        # Paths - read from existing, write to separate location
        self.data_root = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\data")
        self.processed_path = self.data_root / "processed" / "dataset" / f"plant={plant}" / f"unit={unit}"
        self.gap_fill_output = self.data_root / "gap_filled" / f"plant={plant}" / f"unit={unit}"
        self.gap_report_path = self.data_root / "gap_reports"

        # Create output directories
        self.gap_fill_output.mkdir(parents=True, exist_ok=True)
        self.gap_report_path.mkdir(parents=True, exist_ok=True)

        print(f"[GAP FILLER] Initialized for {plant}/{unit}")
        print(f"[GAP FILLER] Mode: {'PREVIEW ONLY' if preview_only else 'WRITE ENABLED'}")
        print(f"[GAP FILLER] Source: {self.processed_path}")
        print(f"[GAP FILLER] Output: {self.gap_fill_output}")

    def detect_gaps(self, tag: str, start_date: str, end_date: str,
                    max_gap_hours: float = 1.0) -> List[Dict]:
        """
        Detect time gaps in existing data for a specific tag.

        Args:
            tag: Tag name to check
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            max_gap_hours: Maximum acceptable gap in hours

        Returns:
            List of gap dictionaries with start/end times
        """
        print(f"\n[DETECTING GAPS] Tag: {tag}")
        print(f"[DETECTING GAPS] Period: {start_date} to {end_date}")

        # Read existing data
        try:
            df = pq.read_table(
                self.processed_path,
                filters=[
                    ('tag', '=', tag),
                    ('timestamp', '>=', pd.Timestamp(start_date)),
                    ('timestamp', '<=', pd.Timestamp(end_date))
                ]
            ).to_pandas()

            if df.empty:
                print(f"[WARNING] No data found for {tag}")
                return []

            print(f"[INFO] Loaded {len(df)} records")

        except Exception as e:
            print(f"[ERROR] Failed to read data: {e}")
            return []

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Calculate time differences
        df['time_diff'] = df['timestamp'].diff()

        # Find gaps larger than threshold
        max_gap_td = pd.Timedelta(hours=max_gap_hours)
        gaps = []

        for idx, row in df[df['time_diff'] > max_gap_td].iterrows():
            gap_start = df.loc[idx - 1, 'timestamp']
            gap_end = row['timestamp']
            gap_duration = row['time_diff']

            gap_info = {
                'tag': tag,
                'gap_start': gap_start.isoformat(),
                'gap_end': gap_end.isoformat(),
                'duration_hours': gap_duration.total_seconds() / 3600,
                'expected_points': int(gap_duration.total_seconds() / 3600),  # Assuming hourly data
                'value_before': df.loc[idx - 1, 'value'],
                'value_after': row['value']
            }

            gaps.append(gap_info)
            print(f"[GAP FOUND] {gap_start} to {gap_end} ({gap_info['duration_hours']:.2f} hours)")

        print(f"[SUMMARY] Found {len(gaps)} gaps > {max_gap_hours} hours")
        return gaps

    def analyze_unit_gaps(self, start_date: str, end_date: str,
                          max_gap_hours: float = 1.0) -> Dict:
        """
        Analyze gaps across all tags in the unit.

        Returns:
            Dictionary with gap analysis results
        """
        print(f"\n{'='*60}")
        print(f"UNIT-WIDE GAP ANALYSIS: {self.plant}/{self.unit}")
        print(f"{'='*60}")

        # Get all tags in unit
        try:
            parquet_file = pq.ParquetFile(self.processed_path)
            all_tags = parquet_file.schema_arrow.metadata.get(b'tags', b'').decode()

            # If metadata not available, scan first partition
            if not all_tags:
                sample_df = pq.read_table(self.processed_path, columns=['tag']).to_pandas()
                all_tags = sample_df['tag'].unique().tolist()
            else:
                all_tags = json.loads(all_tags)

        except Exception as e:
            print(f"[ERROR] Failed to get tag list: {e}")
            return {}

        print(f"[INFO] Found {len(all_tags)} tags to analyze")

        # Analyze each tag
        all_gaps = {}
        gap_count = 0

        for tag in all_tags:
            gaps = self.detect_gaps(tag, start_date, end_date, max_gap_hours)
            if gaps:
                all_gaps[tag] = gaps
                gap_count += len(gaps)

        # Summary
        print(f"\n{'='*60}")
        print(f"GAP ANALYSIS SUMMARY")
        print(f"{'='*60}")
        print(f"Tags with gaps: {len(all_gaps)}/{len(all_tags)}")
        print(f"Total gaps found: {gap_count}")

        # Save report
        report = {
            'plant': self.plant,
            'unit': self.unit,
            'analysis_date': datetime.now().isoformat(),
            'period_start': start_date,
            'period_end': end_date,
            'max_gap_hours': max_gap_hours,
            'summary': {
                'total_tags': len(all_tags),
                'tags_with_gaps': len(all_gaps),
                'total_gaps': gap_count
            },
            'gaps_by_tag': all_gaps
        }

        report_file = self.gap_report_path / f"gap_analysis_{self.plant}_{self.unit}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n[SAVED] Gap report: {report_file}")

        return report

    def preview_fetch_requirements(self, gaps: List[Dict]) -> Dict:
        """
        Preview what data would need to be fetched to fill gaps.

        Returns:
            Dictionary with fetch requirements
        """
        if not gaps:
            return {'status': 'no_gaps', 'message': 'No gaps to fill'}

        print(f"\n{'='*60}")
        print(f"FETCH REQUIREMENTS PREVIEW")
        print(f"{'='*60}")

        total_hours = sum(g['duration_hours'] for g in gaps)
        total_points = sum(g['expected_points'] for g in gaps)

        print(f"Number of gaps: {len(gaps)}")
        print(f"Total duration: {total_hours:.2f} hours")
        print(f"Expected data points: {total_points}")

        # Group by time period
        periods = {}
        for gap in gaps:
            start = pd.Timestamp(gap['gap_start'])
            period_key = start.strftime('%Y-%m')
            if period_key not in periods:
                periods[period_key] = []
            periods[period_key].append(gap)

        print(f"\nGaps by period:")
        for period, period_gaps in sorted(periods.items()):
            print(f"  {period}: {len(period_gaps)} gaps")

        return {
            'status': 'preview',
            'total_gaps': len(gaps),
            'total_hours': total_hours,
            'expected_points': total_points,
            'periods': periods,
            'gaps': gaps
        }

    def fetch_missing_data(self, tag: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """
        Fetch missing data from PI system for a specific gap.
        This is a SAFE fetch that doesn't modify existing files.

        Args:
            tag: Tag name
            start_time: Gap start time (ISO format)
            end_time: Gap end time (ISO format)

        Returns:
            DataFrame with fetched data or None
        """
        print(f"\n[FETCH] Tag: {tag}")
        print(f"[FETCH] Period: {start_time} to {end_time}")

        if self.preview_only:
            print("[PREVIEW MODE] Would fetch data from PI system")
            print("[PREVIEW MODE] No actual fetch performed")
            return None

        # Import Excel refresh module
        try:
            from pi_monitor.excel_refresh import fetch_pi_data_for_tag

            # Fetch data
            df = fetch_pi_data_for_tag(
                tag=tag,
                start_time=start_time,
                end_time=end_time,
                plant=self.plant
            )

            if df is not None and not df.empty:
                print(f"[SUCCESS] Fetched {len(df)} records")
                return df
            else:
                print(f"[WARNING] No data returned from PI")
                return None

        except Exception as e:
            print(f"[ERROR] Fetch failed: {e}")
            return None

    def fill_gaps_safe(self, gaps: List[Dict], auto_approve: bool = False) -> Dict:
        """
        Safely fill detected gaps with new data from PI.
        Writes to separate directory, never modifies originals.

        Args:
            gaps: List of gap dictionaries from detect_gaps()
            auto_approve: If True, skip confirmation prompts

        Returns:
            Dictionary with fill results
        """
        if not gaps:
            return {'status': 'no_gaps', 'filled': 0}

        print(f"\n{'='*60}")
        print(f"GAP FILLING OPERATION")
        print(f"{'='*60}")
        print(f"Mode: {'PREVIEW ONLY' if self.preview_only else 'WRITE ENABLED'}")
        print(f"Gaps to fill: {len(gaps)}")

        if not auto_approve and not self.preview_only:
            response = input("\nProceed with gap filling? (yes/no): ")
            if response.lower() != 'yes':
                print("[CANCELLED] Gap filling cancelled by user")
                return {'status': 'cancelled', 'filled': 0}

        filled_count = 0
        failed_count = 0
        results = []

        for gap in gaps:
            tag = gap['tag']
            start_time = gap['gap_start']
            end_time = gap['gap_end']

            print(f"\n[FILLING GAP] {tag}: {start_time} to {end_time}")

            # Fetch missing data
            new_data = self.fetch_missing_data(tag, start_time, end_time)

            if new_data is not None and not new_data.empty:
                # Save to separate output directory
                output_file = self.gap_fill_output / f"gap_fill_{tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

                if not self.preview_only:
                    new_data.to_parquet(output_file, index=False)
                    print(f"[SAVED] {output_file}")

                filled_count += 1
                results.append({
                    'tag': tag,
                    'gap_start': start_time,
                    'gap_end': end_time,
                    'status': 'filled' if not self.preview_only else 'preview',
                    'records_fetched': len(new_data),
                    'output_file': str(output_file) if not self.preview_only else None
                })
            else:
                failed_count += 1
                results.append({
                    'tag': tag,
                    'gap_start': start_time,
                    'gap_end': end_time,
                    'status': 'failed',
                    'records_fetched': 0
                })

        print(f"\n{'='*60}")
        print(f"GAP FILLING COMPLETE")
        print(f"{'='*60}")
        print(f"Filled: {filled_count}/{len(gaps)}")
        print(f"Failed: {failed_count}/{len(gaps)}")

        return {
            'status': 'complete',
            'filled': filled_count,
            'failed': failed_count,
            'results': results
        }


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(description="Independent Gap Filler - Safe Data Recovery")
    parser.add_argument('--plant', default='ABF', help='Plant name')
    parser.add_argument('--unit', default='07-MT01-K001', help='Unit name')
    parser.add_argument('--start-date', default='2025-09-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-09-30', help='End date (YYYY-MM-DD)')
    parser.add_argument('--max-gap-hours', type=float, default=1.0, help='Maximum acceptable gap (hours)')
    parser.add_argument('--tag', help='Specific tag to analyze (optional)')
    parser.add_argument('--preview-only', action='store_true', default=True, help='Preview mode (default)')
    parser.add_argument('--write-mode', action='store_true', help='Enable writing (disables preview)')
    parser.add_argument('--fill-gaps', action='store_true', help='Attempt to fill detected gaps')

    args = parser.parse_args()

    # Create filler instance
    preview_mode = not args.write_mode
    filler = IndependentGapFiller(
        plant=args.plant,
        unit=args.unit,
        preview_only=preview_mode
    )

    # Detect gaps
    if args.tag:
        # Single tag analysis
        gaps = filler.detect_gaps(args.tag, args.start_date, args.end_date, args.max_gap_hours)

        if gaps:
            # Preview fetch requirements
            requirements = filler.preview_fetch_requirements(gaps)

            # Optionally fill gaps
            if args.fill_gaps:
                results = filler.fill_gaps_safe(gaps)
                print(f"\n[RESULTS] {json.dumps(results, indent=2)}")
    else:
        # Full unit analysis
        report = filler.analyze_unit_gaps(args.start_date, args.end_date, args.max_gap_hours)

        # Optionally fill all gaps
        if args.fill_gaps and report.get('gaps_by_tag'):
            all_gaps = []
            for tag_gaps in report['gaps_by_tag'].values():
                all_gaps.extend(tag_gaps)

            results = filler.fill_gaps_safe(all_gaps)
            print(f"\n[RESULTS] {json.dumps(results, indent=2)}")


if __name__ == '__main__':
    main()
