"""
Safe PI Data Fetcher - Independent data retrieval without system modification.

This module provides safe PI data fetching capabilities that:
- Never modifies existing Parquet files
- Writes to separate gap_filled directory
- Provides detailed logging and validation
- Supports both Excel/PI DataLink and PI Web API methods
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import win32com.client
import time


class SafePIFetcher:
    """Safe, independent PI data fetcher."""

    def __init__(self, plant: str = "ABF"):
        self.plant = plant
        self.excel_path = self._get_excel_path(plant)
        self.output_dir = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\data\gap_filled")

        print(f"[SAFE FETCHER] Initialized for plant: {plant}")
        print(f"[SAFE FETCHER] Excel source: {self.excel_path}")
        print(f"[SAFE FETCHER] Output directory: {self.output_dir}")

    def _get_excel_path(self, plant: str) -> Path:
        """Get Excel file path for the plant."""
        base_path = Path(r"c:\Users\george.gabrielujai\Documents\CodeX\excel")

        excel_files = {
            'PCFS': base_path / "PCFS_Automation.xlsx",
            'PCMSB': base_path / "PCMSB_Automation.xlsx",
            'ABF': base_path / "ABF_Automation.xlsx"
        }

        excel_path = excel_files.get(plant)
        if not excel_path or not excel_path.exists():
            # Fallback to old naming convention
            excel_path = base_path / f"{plant}_Automation_2.xlsx"

        return excel_path

    def fetch_tag_data_excel(self, tag: str, start_time: str, end_time: str,
                             interval: str = "1h") -> Optional[pd.DataFrame]:
        """
        Fetch PI data using Excel/PI DataLink (safe method).

        Args:
            tag: PI tag name
            start_time: Start time (ISO format)
            end_time: End time (ISO format)
            interval: Data interval (default: 1h)

        Returns:
            DataFrame with timestamp and value columns
        """
        print(f"\n[FETCH EXCEL] Tag: {tag}")
        print(f"[FETCH EXCEL] Period: {start_time} to {end_time}")

        try:
            # Parse times
            start_dt = pd.Timestamp(start_time)
            end_dt = pd.Timestamp(end_time)

            # Open Excel (hidden, read-only)
            excel = win32com.client.Dispatch("Excel.Application")
            excel.Visible = False
            excel.DisplayAlerts = False

            try:
                wb = excel.Workbooks.Open(str(self.excel_path), ReadOnly=True)
                ws = wb.Sheets(1)  # Use first sheet

                # Find empty cells for temporary formulas
                temp_row = 100
                time_col = 1  # Column A
                value_col = 2  # Column B

                print(f"[FETCH EXCEL] Creating PI DataLink formulas...")

                # Generate timestamps
                timestamps = pd.date_range(start_dt, end_dt, freq=interval)
                data = []

                for i, ts in enumerate(timestamps):
                    row = temp_row + i

                    # Set timestamp
                    ws.Cells(row, time_col).Value = ts.strftime("%Y-%m-%d %H:%M:%S")

                    # PI DataLink formula: =PIPastVal(tag, timestamp)
                    formula = f'=PIPastVal("{tag}",A{row})'
                    ws.Cells(row, value_col).Formula = formula

                # Refresh PI DataLink data
                print(f"[FETCH EXCEL] Refreshing PI DataLink...")
                wb.RefreshAll()
                excel.CalculateUntilAsyncQueriesDone()

                # Wait for refresh to complete
                time.sleep(2)

                # Read values
                print(f"[FETCH EXCEL] Reading values...")
                for i, ts in enumerate(timestamps):
                    row = temp_row + i
                    value = ws.Cells(row, value_col).Value

                    # Skip errors
                    if value and not isinstance(value, str):
                        data.append({
                            'timestamp': ts,
                            'value': float(value),
                            'tag': tag
                        })

                # Clean up temporary cells
                for i in range(len(timestamps)):
                    row = temp_row + i
                    ws.Cells(row, time_col).Clear()
                    ws.Cells(row, value_col).Clear()

                wb.Close(SaveChanges=False)

                print(f"[SUCCESS] Fetched {len(data)} records")

                if data:
                    return pd.DataFrame(data)
                else:
                    print(f"[WARNING] No valid data returned")
                    return None

            finally:
                excel.Quit()

        except Exception as e:
            print(f"[ERROR] Excel fetch failed: {e}")
            return None

    def fetch_tag_data_webapi(self, tag: str, start_time: str, end_time: str,
                              interval: str = "1h") -> Optional[pd.DataFrame]:
        """
        Fetch PI data using PI Web API (alternative method).

        Note: Requires PI Web API setup and authentication.
        """
        print(f"\n[FETCH WEBAPI] Tag: {tag}")
        print(f"[FETCH WEBAPI] Period: {start_time} to {end_time}")

        try:
            from pi_monitor.webapi import PIWebAPIClient

            # Initialize client
            client = PIWebAPIClient()

            # Fetch data
            df = client.get_interpolated_data(
                tag=tag,
                start_time=start_time,
                end_time=end_time,
                interval=interval
            )

            if df is not None and not df.empty:
                print(f"[SUCCESS] Fetched {len(df)} records")
                return df
            else:
                print(f"[WARNING] No data returned")
                return None

        except Exception as e:
            print(f"[ERROR] Web API fetch failed: {e}")
            print(f"[INFO] Falling back to Excel method...")
            return None

    def fetch_with_retry(self, tag: str, start_time: str, end_time: str,
                         interval: str = "1h", max_retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Fetch data with automatic retry and fallback.

        Tries Web API first, then falls back to Excel method.
        """
        print(f"\n[FETCH RETRY] Tag: {tag}")

        for attempt in range(max_retries):
            print(f"[ATTEMPT {attempt + 1}/{max_retries}]")

            # Try Web API first
            df = self.fetch_tag_data_webapi(tag, start_time, end_time, interval)

            if df is not None and not df.empty:
                return df

            # Fallback to Excel
            print(f"[FALLBACK] Trying Excel method...")
            df = self.fetch_tag_data_excel(tag, start_time, end_time, interval)

            if df is not None and not df.empty:
                return df

            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"[RETRY] Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

        print(f"[FAILED] All fetch attempts failed for {tag}")
        return None

    def save_fetched_data(self, df: pd.DataFrame, tag: str, gap_start: str) -> Path:
        """
        Save fetched data to gap_filled directory.

        Args:
            df: DataFrame with fetched data
            tag: Tag name
            gap_start: Gap start time (for filename)

        Returns:
            Path to saved file
        """
        # Create output directory
        output_dir = self.output_dir / f"plant={self.plant}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gap_date = pd.Timestamp(gap_start).strftime("%Y%m%d")
        filename = f"gap_fill_{tag.replace('/', '_')}_{gap_date}_{timestamp}.parquet"

        output_path = output_dir / filename

        # Save to Parquet
        df.to_parquet(output_path, index=False)

        print(f"[SAVED] {output_path}")
        print(f"[SAVED] {len(df)} records, {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

        return output_path

    def validate_fetched_data(self, df: pd.DataFrame, tag: str,
                              expected_start: str, expected_end: str) -> Dict:
        """
        Validate fetched data quality.

        Returns:
            Dictionary with validation results
        """
        print(f"\n[VALIDATION] Tag: {tag}")

        results = {
            'tag': tag,
            'valid': True,
            'warnings': [],
            'errors': []
        }

        # Check data exists
        if df is None or df.empty:
            results['valid'] = False
            results['errors'].append("No data returned")
            return results

        # Check time range
        actual_start = df['timestamp'].min()
        actual_end = df['timestamp'].max()
        expected_start_dt = pd.Timestamp(expected_start)
        expected_end_dt = pd.Timestamp(expected_end)

        print(f"[VALIDATION] Expected: {expected_start} to {expected_end}")
        print(f"[VALIDATION] Actual: {actual_start} to {actual_end}")

        if actual_start > expected_start_dt:
            results['warnings'].append(f"Data starts late: {actual_start}")

        if actual_end < expected_end_dt:
            results['warnings'].append(f"Data ends early: {actual_end}")

        # Check for nulls
        null_count = df['value'].isna().sum()
        if null_count > 0:
            null_pct = (null_count / len(df)) * 100
            results['warnings'].append(f"{null_count} null values ({null_pct:.2f}%)")

        # Check for constant values
        if df['value'].nunique() == 1:
            results['warnings'].append("All values are identical (potential data quality issue)")

        # Check data continuity
        df_sorted = df.sort_values('timestamp')
        time_diffs = df_sorted['timestamp'].diff()
        max_gap = time_diffs.max()

        if max_gap > pd.Timedelta(hours=2):
            results['warnings'].append(f"Large gap detected in fetched data: {max_gap}")

        # Summary
        print(f"[VALIDATION] Records: {len(df)}")
        print(f"[VALIDATION] Null values: {null_count}")
        print(f"[VALIDATION] Unique values: {df['value'].nunique()}")
        print(f"[VALIDATION] Valid: {results['valid']}")

        if results['warnings']:
            print(f"[VALIDATION] Warnings: {len(results['warnings'])}")
            for warning in results['warnings']:
                print(f"  - {warning}")

        if results['errors']:
            print(f"[VALIDATION] Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"  - {error}")

        return results


def main():
    """Test the safe fetcher."""
    import argparse

    parser = argparse.ArgumentParser(description="Safe PI Data Fetcher")
    parser.add_argument('--plant', default='ABF', help='Plant name')
    parser.add_argument('--tag', required=True, help='PI tag name')
    parser.add_argument('--start', required=True, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', required=True, help='End time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--interval', default='1h', help='Data interval (default: 1h)')
    parser.add_argument('--method', choices=['excel', 'webapi', 'auto'], default='auto',
                       help='Fetch method (default: auto)')

    args = parser.parse_args()

    # Create fetcher
    fetcher = SafePIFetcher(plant=args.plant)

    # Fetch data
    if args.method == 'excel':
        df = fetcher.fetch_tag_data_excel(args.tag, args.start, args.end, args.interval)
    elif args.method == 'webapi':
        df = fetcher.fetch_tag_data_webapi(args.tag, args.start, args.end, args.interval)
    else:
        df = fetcher.fetch_with_retry(args.tag, args.start, args.end, args.interval)

    # Validate and save
    if df is not None:
        validation = fetcher.validate_fetched_data(df, args.tag, args.start, args.end)

        if validation['valid']:
            output_path = fetcher.save_fetched_data(df, args.tag, args.start)
            print(f"\n[COMPLETE] Data saved to: {output_path}")
        else:
            print(f"\n[WARNING] Data validation failed, but saved anyway")
            output_path = fetcher.save_fetched_data(df, args.tag, args.start)
            print(f"[COMPLETE] Data saved to: {output_path}")
    else:
        print(f"\n[FAILED] No data fetched")


if __name__ == '__main__':
    main()
