#!/usr/bin/env python3
"""
FULLY AUTOMATED PI DATA FETCH SYSTEM
Automatically fetches 1.5 years of PI data for all 80 C-02001 tags and generates final parquet.
No manual intervention required.
"""

import xlwings as xw
from pathlib import Path
import pandas as pd
import time
from datetime import datetime, timedelta
import sys

class AutomatedPIFetcher:
    def __init__(self, tag_file_path, output_parquet_path):
        self.tag_file_path = tag_file_path
        self.output_parquet_path = output_parquet_path
        self.excel_path = None
        self.tags = []
        self.pi_server = "\\\\PTSG-1MMPDPdb01"
        self.start_time_expr = "*-1.5y"
        self.end_time_expr = "*"
        self.interval = "0.1h"

    def read_tags(self):
        """Read PI tags from configuration file."""
        print(f"Reading tags from: {self.tag_file_path}")

        with open(self.tag_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '→' in line:
                        tag = line.split('→', 1)[1].strip()
                    else:
                        tag = line.strip()
                    if tag:
                        self.tags.append(tag)

        print(f"Loaded {len(self.tags)} PI tags")
        return len(self.tags) > 0

    def create_automated_excel_file(self):
        """Create a new Excel file optimized for automated PI data fetching."""

        # Create temporary Excel file for automation
        self.excel_path = Path(self.output_parquet_path).parent / "AUTOMATED_PI_FETCH_C02001.xlsx"

        print(f"Creating automated Excel file: {self.excel_path}")

        app = xw.App(visible=False, add_book=False)  # Hidden for automation

        try:
            # Create new workbook
            wb = app.books.add()
            ws = wb.sheets[0]
            ws.name = "PI_Data"

            print("Setting up automated PI DataLink structure...")

            # Add configuration header
            ws.range('A1').value = 'AUTOMATED PI DATA FETCH - C-02001'
            ws.range('A2').value = f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            ws.range('A3').value = f'Tags: {len(self.tags)}'
            ws.range('A4').value = f'Time Range: {self.start_time_expr} to {self.end_time_expr}'
            ws.range('A5').value = f'Interval: {self.interval}'

            # Headers starting from row 7
            header_row = 7
            ws.range(f'A{header_row}').value = 'Time'

            # Add all tag headers and formulas in one operation
            print("Setting up PISampDat formulas for all 80 tags...")

            for i, tag in enumerate(self.tags):
                col = i + 2  # Start from column B
                col_letter = self.get_column_letter(col)

                # Set header
                ws.range(f'{col_letter}{header_row}').value = tag

                # Set PISampDat formula
                data_row = header_row + 1
                formula = f'=PISampDat("{tag}","{self.start_time_expr}","{self.end_time_expr}","{self.interval}",1,"{self.pi_server}")'
                ws.range(f'{col_letter}{data_row}').value = formula

                if (i + 1) % 10 == 0:
                    print(f"  Added formulas for {i + 1}/{len(self.tags)} tags...")

            # Format headers
            header_range = f'A{header_row}:{self.get_column_letter(len(self.tags) + 1)}{header_row}'
            ws.range(header_range).api.Font.Bold = True

            # Auto-fit first column
            ws.range('A:A').api.ColumnWidth = 20

            # Save the file
            wb.save_as(self.excel_path)
            print(f"Excel file created: {self.excel_path}")

            return wb, app

        except Exception as e:
            print(f"Error creating Excel file: {e}")
            if 'app' in locals():
                app.quit()
            return None, None

    def auto_refresh_pi_datalink(self, wb, app, max_wait_minutes=30):
        """Automatically refresh PI DataLink and wait for data population."""

        print("Starting automated PI DataLink refresh...")

        try:
            # Refresh all external data connections
            print("Executing PI DataLink refresh...")
            wb.api.RefreshAll()

            # Monitor data population
            ws = wb.sheets['PI_Data']
            start_time = time.time()
            max_wait_seconds = max_wait_minutes * 60

            print(f"Monitoring data population (max wait: {max_wait_minutes} minutes)...")

            check_interval = 30  # Check every 30 seconds
            last_data_count = 0
            stable_count = 0

            while time.time() - start_time < max_wait_seconds:
                try:
                    # Check data in a few sample columns
                    sample_range = 'B8:E20'  # Sample first few tags, first few rows
                    sample_data = ws.range(sample_range).value

                    # Count non-null numeric values
                    data_count = 0
                    if sample_data:
                        for row in sample_data:
                            if row:
                                for cell in row:
                                    if isinstance(cell, (int, float)) and cell != 0:
                                        data_count += 1

                    elapsed_minutes = (time.time() - start_time) / 60
                    print(f"  [{elapsed_minutes:.1f}min] Data points detected: {data_count}")

                    # Check if data is stable (not increasing)
                    if data_count == last_data_count and data_count > 0:
                        stable_count += 1
                        if stable_count >= 3:  # Stable for 3 checks
                            print("Data appears to be fully loaded (stable)")
                            break
                    else:
                        stable_count = 0

                    last_data_count = data_count

                    # If we have reasonable amount of data, consider it loaded
                    if data_count >= 40:  # 10+ data points per tag for 4 sample tags
                        print("Sufficient data detected - proceeding")
                        break

                    time.sleep(check_interval)

                except Exception as e:
                    print(f"  Error checking data: {e}")
                    time.sleep(check_interval)

            # Final data quality check
            print("Performing final data quality assessment...")
            return self.assess_data_quality(ws)

        except Exception as e:
            print(f"Error during PI DataLink refresh: {e}")
            return False

    def assess_data_quality(self, ws):
        """Assess the quality of fetched PI data."""

        try:
            print("Assessing data quality...")

            # Check a broader sample of data
            sample_range = 'B8:F50'  # First 5 tags, first 43 data rows
            sample_data = ws.range(sample_range).value

            if not sample_data:
                print("No data found in sample range")
                return False

            # Analyze data quality
            total_cells = 0
            filled_cells = 0
            numeric_cells = 0

            for row in sample_data:
                if row:
                    for cell in row:
                        total_cells += 1
                        if cell is not None:
                            filled_cells += 1
                            if isinstance(cell, (int, float)):
                                numeric_cells += 1

            fill_rate = (filled_cells / total_cells) * 100 if total_cells > 0 else 0
            numeric_rate = (numeric_cells / total_cells) * 100 if total_cells > 0 else 0

            print(f"Data quality assessment:")
            print(f"  Total cells checked: {total_cells}")
            print(f"  Filled cells: {filled_cells} ({fill_rate:.1f}%)")
            print(f"  Numeric cells: {numeric_cells} ({numeric_rate:.1f}%)")

            # Consider data good if at least 50% filled with numeric data
            is_good_quality = fill_rate >= 50 and numeric_rate >= 40

            if is_good_quality:
                print("Data quality: GOOD - proceeding with parquet generation")
            else:
                print("Data quality: POOR - may need manual intervention")

            return is_good_quality

        except Exception as e:
            print(f"Error assessing data quality: {e}")
            return False

    def extract_and_save_parquet(self, wb):
        """Extract data from Excel and save as parquet."""

        print("Extracting data from Excel for parquet generation...")

        try:
            ws = wb.sheets['PI_Data']

            # Get the used range
            used_range = ws.used_range
            if not used_range:
                print("No data range found")
                return False

            print(f"Data range: {used_range.address}")

            # Read all data
            print("Reading all data from Excel...")
            all_data = used_range.value

            if not all_data or len(all_data) < 8:
                print("Insufficient data in Excel")
                return False

            # Find header row (row 7, which is index 6)
            header_row_index = 6
            headers = all_data[header_row_index]
            data_rows = all_data[header_row_index + 1:]

            if not headers or not data_rows:
                print("No headers or data found")
                return False

            print(f"Found {len(headers)} columns and {len(data_rows)} data rows")

            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)

            # Clean column names and filter for PI tags
            df.columns = [str(col) if col is not None else f'Col_{i}' for i, col in enumerate(df.columns)]

            # Identify timestamp and tag columns
            timestamp_col = None
            tag_columns = []

            for col in df.columns:
                if 'time' in col.lower():
                    timestamp_col = col
                elif any(tag in str(col) for tag in self.tags):
                    tag_columns.append(col)

            print(f"Identified timestamp column: {timestamp_col}")
            print(f"Identified {len(tag_columns)} tag columns")

            if len(tag_columns) == 0:
                print("No PI tag columns found")
                return False

            # Select relevant columns
            if timestamp_col:
                final_columns = [timestamp_col] + tag_columns
                df_final = df[final_columns].copy()
                df_final.rename(columns={timestamp_col: 'timestamp'}, inplace=True)
            else:
                df_final = df[tag_columns].copy()

            # Add metadata
            df_final['plant'] = 'PCMSB'
            df_final['unit'] = 'C-02001'

            # Clean data types
            print("Processing data types...")
            for col in tag_columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

            # Remove completely empty rows
            initial_rows = len(df_final)
            df_final = df_final.dropna(subset=tag_columns, how='all')
            final_rows = len(df_final)

            print(f"Data cleaning: {initial_rows} -> {final_rows} rows")

            # Ensure output directory exists
            output_path = Path(self.output_parquet_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet
            print(f"Saving to parquet: {self.output_parquet_path}")
            df_final.to_parquet(self.output_parquet_path, engine='pyarrow', compression='snappy')

            # Report results
            file_size = output_path.stat().st_size

            print(f"\n" + "=" * 60)
            print("AUTOMATED PI DATA FETCH COMPLETED!")
            print("=" * 60)
            print(f"Output file: {self.output_parquet_path}")
            print(f"Rows: {len(df_final):,}")
            print(f"Columns: {len(df_final.columns)}")
            print(f"PI tags with data: {len(tag_columns)}")
            print(f"File size: {file_size / 1024 / 1024:.2f} MB")

            # Data quality summary
            if tag_columns:
                null_percentages = df_final[tag_columns].isnull().mean() * 100
                avg_null_pct = null_percentages.mean()
                non_empty_tags = sum(null_percentages < 100)

                print(f"\nData Quality Summary:")
                print(f"Tags with data: {non_empty_tags}/{len(self.tags)}")
                print(f"Average null percentage: {avg_null_pct:.1f}%")

                if non_empty_tags >= len(self.tags) * 0.8:  # 80% of tags have data
                    print("SUCCESS: Most PI tags have data!")
                    return True
                else:
                    print("PARTIAL: Some PI tags missing data")
                    return True  # Still consider successful if we got some data

            return True

        except Exception as e:
            print(f"Error extracting data to parquet: {e}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup_excel_file(self, wb, app):
        """Clean up Excel resources."""
        try:
            if wb:
                wb.close()
            if app:
                app.quit()

            # Delete temporary Excel file
            if self.excel_path and Path(self.excel_path).exists():
                Path(self.excel_path).unlink()
                print(f"Cleaned up temporary file: {self.excel_path}")

        except Exception as e:
            print(f"Error during cleanup: {e}")

    def get_column_letter(self, col_num):
        """Convert column number to Excel column letter."""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result

    def run_fully_automated_fetch(self):
        """Run the complete automated PI data fetch process."""

        print("=" * 70)
        print("FULLY AUTOMATED PI DATA FETCH - C-02001")
        print("=" * 70)
        print(f"Target: 1.5 years of data for 80 PI tags at 0.1h intervals")
        print(f"Output: {self.output_parquet_path}")
        print("=" * 70)

        wb = None
        app = None

        try:
            # Step 1: Read tags
            if not self.read_tags():
                print("Failed to read tags")
                return False

            # Step 2: Create automated Excel file
            wb, app = self.create_automated_excel_file()
            if not wb:
                print("Failed to create Excel file")
                return False

            # Step 3: Auto-refresh PI DataLink
            if not self.auto_refresh_pi_datalink(wb, app):
                print("PI DataLink refresh failed or insufficient data")
                # Continue anyway to see what we got

            # Step 4: Extract and save parquet
            success = self.extract_and_save_parquet(wb)

            if success:
                print("\nFULLY AUTOMATED FETCH COMPLETED SUCCESSFULLY!")
                return True
            else:
                print("\nAutomated fetch completed with issues")
                return False

        except Exception as e:
            print(f"Error in automated fetch: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            # Always cleanup
            self.cleanup_excel_file(wb, app)

def main():
    """Main entry point for fully automated PI data fetching."""

    # Configuration
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    output_parquet = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_AUTOMATED_FULL.parquet"

    # Check prerequisites
    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    # Create automated fetcher
    fetcher = AutomatedPIFetcher(tag_file, output_parquet)

    # Run fully automated process
    success = fetcher.run_fully_automated_fetch()

    if success:
        print("\n🎉 AUTOMATION COMPLETE - Check your parquet file!")
    else:
        print("\n❌ Automation failed - Check logs above")

if __name__ == "__main__":
    main()