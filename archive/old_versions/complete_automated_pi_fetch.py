#!/usr/bin/env python3
"""
COMPLETE AUTOMATED PI FETCH SYSTEM
End-to-end automation: Setup formulas -> Refresh data -> Generate parquet
No manual intervention required.
"""

import xlwings as xw
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
import sys
import tempfile

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def get_column_letter(col_num):
    """Convert column number to Excel column letter."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + ord('A')) + result
        col_num //= 26
    return result

class CompleteAutomatedPIFetch:
    def __init__(self, tag_file_path, output_parquet_path):
        self.tag_file_path = tag_file_path
        self.output_parquet_path = output_parquet_path
        self.tags = []
        self.pi_server = "\\\\PTSG-1MMPDPdb01"
        self.start_time_expr = "*-1.5y"
        self.end_time_expr = "*"
        self.interval = "0.1h"

    def step1_setup_excel_with_formulas(self):
        """Step 1: Create Excel file with proper PISampDat formulas."""

        print("STEP 1: Setting up Excel file with PISampDat formulas...")

        # Read tags
        self.tags = read_tags_file(self.tag_file_path)
        if not self.tags:
            print("Error: No tags found")
            return None, None

        print(f"Loaded {len(self.tags)} PI tags")

        # Create temporary Excel file for the process
        temp_dir = Path(self.output_parquet_path).parent
        temp_excel = temp_dir / "TEMP_PI_FETCH_AUTOMATED.xlsx"

        app = xw.App(visible=False, add_book=False)

        try:
            # Create new workbook
            wb = app.books.add()
            ws = wb.sheets[0]
            ws.name = "PI_Data"

            print("Setting up headers and formulas...")

            # Configuration section
            ws.range('A1').value = 'AUTOMATED PI FETCH - C-02001'
            ws.range('A2').value = f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            ws.range('A3').value = f'PI Server: {self.pi_server}'
            ws.range('A4').value = f'Time Range: {self.start_time_expr} to {self.end_time_expr}'
            ws.range('A5').value = f'Interval: {self.interval}'

            # Headers starting from row 7
            header_row = 7
            ws.range(f'A{header_row}').value = 'Time'

            # Add all tag headers and PISampDat formulas
            for i, tag in enumerate(self.tags):
                col = i + 2  # Start from column B
                col_letter = get_column_letter(col)

                # Set header
                ws.range(f'{col_letter}{header_row}').value = tag

                # Set PISampDat formula in first data row
                data_row = header_row + 1
                formula = f'=PISampDat("{tag}","{self.start_time_expr}","{self.end_time_expr}","{self.interval}",1,"{self.pi_server}")'
                ws.range(f'{col_letter}{data_row}').value = formula

                if (i + 1) % 20 == 0:
                    print(f"  Added {i + 1}/{len(self.tags)} formulas...")

            # Format headers
            header_range = f'A{header_row}:{get_column_letter(len(self.tags) + 1)}{header_row}'
            ws.range(header_range).api.Font.Bold = True

            # Save the file
            wb.save(temp_excel)
            print(f"Excel file created with all formulas: {temp_excel}")

            return wb, app

        except Exception as e:
            print(f"Error in step 1: {e}")
            if 'app' in locals():
                try:
                    app.quit()
                except:
                    pass
            return None, None

    def step2_refresh_pi_data(self, wb, max_wait_minutes=25):
        """Step 2: Refresh PI DataLink and wait for data."""

        print(f"\nSTEP 2: Refreshing PI DataLink data (max wait: {max_wait_minutes} min)...")

        try:
            # Initiate refresh
            print("Initiating PI DataLink refresh...")
            wb.api.RefreshAll()

            ws = wb.sheets['PI_Data']
            start_time = time.time()
            max_wait_seconds = max_wait_minutes * 60

            print("Monitoring data population...")

            check_interval = 45  # Check every 45 seconds
            last_data_count = 0
            stable_iterations = 0

            while time.time() - start_time < max_wait_seconds:
                try:
                    # Check multiple sample areas
                    sample_ranges = ['B8:F15', 'G8:K15', 'L8:P15']
                    total_data_count = 0

                    for sample_range in sample_ranges:
                        try:
                            sample_data = ws.range(sample_range).value
                            if sample_data:
                                for row in sample_data:
                                    if row:
                                        for cell in row:
                                            if isinstance(cell, (int, float)) and cell != 0:
                                                total_data_count += 1
                        except:
                            continue

                    elapsed_minutes = (time.time() - start_time) / 60
                    print(f"  [{elapsed_minutes:.1f}min] Data points detected: {total_data_count}")

                    # Check if data is stable (not increasing much)
                    if abs(total_data_count - last_data_count) <= 2:
                        stable_iterations += 1
                        if stable_iterations >= 3 and total_data_count > 50:
                            print("Data appears stable and sufficient - proceeding")
                            return True
                    else:
                        stable_iterations = 0

                    last_data_count = total_data_count

                    # If we have a lot of data, consider it done
                    if total_data_count >= 100:
                        print("Substantial data detected - proceeding")
                        return True

                    time.sleep(check_interval)

                except Exception as e:
                    print(f"  Error checking data: {e}")
                    time.sleep(check_interval)

            print(f"Timeout reached after {max_wait_minutes} minutes")
            return last_data_count > 20  # Return True if we got some data

        except Exception as e:
            print(f"Error in step 2: {e}")
            return False

    def step3_extract_to_parquet(self, wb):
        """Step 3: Extract data from Excel and save as parquet."""

        print("\nSTEP 3: Extracting data to parquet...")

        try:
            ws = wb.sheets['PI_Data']

            # Get the full data range
            used_range = ws.used_range
            if not used_range:
                print("No data range found")
                return False

            print(f"Reading data from range: {used_range.address}")

            # Read all data
            all_data = used_range.value
            if not all_data or len(all_data) < 8:
                print("Insufficient data")
                return False

            # Extract headers (row 7, index 6) and data (row 8+, index 7+)
            headers = all_data[6]  # Row 7 (0-indexed = 6)
            data_rows = all_data[7:]  # Row 8+ (0-indexed = 7+)

            if not headers or not data_rows:
                print("No headers or data found")
                return False

            print(f"Found {len(headers)} columns, {len(data_rows)} data rows")

            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)

            # Clean column names
            df.columns = [str(col) if col is not None else f'Col_{i}' for i, col in enumerate(df.columns)]

            # Identify timestamp and tag columns
            timestamp_col = None
            tag_columns = []

            for col in df.columns:
                if 'time' in col.lower():
                    timestamp_col = col
                elif any(tag in str(col) for tag in self.tags):
                    tag_columns.append(col)

            print(f"Timestamp column: {timestamp_col}")
            print(f"PI tag columns: {len(tag_columns)}")

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

            # Process data types
            print("Processing data types...")
            for col in tag_columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

            # Remove completely empty rows
            initial_rows = len(df_final)
            df_final = df_final.dropna(subset=tag_columns, how='all')
            final_rows = len(df_final)

            print(f"Data cleaning: {initial_rows} -> {final_rows} rows")

            if final_rows == 0:
                print("No valid data after cleaning")
                return False

            # Ensure output directory exists
            output_path = Path(self.output_parquet_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet
            print(f"Saving to: {self.output_parquet_path}")
            df_final.to_parquet(self.output_parquet_path, engine='pyarrow', compression='snappy')

            # Analyze results
            file_size = output_path.stat().st_size

            print(f"\n" + "=" * 60)
            print("COMPLETE AUTOMATION SUCCESSFUL!")
            print("=" * 60)
            print(f"Output: {self.output_parquet_path}")
            print(f"Rows: {len(df_final):,}")
            print(f"Columns: {len(df_final.columns)}")
            print(f"PI tags: {len(tag_columns)}/{len(self.tags)}")
            print(f"Size: {file_size / 1024 / 1024:.2f} MB")

            # Quality assessment
            if tag_columns:
                null_pct = df_final[tag_columns].isnull().mean().mean() * 100
                data_tags = sum(df_final[tag_columns].count() > 0)

                print(f"\nQuality Assessment:")
                print(f"Tags with data: {data_tags}/{len(self.tags)}")
                print(f"Average null rate: {null_pct:.1f}%")

                if file_size > 10 * 1024 * 1024:  # > 10MB
                    print("EXCELLENT: Large comprehensive dataset!")
                elif file_size > 3 * 1024 * 1024:  # > 3MB
                    print("GOOD: Substantial dataset generated!")
                elif data_tags >= len(self.tags) * 0.5:  # 50%+ tags
                    print("ACCEPTABLE: Partial dataset with good coverage!")
                else:
                    print("LIMITED: Small dataset, but some data retrieved!")

            return True

        except Exception as e:
            print(f"Error in step 3: {e}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup(self, wb, app):
        """Clean up resources."""
        try:
            if wb:
                wb.close()
            if app:
                app.quit()

            # Clean up temp file
            temp_dir = Path(self.output_parquet_path).parent
            temp_excel = temp_dir / "TEMP_PI_FETCH_AUTOMATED.xlsx"
            if temp_excel.exists():
                temp_excel.unlink()

        except Exception as e:
            print(f"Cleanup error: {e}")

    def run_complete_automation(self):
        """Run the complete end-to-end automated process."""

        print("=" * 70)
        print("COMPLETE AUTOMATED PI DATA FETCH - C-02001")
        print("NO MANUAL INTERVENTION REQUIRED")
        print("=" * 70)
        print(f"Target: All 80 PI tags, 1.5 years, 0.1h intervals")
        print(f"Output: {self.output_parquet_path}")
        print("=" * 70)

        wb = None
        app = None

        try:
            # Step 1: Setup Excel with formulas
            wb, app = self.step1_setup_excel_with_formulas()
            if not wb:
                return False

            # Step 2: Refresh PI data
            if not self.step2_refresh_pi_data(wb):
                print("Data refresh had issues, but continuing with extraction...")

            # Step 3: Extract to parquet
            success = self.step3_extract_to_parquet(wb)

            return success

        except Exception as e:
            print(f"Error in complete automation: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            self.cleanup(wb, app)

def main():
    """Main entry point."""

    # Configuration
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    output_parquet = r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed\PCMSB_C-02001_COMPLETE_AUTO.parquet"

    # Validation
    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    # Create and run automation
    fetcher = CompleteAutomatedPIFetch(tag_file, output_parquet)
    success = fetcher.run_complete_automation()

    if success:
        print("\nCOMPLETE AUTOMATION FINISHED SUCCESSFULLY!")
        print("Your parquet file is ready with PI data!")
    else:
        print("\nAutomation completed with issues.")

if __name__ == "__main__":
    main()