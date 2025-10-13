#!/usr/bin/env python3
"""
Create a completely new Excel file with all 80 C-02001 PI tags properly configured.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def read_tags_file(tag_file_path):
    """Read PI tags from the configuration file."""
    tags = []
    with open(tag_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Remove any numbering prefix like "1→"
                if '→' in line:
                    tag = line.split('→', 1)[1].strip()
                else:
                    tag = line.strip()
                if tag:
                    tags.append(tag)
    return tags

def create_new_excel_with_all_tags(tag_file_path, output_path):
    """Create a new Excel file with all 80 PI tags as columns."""

    print(f"Reading tags from: {tag_file_path}")
    tags = read_tags_file(tag_file_path)
    print(f"Found {len(tags)} tags for C-02001")

    if not tags:
        print("No tags found!")
        return False

    # Create DataFrame structure
    print("Creating Excel structure...")

    # Calculate time range (1.5 years, 0.1h intervals)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=int(1.5 * 365))

    # Generate time series with 0.1h intervals
    time_points = pd.date_range(start=start_time, end=end_time, freq='6T')  # 6 minutes = 0.1 hours
    print(f"Generated {len(time_points)} time points")

    # Create column structure
    columns = ['Timestamp'] + tags
    print(f"Creating DataFrame with {len(columns)} columns")

    # Create empty DataFrame with proper structure
    df = pd.DataFrame(index=range(len(time_points)), columns=columns)
    df['Timestamp'] = time_points

    # Add sample data for the first few time points (for testing)
    print("Adding sample data...")
    import numpy as np
    for i, tag in enumerate(tags):
        # Add some sample random data for first 100 rows
        sample_data = np.random.normal(50 + i, 5, min(100, len(time_points)))
        df.loc[:len(sample_data)-1, tag] = sample_data

    # Save to Excel
    print(f"Saving to Excel: {output_path}")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Configuration sheet
        config_df = pd.DataFrame({
            'Parameter': ['Start Time', 'End Time', 'Interval', 'Unit', 'Total Tags', 'Data Points'],
            'Value': [
                start_time.strftime('%m/%d/%Y %H:%M:%S'),
                end_time.strftime('%m/%d/%Y %H:%M:%S'),
                '0.1h (6 minutes)',
                'C-02001',
                len(tags),
                len(time_points)
            ]
        })
        config_df.to_excel(writer, sheet_name='Config', index=False)

        # Main data sheet with sample data
        df.to_excel(writer, sheet_name='Data', index=False)

        # PI Tags list sheet
        tags_df = pd.DataFrame({'PI_Tags': tags})
        tags_df.to_excel(writer, sheet_name='PI_Tags', index=False)

    print(f"Successfully created Excel file with {len(tags)} PI tag columns!")
    return True

def main():
    """Main entry point."""
    tag_file = r"C:\Users\george.gabrielujai\Documents\CodeX\config\tags_pcmsb_c02001.txt"
    output_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"

    # Check if tag file exists
    if not Path(tag_file).exists():
        print(f"Tag file not found: {tag_file}")
        return

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("CREATING NEW EXCEL FILE WITH ALL 80 C-02001 PI TAGS")
    print("=" * 70)

    success = create_new_excel_with_all_tags(tag_file, output_file)

    if success:
        print("\n" + "=" * 50)
        print("NEW EXCEL FILE CREATED SUCCESSFULLY!")
        print("=" * 50)
        print(f"\nFile location: {output_file}")
        print("\nFile contains:")
        print("- Config sheet with parameters")
        print("- Data sheet with all 80 PI tag columns")
        print("- PI_Tags sheet with tag list")
        print("- Sample data for first 100 time points")
        print("\nNext steps:")
        print("1. Configure PI DataLink in the Data sheet")
        print("2. Replace sample data with real PI data")
        print("3. Use this file for parquet generation")
    else:
        print("Failed to create new Excel file")

if __name__ == "__main__":
    main()