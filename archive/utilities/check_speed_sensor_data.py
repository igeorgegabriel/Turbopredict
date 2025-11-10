#!/usr/bin/env python3
"""
Check speed sensor data for C-02001 to enable proper speed compensation in anomaly detection.
"""

import pandas as pd
from pathlib import Path

def check_speed_sensor_data():
    """Check if speed sensor data is available and accessible."""

    processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")

    print("=" * 60)
    print("CHECKING SPEED SENSOR DATA FOR C-02001")
    print("=" * 60)

    # Check C-02001 file
    c02001_file = processed_dir / "C-02001_1y_0p1h.parquet"

    if not c02001_file.exists():
        print("C-02001 file not found!")
        return False

    try:
        df = pd.read_parquet(c02001_file)
        print(f"C-02001 data loaded: {len(df):,} rows")

        # Look specifically for the speed sensor
        speed_sensor = "PCM.C-02001.020SI6601.PV"

        # Check if speed sensor exists in the data
        speed_data = df[df['tag'] == speed_sensor]

        if len(speed_data) > 0:
            print(f"\n✓ Speed sensor found: {speed_sensor}")
            print(f"  Data points: {len(speed_data):,}")
            print(f"  Time range: {speed_data['time'].min()} to {speed_data['time'].max()}")

            # Check data quality
            non_null_values = speed_data['value'].count()
            null_values = speed_data['value'].isnull().sum()
            print(f"  Non-null values: {non_null_values:,}")
            print(f"  Null values: {null_values:,}")

            if non_null_values > 0:
                print(f"  Value range: {speed_data['value'].min():.2f} to {speed_data['value'].max():.2f}")
                print(f"  Mean value: {speed_data['value'].mean():.2f}")
                print(f"  Sample values: {speed_data['value'].dropna().head(5).tolist()}")

                print(f"\n✓ Speed sensor data is AVAILABLE and has good quality!")
                return True
            else:
                print(f"\n❌ Speed sensor exists but has no valid data")
                return False
        else:
            print(f"\n❌ Speed sensor NOT found: {speed_sensor}")

            # Show available sensors for reference
            available_tags = df['tag'].unique()
            si_tags = [tag for tag in available_tags if 'SI' in tag]
            print(f"Available SI (Speed Indicator) tags: {si_tags}")

            return False

    except Exception as e:
        print(f"Error checking speed sensor data: {e}")
        return False

def main():
    success = check_speed_sensor_data()

    if success:
        print(f"\n" + "=" * 60)
        print("SPEED SENSOR STATUS: READY FOR COMPENSATION")
        print("=" * 60)
        print("✓ Speed sensor data is available")
        print("✓ System should be able to use speed compensation")
        print("✓ Option [2] analysis with speed compensation should work")
    else:
        print(f"\n" + "=" * 60)
        print("SPEED SENSOR STATUS: NEEDS INVESTIGATION")
        print("=" * 60)
        print("❌ Speed sensor data may need correction")
        print("⚠️ System will use uncompensated analysis")

if __name__ == "__main__":
    main()