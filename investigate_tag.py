#!/usr/bin/env python3
"""
Investigate specific problematic tags to understand why they fail
"""

from pi_monitor.parquet_database import ParquetDatabase
import pandas as pd
import numpy as np

def investigate_tag(unit, tag):
    """Investigate why a specific tag is failing anomaly detection"""
    
    print(f"INVESTIGATING: {tag}")
    print("=" * 60)
    
    db = ParquetDatabase()
    df = db.get_unit_data(unit)
    
    if df.empty:
        print("No data available")
        return
        
    # Get data for this specific tag
    tag_df = df[df['tag'] == tag].copy()
    
    if tag_df.empty:
        print("No data for this tag")
        return
        
    print(f"Total records: {len(tag_df):,}")
    
    # Basic statistics
    values = tag_df['value'].dropna()
    if len(values) == 0:
        print("No valid values")
        return
        
    print(f"\nBASIC STATISTICS:")
    print(f"Count: {len(values):,}")
    print(f"Mean: {values.mean():.6f}")
    print(f"Std: {values.std():.6f}")
    print(f"Min: {values.min():.6f}")
    print(f"Max: {values.max():.6f}")
    print(f"Range: {values.max() - values.min():.6f}")
    
    # Check for stuck values
    unique_values = values.nunique()
    print(f"\nUNIQUE VALUES: {unique_values}")
    
    if unique_values <= 10:
        print("VALUE DISTRIBUTION:")
        value_counts = values.value_counts().head(10)
        for val, count in value_counts.items():
            pct = count / len(values) * 100
            print(f"  {val:.6f}: {count:,} times ({pct:.1f}%)")
    
    # Check for constant values (sensor stuck)
    if unique_values == 1:
        print(f"\n*** SENSOR STUCK AT: {values.iloc[0]:.6f} ***")
        return "STUCK_SENSOR"
    
    # Check for very low variation (nearly stuck)
    cv = values.std() / abs(values.mean()) * 100 if values.mean() != 0 else float('inf')
    print(f"\nCOEFFICIENT OF VARIATION: {cv:.3f}%")
    
    if cv < 0.01:
        print("*** SENSOR NEARLY STUCK (< 0.01% variation) ***")
        return "NEARLY_STUCK"
    
    # Check for impossible values based on tag type
    tag_type = "PRESSURE" if "PIA" in tag else "UNKNOWN"
    
    if tag_type == "PRESSURE":
        negative_values = (values < 0).sum()
        zero_values = (values == 0).sum()
        very_high = (values > 1000).sum()
        
        print(f"\nPRESSURE VALIDATION:")
        print(f"Negative values: {negative_values} ({negative_values/len(values)*100:.1f}%)")
        print(f"Zero values: {zero_values} ({zero_values/len(values)*100:.1f}%)")
        print(f"Very high (>1000): {very_high} ({very_high/len(values)*100:.1f}%)")
        
        if negative_values > len(values) * 0.1:
            print("*** MANY NEGATIVE PRESSURE VALUES (SENSOR ISSUE) ***")
            return "NEGATIVE_PRESSURE"
    
    # Sample recent values
    print(f"\nRECENT VALUES (last 20):")
    recent_values = values.tail(20)
    for i, val in enumerate(recent_values):
        print(f"{val:.6f}", end="  ")
        if (i + 1) % 5 == 0:
            print()
    print()
    
    # Time analysis if available
    if 'time' in tag_df.columns:
        tag_df['time'] = pd.to_datetime(tag_df['time'])
        time_range = tag_df['time'].max() - tag_df['time'].min()
        print(f"\nTIME ANALYSIS:")
        print(f"Date range: {tag_df['time'].min()} to {tag_df['time'].max()}")
        print(f"Total span: {time_range}")
        
        # Check for data gaps
        if len(tag_df) > 1:
            time_diffs = tag_df['time'].diff().dropna()
            avg_interval = time_diffs.mean()
            max_gap = time_diffs.max()
            print(f"Average interval: {avg_interval}")
            print(f"Largest gap: {max_gap}")
    
    return "UNKNOWN_ISSUE"

def investigate_pia308_series():
    """Investigate all three PIA308 pressure indicators"""
    
    tags = [
        'PCFS_K-31-01_31PIA308A_PV',
        'PCFS_K-31-01_31PIA308B_PV', 
        'PCFS_K-31-01_31PIA308C_PV'
    ]
    
    print("INVESTIGATING K-31-01 PIA308 PRESSURE INDICATORS")
    print("=" * 80)
    print("These tags show 100% anomaly detection failure")
    print("=" * 80)
    
    for tag in tags:
        result = investigate_tag('K-31-01', tag)
        print(f"RESULT: {result}")
        print("\n" + "=" * 80 + "\n")

if __name__ == "__main__":
    investigate_pia308_series()