#!/usr/bin/env python3
"""
Simple anomaly plotting for latest 3 months
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob

def create_simple_plots():
    """Create simple anomaly plots"""
    
    print("CREATING SIMPLE ANOMALY PLOTS")
    print("=" * 50)
    
    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (12, 6)
    
    # Create output directory
    reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = reports_dir / f"anomaly_plots_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output directory: {output_dir}")
    
    # Find parquet files
    parquet_files = glob.glob("data/processed/*.parquet")
    print(f"Found {len(parquet_files)} parquet files")
    
    if not parquet_files:
        print("No parquet files found!")
        return
    
    # Process first parquet file for demo
    for parquet_file in parquet_files[:1]:  # Just first file for speed
        print(f"\nProcessing: {parquet_file}")
        
        try:
            # Read data
            df = pd.read_parquet(parquet_file)
            print(f"Loaded {len(df):,} records")
            
            if 'time' not in df.columns:
                print("No time column found")
                continue
                
            # Convert time and filter last 3 months
            df['time'] = pd.to_datetime(df['time'])
            cutoff = datetime.now() - timedelta(days=90)
            recent_df = df[df['time'] >= cutoff].copy()
            
            print(f"Recent data (3 months): {len(recent_df):,} records")
            
            if recent_df.empty:
                print("No recent data")
                continue
            
            # Get unique tags (limit to first 4 for speed)
            unique_tags = recent_df['tag'].unique()[:4]
            print(f"Processing {len(unique_tags)} tags")
            
            # Create plots for each tag
            for i, tag in enumerate(unique_tags):
                print(f"  Plotting tag {i+1}/{len(unique_tags)}: {tag}")
                
                tag_data = recent_df[recent_df['tag'] == tag].copy()
                if len(tag_data) < 10:
                    continue
                    
                tag_data = tag_data.sort_values('time')
                
                # Simple anomaly detection using IQR
                Q1 = tag_data['value'].quantile(0.25)
                Q3 = tag_data['value'].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Find anomalies
                anomalies = tag_data[
                    (tag_data['value'] < lower_bound) | 
                    (tag_data['value'] > upper_bound)
                ]
                
                # Create plot
                fig, ax = plt.subplots(figsize=(12, 6))
                
                # Plot normal data
                ax.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1, label='Values')
                
                # Highlight anomalies
                if not anomalies.empty:
                    ax.scatter(anomalies['time'], anomalies['value'], 
                             c='red', s=30, alpha=0.8, label=f'Anomalies ({len(anomalies)})', zorder=5)
                
                # Add threshold lines
                ax.axhline(y=upper_bound, color='orange', linestyle='--', alpha=0.7, label='Upper Threshold')
                ax.axhline(y=lower_bound, color='orange', linestyle='--', alpha=0.7, label='Lower Threshold')
                
                # Formatting
                ax.set_title(f'{tag}\nLast 3 Months - {len(anomalies)} Anomalies ({len(anomalies)/len(tag_data)*100:.1f}%)', 
                           fontweight='bold')
                ax.set_xlabel('Date')
                ax.set_ylabel('Value')
                ax.grid(True, alpha=0.3)
                ax.legend()
                
                # Rotate x-axis labels
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Save plot
                safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_')[:50]
                plt.savefig(output_dir / f'{safe_tag_name}.png', dpi=150, bbox_inches='tight')
                plt.close()
            
            break  # Only process first file
            
        except Exception as e:
            print(f"Error processing {parquet_file}: {e}")
            continue
    
    # Create summary
    summary_file = output_dir / "README.txt"
    with open(summary_file, 'w') as f:
        f.write(f"Anomaly Plots Generated: {datetime.now()}\n")
        f.write(f"Time Period: Last 3 months\n")
        f.write(f"Detection Method: IQR (Interquartile Range)\n")
        f.write(f"Anomaly Threshold: 1.5 * IQR beyond Q1/Q3\n")
    
    print(f"\nPlots saved to: {output_dir}")
    print(f"Summary saved to: {summary_file}")
    
    return output_dir

if __name__ == "__main__":
    create_simple_plots()