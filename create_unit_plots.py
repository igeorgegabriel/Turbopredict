#!/usr/bin/env python3
"""
Create anomaly plots organized by unit folders
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob

def create_unit_organized_plots():
    """Create plots organized by unit"""
    
    print("CREATING UNIT-ORGANIZED ANOMALY PLOTS")
    print("=" * 60)
    
    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (14, 8)
    
    # Create main output directory
    reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    main_output_dir = reports_dir / f"unit_anomaly_plots_{timestamp}"
    main_output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Main output directory: {main_output_dir}")
    
    # Process each unit
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    cutoff_date = datetime.now() - timedelta(days=90)
    
    print(f"Analyzing data from: {cutoff_date.strftime('%Y-%m-%d')} onwards")
    
    for unit in units:
        print(f"\nProcessing Unit: {unit}")
        
        # Create unit directory
        unit_dir = main_output_dir / unit
        unit_dir.mkdir(exist_ok=True)
        
        # Find parquet files for this unit
        unit_files = glob.glob(f"data/processed/*{unit}*.parquet")
        
        if not unit_files:
            print(f"  No parquet files found for {unit}")
            continue
            
        print(f"  Found {len(unit_files)} file(s)")
        
        all_unit_data = pd.DataFrame()
        
        # Load all data for this unit
        for file in unit_files:
            try:
                df = pd.read_parquet(file)
                all_unit_data = pd.concat([all_unit_data, df], ignore_index=True)
                print(f"    Loaded {len(df):,} records from {os.path.basename(file)}")
            except Exception as e:
                print(f"    Error loading {file}: {e}")
                continue
        
        if all_unit_data.empty:
            print(f"  No data loaded for {unit}")
            continue
            
        # Filter to last 3 months
        all_unit_data['time'] = pd.to_datetime(all_unit_data['time'])
        recent_data = all_unit_data[all_unit_data['time'] >= cutoff_date].copy()
        
        print(f"  Total records: {len(all_unit_data):,}")
        print(f"  Recent records (3 months): {len(recent_data):,}")
        
        if recent_data.empty:
            print(f"  No recent data for {unit}")
            continue
            
        # Get unique tags (limit to top 10 for manageable output)
        unique_tags = recent_data['tag'].unique()
        
        # Prioritize problematic tags by finding those with more variation
        tag_stats = []
        for tag in unique_tags:
            tag_data = recent_data[recent_data['tag'] == tag]['value']
            if len(tag_data) > 100:  # Only tags with sufficient data
                cv = tag_data.std() / tag_data.mean() if tag_data.mean() != 0 else 0
                tag_stats.append((tag, cv, len(tag_data)))
        
        # Sort by coefficient of variation (more variable = more interesting)
        tag_stats.sort(key=lambda x: x[1], reverse=True)
        selected_tags = [tag for tag, _, _ in tag_stats[:8]]  # Top 8 most variable
        
        print(f"  Selected {len(selected_tags)} most variable tags for plotting")
        
        # Create individual plots
        for i, tag in enumerate(selected_tags):
            print(f"    Plotting {i+1}/{len(selected_tags)}: {tag}")
            
            tag_data = recent_data[recent_data['tag'] == tag].copy()
            if len(tag_data) < 50:
                continue
                
            tag_data = tag_data.sort_values('time')
            
            # Enhanced anomaly detection using IQR
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
            
            # Create enhanced plot
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
            
            # Top plot: Time series with anomalies
            ax1.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.6, linewidth=1, label='Values')
            
            # Highlight anomalies
            if not anomalies.empty:
                ax1.scatter(anomalies['time'], anomalies['value'], 
                           c='red', s=40, alpha=0.9, label=f'Anomalies ({len(anomalies)})', 
                           zorder=5, edgecolors='darkred')
            
            # Add threshold lines and fill normal range
            ax1.fill_between(tag_data['time'], lower_bound, upper_bound, 
                           alpha=0.1, color='green', label='Normal Range')
            ax1.axhline(y=upper_bound, color='orange', linestyle='--', alpha=0.8, label='Upper Threshold')
            ax1.axhline(y=lower_bound, color='orange', linestyle='--', alpha=0.8, label='Lower Threshold')
            
            # Add statistics text
            stats_text = f"""Statistics (Last 3 Months):
Mean: {tag_data['value'].mean():.2f}
Std Dev: {tag_data['value'].std():.2f}
Min: {tag_data['value'].min():.2f}
Max: {tag_data['value'].max():.2f}
Data Points: {len(tag_data):,}
Anomalies: {len(anomalies)} ({len(anomalies)/len(tag_data)*100:.2f}%)"""
            
            ax1.text(0.02, 0.98, stats_text, transform=ax1.transAxes, fontsize=9, 
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(f'{unit} - {tag}\nTime Series with Anomaly Detection', fontweight='bold', fontsize=12)
            ax1.set_ylabel('Value')
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper right')
            
            # Bottom plot: Distribution histogram
            ax2.hist(tag_data['value'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
            ax2.axvline(x=tag_data['value'].mean(), color='red', linestyle='-', linewidth=2, label='Mean')
            ax2.axvline(x=upper_bound, color='orange', linestyle='--', linewidth=2, label='Upper Threshold')
            ax2.axvline(x=lower_bound, color='orange', linestyle='--', linewidth=2, label='Lower Threshold')
            
            ax2.set_title('Value Distribution', fontweight='bold')
            ax2.set_xlabel('Value')
            ax2.set_ylabel('Frequency')
            ax2.grid(True, alpha=0.3)
            ax2.legend()
            
            plt.tight_layout()
            
            # Save with safe filename
            safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_')[:60]
            plt.savefig(unit_dir / f'{safe_tag_name}.png', dpi=200, bbox_inches='tight')
            plt.close()
        
        # Create unit summary file
        summary_file = unit_dir / f"{unit}_summary.txt"
        with open(summary_file, 'w') as f:
            f.write(f"Unit: {unit}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Analysis Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write(f"Total Data Points: {len(all_unit_data):,}\n")
            f.write(f"Recent Data Points (3 months): {len(recent_data):,}\n")
            f.write(f"Total Tags: {len(unique_tags)}\n")
            f.write(f"Tags Plotted: {len(selected_tags)}\n")
            f.write(f"Detection Method: IQR (Interquartile Range)\n")
            f.write(f"Anomaly Threshold: Values beyond Q1-1.5*IQR or Q3+1.5*IQR\n\n")
            
            f.write("Plotted Tags:\n")
            for i, tag in enumerate(selected_tags, 1):
                tag_data = recent_data[recent_data['tag'] == tag]
                Q1 = tag_data['value'].quantile(0.25)
                Q3 = tag_data['value'].quantile(0.75)
                IQR = Q3 - Q1
                anomaly_count = len(tag_data[
                    (tag_data['value'] < Q1 - 1.5 * IQR) | 
                    (tag_data['value'] > Q3 + 1.5 * IQR)
                ])
                f.write(f"{i:2d}. {tag}: {anomaly_count} anomalies ({anomaly_count/len(tag_data)*100:.1f}%)\n")
    
    # Create main summary
    main_summary = main_output_dir / "OVERVIEW.txt"
    with open(main_summary, 'w') as f:
        f.write(f"ANOMALY ANALYSIS OVERVIEW\n")
        f.write(f"=" * 50 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: Last 3 months ({cutoff_date.strftime('%Y-%m-%d')} onwards)\n")
        f.write(f"Units Analyzed: {', '.join(units)}\n")
        f.write(f"Detection Method: IQR-based anomaly detection\n")
        f.write(f"Output Structure: Each unit has its own folder with individual tag plots\n\n")
        
        f.write("Folder Structure:\n")
        for unit in units:
            unit_dir = main_output_dir / unit
            if unit_dir.exists():
                plot_files = list(unit_dir.glob("*.png"))
                f.write(f"  {unit}/: {len(plot_files)} plots\n")
    
    print(f"\nAll plots completed!")
    print(f"Main directory: {main_output_dir}")
    print(f"Overview file: {main_summary}")
    
    return main_output_dir

if __name__ == "__main__":
    create_unit_organized_plots()