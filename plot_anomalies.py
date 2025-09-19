#!/usr/bin/env python3
"""
Plot anomalies from latest 3 months of data organized by unit
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from pi_monitor.tuned_anomaly_detection import enhanced_anomaly_detection
import glob

def setup_plot_style():
    """Setup matplotlib style for better plots"""
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (15, 8)
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.alpha'] = 0.3

def create_anomaly_plots():
    """Create anomaly plots for all units"""
    
    print("CREATING ANOMALY PLOTS FOR LATEST 3 MONTHS")
    print("=" * 60)
    
    # Setup
    setup_plot_style()
    
    # Create reports directory structure
    reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    anomaly_dir = reports_dir / f"anomaly_plots_{timestamp}"
    anomaly_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Creating plots in: {anomaly_dir}")
    
    # Get available units
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']  # Known units
    print(f"Found {len(units)} units: {units}")
    
    # Calculate 3-month cutoff
    cutoff_date = datetime.now() - timedelta(days=90)
    print(f"Analyzing data from: {cutoff_date.strftime('%Y-%m-%d')} onwards")
    
    for unit in units:
        print(f"\nProcessing {unit}...")
        
        try:
            # Get unit data from Parquet files directly
            parquet_files = glob.glob(f"data/processed/*{unit}*.parquet")
            if not parquet_files:
                parquet_files = glob.glob(f"data/processed/*.parquet")
                
            unit_data = pd.DataFrame()
            for file in parquet_files:
                try:
                    df = pd.read_parquet(file)
                    if not df.empty and 'tag' in df.columns:
                        # Filter for this unit if tag contains unit name
                        unit_df = df[df['tag'].str.contains(unit, case=False, na=False)]
                        if not unit_df.empty:
                            unit_data = pd.concat([unit_data, unit_df], ignore_index=True)
                except Exception as e:
                    print(f"    Error reading {file}: {e}")
                    continue
            
            if unit_data.empty:
                print(f"  No data for {unit}")
                continue
                
            # Filter to last 3 months
            unit_data['time'] = pd.to_datetime(unit_data['time'])
            recent_data = unit_data[unit_data['time'] >= cutoff_date].copy()
            
            if recent_data.empty:
                print(f"  No recent data for {unit}")
                continue
                
            print(f"  Data points: {len(recent_data):,} (last 3 months)")
            
            # Run anomaly detection
            anomaly_results = enhanced_anomaly_detection(recent_data, unit)
            by_tag = anomaly_results.get('by_tag', {})
            
            if not by_tag:
                print(f"  No anomalies found for {unit}")
                continue
                
            # Get top 6 problematic tags for plotting
            sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
            top_tags = sorted_tags[:6]
            
            print(f"  Plotting {len(top_tags)} most problematic tags")
            
            # Create plots for this unit
            create_unit_plots(unit, recent_data, top_tags, anomaly_dir)
            
        except Exception as e:
            print(f"  ERROR processing {unit}: {e}")
            continue
    
    print(f"\nPlots created in: {anomaly_dir}")
    return anomaly_dir

def create_unit_plots(unit, data, top_tags, output_dir):
    """Create plots for a specific unit"""
    
    # Create unit-specific directory
    unit_dir = output_dir / unit
    unit_dir.mkdir(exist_ok=True)
    
    # Plot 1: Overview plot with all top tags
    create_overview_plot(unit, data, top_tags, unit_dir)
    
    # Plot 2: Individual plots for each tag
    create_individual_plots(unit, data, top_tags, unit_dir)
    
    # Plot 3: Anomaly summary plot
    create_anomaly_summary_plot(unit, top_tags, unit_dir)

def create_overview_plot(unit, data, top_tags, unit_dir):
    """Create overview plot with multiple tags"""
    
    fig, axes = plt.subplots(3, 2, figsize=(20, 15))
    fig.suptitle(f'{unit} - Top 6 Problematic Tags (Last 3 Months)', fontsize=16, fontweight='bold')
    
    axes = axes.flatten()
    
    for i, (tag, tag_info) in enumerate(top_tags):
        if i >= 6:
            break
            
        ax = axes[i]
        
        # Get tag data
        tag_data = data[data['tag'] == tag].copy()
        if tag_data.empty:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{tag} - No Data')
            continue
            
        # Sort by time
        tag_data = tag_data.sort_values('time')
        
        # Plot time series
        ax.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1, label='Values')

        # Add 2.5-sigma (stable) lines and stabilized overlay
        try:
            vals = tag_data['value'].astype(float).values
            mean0 = float(np.nanmean(vals))
            std0 = float(np.nanstd(vals, ddof=0))
            if not np.isfinite(std0) or std0 <= 1e-12:
                std0 = float(np.nanstd(vals, ddof=1)) or 1.0
            z0 = (vals - mean0) / (std0 if std0 else 1.0)
            stable = np.isfinite(z0) & (np.abs(z0) <= 2.5)
            mean_s = float(np.nanmean(vals[stable])) if stable.any() else mean0
            std_s = float(np.nanstd(vals[stable], ddof=0)) if stable.sum() > 1 else std0
            if not np.isfinite(std_s) or std_s <= 1e-12:
                std_s = std0 if std0 > 0 else 1.0
            up25 = mean_s + 2.5 * std_s
            lo25 = mean_s - 2.5 * std_s
            ax.axhline(y=up25, color='purple', linestyle='--', alpha=0.9, label='+2.5σ (stable)')
            ax.axhline(y=lo25, color='purple', linestyle='--', alpha=0.9, label='-2.5σ (stable)')
            ax.axhline(y=mean_s, color='gray', linestyle=':', alpha=0.8, label='Mean (stable)')
            # Stabilized overlay
            z1 = (vals - mean_s) / (std_s if std_s else 1.0)
            spikes = np.abs(z1) > 2.5
            s = pd.Series(vals, index=pd.to_datetime(tag_data['time']))
            s.loc[spikes] = np.nan
            s_stable = s.interpolate(method='time', limit_direction='both').ffill()
            ax.plot(tag_data['time'], s_stable.values, color='green', linewidth=1.1, alpha=0.9, label='Stable (interpolated)')
        except Exception:
            pass
        
        # Highlight anomalies
        thresholds = tag_info.get('thresholds', {})
        if thresholds:
            upper_limit = thresholds.get('upper', tag_data['value'].quantile(0.95))
            lower_limit = thresholds.get('lower', tag_data['value'].quantile(0.05))
            
            # Mark anomalies
            anomalies = tag_data[(tag_data['value'] > upper_limit) | (tag_data['value'] < lower_limit)]
            if not anomalies.empty:
                ax.scatter(anomalies['time'], anomalies['value'], c='red', s=30, alpha=0.8, label='Anomalies', zorder=5)
                
            # Add threshold lines
            ax.axhline(y=upper_limit, color='orange', linestyle='--', alpha=0.7, label='Upper Limit')
            ax.axhline(y=lower_limit, color='orange', linestyle='--', alpha=0.7, label='Lower Limit')
        
        # Formatting
        ax.set_title(f'{tag}\n{tag_info.get("count", 0)} anomalies ({tag_info.get("rate", 0)*100:.1f}%)')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        
        if i == 0:
            ax.legend(loc='upper right', fontsize=8)
    
    # Hide unused subplots
    for i in range(len(top_tags), 6):
        axes[i].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(unit_dir / f'{unit}_overview.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_individual_plots(unit, data, top_tags, unit_dir):
    """Create individual detailed plots for each tag"""
    
    for tag, tag_info in top_tags:
        tag_data = data[data['tag'] == tag].copy()
        if tag_data.empty:
            continue
            
        tag_data = tag_data.sort_values('time')
        
        fig, ax = plt.subplots(figsize=(15, 8))
        
        # Plot time series
        ax.plot(tag_data['time'], tag_data['value'], 'b-', linewidth=1.5, alpha=0.8, label='Values')

        # Add 2.5-sigma (stable) lines and stabilized overlay
        try:
            vals = tag_data['value'].astype(float).values
            mean0 = float(np.nanmean(vals))
            std0 = float(np.nanstd(vals, ddof=0))
            if not np.isfinite(std0) or std0 <= 1e-12:
                std0 = float(np.nanstd(vals, ddof=1)) or 1.0
            z0 = (vals - mean0) / (std0 if std0 else 1.0)
            stable = np.isfinite(z0) & (np.abs(z0) <= 2.5)
            mean_s = float(np.nanmean(vals[stable])) if stable.any() else mean0
            std_s = float(np.nanstd(vals[stable], ddof=0)) if stable.sum() > 1 else std0
            if not np.isfinite(std_s) or std_s <= 1e-12:
                std_s = std0 if std0 > 0 else 1.0
            up25 = mean_s + 2.5 * std_s
            lo25 = mean_s - 2.5 * std_s
            ax.axhline(y=up25, color='purple', linestyle='--', alpha=0.9, label='+2.5σ (stable)')
            ax.axhline(y=lo25, color='purple', linestyle='--', alpha=0.9, label='-2.5σ (stable)')
            ax.axhline(y=mean_s, color='gray', linestyle=':', alpha=0.8, label='Mean (stable)')
            # Stabilized overlay
            z1 = (vals - mean_s) / (std_s if std_s else 1.0)
            spikes = np.abs(z1) > 2.5
            s = pd.Series(vals, index=pd.to_datetime(tag_data['time']))
            s.loc[spikes] = np.nan
            s_stable = s.interpolate(method='time', limit_direction='both').ffill()
            ax.plot(tag_data['time'], s_stable.values, color='green', linewidth=1.5, alpha=0.9, label='Stable (interpolated)')
        except Exception:
            pass
        
        # Add anomaly detection information
        thresholds = tag_info.get('thresholds', {})
        if thresholds:
            upper_limit = thresholds.get('upper', tag_data['value'].quantile(0.95))
            lower_limit = thresholds.get('lower', tag_data['value'].quantile(0.05))
            
            # Highlight anomalies
            anomalies = tag_data[(tag_data['value'] > upper_limit) | (tag_data['value'] < lower_limit)]
            if not anomalies.empty:
                ax.scatter(anomalies['time'], anomalies['value'], c='red', s=50, alpha=0.9, 
                          label=f'Anomalies ({len(anomalies)})', zorder=5, edgecolors='darkred')
                
            # Add threshold bands
            ax.fill_between(tag_data['time'], lower_limit, upper_limit, alpha=0.1, color='green', label='Normal Range')
            ax.axhline(y=upper_limit, color='orange', linestyle='--', alpha=0.8, label='Upper Limit')
            ax.axhline(y=lower_limit, color='orange', linestyle='--', alpha=0.8, label='Lower Limit')
        
        # Add statistics
        stats_text = f"""Statistics:
Mean: {tag_data['value'].mean():.2f}
Std: {tag_data['value'].std():.2f}
Min: {tag_data['value'].min():.2f}
Max: {tag_data['value'].max():.2f}
Anomalies: {tag_info.get('count', 0)} ({tag_info.get('rate', 0)*100:.2f}%)
Method: {tag_info.get('method', 'Unknown')}"""
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=9, 
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        # Formatting
        ax.set_title(f'{unit} - {tag}\nAnomaly Analysis (Last 3 Months)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Value', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper right')
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        # Save with safe filename
        safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_')
        plt.savefig(unit_dir / f'{unit}_{safe_tag_name}.png', dpi=300, bbox_inches='tight')
        plt.close()

def create_anomaly_summary_plot(unit, top_tags, unit_dir):
    """Create summary bar chart of anomalies"""
    
    if not top_tags:
        return
        
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Extract data
    tag_names = [tag[:25] + '...' if len(tag) > 25 else tag for tag, _ in top_tags]
    counts = [info.get('count', 0) for _, info in top_tags]
    rates = [info.get('rate', 0) * 100 for _, info in top_tags]
    methods = [info.get('method', 'Unknown') for _, info in top_tags]
    
    # Color mapping for methods
    method_colors = {
        'MTD': 'blue',
        'Isolation Forest': 'green', 
        'IF': 'green',
        'baseline_calibrated': 'purple',
        'Unknown': 'gray'
    }
    
    colors = []
    for method in methods:
        if 'MTD' in method and 'IF' in method:
            colors.append('red')  # Both methods
        elif 'MTD' in method:
            colors.append('blue')
        elif 'IF' in method or 'Isolation Forest' in method:
            colors.append('green')
        else:
            colors.append(method_colors.get(method, 'gray'))
    
    # Plot 1: Anomaly counts
    bars1 = ax1.bar(range(len(counts)), counts, color=colors, alpha=0.7)
    ax1.set_title(f'{unit} - Anomaly Counts by Tag', fontweight='bold')
    ax1.set_xlabel('Tags')
    ax1.set_ylabel('Anomaly Count')
    ax1.set_xticks(range(len(tag_names)))
    ax1.set_xticklabels(tag_names, rotation=45, ha='right')
    ax1.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, count in zip(bars1, counts):
        if count > 0:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    str(count), ha='center', va='bottom', fontweight='bold')
    
    # Plot 2: Anomaly rates
    bars2 = ax2.bar(range(len(rates)), rates, color=colors, alpha=0.7)
    ax2.set_title(f'{unit} - Anomaly Rates by Tag', fontweight='bold')
    ax2.set_xlabel('Tags')
    ax2.set_ylabel('Anomaly Rate (%)')
    ax2.set_xticks(range(len(tag_names)))
    ax2.set_xticklabels(tag_names, rotation=45, ha='right')
    ax2.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, rate in zip(bars2, rates):
        if rate > 0:
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    # Add legend
    legend_elements = [
        plt.Rectangle((0,0),1,1, facecolor='red', alpha=0.7, label='MTD + Isolation Forest'),
        plt.Rectangle((0,0),1,1, facecolor='blue', alpha=0.7, label='MTD Only'),
        plt.Rectangle((0,0),1,1, facecolor='green', alpha=0.7, label='Isolation Forest Only'),
        plt.Rectangle((0,0),1,1, facecolor='gray', alpha=0.7, label='Other Methods')
    ]
    ax2.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(unit_dir / f'{unit}_summary.png', dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    output_dir = create_anomaly_plots()
    print(f"\nAll plots saved to: {output_dir}")
