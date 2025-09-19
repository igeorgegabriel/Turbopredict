#!/usr/bin/env python3
"""
Enhanced anomaly plotting using MTD + Isolation Forest (same as CLI option [2])
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

# Ensure AE is enabled and required for verification by default (can be overridden by env)
import os as _plot_env
_plot_env.environ.setdefault('ENABLE_AE_LIVE', _plot_env.getenv('ENABLE_AE_LIVE','1'))
_plot_env.environ.setdefault('REQUIRE_AE', _plot_env.getenv('REQUIRE_AE','1'))

from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def create_enhanced_plots():
    """Create plots using the same enhanced detection as CLI option [2]"""
    
    print("CREATING ENHANCED ANOMALY PLOTS (MTD + ISOLATION FOREST)")
    print("=" * 70)
    
    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (16, 10)
    
    # Create output directory
    reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = reports_dir / f"enhanced_anomaly_plots_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output directory: {output_dir}")
    
    # Process each unit
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    cutoff_date = datetime.now() - timedelta(days=90)
    
    print(f"Analyzing data from: {cutoff_date.strftime('%Y-%m-%d')} onwards")
    print("Using: Smart Anomaly Detection (MTD + Isolation Forest + Unit Status Awareness)")
    
    for unit in units:
        print(f"\nProcessing Unit: {unit}")
        
        # Create unit directory
        unit_dir = output_dir / unit
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
            
        # Run enhanced anomaly detection (same as CLI option [2])
        print(f"  Running enhanced anomaly detection...")
        try:
            anomaly_results = smart_anomaly_detection(recent_data, unit)
            
            unit_status = anomaly_results.get('unit_status', {})
            analysis_performed = anomaly_results.get('anomaly_analysis_performed', True)
            
            print(f"    Unit Status: {unit_status.get('status', 'UNKNOWN')}")
            print(f"    Analysis Performed: {analysis_performed}")
            print(f"    Total Anomalies: {anomaly_results.get('total_anomalies', 0):,}")
            print(f"    Detection Method: {anomaly_results.get('method', 'Unknown')}")
            # Expose detailed times to tag-plot function via a module-level hook
            try:
                globals()['_last_anomaly_details'] = anomaly_results.get('details', {})
            except Exception:
                globals()['_last_anomaly_details'] = {}
            
            # Get problematic tags from enhanced detection
            by_tag = anomaly_results.get('by_tag', {})
            
            if not by_tag:
                print(f"    No anomalous tags found for {unit}")
                create_unit_summary(unit, unit_dir, recent_data, anomaly_results, [])
                continue
                
            # Sort tags by anomaly count
            sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
            top_tags = sorted_tags[:10]  # Top 10 problematic tags
            
            print(f"    Found {len(by_tag)} problematic tags, plotting top {len(top_tags)}")
            
            # Create plots for problematic tags
            for i, (tag, tag_info) in enumerate(top_tags):
                print(f"      Plotting {i+1}/{len(top_tags)}: {tag}")
                create_enhanced_tag_plot(unit, tag, tag_info, recent_data, unit_dir)
            
            # Create unit summary
            create_unit_summary(unit, unit_dir, recent_data, anomaly_results, top_tags)
            
        except Exception as e:
            print(f"    Error in enhanced detection: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Create main overview
    create_main_overview(output_dir, units)
    
    print(f"\nEnhanced plots completed!")
    print(f"Location: {output_dir}")
    
    return output_dir

def create_enhanced_tag_plot(unit, tag, tag_info, data, unit_dir):
    """Create enhanced plot for a specific tag using detection results"""
    
    # Get tag data
    tag_data = data[data['tag'] == tag].copy()
    if tag_data.empty or len(tag_data) < 10:
        return
        
    tag_data = tag_data.sort_values('time')
    
    # Get detection information
    count = tag_info.get('count', 0)
    rate = tag_info.get('rate', 0) * 100
    method = tag_info.get('method', 'Unknown')
    confidence = tag_info.get('confidence', 'UNKNOWN')
    
    # Extract MTD and Isolation Forest counts
    mtd_count = tag_info.get('mtd_count', 0)
    iso_count = tag_info.get('isolation_forest_count', 0)
    
    # Get thresholds if available
    thresholds = tag_info.get('thresholds', {})
    
    # Create figure with subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12))
    
    # Plot 1: Time series with enhanced anomaly detection
    ax1.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1.5, label='Values')

    # Compute stable region stats and 2.5-sigma bands, plus stabilized series
    try:
        vals = tag_data['value'].astype(float).values
        mean0 = float(np.nanmean(vals))
        std0 = float(np.nanstd(vals, ddof=0))
        if not np.isfinite(std0) or std0 <= 1e-12:
            std0 = float(np.nanstd(vals, ddof=1)) or 1.0
        # First pass: exclude high spikes by 2.5-sigma
        z0 = (vals - mean0) / (std0 if std0 else 1.0)
        stable_mask0 = np.isfinite(z0) & (np.abs(z0) <= 2.5)
        stable_vals = vals[stable_mask0]
        mean_stable = float(np.nanmean(stable_vals)) if stable_vals.size > 0 else mean0
        std_stable = float(np.nanstd(stable_vals, ddof=0)) if stable_vals.size > 1 else std0
        if not np.isfinite(std_stable) or std_stable <= 1e-12:
            std_stable = std0 if std0 > 0 else 1.0

        up_25 = mean_stable + 2.5 * std_stable
        lo_25 = mean_stable - 2.5 * std_stable

        # Add 2.5-sigma lines and stable mean
        ax1.axhline(y=up_25, color='purple', linestyle='--', alpha=0.9, label='+2.5Ïƒ (stable)')
        ax1.axhline(y=lo_25, color='purple', linestyle='--', alpha=0.9, label='-2.5Ïƒ (stable)')
        ax1.axhline(y=mean_stable, color='gray', linestyle=':', alpha=0.8, label='Mean (stable)')

        # Build stabilized series: replace spikes (>2.5Ïƒ vs stable) with interpolated/ffilled
        z1 = (vals - mean_stable) / (std_stable if std_stable else 1.0)
        spike_mask = np.abs(z1) > 2.5
        s = pd.Series(vals, index=pd.to_datetime(tag_data['time']))
        s.loc[spike_mask] = np.nan
        s_stable = s.interpolate(method='time', limit_direction='both').ffill()
        ax1.plot(tag_data['time'], s_stable.values, color='green', linewidth=1.5, alpha=0.9, label='Stable (interpolated)')
    except Exception:
        mean_stable = float(tag_data['value'].mean())
        std_stable = float(tag_data['value'].std())

    # Overlay markers from detection details (2.5Ïƒ candidates, AE, MTD, IF)
    try:
        details = globals().get('_last_anomaly_details') or {}
        sigma_by_tag = details.get('sigma_2p5_times_by_tag', {}) if isinstance(details, dict) else {}
        verify_by_tag = details.get('verification_times_by_tag', {}) if isinstance(details, dict) else {}
        ae_times = details.get('ae_times', []) if isinstance(details, dict) else []

        # Helper to mask times present in this tag's series
        def _mask_times(ts_list):
            if not ts_list:
                return np.zeros(len(tag_data), dtype=bool)
            tset = set(pd.to_datetime(ts_list))
            series_t = pd.to_datetime(tag_data['time'])
            if getattr(series_t.dt, 'tz', None) is not None:
                series_t = series_t.dt.tz_convert(None)
            return series_t.isin(tset).values

        # 2.5-sigma candidate points
        sigma_times = sigma_by_tag.get(tag, []) or []
        ms = _mask_times(sigma_times)
        if ms.any():
            ax1.scatter(tag_data['time'][ms], tag_data['value'][ms], facecolors='none', edgecolors='purple', s=40, label='2.5Ïƒ candidate', zorder=6)

        # MTD and IF confirmations
        vdetail = verify_by_tag.get(tag, {}) or {}
        mtd_times = vdetail.get('mtd', []) or []
        if_times = vdetail.get('if', []) or []
        mm = _mask_times(mtd_times)
        if mm.any():
            ax1.scatter(tag_data['time'][mm], tag_data['value'][mm], c='red', marker='x', s=50, label='MTD confirmed', zorder=7)
        im = _mask_times(if_times)
        if im.any():
            ax1.scatter(tag_data['time'][im], tag_data['value'][im], c='orange', marker='s', s=30, label='IF confirmed', zorder=7)

        # AE times are global; draw faint vertical lines
        for t in pd.to_datetime(ae_times):
            ax1.axvline(t, color='gray', linestyle='--', alpha=0.15)
    except Exception:
        pass
    
    # Add threshold lines if available
    if thresholds:
        upper_limit = thresholds.get('upper', tag_data['value'].quantile(0.95))
        lower_limit = thresholds.get('lower', tag_data['value'].quantile(0.05))
        
        # Highlight anomalies based on thresholds
        anomalies = tag_data[
            (tag_data['value'] > upper_limit) | 
            (tag_data['value'] < lower_limit)
        ]
        
        if not anomalies.empty:
            ax1.scatter(anomalies['time'], anomalies['value'], 
                       c='red', s=40, alpha=0.9, label=f'Threshold Anomalies ({len(anomalies)})', 
                       zorder=5, edgecolors='darkred')
        
        # Add threshold bands
        ax1.fill_between(tag_data['time'], lower_limit, upper_limit, 
                        alpha=0.1, color='green', label='Normal Range (MTD)')
        ax1.axhline(y=upper_limit, color='orange', linestyle='--', alpha=0.8, label='Upper Threshold')
        ax1.axhline(y=lower_limit, color='orange', linestyle='--', alpha=0.8, label='Lower Threshold')
    
    # Add enhanced detection info
    detection_text = f"""Enhanced Detection Results:
Method: {method}
Total Anomalies: {count:,} ({rate:.2f}%)
MTD Count: {mtd_count}
Isolation Forest Count: {iso_count}
Confidence: {confidence}
Baseline Tuned: {tag_info.get('baseline_tuned', False)}"""
    
    ax1.text(0.02, 0.98, detection_text, transform=ax1.transAxes, fontsize=9, 
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    ax1.set_title(f'{unit} - {tag}\nEnhanced Anomaly Detection (MTD + Isolation Forest)', 
                 fontweight='bold', fontsize=12)
    ax1.set_ylabel('Value')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right', fontsize=8)
    
    # Plot 2: Distribution with detection methods
    ax2.hist(tag_data['value'], bins=50, alpha=0.7, color='skyblue', edgecolor='black', label='Distribution')
    ax2.axvline(x=tag_data['value'].mean(), color='red', linestyle='-', linewidth=2, label='Mean')

    # Add 2.5-sigma lines from stable region
    try:
        ax2.axvline(x=mean_stable + 2.5 * std_stable, color='purple', linestyle='--', linewidth=2, label='+2.5Ïƒ (stable)')
        ax2.axvline(x=mean_stable - 2.5 * std_stable, color='purple', linestyle='--', linewidth=2, label='-2.5Ïƒ (stable)')
    except Exception:
        pass
    
    if thresholds:
        ax2.axvline(x=thresholds.get('upper', 0), color='orange', linestyle='--', linewidth=2, label='Upper Threshold')
        ax2.axvline(x=thresholds.get('lower', 0), color='orange', linestyle='--', linewidth=2, label='Lower Threshold')
    
    # Add statistics
    stats_text = f"""Statistics (Last 3 Months):
Mean: {tag_data['value'].mean():.3f}
Std Dev: {tag_data['value'].std():.3f}
Stable Mean: {mean_stable:.3f}
Stable Std: {std_stable:.3f}
Min: {tag_data['value'].min():.3f}
Max: {tag_data['value'].max():.3f}
Data Points: {len(tag_data):,}"""
    
    ax2.text(0.75, 0.95, stats_text, transform=ax2.transAxes, fontsize=9, 
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    ax2.set_title('Value Distribution Analysis', fontweight='bold')
    ax2.set_xlabel('Value')
    ax2.set_ylabel('Frequency')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    # Plot 3: Detection method breakdown
    if mtd_count > 0 or iso_count > 0:
        methods = []
        counts = []
        colors = []
        
        if mtd_count > 0:
            methods.append(f'MTD\n({mtd_count})')
            counts.append(mtd_count)
            colors.append('blue')
            
        if iso_count > 0:
            methods.append(f'Isolation Forest\n({iso_count})')
            counts.append(iso_count)
            colors.append('green')
        
        bars = ax3.bar(methods, counts, color=colors, alpha=0.7)
        ax3.set_title('Detection Method Breakdown', fontweight='bold')
        ax3.set_ylabel('Anomaly Count')
        ax3.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, count in zip(bars, counts):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.01,
                    str(count), ha='center', va='bottom', fontweight='bold')
    else:
        ax3.text(0.5, 0.5, 'No anomalies detected by enhanced methods', 
                ha='center', va='center', transform=ax3.transAxes, fontsize=12)
        ax3.set_title('Detection Method Breakdown', fontweight='bold')
    
    plt.tight_layout()
    
    # Save with enhanced filename
    safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_')[:50]
    filename = f'{safe_tag_name}_enhanced.png'
    plt.savefig(unit_dir / filename, dpi=200, bbox_inches='tight')
    plt.close()

def create_unit_summary(unit, unit_dir, data, anomaly_results, top_tags):
    """Create enhanced unit summary"""
    
    summary_file = unit_dir / f"{unit}_enhanced_summary.txt"
    
    with open(summary_file, 'w') as f:
        f.write(f"ENHANCED ANOMALY ANALYSIS SUMMARY\n")
        f.write(f"=" * 50 + "\n")
        f.write(f"Unit: {unit}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: Last 3 months\n")
        f.write(f"Detection System: Smart Anomaly Detection (MTD + Isolation Forest)\n\n")
        
        # Unit status
        unit_status = anomaly_results.get('unit_status', {})
        f.write(f"UNIT STATUS:\n")
        f.write(f"Status: {unit_status.get('status', 'UNKNOWN')}\n")
        f.write(f"Message: {unit_status.get('message', 'N/A')}\n")
        f.write(f"Analysis Performed: {anomaly_results.get('anomaly_analysis_performed', True)}\n\n")
        
        # Overall results
        f.write(f"OVERALL RESULTS:\n")
        f.write(f"Detection Method: {anomaly_results.get('method', 'Unknown')}\n")
        f.write(f"Total Anomalies: {anomaly_results.get('total_anomalies', 0):,}\n")
        f.write(f"Anomaly Rate: {anomaly_results.get('anomaly_rate', 0)*100:.2f}%\n")
        f.write(f"Total Data Points: {len(data):,}\n")
        f.write(f"Unique Tags: {data['tag'].nunique()}\n")
        f.write(f"Problematic Tags: {len(anomaly_results.get('by_tag', {}))}\n\n")
        
        # Top problematic tags
        if top_tags:
            f.write(f"TOP PROBLEMATIC TAGS:\n")
            f.write(f"{'Rank':<4} {'Tag':<50} {'Total':<8} {'MTD':<6} {'IF':<6} {'Rate%':<8} {'Method':<25}\n")
            f.write("-" * 110 + "\n")
            
            for i, (tag, tag_info) in enumerate(top_tags, 1):
                total = tag_info.get('count', 0)
                mtd = tag_info.get('mtd_count', 0)
                iso = tag_info.get('isolation_forest_count', 0)
                rate = tag_info.get('rate', 0) * 100
                method = tag_info.get('method', 'Unknown')
                
                # Truncate long tag names
                display_tag = tag[:50] if len(tag) <= 50 else tag[:47] + "..."
                
                f.write(f"{i:<4} {display_tag:<50} {total:<8} {mtd:<6} {iso:<6} {rate:<8.2f} {method:<25}\n")
        else:
            f.write("No problematic tags found - all systems operating normally!\n")

def create_main_overview(output_dir, units):
    """Create main overview file"""
    
    overview_file = output_dir / "ENHANCED_OVERVIEW.txt"
    
    with open(overview_file, 'w') as f:
        f.write(f"ENHANCED ANOMALY ANALYSIS OVERVIEW\n")
        f.write(f"=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: Last 3 months\n")
        f.write(f"Detection System: Smart Anomaly Detection\n")
        f.write(f"  - MTD (Mahalanobis-Taguchi Distance)\n")
        f.write(f"  - Isolation Forest\n")
        f.write(f"  - Unit Status Awareness\n")
        f.write(f"  - Baseline Calibration (where available)\n\n")
        
        f.write(f"UNITS ANALYZED: {', '.join(units)}\n\n")
        
        f.write("FOLDER STRUCTURE:\n")
        for unit in units:
            unit_dir = output_dir / unit
            if unit_dir.exists():
                plot_files = list(unit_dir.glob("*_enhanced.png"))
                summary_files = list(unit_dir.glob("*_enhanced_summary.txt"))
                f.write(f"  {unit}/: {len(plot_files)} enhanced plots, {len(summary_files)} summary\n")
        
        f.write(f"\nThis analysis uses the SAME detection methods as CLI option [2]\n")
        f.write(f"All plots show MTD + Isolation Forest results with detection method breakdown.\n")

if __name__ == "__main__":
    create_enhanced_plots()



