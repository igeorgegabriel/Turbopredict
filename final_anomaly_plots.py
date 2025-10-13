#!/usr/bin/env python3
"""
⛔ DISABLED - Extended Analysis Plotting System
This script has been replaced by anomaly-triggered plotting system.
Use: python turbopredict.py → Option [2] for anomaly detection with automatic plotting.

OLD: Generated SMART_ANALYSIS.png for all tags (noisy, redundant)
NEW: Generates ANOMALY_*.png only for recent (<24h) verified anomalies (focused, actionable)
"""

import sys
print("="*80)
print("[DISABLED] ERROR: This script has been DISABLED")
print("="*80)
print("\nThis extended analysis plotting system has been replaced.")
print("\nPlease use the anomaly-triggered plotting system instead:")
print("  python turbopredict.py")
print("  Select Option [2]: ANALYZE ALL UNITS")
print("\nNew system generates plots only for:")
print("  - Recent anomalies (<24 hours old)")
print("  - CRITICAL and HIGH priority only")
print("  - Verified by detection pipeline")
print("  - Automatic PDF compilation")
print("="*80)
sys.exit(1)

# Old code disabled below
import os_DISABLED
import sys_DISABLED
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import glob

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from pi_monitor.smart_anomaly_detection import smart_anomaly_detection

def create_final_plots():
    """Create plots with proper folder structure"""
    
    print("CREATING FINAL ANOMALY PLOTS WITH ENHANCED FOLDER STRUCTURE")
    print("=" * 80)
    
    # Setup
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (16, 10)
    
    # Create enhanced folder structure
    base_reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
    
    # Analysis date (3 months back to today)
    cutoff_date = datetime.now() - timedelta(days=90)
    analysis_date = f"{cutoff_date.strftime('%Y-%m-%d')}_to_{datetime.now().strftime('%Y-%m-%d')}_Analysis"
    
    # Plotting time
    plotting_time = datetime.now().strftime('%H-%M-%S_PlottingTime')
    
    # Final structure: reports/YYYY-MM-DD_to_YYYY-MM-DD_Analysis/HH-MM-SS_PlottingTime/
    main_output_dir = base_reports_dir / analysis_date / plotting_time
    main_output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Enhanced folder structure:")
    print(f"  Base: {base_reports_dir}")
    print(f"  Analysis Period: {analysis_date}")
    print(f"  Plotting Time: {plotting_time}")
    print(f"  Full Path: {main_output_dir}")
    
    # Process each unit
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    
    print(f"\nAnalyzing data from: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
    print("Detection System: Smart Anomaly Detection (MTD + Isolation Forest + Unit Status)")
    
    overall_results = {}
    
    for unit in units:
        print(f"\n{'='*60}")
        print(f"PROCESSING UNIT: {unit}")
        print(f"{'='*60}")
        
        # Create unit directory
        unit_dir = main_output_dir / unit
        unit_dir.mkdir(exist_ok=True)
        
        # Find parquet files for this unit
        unit_files = glob.glob(f"data/processed/*{unit}*.parquet")
        
        if not unit_files:
            print(f"  [WARNING] No parquet files found for {unit}")
            continue
            
        print(f"  Data Sources: {len(unit_files)} file(s)")
        
        all_unit_data = pd.DataFrame()
        
        # Load all data for this unit
        for file in unit_files:
            try:
                df = pd.read_parquet(file)
                all_unit_data = pd.concat([all_unit_data, df], ignore_index=True)
                print(f"    [OK] Loaded {len(df):,} records from {os.path.basename(file)}")
            except Exception as e:
                print(f"    [FAIL] Error loading {file}: {e}")
                continue
        
        if all_unit_data.empty:
            print(f"  [ERROR] No data loaded for {unit}")
            continue
            
        # Filter to analysis period
        all_unit_data['time'] = pd.to_datetime(all_unit_data['time'])
        recent_data = all_unit_data[all_unit_data['time'] >= cutoff_date].copy()
        
        print(f"  Data Summary:")
        print(f"    Total Records: {len(all_unit_data):,}")
        print(f"    Analysis Period Records: {len(recent_data):,}")
        print(f"    Unique Tags: {recent_data['tag'].nunique()}")
        
        if recent_data.empty:
            print(f"  [ERROR] No data in analysis period for {unit}")
            continue
            
        # Run enhanced anomaly detection
        print(f"  Running Smart Anomaly Detection...")
        try:
            anomaly_results = smart_anomaly_detection(recent_data, unit)
            
            # Extract key information
            unit_status = anomaly_results.get('unit_status', {})
            analysis_performed = anomaly_results.get('anomaly_analysis_performed', True)
            total_anomalies = anomaly_results.get('total_anomalies', 0)
            method = anomaly_results.get('method', 'Unknown')
            
            print(f"    Unit Status: {unit_status.get('status', 'UNKNOWN')}")
            print(f"    Status Message: {unit_status.get('message', 'N/A')}")
            print(f"    Analysis Performed: {analysis_performed}")
            print(f"    Detection Method: {method}")
            print(f"    Total Anomalies: {total_anomalies:,}")
            
            # Store results for overall summary
            overall_results[unit] = {
                'unit_status': unit_status,
                'analysis_performed': analysis_performed,
                'total_anomalies': total_anomalies,
                'method': method,
                'total_records': len(recent_data),
                'unique_tags': recent_data['tag'].nunique()
            }
            
            # Get problematic tags
            by_tag = anomaly_results.get('by_tag', {})
            
            if not by_tag:
                print(f"    [OK] No problematic tags found - unit operating normally!")
                create_enhanced_unit_summary(unit, unit_dir, recent_data, anomaly_results, [], cutoff_date)
                continue
                
            # Sort and select top problematic tags
            sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
            top_tags = sorted_tags[:12]  # Top 12 for comprehensive analysis
            
            print(f"    Problematic Tags Found: {len(by_tag)}")
            print(f"    Creating Plots: {len(top_tags)} (top problematic)")
            
            # Create plots for problematic tags
            for i, (tag, tag_info) in enumerate(top_tags):
                print(f"      [{i+1:2d}/{len(top_tags)}] Plotting: {tag[:60]}...")
                try:
                    create_professional_tag_plot(unit, tag, tag_info, recent_data, unit_dir, cutoff_date)
                    print(f"        [OK] Plot created successfully")
                except Exception as e:
                    print(f"        [FAIL] Plot failed: {e}")
            
            # Create unit summary
            create_enhanced_unit_summary(unit, unit_dir, recent_data, anomaly_results, top_tags, cutoff_date)
            
            # Store for overall results
            overall_results[unit]['problematic_tags'] = len(by_tag)
            overall_results[unit]['plots_created'] = len(top_tags)
            
        except Exception as e:
            print(f"    [FAIL] Enhanced detection failed: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Create comprehensive overview
    create_comprehensive_overview(main_output_dir, overall_results, cutoff_date, analysis_date, plotting_time)
    
    print(f"\n{'='*80}")
    print(f"FINAL ANOMALY ANALYSIS COMPLETED")
    print(f"{'='*80}")
    print(f"Location: {main_output_dir}")
    print(f"Structure: Analysis_Period/Plotting_Time/Units/")
    print(f"Detection: MTD + Isolation Forest + Unit Status Awareness")
    
    return main_output_dir

def create_professional_tag_plot(unit, tag, tag_info, data, unit_dir, cutoff_date):
    """Create professional-grade tag plot"""
    
    # Get tag data
    tag_data = data[data['tag'] == tag].copy()
    if tag_data.empty or len(tag_data) < 10:
        return
        
    tag_data = tag_data.sort_values('time')
    
    # Extract detection information
    count = tag_info.get('count', 0)
    rate = tag_info.get('rate', 0) * 100
    method = tag_info.get('method', 'Unknown')
    confidence = tag_info.get('confidence', 'UNKNOWN')
    mtd_count = tag_info.get('mtd_count', 0)
    iso_count = tag_info.get('isolation_forest_count', 0)
    baseline_tuned = tag_info.get('baseline_tuned', False)
    thresholds = tag_info.get('thresholds', {})
    
    # Create enhanced figure
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 2, height_ratios=[2, 1, 1], hspace=0.3, wspace=0.3)
    
    # Main time series plot (top, spanning both columns)
    ax_main = fig.add_subplot(gs[0, :])
    
    # Plot time series
    ax_main.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1.5, 
                label=f'Values ({len(tag_data):,} points)', zorder=2)
    
    # Add threshold visualization
    if thresholds:
        upper_limit = thresholds.get('upper', tag_data['value'].quantile(0.95))
        lower_limit = thresholds.get('lower', tag_data['value'].quantile(0.05))
        
        # Highlight anomalies
        anomalies = tag_data[
            (tag_data['value'] > upper_limit) | 
            (tag_data['value'] < lower_limit)
        ]
        
        if not anomalies.empty:
            ax_main.scatter(anomalies['time'], anomalies['value'], 
                           c='red', s=50, alpha=0.9, label=f'Detected Anomalies ({len(anomalies)})', 
                           zorder=5, edgecolors='darkred', linewidth=1)
        
        # Threshold visualization
        ax_main.fill_between(tag_data['time'], lower_limit, upper_limit, 
                            alpha=0.15, color='green', label='Normal Operating Range', zorder=1)
        ax_main.axhline(y=upper_limit, color='orange', linestyle='--', alpha=0.8, 
                       linewidth=2, label=f'Upper Threshold ({upper_limit:.3f})')
        ax_main.axhline(y=lower_limit, color='orange', linestyle='--', alpha=0.8, 
                       linewidth=2, label=f'Lower Threshold ({lower_limit:.3f})')
    
    # Enhanced title and labels
    title = f'{unit} - {tag}\nSmart Anomaly Detection Results'
    ax_main.set_title(title, fontweight='bold', fontsize=14, pad=20)
    ax_main.set_ylabel('Value', fontsize=12, fontweight='bold')
    ax_main.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax_main.legend(loc='upper left', fontsize=10, framealpha=0.9)
    
    # Add detection summary box
    detection_summary = f"""DETECTION SUMMARY
Method: {method}
Total Anomalies: {count:,} ({rate:.2f}%)
MTD Detections: {mtd_count}
Isolation Forest: {iso_count}
Confidence: {confidence}
Baseline Tuned: {'Yes' if baseline_tuned else 'No'}
Analysis Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}"""
    
    ax_main.text(0.99, 0.99, detection_summary, transform=ax_main.transAxes, 
                fontsize=9, verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8, edgecolor='blue'))
    
    # Distribution plot (bottom left)
    ax_dist = fig.add_subplot(gs[1, 0])
    n, bins, patches = ax_dist.hist(tag_data['value'], bins=50, alpha=0.7, color='skyblue', 
                                   edgecolor='black', linewidth=0.5)
    ax_dist.axvline(x=tag_data['value'].mean(), color='red', linestyle='-', linewidth=2, 
                   label=f'Mean ({tag_data["value"].mean():.3f})')
    
    if thresholds:
        ax_dist.axvline(x=thresholds.get('upper', 0), color='orange', linestyle='--', 
                       linewidth=2, label='Upper Threshold')
        ax_dist.axvline(x=thresholds.get('lower', 0), color='orange', linestyle='--', 
                       linewidth=2, label='Lower Threshold')
    
    ax_dist.set_title('Value Distribution', fontweight='bold', fontsize=12)
    ax_dist.set_xlabel('Value')
    ax_dist.set_ylabel('Frequency')
    ax_dist.grid(True, alpha=0.3)
    ax_dist.legend(fontsize=8)
    
    # Detection method breakdown (bottom right)
    ax_methods = fig.add_subplot(gs[1, 1])
    
    if mtd_count > 0 or iso_count > 0:
        methods = []
        counts = []
        colors = []
        
        if mtd_count > 0:
            methods.append(f'MTD')
            counts.append(mtd_count)
            colors.append('#2E86AB')  # Blue
            
        if iso_count > 0:
            methods.append(f'Isolation\nForest')
            counts.append(iso_count)
            colors.append('#A23B72')  # Magenta
        
        bars = ax_methods.bar(methods, counts, color=colors, alpha=0.8, edgecolor='black', linewidth=1)
        
        # Add value labels
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax_methods.text(bar.get_x() + bar.get_width()/2., height + max(counts)*0.02,
                           f'{count}', ha='center', va='bottom', fontweight='bold', fontsize=11)
        
        ax_methods.set_title('Detection Method Breakdown', fontweight='bold', fontsize=12)
        ax_methods.set_ylabel('Anomaly Count')
        ax_methods.grid(True, alpha=0.3, axis='y')
        
        # Set y-axis to start from 0
        ax_methods.set_ylim(0, max(counts) * 1.1)
    else:
        ax_methods.text(0.5, 0.5, 'No Anomalies\nDetected', ha='center', va='center', 
                       transform=ax_methods.transAxes, fontsize=14, fontweight='bold',
                       bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
        ax_methods.set_title('Detection Method Breakdown', fontweight='bold', fontsize=12)
        ax_methods.set_xticks([])
        ax_methods.set_yticks([])
    
    # Statistics panel (bottom, spanning both columns)
    ax_stats = fig.add_subplot(gs[2, :])
    ax_stats.axis('off')
    
    # Calculate comprehensive statistics
    stats_data = tag_data['value']
    stats_text = f"""COMPREHENSIVE STATISTICS
Mean: {stats_data.mean():.6f}  |  Median: {stats_data.median():.6f}  |  Std Dev: {stats_data.std():.6f}  |  Variance: {stats_data.var():.6f}
Min: {stats_data.min():.6f}  |  Max: {stats_data.max():.6f}  |  Range: {stats_data.max() - stats_data.min():.6f}  |  IQR: {stats_data.quantile(0.75) - stats_data.quantile(0.25):.6f}
Q1: {stats_data.quantile(0.25):.6f}  |  Q3: {stats_data.quantile(0.75):.6f}  |  Skewness: {stats_data.skew():.3f}  |  Kurtosis: {stats_data.kurtosis():.3f}
Data Points: {len(stats_data):,}  |  Time Span: {(tag_data['time'].max() - tag_data['time'].min()).days} days  |  Anomaly Rate: {rate:.3f}%"""
    
    ax_stats.text(0.5, 0.5, stats_text, ha='center', va='center', transform=ax_stats.transAxes,
                 fontsize=10, fontfamily='monospace',
                 bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.8, edgecolor='brown'))
    
    # Format x-axis for main plot
    ax_main.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save with enhanced filename
    safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_')[:50]
    filename = f'{safe_tag_name}_SMART_ANALYSIS.png'
    plt.savefig(unit_dir / filename, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def create_enhanced_unit_summary(unit, unit_dir, data, anomaly_results, top_tags, cutoff_date):
    """Create comprehensive unit summary"""
    
    summary_file = unit_dir / f"{unit}_ANALYSIS_REPORT.txt"
    
    with open(summary_file, 'w') as f:
        f.write(f"{'='*80}\n")
        f.write(f"SMART ANOMALY DETECTION ANALYSIS REPORT\n")
        f.write(f"{'='*80}\n")
        f.write(f"Unit: {unit}\n")
        f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')} (90 days)\n")
        f.write(f"Detection System: Smart Anomaly Detection (MTD + Isolation Forest)\n")
        f.write(f"{'='*80}\n\n")
        
        # Unit operational status
        unit_status = anomaly_results.get('unit_status', {})
        f.write(f"UNIT OPERATIONAL STATUS:\n")
        f.write(f"-" * 40 + "\n")
        f.write(f"Status: {unit_status.get('status', 'UNKNOWN')}\n")
        f.write(f"Message: {unit_status.get('message', 'N/A')}\n")
        f.write(f"Analysis Performed: {anomaly_results.get('anomaly_analysis_performed', True)}\n")
        f.write(f"Analysis Period (Hours): {unit_status.get('analysis_period_hours', 'N/A')}\n\n")
        
        # Speed sensor information (if available)
        speed_sensors = unit_status.get('speed_sensors', {})
        if speed_sensors:
            f.write(f"SPEED SENSOR ANALYSIS:\n")
            f.write(f"-" * 40 + "\n")
            for sensor, info in speed_sensors.items():
                f.write(f"  {sensor}:\n")
                f.write(f"    Status: {info.get('status', 'UNKNOWN')}\n")
                f.write(f"    Mean Speed: {info.get('mean_speed', 0):.2f} RPM\n")
                f.write(f"    Recent Speed: {info.get('recent_speed', 0):.2f} RPM\n")
                f.write(f"    Zero Readings: {info.get('zero_percentage', 0):.1f}%\n")
            f.write("\n")
        
        # Overall anomaly results
        f.write(f"ANOMALY DETECTION RESULTS:\n")
        f.write(f"-" * 40 + "\n")
        f.write(f"Detection Method: {anomaly_results.get('method', 'Unknown')}\n")
        f.write(f"Total Anomalies: {anomaly_results.get('total_anomalies', 0):,}\n")
        f.write(f"Overall Anomaly Rate: {anomaly_results.get('anomaly_rate', 0)*100:.3f}%\n")
        f.write(f"Total Data Points: {len(data):,}\n")
        f.write(f"Unique Tags: {data['tag'].nunique():,}\n")
        f.write(f"Problematic Tags: {len(anomaly_results.get('by_tag', {})):,}\n")
        f.write(f"Unit Status Considered: {anomaly_results.get('unit_status_considered', False)}\n")
        f.write(f"Operating Mode: {anomaly_results.get('unit_operating_mode', 'UNKNOWN')}\n\n")
        
        # Detailed problematic tags analysis
        if top_tags:
            f.write(f"DETAILED PROBLEMATIC TAGS ANALYSIS:\n")
            f.write(f"-" * 80 + "\n")
            f.write(f"{'Rank':<4} {'Tag':<50} {'Total':<8} {'MTD':<6} {'IF':<6} {'Rate%':<8} {'Confidence':<12} {'Method':<20}\n")
            f.write("-" * 120 + "\n")
            
            for i, (tag, tag_info) in enumerate(top_tags, 1):
                total = tag_info.get('count', 0)
                mtd = tag_info.get('mtd_count', 0)
                iso = tag_info.get('isolation_forest_count', 0)
                rate = tag_info.get('rate', 0) * 100
                confidence = tag_info.get('confidence', 'UNKNOWN')
                method = tag_info.get('method', 'Unknown')
                
                # Truncate long tag names for display
                display_tag = tag[:50] if len(tag) <= 50 else tag[:47] + "..."
                
                f.write(f"{i:<4} {display_tag:<50} {total:<8} {mtd:<6} {iso:<6} {rate:<8.2f} {confidence:<12} {method:<20}\n")
                
                # Add threshold information if available
                thresholds = tag_info.get('thresholds', {})
                if thresholds:
                    f.write(f"     Thresholds: Lower={thresholds.get('lower', 'N/A'):.3f}, Upper={thresholds.get('upper', 'N/A'):.3f}\n")
                
                # Add baseline information
                baseline_tuned = tag_info.get('baseline_tuned', False)
                f.write(f"     Baseline Tuned: {'Yes' if baseline_tuned else 'No'}\n")
                f.write("\n")
        else:
            f.write(f"EXCELLENT NEWS: NO PROBLEMATIC TAGS DETECTED!\n")
            f.write(f"-" * 50 + "\n")
            f.write(f"All tags for unit {unit} are operating within normal parameters.\n")
            f.write(f"The Smart Anomaly Detection system found no anomalies requiring attention.\n\n")
        
        # Recommendations
        f.write(f"MAINTENANCE RECOMMENDATIONS:\n")
        f.write(f"-" * 40 + "\n")
        if top_tags:
            critical_tags = [tag for tag, info in top_tags if info.get('rate', 0) * 100 >= 10]
            high_tags = [tag for tag, info in top_tags if 5 <= info.get('rate', 0) * 100 < 10]
            
            if critical_tags:
                f.write(f"CRITICAL PRIORITY (≥10% anomaly rate): {len(critical_tags)} tags\n")
                f.write(f"  → Immediate inspection and maintenance required\n")
                
            if high_tags:
                f.write(f"HIGH PRIORITY (5-10% anomaly rate): {len(high_tags)} tags\n")
                f.write(f"  → Schedule maintenance within next planned outage\n")
                
            f.write(f"MONITORING: Continue monitoring all {len(top_tags)} problematic tags\n")
        else:
            f.write(f"[OK] No immediate maintenance required\n")
            f.write(f"[OK] Continue routine monitoring schedule\n")
            f.write(f"[OK] Unit {unit} is operating optimally\n")
        
        f.write(f"\n{'='*80}\n")
        f.write(f"END OF ANALYSIS REPORT\n")
        f.write(f"{'='*80}\n")

def create_comprehensive_overview(output_dir, results, cutoff_date, analysis_date, plotting_time):
    """Create comprehensive analysis overview"""
    
    overview_file = output_dir / "COMPREHENSIVE_ANALYSIS_OVERVIEW.txt"
    
    with open(overview_file, 'w') as f:
        f.write(f"{'='*100}\n")
        f.write(f"COMPREHENSIVE SMART ANOMALY DETECTION OVERVIEW\n")
        f.write(f"{'='*100}\n")
        f.write(f"Analysis Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Analysis Period: {analysis_date}\n")
        f.write(f"Plotting Time: {plotting_time}\n")
        f.write(f"Detection System: Smart Anomaly Detection (MTD + Isolation Forest + Unit Status)\n")
        f.write(f"Data Period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')} (90 days)\n")
        f.write(f"{'='*100}\n\n")
        
        # Overall summary
        total_records = sum(r.get('total_records', 0) for r in results.values())
        total_anomalies = sum(r.get('total_anomalies', 0) for r in results.values())
        total_tags = sum(r.get('unique_tags', 0) for r in results.values())
        total_problematic = sum(r.get('problematic_tags', 0) for r in results.values())
        
        f.write(f"FLEET OVERVIEW:\n")
        f.write(f"-" * 50 + "\n")
        f.write(f"Units Analyzed: {len(results)}\n")
        f.write(f"Total Data Points: {total_records:,}\n")
        f.write(f"Total Anomalies: {total_anomalies:,}\n")
        f.write(f"Overall Anomaly Rate: {(total_anomalies/total_records*100):.3f}%\n") if total_records > 0 else f.write("Overall Anomaly Rate: 0.000%\n")
        f.write(f"Total Tags Monitored: {total_tags:,}\n")
        f.write(f"Problematic Tags: {total_problematic:,}\n\n")
        
        # Unit-by-unit summary
        f.write(f"UNIT-BY-UNIT SUMMARY:\n")
        f.write(f"-" * 80 + "\n")
        f.write(f"{'Unit':<10} {'Status':<15} {'Records':<12} {'Anomalies':<12} {'Rate%':<8} {'Tags':<6} {'Issues':<8} {'Plots':<6}\n")
        f.write("-" * 80 + "\n")
        
        for unit, data in results.items():
            status = data.get('unit_status', {}).get('status', 'UNKNOWN')
            records = data.get('total_records', 0)
            anomalies = data.get('total_anomalies', 0)
            rate = (anomalies/records*100) if records > 0 else 0
            tags = data.get('unique_tags', 0)
            issues = data.get('problematic_tags', 0)
            plots = data.get('plots_created', 0)
            
            f.write(f"{unit:<10} {status:<15} {records:<12,} {anomalies:<12,} {rate:<8.2f} {tags:<6} {issues:<8} {plots:<6}\n")
        
        f.write("\n")
        
        # Detailed unit analysis
        f.write(f"DETAILED UNIT ANALYSIS:\n")
        f.write(f"-" * 50 + "\n")
        
        for unit, data in results.items():
            f.write(f"\n{unit}:\n")
            f.write(f"  Status: {data.get('unit_status', {}).get('status', 'UNKNOWN')}\n")
            f.write(f"  Message: {data.get('unit_status', {}).get('message', 'N/A')}\n")
            f.write(f"  Analysis Method: {data.get('method', 'Unknown')}\n")
            f.write(f"  Analysis Performed: {data.get('analysis_performed', True)}\n")
            f.write(f"  Data Records: {data.get('total_records', 0):,}\n")
            f.write(f"  Anomalies Found: {data.get('total_anomalies', 0):,}\n")
            f.write(f"  Anomaly Rate: {(data.get('total_anomalies', 0)/data.get('total_records', 1)*100):.3f}%\n")
            f.write(f"  Problematic Tags: {data.get('problematic_tags', 0)}\n")
            f.write(f"  Plots Created: {data.get('plots_created', 0)}\n")
        
        f.write(f"\n{'='*100}\n")
        f.write(f"FOLDER STRUCTURE:\n")
        f.write(f"{'='*100}\n")
        f.write(f"reports/\n")
        f.write(f"  └── {analysis_date}/\n")
        f.write(f"      └── {plotting_time}/\n")
        
        for unit in results.keys():
            unit_dir = output_dir / unit
            if unit_dir.exists():
                plot_files = list(unit_dir.glob("*_SMART_ANALYSIS.png"))
                summary_files = list(unit_dir.glob("*_ANALYSIS_REPORT.txt"))
                f.write(f"          ├── {unit}/\n")
                f.write(f"          │   ├── {len(plot_files)} Smart Analysis Plots\n")
                f.write(f"          │   └── {len(summary_files)} Analysis Report\n")
        
        f.write(f"          └── COMPREHENSIVE_ANALYSIS_OVERVIEW.txt (this file)\n\n")
        
        f.write(f"This analysis uses the SAME detection algorithms as CLI option [2]:\n")
        f.write(f"[OK] MTD (Mahalanobis-Taguchi Distance) Detection\n")
        f.write(f"[OK] Isolation Forest Machine Learning Detection\n")
        f.write(f"[OK] Unit Status Awareness (Speed Sensor Monitoring)\n")
        f.write(f"[OK] Baseline Calibration (where configured)\n")
        f.write(f"[OK] Process-Aware Engineering Limits\n\n")
        
        f.write(f"{'='*100}\n")
        f.write(f"END OF COMPREHENSIVE OVERVIEW\n")
        f.write(f"{'='*100}\n")

if __name__ == "__main__":
    create_final_plots()