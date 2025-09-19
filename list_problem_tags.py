#!/usr/bin/env python3
"""
List problematic tags that fail anomaly detection
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner

def list_problem_tags(unit='K-31-01'):
    """List all problematic tags for troubleshooting"""
    
    print(f"PROBLEM TAGS - {unit}")
    print("=" * 60)
    
    scanner = ParquetAutoScanner()
    results = scanner.analyze_unit_data(unit, run_anomaly_detection=True)
    
    if 'anomalies' not in results:
        print("No anomaly data available")
        return
        
    anomalies = results['anomalies']
    by_tag = anomalies.get('by_tag', {})
    
    if not by_tag:
        print("No anomalous tags found")
        return
    
    # Sort by anomaly count (highest first)
    sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
    
    print(f"{'RANK':<4} {'TAG':<40} {'ANOMALIES':<10} {'RATE %':<8}")
    print("-" * 70)
    
    for i, (tag, tag_data) in enumerate(sorted_tags, 1):
        count = tag_data.get('count', 0)
        rate = tag_data.get('rate', 0) * 100
        
        if count > 0:  # Only show tags with actual anomalies
            print(f"{i:<4} {tag:<40} {count:<10} {rate:<8.2f}")
    
    print("-" * 70)
    print(f"Total problematic tags: {len([t for t in sorted_tags if t[1].get('count', 0) > 0])}")

if __name__ == "__main__":
    import sys
    unit = sys.argv[1] if len(sys.argv) > 1 else 'K-31-01'
    list_problem_tags(unit)