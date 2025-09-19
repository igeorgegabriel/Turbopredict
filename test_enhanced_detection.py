#!/usr/bin/env python3
"""
Test the enhanced detection method display
"""

from pi_monitor.tuned_anomaly_detection import enhanced_anomaly_detection
from pi_monitor.parquet_database import ParquetDatabase
import pandas as pd

def test_enhanced_detection_display():
    """Test the enhanced detection with both MTD and Isolation Forest"""
    
    print("TESTING ENHANCED DETECTION METHOD DISPLAY")
    print("=" * 60)
    
    # Initialize database
    db = ParquetDatabase()
    
    # Test with K-31-01 unit
    unit = 'K-31-01'
    print(f"\nTesting enhanced detection for {unit}...")
    
    try:
        # Get unit data
        unit_data = db.get_unit_data(unit)
        
        if unit_data.empty:
            print(f"No data available for {unit}")
            return
            
        print(f"Data loaded: {len(unit_data):,} records")
        
        # Run enhanced detection
        results = enhanced_anomaly_detection(unit_data, unit)
        
        print(f"\nDetection Method: {results.get('method', 'Unknown')}")
        print(f"Total Anomalies: {results.get('total_anomalies', 0):,}")
        print(f"Anomaly Rate: {results.get('anomaly_rate', 0):.2%}")
        
        # Show detection breakdown by tag
        by_tag = results.get('by_tag', {})
        
        if by_tag:
            print(f"\nDETAILED TAG ANALYSIS (Top 10):")
            print("-" * 80)
            print(f"{'TAG':<35} {'TOTAL':<8} {'MTD':<6} {'IF':<6} {'METHOD':<20}")
            print("-" * 80)
            
            # Sort by total count
            sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
            
            for i, (tag, tag_info) in enumerate(sorted_tags[:10], 1):
                total_count = tag_info.get('count', 0)
                mtd_count = tag_info.get('mtd_count', 0)
                iso_count = tag_info.get('isolation_forest_count', 0)
                method = tag_info.get('method', 'Unknown')
                
                # Truncate long tag names
                display_tag = tag[-35:] if len(tag) > 35 else tag
                
                print(f"{display_tag:<35} {total_count:<8} {mtd_count:<6} {iso_count:<6} {method:<20}")
                
                # Show detection breakdown if available
                breakdown = tag_info.get('detection_breakdown', {})
                if breakdown:
                    mtd_info = breakdown.get('mtd', {})
                    iso_info = breakdown.get('isolation_forest', {})
                    
                    print(f"  -> MTD Available: {mtd_info.get('available', False)}, "
                          f"IF Available: {iso_info.get('available', False)}")
        else:
            print("No anomalous tags found!")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_detection_display()