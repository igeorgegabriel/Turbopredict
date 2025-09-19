#!/usr/bin/env python3
"""
Test enhanced detection with real baseline configuration
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from pi_monitor.tuned_anomaly_detection import TunedAnomalyDetector
import pandas as pd
import numpy as np
import json

def test_with_baseline_config():
    """Test enhanced detection with baseline configuration"""
    
    print("TESTING ENHANCED DETECTION WITH BASELINE CONFIG")
    print("=" * 60)
    
    # Check if baseline config exists
    config_file = "baseline_config_K-31-01.json"
    if not os.path.exists(config_file):
        print(f"[INFO] Baseline config {config_file} not found")
        print("Testing with simulated baseline config...")
        
        # Create minimal test config
        test_config = {
            "tag_configurations": {
                "TEST_TAG": {
                    "thresholds": {
                        "upper_limit": 60.0,
                        "lower_limit": 40.0
                    }
                }
            }
        }
        
        with open("test_baseline_config.json", "w") as f:
            json.dump(test_config, f, indent=2)
        
        config_file = "test_baseline_config.json"
    
    try:
        # Create detector with baseline config
        detector = TunedAnomalyDetector(config_file)
        print(f"[OK] Detector created with config: {config_file}")
        print(f"[OK] Config loaded: {detector.config_loaded}")
        
        # Create sample data with known anomalies
        np.random.seed(42)  # For reproducible results
        normal_values = np.random.normal(50, 2, 95)  # Normal data around 50
        anomaly_values = [100, 5, 150, 0, 200]  # Clear outliers
        
        all_values = np.concatenate([normal_values, anomaly_values])
        np.random.shuffle(all_values)  # Mix them up
        
        data = {
            'tag': ['TEST_TAG'] * 100,
            'value': all_values,
            'time': pd.date_range('2024-01-01', periods=100, freq='h')
        }
        df = pd.DataFrame(data)
        
        print(f"\nTesting with {len(df)} data points...")
        print(f"Value range: {df['value'].min():.1f} to {df['value'].max():.1f}")
        
        # Run enhanced detection
        results = detector.detect_anomalies_with_tuning(df, 'TEST_UNIT')
        
        print(f"\nRESULTS:")
        print(f"Method: {results.get('method', 'Unknown')}")
        print(f"Config loaded: {results.get('config_loaded', False)}")
        print(f"Total anomalies: {results.get('total_anomalies', 0)}")
        print(f"Anomaly rate: {results.get('anomaly_rate', 0):.2%}")
        
        # Check tag-level results
        by_tag = results.get('by_tag', {})
        
        if by_tag:
            print(f"\nTAG-LEVEL ANALYSIS:")
            for tag, tag_info in by_tag.items():
                print(f"\nTag: {tag}")
                print(f"  Total count: {tag_info.get('count', 0)}")
                print(f"  Rate: {tag_info.get('rate', 0):.2%}")
                print(f"  Method: {tag_info.get('method', 'Unknown')}")
                
                # Check for enhanced tracking
                mtd_count = tag_info.get('mtd_count', None)
                iso_count = tag_info.get('isolation_forest_count', None)
                
                if mtd_count is not None:
                    print(f"  MTD count: {mtd_count}")
                if iso_count is not None:
                    print(f"  Isolation Forest count: {iso_count}")
                
                # Check detection breakdown
                breakdown = tag_info.get('detection_breakdown', {})
                if breakdown:
                    print(f"  Detection breakdown available: Yes")
                    mtd_info = breakdown.get('mtd', {})
                    iso_info = breakdown.get('isolation_forest', {})
                    
                    print(f"    MTD: {mtd_info.get('count', 0)} anomalies, available: {mtd_info.get('available', False)}")
                    print(f"    IF: {iso_info.get('count', 0)} anomalies, available: {iso_info.get('available', False)}")
                else:
                    print(f"  Detection breakdown available: No")
        else:
            print("\nNo anomalous tags found!")
            
        return True
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up test config
        if os.path.exists("test_baseline_config.json"):
            os.remove("test_baseline_config.json")

if __name__ == "__main__":
    test_with_baseline_config()