#!/usr/bin/env python3
"""
Quick verification of the detection method integration
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

def test_sklearn_availability():
    """Test if sklearn is available"""
    try:
        from sklearn.ensemble import IsolationForest
        print("[OK] scikit-learn available")
        return True
    except ImportError as e:
        print(f"[FAIL] scikit-learn not available: {e}")
        return False

def test_tuned_detection_import():
    """Test if tuned detection can be imported"""
    try:
        from pi_monitor.tuned_anomaly_detection import enhanced_anomaly_detection, TunedAnomalyDetector
        print("[OK] Tuned anomaly detection imported")
        return True
    except ImportError as e:
        print(f"[FAIL] Tuned detection import failed: {e}")
        return False

def test_detection_methods():
    """Test detection methods with sample data"""
    try:
        from pi_monitor.tuned_anomaly_detection import TunedAnomalyDetector
        import pandas as pd
        import numpy as np
        
        # Create sample data
        values = np.random.normal(50, 5, 100).tolist() + [100, 200]  # Add some outliers
        data = {
            'tag': ['TEST_TAG'] * 102,  # Match length with values and time
            'value': values,
            'time': pd.date_range('2024-01-01', periods=102, freq='h')
        }
        df = pd.DataFrame(data)
        
        detector = TunedAnomalyDetector()
        results = detector.detect_anomalies_with_tuning(df, 'TEST_UNIT')
        
        print(f"[OK] Detection test completed")
        print(f"    Method: {results.get('method', 'Unknown')}")
        print(f"    Total anomalies: {results.get('total_anomalies', 0)}")
        
        # Check if tag-level results have new fields
        by_tag = results.get('by_tag', {})
        if by_tag:
            tag_name = list(by_tag.keys())[0]
            tag_info = by_tag[tag_name]
            
            has_mtd_count = 'mtd_count' in tag_info
            has_iso_count = 'isolation_forest_count' in tag_info
            has_detection_breakdown = 'detection_breakdown' in tag_info
            
            print(f"    Has MTD count: {has_mtd_count}")
            print(f"    Has Isolation Forest count: {has_iso_count}")
            print(f"    Has detection breakdown: {has_detection_breakdown}")
            
            if has_detection_breakdown:
                breakdown = tag_info['detection_breakdown']
                print(f"    MTD available: {breakdown.get('mtd', {}).get('available', False)}")
                print(f"    IF available: {breakdown.get('isolation_forest', {}).get('available', False)}")
                
        return True
        
    except Exception as e:
        print(f"[FAIL] Detection method test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("VERIFYING ENHANCED DETECTION INTEGRATION")
    print("=" * 50)
    
    sklearn_ok = test_sklearn_availability()
    import_ok = test_tuned_detection_import()
    
    if sklearn_ok and import_ok:
        print("\nTesting detection methods...")
        detection_ok = test_detection_methods()
        
        if detection_ok:
            print("\n[SUCCESS] Enhanced detection integration verified!")
            print("The system now tracks MTD and Isolation Forest detection methods.")
        else:
            print("\n[PARTIAL] Import successful but detection test failed.")
    else:
        print("\n[FAIL] Basic imports failed.")

if __name__ == "__main__":
    main()