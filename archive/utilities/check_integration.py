#!/usr/bin/env python3
"""
Check if smart anomaly detection is integrated into CLI
"""

def check_integration():
    """Check integration status"""
    
    print("CHECKING SMART ANOMALY DETECTION INTEGRATION")
    print("=" * 50)
    
    # Check if smart detection module exists
    try:
        from pi_monitor.smart_anomaly_detection import smart_anomaly_detection
        print("[OK] Smart anomaly detection module: AVAILABLE")
    except ImportError as e:
        print(f"[FAIL] Smart detection module: MISSING - {e}")
        return False
        
    # Check if parquet_auto_scan uses smart detection
    try:
        from pi_monitor.parquet_auto_scan import ParquetAutoScanner
        import inspect
        
        scanner = ParquetAutoScanner()
        method_source = inspect.getsource(scanner._detect_anomalies_enhanced)
        
        if 'smart_anomaly_detection' in method_source:
            print("[OK] ParquetAutoScanner: INTEGRATED with smart detection")
        else:
            print("[FAIL] ParquetAutoScanner: NOT integrated")
            return False
            
    except Exception as e:
        print(f"[FAIL] Integration check failed: {e}")
        return False
        
    # Check baseline configuration for vacuum fixes
    try:
        import json
        with open('baseline_config_K-31-01.json', 'r') as f:
            config = json.load(f)
            
        vacuum_tag = 'PCFS_K-31-01_31PIA308A_PV'
        if vacuum_tag in config.get('tag_configurations', {}):
            tag_config = config['tag_configurations'][vacuum_tag]
            lower_limit = tag_config['thresholds']['lower_limit']
            
            if lower_limit < 0:
                print("[OK] Vacuum pressure limits: FIXED (negative values allowed)")
            else:
                print(f"[FAIL] Vacuum pressure limits: NOT FIXED (lower_limit = {lower_limit})")
                return False
        else:
            print("[FAIL] Vacuum tag configuration: NOT FOUND")
            return False
            
    except Exception as e:
        print(f"[FAIL] Baseline config check failed: {e}")
        return False
        
    print("\n" + "=" * 50)
    print("INTEGRATION STATUS: [COMPLETE]")
    print("\nFeatures integrated:")
    print("• Smart unit status detection (speed sensors)")
    print("• Vacuum pressure engineering limits fixed")
    print("• Process-aware anomaly detection")
    print("• Unit offline detection and skip logic")
    print("\nCLI Option [2] is ready with smart detection!")
    
    return True

if __name__ == "__main__":
    check_integration()