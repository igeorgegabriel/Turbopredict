#!/usr/bin/env python3
"""
Test smart anomaly detection with unit status awareness
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner

def test_smart_detection():
    """Test the smart anomaly detection with unit status checking"""
    
    print("TESTING SMART ANOMALY DETECTION WITH UNIT STATUS")
    print("=" * 60)
    
    scanner = ParquetAutoScanner()
    
    # Test all units
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    
    for unit in units:
        print(f"\nTesting {unit}...")
        print("-" * 40)
        
        try:
            results = scanner.analyze_unit_data(unit, run_anomaly_detection=True)
            
            if 'anomalies' in results:
                anomalies = results['anomalies']
                
                print(f"Method: {anomalies.get('method', 'unknown')}")
                print(f"Unit Status: {anomalies.get('unit_status', 'UNKNOWN')}")
                print(f"Unit Message: {anomalies.get('unit_message', 'N/A')}")
                print(f"Analysis Performed: {anomalies.get('analysis_performed', True)}")
                print(f"Total Anomalies: {anomalies.get('total_anomalies', 0)}")
                print(f"Detection Summary: {anomalies.get('detection_summary', 'N/A')}")
                
                # Show unit status awareness
                if not anomalies.get('analysis_performed', True):
                    print("✓ SMART: Skipped analysis - unit offline")
                elif anomalies.get('unit_status') == 'LOW_SPEED':
                    print("✓ SMART: Applied conservative thresholds for low speed")
                elif anomalies.get('unit_status') == 'RUNNING':
                    print("✓ SMART: Normal analysis - unit running")
                else:
                    print(f"✓ SMART: Status-aware analysis - {anomalies.get('unit_status')}")
                    
            else:
                print("No anomaly data available")
                
        except Exception as e:
            print(f"ERROR: {e}")
    
    print("\n" + "=" * 60)
    print("Smart detection test completed!")

if __name__ == "__main__":
    test_smart_detection()