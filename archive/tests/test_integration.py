#!/usr/bin/env python3
"""
Test the integrated enhanced anomaly detection system
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner

def test_enhanced_detection():
    """Test the enhanced anomaly detection integration"""
    
    print("TESTING ENHANCED ANOMALY DETECTION INTEGRATION")
    print("=" * 60)
    
    scanner = ParquetAutoScanner()
    
    # Test K-31-01 with enhanced detection
    print("Testing K-31-01 with enhanced anomaly detection...")
    
    try:
        results = scanner.analyze_unit_data('K-31-01', run_anomaly_detection=True)
        
        if 'anomalies' in results:
            anomalies = results['anomalies']
            
            print(f"Method used: {anomalies.get('method', 'unknown')}")
            print(f"Baseline calibrated: {anomalies.get('baseline_calibrated', False)}")
            print(f"Total anomalies: {anomalies.get('total_anomalies', 0)}")
            print(f"Anomaly rate: {anomalies.get('anomaly_rate', 0)*100:.2f}%")
            print(f"Detection summary: {anomalies.get('detection_summary', 'N/A')}")
            
            if anomalies.get('by_tag'):
                print(f"Tags with anomalies: {len(anomalies['by_tag'])}")
                
                # Show top 3 anomalous tags
                tag_items = list(anomalies['by_tag'].items())[:3]
                for tag, tag_data in tag_items:
                    print(f"  {tag}: {tag_data.get('count', 0)} anomalies ({tag_data.get('rate', 0)*100:.2f}%)")
                    
        else:
            print("No anomaly data in results")
            
        print("\nIntegration test completed successfully!")
        
    except Exception as e:
        print(f"Integration test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_detection()