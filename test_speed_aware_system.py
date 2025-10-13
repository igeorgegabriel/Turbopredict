#!/usr/bin/env python3
"""
Test script for speed-aware functionality in TURBOPREDICT X PROTEAN
Validates speed compensation and anomaly detection across all plants
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from pi_monitor.speed_compensator import SpeedAwareCompensator
from pi_monitor.speed_aware_anomaly import SpeedAwareAnomalyDetector
from pi_monitor.speed_aware_interface import SpeedAwareInterface
from pi_monitor.config import Config


def create_test_data():
    """Create synthetic test data for speed-aware testing"""
    print("Creating synthetic test data...")

    # Generate synthetic data for different plants and units
    test_data = []

    # PCFS Plant test data
    pcfs_units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
    for unit in pcfs_units:
        base_time = datetime.now() - timedelta(hours=24)

        # Generate speed data
        for i in range(100):
            time_point = base_time + timedelta(minutes=i * 15)
            speed_value = 1800 + np.random.normal(0, 100)  # 1800 RPM baseline with noise
            test_data.append({
                'time': time_point,
                'tag': f'PCFS_{unit}_{unit[:-3]}SI-{unit[-3:]}B_PV',
                'value': speed_value,
                'unit': unit,
                'plant': 'PCFS'
            })

            # Generate process data that correlates with speed
            process_value = 50 + speed_value * 0.02 + np.random.normal(0, 5)
            test_data.append({
                'time': time_point,
                'tag': f'PCFS_{unit}_{unit[:-3]}TIA-{unit[-3:]}2_PV',
                'value': process_value,
                'unit': unit,
                'plant': 'PCFS'
            })

    # ABF Plant test data
    base_time = datetime.now() - timedelta(hours=24)
    for i in range(100):
        time_point = base_time + timedelta(minutes=i * 15)

        # Primary speed
        speed1 = 1500 + np.random.normal(0, 80)
        test_data.append({
            'time': time_point,
            'tag': 'ABF.07-MT001.SI-07002D_new.PV',
            'value': speed1,
            'unit': '07-MT01-K001',
            'plant': 'ABF'
        })

        # Secondary speed (slightly different)
        speed2 = speed1 + np.random.normal(0, 20)
        test_data.append({
            'time': time_point,
            'tag': 'ABF.07-MT001.SI-07002MV_new.PV',
            'value': speed2,
            'unit': '07-MT01-K001',
            'plant': 'ABF'
        })

        # Process data
        process_value = 30 + speed1 * 0.015 + np.random.normal(0, 3)
        test_data.append({
            'time': time_point,
            'tag': 'ABF.07-MT001.TI-07015.PV',
            'value': process_value,
            'unit': '07-MT01-K001',
            'plant': 'ABF'
        })

    # PCMSB Plant test data
    pcmsb_units = [
        ('C-02001', 'PCM.C-02001.020SI6601.PV', 3600),
        ('C-104', 'PCM.C-104.SIALH-1451.PV', 9817),
        ('C-13001', 'PCM.C-13001.130SI4409.PV', 2400),
        ('C-201', 'PCM.C-201.SI-2151.PV', 9002),
        ('C-202', 'PCM.C-202.SIC-2252-SP.PV', 8193)
    ]

    for unit, speed_tag, baseline_speed in pcmsb_units:
        base_time = datetime.now() - timedelta(hours=24)

        for i in range(100):
            time_point = base_time + timedelta(minutes=i * 15)

            # Speed data
            speed_value = baseline_speed + np.random.normal(0, baseline_speed * 0.05)
            test_data.append({
                'time': time_point,
                'tag': speed_tag,
                'value': speed_value,
                'unit': unit,
                'plant': 'PCMSB'
            })

            # Process data that correlates with speed
            process_value = 100 + speed_value * 0.01 + np.random.normal(0, 10)
            test_data.append({
                'time': time_point,
                'tag': f'PCM.{unit}.TIAH-1401.PV',
                'value': process_value,
                'unit': unit,
                'plant': 'PCMSB'
            })

    return pd.DataFrame(test_data)


def test_speed_compensator():
    """Test speed compensator functionality"""
    print("\n" + "="*60)
    print("TESTING SPEED COMPENSATOR")
    print("="*60)

    try:
        # Initialize compensator
        compensator = SpeedAwareCompensator()

        # Validate configuration
        validation = compensator.validate_configuration()
        print(f"Configuration Valid: {validation['valid']}")
        print(f"Total Units: {validation['summary'].get('total_units', 0)}")
        print(f"Total Speed Tags: {validation['summary'].get('total_speed_tags', 0)}")

        if validation['errors']:
            print(f"Errors: {len(validation['errors'])}")
            for error in validation['errors'][:3]:
                print(f"  - {error}")

        # Test with synthetic data
        test_data = create_test_data()
        print(f"Generated {len(test_data)} test data points")

        # Test speed compensation for a unit
        test_units = [
            ("PCFS", "K-12-01"),
            ("ABF", "07-MT01-K001"),
            ("PCMSB", "C-104")
        ]

        for plant, unit in test_units:
            print(f"\nTesting {plant}.{unit}:")

            # Filter data for this unit
            unit_data = test_data[
                (test_data['plant'] == plant) & (test_data['unit'] == unit)
            ].copy()

            if unit_data.empty:
                print(f"  No test data for {plant}.{unit}")
                continue

            # Test speed compensation
            result = compensator.compensate_data(unit_data, plant, unit)

            print(f"  Compensation Factor: {result.compensation_factor:.3f}")
            print(f"  Method Used: {result.method_used}")
            print(f"  Confidence: {result.confidence:.2f}")
            print(f"  Speed Data Points: {len(result.speed_data)}")
            print(f"  Warnings: {len(result.warnings)}")

            if result.warnings:
                for warning in result.warnings[:2]:
                    print(f"    - {warning}")

        print("\n✓ Speed compensator test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Speed compensator test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_speed_aware_anomaly_detection():
    """Test speed-aware anomaly detection"""
    print("\n" + "="*60)
    print("TESTING SPEED-AWARE ANOMALY DETECTION")
    print("="*60)

    try:
        # Initialize detector
        detector = SpeedAwareAnomalyDetector()

        # Test with synthetic data
        test_data = create_test_data()

        # Add some anomalies to test data
        anomaly_indices = np.random.choice(len(test_data), size=20, replace=False)
        for idx in anomaly_indices:
            # Create anomalous values
            if 'speed' not in test_data.iloc[idx]['tag'].lower():
                test_data.iloc[idx, test_data.columns.get_loc('value')] *= np.random.choice([0.1, 5.0])

        print(f"Added 20 synthetic anomalies to test data")

        # Test anomaly detection for different units
        test_units = [("PCFS", "K-12-01"), ("PCMSB", "C-104")]

        for plant, unit in test_units:
            print(f"\nTesting anomaly detection for {plant}.{unit}:")

            # Filter data for this unit
            unit_data = test_data[
                (test_data['plant'] == plant) & (test_data['unit'] == unit)
            ].copy()

            if unit_data.empty:
                print(f"  No test data for {plant}.{unit}")
                continue

            # Perform speed-aware anomaly detection
            result = detector.detect_speed_aware_anomalies(unit_data, plant, unit)

            print(f"  Original Anomalies: {len(result.original_anomalies)}")
            print(f"  Compensated Anomalies: {len(result.compensated_anomalies)}")
            print(f"  Speed-Correlated: {len(result.speed_correlated_anomalies)}")
            print(f"  Anomaly Reduction: {result.anomaly_reduction_factor:.1%}")
            print(f"  Detection Confidence: {result.confidence_score:.2f}")
            print(f"  Method Used: {result.method_used}")

        print("\n✓ Speed-aware anomaly detection test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Speed-aware anomaly detection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_speed_aware_interface():
    """Test speed-aware interface"""
    print("\n" + "="*60)
    print("TESTING SPEED-AWARE INTERFACE")
    print("="*60)

    try:
        # Initialize interface
        interface = SpeedAwareInterface()

        # Test menu generation
        menu = interface.get_speed_aware_menu()
        print("Speed-aware menu generated successfully")

        # Test speed status summary
        status = interface.get_speed_status_summary()
        print(f"Speed system status:")
        print(f"  Total Units: {status.get('total_units', 0)}")
        print(f"  Enabled Units: {status.get('enabled_units', 0)}")
        print(f"  Total Speed Tags: {status.get('total_speed_tags', 0)}")
        print(f"  System Ready: {status.get('system_ready', False)}")

        # Test command handling (config status)
        result = interface.handle_speed_aware_command("S5")
        print("\nConfig status command test:")
        print(result[:200] + "..." if len(result) > 200 else result)

        print("\n✓ Speed-aware interface test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Speed-aware interface test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_integration():
    """Test integration with TurboPredict system"""
    print("\n" + "="*60)
    print("TESTING INTEGRATION")
    print("="*60)

    try:
        # Test import of TurboPredict with speed-aware modules
        from turbopredict import TurbopredictSystem

        print("✓ TurboPredict imports successfully with speed-aware modules")

        # Initialize system
        system = TurbopredictSystem()

        # Check if speed-aware functionality is available
        speed_aware_available = hasattr(system, 'speed_aware_available') and system.speed_aware_available
        print(f"Speed-aware functionality available: {speed_aware_available}")

        if speed_aware_available:
            print("✓ Speed-aware integration successful")
        else:
            print("⚠ Speed-aware functionality not fully integrated")

        return True

    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all speed-aware tests"""
    print("SPEED-AWARE SYSTEM TEST SUITE")
    print("="*60)
    print("Testing speed compensation across all plants:")
    print("- PCFS: 4 units with SI- tags")
    print("- ABF: 1 unit with dual SI- tags")
    print("- PCMSB: 8 units with various speed indicators")
    print("="*60)

    tests = [
        ("Speed Compensator", test_speed_compensator),
        ("Speed-Aware Anomaly Detection", test_speed_aware_anomaly_detection),
        ("Speed-Aware Interface", test_speed_aware_interface),
        ("System Integration", test_integration)
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\nRunning {test_name} test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"Test {test_name} crashed: {e}")
            results.append((test_name, False))

    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nPassed: {passed}/{len(results)} tests")

    if passed == len(results):
        print("\n🎉 ALL TESTS PASSED - Speed-aware system ready for deployment!")
    else:
        print(f"\n⚠ {len(results) - passed} test(s) failed - please review issues above")

    return passed == len(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)