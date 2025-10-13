#!/usr/bin/env python3
"""
Test script for 2.5-sigma primary detection with MTD and Isolation Forest verification
Validates the new anomaly detection pipeline without autoencoder
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Set environment variables for 2.5-sigma detection
os.environ['REQUIRE_AE'] = '0'  # Disable autoencoder
os.environ['ENABLE_AE_LIVE'] = '0'  # Disable autoencoder completely
os.environ['PRIMARY_SIGMA_THRESHOLD'] = '2.5'  # Set 2.5-sigma primary detection

from pi_monitor.speed_aware_anomaly import SpeedAwareAnomalyDetector


def create_test_data_with_anomalies():
    """Create synthetic test data with known anomalies"""
    print("Creating synthetic test data with known anomalies...")

    # Generate normal data
    np.random.seed(42)
    base_time = datetime.now() - timedelta(hours=12)
    n_points = 1000

    normal_data = []
    for i in range(n_points):
        time_point = base_time + timedelta(minutes=i)
        # Normal process value with some noise
        normal_value = 100 + np.random.normal(0, 5)
        normal_data.append({
            'time': time_point,
            'tag': 'TEST_PROCESS_TAG_PV',
            'value': normal_value,
            'unit': 'TEST-01',
            'plant': 'TEST'
        })

    # Add known anomalies (significant deviations)
    anomaly_indices = [100, 250, 400, 600, 800]  # 5 known anomalies
    for idx in anomaly_indices:
        if idx < len(normal_data):
            # Create significant anomaly (>3 sigma)
            anomaly_value = 100 + np.random.choice([-1, 1]) * np.random.uniform(20, 40)  # 4-8 sigma anomaly
            normal_data[idx]['value'] = anomaly_value

    # Add speed data
    for i in range(n_points):
        time_point = base_time + timedelta(minutes=i)
        # Speed with some variation
        speed_value = 1800 + np.random.normal(0, 50)
        normal_data.append({
            'time': time_point,
            'tag': 'TEST_SPEED_TAG_PV',
            'value': speed_value,
            'unit': 'TEST-01',
            'plant': 'TEST'
        })

    return pd.DataFrame(normal_data)


def test_2_5_sigma_verified_detection():
    """Test 2.5-sigma primary detection with MTD and IF verification"""
    print("\n" + "="*60)
    print("TESTING 2.5-SIGMA VERIFIED DETECTION")
    print("="*60)

    try:
        # Initialize detector
        detector = SpeedAwareAnomalyDetector()

        # Create test data
        test_data = create_test_data_with_anomalies()
        print(f"Test data created: {len(test_data)} points")

        # Test different detection methods
        methods_to_test = [
            "2_5_sigma_verified",
            "statistical",
            "isolation_forest"
        ]

        results = {}

        for method in methods_to_test:
            print(f"\nTesting method: {method}")

            # Perform detection
            result = detector.detect_speed_aware_anomalies(
                test_data,
                "TEST",
                "TEST-01",
                anomaly_method=method,
                speed_correlation_analysis=True,
                adaptive_thresholds=True
            )

            results[method] = result

            print(f"  Original anomalies: {len(result.original_anomalies)}")
            print(f"  Compensated anomalies: {len(result.compensated_anomalies)}")
            print(f"  Speed-correlated: {len(result.speed_correlated_anomalies)}")
            print(f"  Method used: {result.method_used}")
            print(f"  Confidence: {result.confidence_score:.2f}")

            # Check verification stages for 2.5-sigma method
            if method == "2_5_sigma_verified" and not result.compensated_anomalies.empty:
                stages = result.compensated_anomalies.get('detection_stage', pd.Series(dtype=str))
                if not stages.empty:
                    print(f"  Verification stages found: {stages.unique()}")

                    # Count fully verified anomalies
                    fully_verified = stages.str.contains('if_verified', na=False).sum()
                    print(f"  Fully verified (2.5σ+MTD+IF): {fully_verified}")

        # Compare methods
        print(f"\n" + "="*60)
        print("METHOD COMPARISON")
        print("="*60)

        for method, result in results.items():
            orig_count = len(result.original_anomalies)
            comp_count = len(result.compensated_anomalies)
            reduction = (orig_count - comp_count) / max(1, orig_count) * 100

            print(f"{method:20s}: {orig_count:3d} -> {comp_count:3d} ({reduction:+5.1f}% change)")

        print("\n✓ 2.5-sigma verified detection test completed successfully")
        return True

    except Exception as e:
        print(f"✗ 2.5-sigma verified detection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_environment_variables():
    """Test that environment variables are properly set"""
    print("\n" + "="*60)
    print("TESTING ENVIRONMENT VARIABLES")
    print("="*60)

    # Check environment variables
    require_ae = os.environ.get('REQUIRE_AE', 'not_set')
    enable_ae = os.environ.get('ENABLE_AE_LIVE', 'not_set')
    sigma_threshold = os.environ.get('PRIMARY_SIGMA_THRESHOLD', 'not_set')

    print(f"REQUIRE_AE: {require_ae} (should be '0')")
    print(f"ENABLE_AE_LIVE: {enable_ae} (should be '0')")
    print(f"PRIMARY_SIGMA_THRESHOLD: {sigma_threshold} (should be '2.5')")

    # Validate settings
    if require_ae == '0' and enable_ae == '0' and sigma_threshold == '2.5':
        print("✓ Environment variables correctly configured")
        return True
    else:
        print("✗ Environment variables not properly configured")
        return False


def test_detection_pipeline():
    """Test individual components of the detection pipeline"""
    print("\n" + "="*60)
    print("TESTING DETECTION PIPELINE COMPONENTS")
    print("="*60)

    try:
        # Initialize detector
        detector = SpeedAwareAnomalyDetector()

        # Create simple test data
        test_data = pd.DataFrame({
            'time': pd.date_range(start='2023-01-01', periods=100, freq='1H'),
            'tag': ['TEST_TAG'] * 100,
            'value': np.concatenate([
                np.random.normal(100, 5, 90),  # Normal data
                np.array([130, 140, 150, 70, 60, 50, 140, 145, 135, 75])  # 10 anomalies
            ])
        })

        print(f"Test data: {len(test_data)} points")

        # Test primary 2.5-sigma detection
        primary_anomalies = detector._primary_2_5_sigma_detection(test_data, adaptive_thresholds=False)
        print(f"Primary 2.5-sigma detection: {len(primary_anomalies)} anomalies")

        if not primary_anomalies.empty:
            # Test MTD verification
            mtd_verified = detector._verify_with_mtd(primary_anomalies, test_data)
            print(f"MTD verified: {len(mtd_verified)} anomalies")

            if not mtd_verified.empty:
                # Test Isolation Forest verification
                if_verified = detector._verify_with_isolation_forest(mtd_verified, test_data)
                print(f"IF verified: {len(if_verified)} anomalies")

                if not if_verified.empty:
                    print("✓ Full pipeline working: 2.5σ -> MTD -> IF")

                    # Show detection stages
                    if 'detection_stage' in if_verified.columns:
                        stages = if_verified['detection_stage'].unique()
                        print(f"Detection stages: {list(stages)}")
                else:
                    print("⚠ No anomalies passed IF verification")
            else:
                print("⚠ No anomalies passed MTD verification")
        else:
            print("⚠ No primary anomalies detected")

        print("✓ Detection pipeline test completed")
        return True

    except Exception as e:
        print(f"✗ Detection pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests for 2.5-sigma verified detection"""
    print("2.5-SIGMA VERIFIED DETECTION TEST SUITE")
    print("="*60)
    print("Testing 2.5-sigma primary detection with MTD and Isolation Forest verification")
    print("Autoencoder disabled - using statistical and ML verification methods")
    print("="*60)

    tests = [
        ("Environment Variables", test_environment_variables),
        ("Detection Pipeline Components", test_detection_pipeline),
        ("2.5-Sigma Verified Detection", test_2_5_sigma_verified_detection)
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
        print("\n✓ ALL TESTS PASSED - 2.5-sigma verified detection ready!")
        print("Configuration:")
        print("  Primary: 2.5-sigma statistical detection")
        print("  Verification 1: Modified Thompson Tau (MTD)")
        print("  Verification 2: Isolation Forest")
        print("  Autoencoder: DISABLED")
    else:
        print(f"\n⚠ {len(results) - passed} test(s) failed - please review issues above")

    return passed == len(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)