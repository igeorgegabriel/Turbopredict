#!/usr/bin/env python3
"""
Test 24-hour recency filter for anomaly detection
Verifies that only NEW anomalies (<24 hours) are plotted/reported
"""

from datetime import datetime, timedelta
import pandas as pd

def test_recency_filter():
    """Test the recency filter logic"""

    # Simulate tag info with different recency patterns
    test_cases = [
        {
            'name': 'CRITICAL - Recent anomaly (last 24h)',
            'tag_info': {
                'priority': 'CRITICAL',
                'recency_breakdown': {
                    'last_24h': 5,
                    'last_7d': 2,
                    'last_30d': 1,
                    'older': 0
                },
                'sigma_2_5_count': 8,
                'autoencoder_count': 5,
                'mtd_count': 6,
                'isolation_forest_count': 4,
                'confidence': 'HIGH'
            },
            'expected': True  # Should be plotted
        },
        {
            'name': 'HIGH - Recent anomaly (last 24h)',
            'tag_info': {
                'priority': 'HIGH',
                'recency_breakdown': {
                    'last_24h': 3,
                    'last_7d': 5,
                    'last_30d': 2,
                    'older': 0
                },
                'sigma_2_5_count': 10,
                'autoencoder_count': 8,
                'mtd_count': 7,
                'isolation_forest_count': 6,
                'confidence': 'HIGH'
            },
            'expected': True  # Should be plotted
        },
        {
            'name': 'MEDIUM - Recent anomaly (but filtered by priority)',
            'tag_info': {
                'priority': 'MEDIUM',
                'recency_breakdown': {
                    'last_24h': 10,  # Has recent anomalies
                    'last_7d': 5,
                    'last_30d': 0,
                    'older': 0
                },
                'sigma_2_5_count': 15,
                'autoencoder_count': 10,
                'mtd_count': 12,
                'isolation_forest_count': 8,
                'confidence': 'HIGH'
            },
            'expected': False  # Should NOT be plotted (priority too low)
        },
        {
            'name': 'CRITICAL - NO recent anomalies (historical only)',
            'tag_info': {
                'priority': 'CRITICAL',
                'recency_breakdown': {
                    'last_24h': 0,  # NO recent anomalies
                    'last_7d': 5,
                    'last_30d': 10,
                    'older': 20
                },
                'sigma_2_5_count': 35,
                'autoencoder_count': 30,
                'mtd_count': 25,
                'isolation_forest_count': 20,
                'confidence': 'HIGH'
            },
            'expected': False  # Should NOT be plotted (all historical >24h)
        },
        {
            'name': 'CRITICAL - Recent but no verification',
            'tag_info': {
                'priority': 'CRITICAL',
                'recency_breakdown': {
                    'last_24h': 10,
                    'last_7d': 0,
                    'last_30d': 0,
                    'older': 0
                },
                'sigma_2_5_count': 10,
                'autoencoder_count': 0,
                'mtd_count': 0,  # NO verification
                'isolation_forest_count': 0,  # NO verification
                'confidence': 'LOW'
            },
            'expected': False  # Should NOT be plotted (no verification)
        }
    ]

    # Import the verification function
    from pi_monitor.anomaly_triggered_plots import AnomalyTriggeredPlotter
    plotter = AnomalyTriggeredPlotter()

    print("\n" + "="*80)
    print("24-HOUR RECENCY FILTER TEST")
    print("="*80)
    print("\nTesting anomaly verification with 24-hour recency requirement...\n")

    passed = 0
    failed = 0

    for test in test_cases:
        result = plotter._is_anomaly_verified(test['tag_info'])
        expected = test['expected']
        status = "PASS" if result == expected else "FAIL"

        if result == expected:
            passed += 1
        else:
            failed += 1

        print(f"[{status}] {test['name']}")
        print(f"      Expected: {expected}, Got: {result}")
        print(f"      Details: Priority={test['tag_info']['priority']}, "
              f"24h_count={test['tag_info']['recency_breakdown']['last_24h']}, "
              f"MTD={test['tag_info']['mtd_count']}, IF={test['tag_info']['isolation_forest_count']}")
        print()

    print("="*80)
    print(f"TEST SUMMARY: {passed} passed, {failed} failed")
    print("="*80)

    if failed == 0:
        print("\n[SUCCESS] All tests passed! 24-hour recency filter working correctly.")
    else:
        print(f"\n[FAILURE] {failed} test(s) failed. Check filter logic.")

if __name__ == "__main__":
    test_recency_filter()
