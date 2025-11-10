#!/usr/bin/env python3
"""
Test Weighted Confidence Scoring System
Validates that confidence scores are calculated correctly and thresholds work as expected
"""

def test_confidence_scoring():
    """Test confidence score calculation logic"""

    print("\n" + "="*80)
    print("WEIGHTED CONFIDENCE SCORING TEST")
    print("="*80)
    print("\nTesting confidence score calculations and priority-based thresholds...\n")

    test_cases = [
        {
            'name': 'PERFECT DETECTION - All detectors fire',
            'detections': {
                'sigma_2_5_count': 10,  # 10 * 4 = 40 (capped)
                'autoencoder_count': 10,  # 10 * 3 = 30 (capped)
                'mtd_count': 10,  # 10 * 2 = 20 (capped)
                'isolation_forest_count': 10,  # 10 * 1 = 10 (capped)
            },
            'expected_score': 100.0,
            'expected_level': 'VERY_HIGH',
            'priority': 'CRITICAL',
            'should_plot': True
        },
        {
            'name': 'PRIMARY ONLY - No verification',
            'detections': {
                'sigma_2_5_count': 10,  # 40 points
                'autoencoder_count': 10,  # 30 points
                'mtd_count': 0,  # 0 points
                'isolation_forest_count': 0,  # 0 points
            },
            'expected_score': 70.0,
            'expected_level': 'HIGH',
            'priority': 'HIGH',
            'should_plot': False  # No verification, should fail safety check
        },
        {
            'name': 'STRONG SIGMA + MTD VERIFICATION',
            'detections': {
                'sigma_2_5_count': 5,  # 5 * 4 = 20
                'autoencoder_count': 0,  # 0
                'mtd_count': 5,  # 5 * 2 = 10
                'isolation_forest_count': 0,  # 0
            },
            'expected_score': 30.0,
            'expected_level': 'LOW',
            'priority': 'CRITICAL',
            'should_plot': False  # Score 30 < threshold 50 for CRITICAL
        },
        {
            'name': 'BALANCED DETECTION - All contribute',
            'detections': {
                'sigma_2_5_count': 10,  # 40 (capped)
                'autoencoder_count': 5,  # 15
                'mtd_count': 10,  # 20 (capped)
                'isolation_forest_count': 3,  # 3
            },
            'expected_score': 78.0,
            'expected_level': 'HIGH',
            'priority': 'HIGH',
            'should_plot': True  # Score 78 >= threshold 60 for HIGH
        },
        {
            'name': 'MINIMAL DETECTION - Edge case',
            'detections': {
                'sigma_2_5_count': 2,  # 8
                'autoencoder_count': 0,  # 0
                'mtd_count': 1,  # 2
                'isolation_forest_count': 1,  # 1
            },
            'expected_score': 11.0,
            'expected_level': 'LOW',
            'priority': 'CRITICAL',
            'should_plot': False  # Score 11 < threshold 50 for CRITICAL
        },
        {
            'name': 'HIGH CONFIDENCE CRITICAL',
            'detections': {
                'sigma_2_5_count': 10,  # 40
                'autoencoder_count': 4,  # 12
                'mtd_count': 3,  # 6
                'isolation_forest_count': 2,  # 2
            },
            'expected_score': 60.0,
            'expected_level': 'HIGH',
            'priority': 'CRITICAL',
            'should_plot': True  # Score 60 >= threshold 50 for CRITICAL
        },
    ]

    # Calculate scores
    def calculate_confidence_score(detections):
        score = 0.0
        # Primary detectors (70 points)
        if detections['sigma_2_5_count'] > 0:
            score += min(40.0, detections['sigma_2_5_count'] * 4.0)
        if detections['autoencoder_count'] > 0:
            score += min(30.0, detections['autoencoder_count'] * 3.0)
        # Verification layer (30 points)
        if detections['mtd_count'] > 0:
            score += min(20.0, detections['mtd_count'] * 2.0)
        if detections['isolation_forest_count'] > 0:
            score += min(10.0, detections['isolation_forest_count'] * 1.0)
        return score

    def get_confidence_level(score):
        if score >= 80:
            return 'VERY_HIGH'
        elif score >= 60:
            return 'HIGH'
        elif score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'

    def should_plot(detections, score, priority):
        # Priority-based thresholds
        thresholds = {
            'CRITICAL': 50,
            'HIGH': 60,
            'MEDIUM': 70,
            'LOW': 80
        }
        threshold = thresholds.get(priority, 80)

        # Check threshold
        if score < threshold:
            return False

        # Safety checks
        primary = detections['sigma_2_5_count'] > 0 or detections['autoencoder_count'] > 0
        verification = detections['mtd_count'] > 0 or detections['isolation_forest_count'] > 0

        return primary and verification

    passed = 0
    failed = 0

    for test in test_cases:
        score = calculate_confidence_score(test['detections'])
        level = get_confidence_level(score)
        will_plot = should_plot(test['detections'], score, test['priority'])

        score_match = abs(score - test['expected_score']) < 0.1
        level_match = level == test['expected_level']
        plot_match = will_plot == test['should_plot']

        all_pass = score_match and level_match and plot_match

        if all_pass:
            passed += 1
            status = "PASS"
        else:
            failed += 1
            status = "FAIL"

        print(f"[{status}] {test['name']}")
        print(f"      Priority: {test['priority']}")
        print(f"      Detections: 2.5Sigma={test['detections']['sigma_2_5_count']}, "
              f"AE={test['detections']['autoencoder_count']}, "
              f"MTD={test['detections']['mtd_count']}, "
              f"IF={test['detections']['isolation_forest_count']}")
        print(f"      Score: {score:.1f}/100 (expected {test['expected_score']:.1f}) "
              f"{'OK' if score_match else 'FAIL'}")
        print(f"      Level: {level} (expected {test['expected_level']}) "
              f"{'OK' if level_match else 'FAIL'}")
        print(f"      Will Plot: {will_plot} (expected {test['should_plot']}) "
              f"{'OK' if plot_match else 'FAIL'}")
        print()

    print("="*80)
    print(f"TEST SUMMARY: {passed} passed, {failed} failed")
    print("="*80)

    if failed == 0:
        print("\n[SUCCESS] All weighted confidence tests passed.")
        print("\nConfidence Scoring System:")
        print("  Primary Detectors (70 points):")
        print("    - 2.5-Sigma: max 40 points (4 points per detection)")
        print("    - AutoEncoder: max 30 points (3 points per detection)")
        print("  Verification Layer (30 points):")
        print("    - MTD: max 20 points (2 points per detection)")
        print("    - Isolation Forest: max 10 points (1 point per detection)")
        print("\nPriority-Based Thresholds:")
        print("    - CRITICAL: >=50/100 (Lenient - catch all critical issues)")
        print("    - HIGH: >=60/100 (Standard - balanced detection)")
        print("    - MEDIUM: >=70/100 (Strict - high confidence only)")
        print("    - LOW: >=80/100 (Very strict - extreme confidence required)")
    else:
        print(f"\n[FAILURE] {failed} test(s) failed. Check scoring logic.")

if __name__ == "__main__":
    test_confidence_scoring()
