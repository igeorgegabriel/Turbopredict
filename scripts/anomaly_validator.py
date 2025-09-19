#!/usr/bin/env python3
"""
PI Tag Anomaly Validator for TURBOPREDICT X PROTEAN
Helps distinguish genuine anomalies from false positives
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner


class PITagAnomalyValidator:
    """Validate PI tag anomalies to distinguish genuine from false positives"""
    
    def __init__(self):
        self.scanner = ParquetAutoScanner()
        
    def validate_tag_anomalies(self, unit: str, top_n_tags: int = 10):
        """
        Validate top anomalous tags with multiple validation methods
        
        Args:
            unit: Unit to analyze
            top_n_tags: Number of top anomalous tags to validate
        """
        print(f"PI TAG ANOMALY VALIDATION - {unit}")
        print("=" * 80)
        
        # Get anomaly analysis
        analysis = self.scanner.analyze_unit_data(unit, run_anomaly_detection=True)
        
        if 'anomalies' not in analysis:
            print("ERROR: No anomaly data available")
            return
            
        anomaly_data = analysis['anomalies']
        tag_anomalies = anomaly_data.get('by_tag', {})
        
        if not tag_anomalies:
            print("ERROR: No tag-level anomaly data available")
            return
            
        # Sort tags by anomaly count
        sorted_tags = sorted(tag_anomalies.items(), 
                           key=lambda x: x[1].get('count', 0), 
                           reverse=True)
        
        print(f"Validating top {top_n_tags} anomalous tags...")
        print()
        
        # Load unit data for validation
        df = self.scanner.db.get_unit_data(unit)
        if df.empty:
            print("ERROR: No unit data available")
            return
            
        validation_results = []
        
        for i, (tag, anomaly_info) in enumerate(sorted_tags[:top_n_tags]):
            print(f"[{i+1}/{top_n_tags}] Validating: {tag}")
            print("-" * 60)
            
            # Get tag-specific data
            tag_df = df[df['tag'] == tag].copy()
            if tag_df.empty:
                print("  ERROR: No data for this tag")
                continue
                
            anomaly_count = anomaly_info.get('count', 0)
            anomaly_rate = anomaly_info.get('rate', 0) * 100
            
            print(f"  Reported anomalies: {anomaly_count} ({anomaly_rate:.1f}%)")
            
            # Validation Method 1: Statistical validation
            validation = self._validate_statistical(tag_df, tag)
            
            # Validation Method 2: Engineering limits check  
            eng_validation = self._validate_engineering_limits(tag_df, tag)
            
            # Validation Method 3: Temporal pattern analysis
            temporal_validation = self._validate_temporal_patterns(tag_df, tag)
            
            # Combine validation results
            overall_validation = self._combine_validations(
                validation, eng_validation, temporal_validation
            )
            
            validation_results.append({
                'tag': tag,
                'reported_anomalies': anomaly_count,
                'reported_rate': anomaly_rate,
                'validation': overall_validation
            })
            
            # Print validation result
            confidence = overall_validation['confidence']
            verdict = overall_validation['verdict']
            reason = overall_validation['reason']
            
            print(f"  VALIDATION: {verdict} (Confidence: {confidence}%)")
            print(f"  REASON: {reason}")
            print()
            
        # Summary
        self._print_validation_summary(validation_results)
        
        return validation_results
    
    def _validate_statistical(self, tag_df: pd.DataFrame, tag: str) -> dict:
        """Statistical validation using multiple statistical tests"""
        try:
            values = tag_df['value'].dropna()
            if len(values) < 30:
                return {'method': 'statistical', 'valid': False, 'reason': 'Insufficient data'}
            
            # Basic statistics
            mean = values.mean()
            std = values.std()
            median = values.median()
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1
            
            # Multiple outlier detection methods
            # Method 1: Z-score (3-sigma rule)
            z_scores = np.abs((values - mean) / std)
            z_outliers = np.sum(z_scores > 3)
            z_rate = z_outliers / len(values) * 100
            
            # Method 2: IQR method (1.5 * IQR)
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            iqr_outliers = np.sum((values < lower_bound) | (values > upper_bound))
            iqr_rate = iqr_outliers / len(values) * 100
            
            # Method 3: Modified Z-score (median-based, more robust)
            mad = np.median(np.abs(values - median))
            modified_z_scores = 0.6745 * (values - median) / mad if mad > 0 else np.zeros_like(values)
            modified_z_outliers = np.sum(np.abs(modified_z_scores) > 3.5)
            modified_z_rate = modified_z_outliers / len(values) * 100
            
            # Validation logic
            avg_outlier_rate = np.mean([z_rate, iqr_rate, modified_z_rate])
            
            if avg_outlier_rate < 1:  # < 1% outliers is normal
                verdict = "LIKELY_FALSE_POSITIVE"
                confidence = 85
            elif avg_outlier_rate < 5:  # 1-5% may be genuine
                verdict = "INVESTIGATE_FURTHER" 
                confidence = 60
            else:  # > 5% likely genuine issues
                verdict = "LIKELY_GENUINE"
                confidence = 80
                
            return {
                'method': 'statistical',
                'valid': True,
                'verdict': verdict,
                'confidence': confidence,
                'z_outliers': z_outliers,
                'iqr_outliers': iqr_outliers,
                'avg_rate': avg_outlier_rate,
                'stats': {
                    'mean': mean,
                    'std': std,
                    'median': median,
                    'iqr': iqr
                }
            }
            
        except Exception as e:
            return {'method': 'statistical', 'valid': False, 'reason': str(e)}
    
    def _validate_engineering_limits(self, tag_df: pd.DataFrame, tag: str) -> dict:
        """Validate against engineering limits and physical constraints"""
        try:
            values = tag_df['value'].dropna()
            if len(values) == 0:
                return {'method': 'engineering', 'valid': False, 'reason': 'No values'}
            
            # Determine tag type from name
            tag_type = self._classify_tag_type(tag)
            
            # Check for impossible values based on tag type
            impossible_values = 0
            suspicious_values = 0
            
            if tag_type == 'temperature':
                # Temperature checks (assuming Celsius)
                impossible_values = np.sum((values < -50) | (values > 1000))  # Extreme temps
                suspicious_values = np.sum((values < 0) | (values > 200))   # Unusual temps
                
            elif tag_type == 'pressure':
                # Pressure checks (assuming bar or similar)
                impossible_values = np.sum(values < 0)  # Negative pressure impossible
                suspicious_values = np.sum(values > 100)  # Very high pressure
                
            elif tag_type == 'flow':
                # Flow checks
                impossible_values = np.sum(values < 0)  # Negative flow usually impossible
                suspicious_values = np.sum(values > 10000)  # Very high flow
                
            elif tag_type == 'level':
                # Level checks (usually 0-100%)
                impossible_values = np.sum((values < 0) | (values > 100))
                
            elif tag_type == 'speed':
                # Speed/RPM checks  
                impossible_values = np.sum(values < 0)  # Negative speed impossible
                suspicious_values = np.sum(values > 10000)  # Very high RPM
                
            # Calculate rates
            impossible_rate = impossible_values / len(values) * 100
            suspicious_rate = suspicious_values / len(values) * 100
            
            # Validation logic
            if impossible_rate > 5:
                verdict = "DATA_QUALITY_ISSUE"
                confidence = 90
            elif suspicious_rate > 20:
                verdict = "INVESTIGATE_SENSOR"
                confidence = 75
            else:
                verdict = "ENGINEERING_OK"
                confidence = 70
                
            return {
                'method': 'engineering',
                'valid': True,
                'verdict': verdict,
                'confidence': confidence,
                'tag_type': tag_type,
                'impossible_values': impossible_values,
                'suspicious_values': suspicious_values,
                'impossible_rate': impossible_rate,
                'suspicious_rate': suspicious_rate
            }
            
        except Exception as e:
            return {'method': 'engineering', 'valid': False, 'reason': str(e)}
    
    def _validate_temporal_patterns(self, tag_df: pd.DataFrame, tag: str) -> dict:
        """Validate temporal patterns to detect sensor issues"""
        try:
            if 'time' not in tag_df.columns:
                return {'method': 'temporal', 'valid': False, 'reason': 'No time data'}
                
            # Sort by time
            tag_df = tag_df.sort_values('time')
            values = tag_df['value'].dropna()
            
            if len(values) < 10:
                return {'method': 'temporal', 'valid': False, 'reason': 'Insufficient data'}
            
            # Check for common sensor failure patterns
            
            # Pattern 1: Stuck sensor (same value repeated)
            consecutive_same = 0
            max_consecutive = 0
            current_consecutive = 1
            
            for i in range(1, len(values)):
                if abs(values.iloc[i] - values.iloc[i-1]) < 0.001:  # Essentially same value
                    current_consecutive += 1
                else:
                    max_consecutive = max(max_consecutive, current_consecutive)
                    current_consecutive = 1
            max_consecutive = max(max_consecutive, current_consecutive)
            
            stuck_rate = max_consecutive / len(values) * 100
            
            # Pattern 2: Excessive noise (high standard deviation of differences)
            if len(values) > 1:
                diffs = np.diff(values)
                noise_level = np.std(diffs)
                signal_level = np.std(values)
                noise_ratio = noise_level / signal_level if signal_level > 0 else 0
            else:
                noise_ratio = 0
                
            # Pattern 3: Sudden jumps (large step changes)
            if len(values) > 1:
                large_jumps = np.sum(np.abs(diffs) > 5 * np.std(diffs))
                jump_rate = large_jumps / len(diffs) * 100
            else:
                jump_rate = 0
                
            # Validation logic
            if stuck_rate > 30:
                verdict = "STUCK_SENSOR"
                confidence = 85
            elif noise_ratio > 2:
                verdict = "NOISY_SENSOR" 
                confidence = 80
            elif jump_rate > 5:
                verdict = "UNSTABLE_SENSOR"
                confidence = 75
            else:
                verdict = "TEMPORAL_OK"
                confidence = 70
                
            return {
                'method': 'temporal',
                'valid': True,
                'verdict': verdict,
                'confidence': confidence,
                'stuck_rate': stuck_rate,
                'noise_ratio': noise_ratio,
                'jump_rate': jump_rate
            }
            
        except Exception as e:
            return {'method': 'temporal', 'valid': False, 'reason': str(e)}
    
    def _classify_tag_type(self, tag: str) -> str:
        """Classify PI tag type from tag name"""
        tag_upper = tag.upper()
        
        if any(x in tag_upper for x in ['TI', 'TIA', 'TE', 'TEMP']):
            return 'temperature'
        elif any(x in tag_upper for x in ['PI', 'PIA', 'PE', 'PRESS']):
            return 'pressure'
        elif any(x in tag_upper for x in ['FI', 'FIA', 'FE', 'FLOW']):
            return 'flow'
        elif any(x in tag_upper for x in ['LI', 'LIA', 'LE', 'LEVEL']):
            return 'level'
        elif any(x in tag_upper for x in ['SI', 'SIA', 'SPEED', 'RPM']):
            return 'speed'
        elif any(x in tag_upper for x in ['XI', 'XIA', 'VIBR', 'VIB']):
            return 'vibration'
        elif any(x in tag_upper for x in ['ZI', 'ZIA', 'VALVE', 'POS']):
            return 'position'
        else:
            return 'unknown'
    
    def _combine_validations(self, stat_val: dict, eng_val: dict, temp_val: dict) -> dict:
        """Combine multiple validation results into overall assessment"""
        
        # Extract valid results
        valid_results = [v for v in [stat_val, eng_val, temp_val] if v.get('valid', False)]
        
        if not valid_results:
            return {
                'verdict': 'VALIDATION_FAILED',
                'confidence': 0,
                'reason': 'All validation methods failed'
            }
        
        # Collect verdicts and confidences
        verdicts = [r['verdict'] for r in valid_results]
        confidences = [r['confidence'] for r in valid_results]
        
        # Decision logic
        if 'DATA_QUALITY_ISSUE' in verdicts or 'STUCK_SENSOR' in verdicts:
            return {
                'verdict': 'FALSE_POSITIVE',
                'confidence': max(confidences),
                'reason': 'Sensor or data quality issue detected'
            }
        elif 'LIKELY_GENUINE' in verdicts:
            return {
                'verdict': 'GENUINE_ANOMALY',
                'confidence': max(confidences),
                'reason': 'Statistical analysis confirms genuine anomalies'
            }
        elif 'INVESTIGATE_FURTHER' in verdicts or 'INVESTIGATE_SENSOR' in verdicts:
            return {
                'verdict': 'REQUIRES_INVESTIGATION',
                'confidence': int(np.mean(confidences)),
                'reason': 'Mixed validation results - manual investigation needed'
            }
        else:
            return {
                'verdict': 'LIKELY_FALSE_POSITIVE',
                'confidence': int(np.mean(confidences)),
                'reason': 'Multiple validation methods suggest false positive'
            }
    
    def _print_validation_summary(self, results: list):
        """Print validation summary"""
        print("=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        
        genuine = len([r for r in results if r['validation']['verdict'] == 'GENUINE_ANOMALY'])
        false_pos = len([r for r in results if 'FALSE_POSITIVE' in r['validation']['verdict']])
        investigate = len([r for r in results if 'INVESTIGATION' in r['validation']['verdict']])
        
        print(f"Genuine anomalies: {genuine}")
        print(f"False positives: {false_pos}")
        print(f"Require investigation: {investigate}")
        print()
        
        print("RECOMMENDED ACTIONS:")
        print("-" * 40)
        
        for result in results:
            tag = result['tag']
            verdict = result['validation']['verdict']
            
            if verdict == 'GENUINE_ANOMALY':
                print(f"✓ {tag}: INVESTIGATE PROCESS CONDITION")
            elif 'FALSE_POSITIVE' in verdict:
                print(f"✗ {tag}: CHECK SENSOR/DATA QUALITY")
            elif 'INVESTIGATION' in verdict:
                print(f"? {tag}: MANUAL REVIEW REQUIRED")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PI Tag Anomaly Validator')
    parser.add_argument('--unit', default='K-31-01', help='Unit to analyze (default: K-31-01)')
    parser.add_argument('--top-tags', type=int, default=10, help='Number of top tags to validate (default: 10)')
    
    args = parser.parse_args()
    
    validator = PITagAnomalyValidator()
    validator.validate_tag_anomalies(args.unit, args.top_tags)


if __name__ == "__main__":
    main()