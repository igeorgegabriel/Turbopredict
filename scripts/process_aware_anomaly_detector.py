#!/usr/bin/env python3
"""
Process-Aware Anomaly Detector for TURBOPREDICT X PROTEAN
Uses 3-month baseline tuning to eliminate false positives
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase


class ProcessAwareAnomalyDetector:
    """Anomaly detector calibrated with 3-month baseline analysis"""
    
    def __init__(self, baseline_config_path: str = None):
        self.db = ParquetDatabase()
        self.baseline_config = None
        
        if baseline_config_path and Path(baseline_config_path).exists():
            with open(baseline_config_path, 'r') as f:
                self.baseline_config = json.load(f)
                print(f"Loaded baseline configuration from: {baseline_config_path}")
        else:
            print("WARNING: No baseline configuration loaded. Using default thresholds.")
    
    def detect_anomalies_tuned(self, unit: str, hours_back: int = 24) -> dict:
        """
        Detect anomalies using process-aware tuned thresholds
        
        Args:
            unit: Unit to analyze
            hours_back: Hours of recent data to analyze
            
        Returns:
            dict: Anomaly analysis results with reduced false positives
        """
        print(f"PROCESS-AWARE ANOMALY DETECTION - {unit}")
        print("=" * 80)
        print("Using 3-month baseline calibration to eliminate false positives...")
        print()
        
        # Load recent data
        df = self.db.get_unit_data(unit)
        if df.empty:
            return {'error': 'No data available'}
            
        # Filter to recent time period
        if 'time' in df.columns:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()
            
        if df.empty:
            return {'error': f'No data in last {hours_back} hours'}
            
        print(f"Analyzing {len(df):,} records from last {hours_back} hours")
        
        # Get unique tags
        unique_tags = df['tag'].unique()
        print(f"Processing {len(unique_tags)} PI tags with baseline calibration...")
        print()
        
        # Analyze each tag with tuned thresholds
        tag_anomalies = {}
        total_anomalies = 0
        total_records = 0
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            tag_analysis = self._detect_tag_anomalies_tuned(tag, tag_df)
            
            if tag_analysis and tag_analysis['anomaly_count'] > 0:
                tag_anomalies[tag] = tag_analysis
                total_anomalies += tag_analysis['anomaly_count']
                
            total_records += len(tag_df)
            
        # Calculate overall statistics
        overall_anomaly_rate = total_anomalies / total_records * 100 if total_records > 0 else 0
        
        print(f"TUNED ANOMALY DETECTION RESULTS:")
        print(f"Total records processed: {total_records:,}")
        print(f"Total anomalies detected: {total_anomalies:,}")
        print(f"Overall anomaly rate: {overall_anomaly_rate:.2f}%")
        print(f"Tags with anomalies: {len(tag_anomalies)}")
        print()
        
        # Sort tags by anomaly count
        sorted_anomalies = sorted(tag_anomalies.items(), 
                                key=lambda x: x[1]['anomaly_count'], 
                                reverse=True)
        
        # Print top anomalous tags
        if sorted_anomalies:
            print("TOP ANOMALOUS TAGS (Process-Aware Detection):")
            print("-" * 60)
            print(f"{'Rank':<5} {'Tag':<35} {'Anomalies':<10} {'Rate %':<8} {'Confidence'}")
            print("-" * 60)
            
            for i, (tag, analysis) in enumerate(sorted_anomalies[:15]):
                rate = analysis['anomaly_rate']
                count = analysis['anomaly_count']
                confidence = analysis['confidence']
                
                print(f"{i+1:<5} {tag:<35} {count:<10} {rate:<8.2f} {confidence}")
                
        return {
            'unit': unit,
            'analysis_period_hours': hours_back,
            'total_records': total_records,
            'total_anomalies': total_anomalies,
            'overall_anomaly_rate': overall_anomaly_rate,
            'anomalous_tags': len(tag_anomalies),
            'tag_anomalies': tag_anomalies,
            'baseline_calibrated': self.baseline_config is not None,
            'detection_timestamp': datetime.now().isoformat()
        }
    
    def _detect_tag_anomalies_tuned(self, tag: str, tag_df: pd.DataFrame) -> dict:
        """Detect anomalies for individual tag using tuned thresholds"""
        
        values = tag_df['value'].dropna()
        if len(values) < 10:
            return None
            
        # Get baseline configuration for this tag
        tag_config = None
        if self.baseline_config and 'tag_configurations' in self.baseline_config:
            tag_config = self.baseline_config['tag_configurations'].get(tag)
            
        if tag_config:
            # Use tuned thresholds from baseline analysis
            thresholds = tag_config['thresholds']
            upper_limit = thresholds['upper_limit']
            lower_limit = thresholds['lower_limit']
            sensitivity = tag_config['sensitivity']
            
            # Process-aware anomaly detection
            anomalies = ((values < lower_limit) | (values > upper_limit))
            anomaly_count = anomalies.sum()
            confidence = "HIGH (Baseline-Tuned)"
            
        else:
            # Fallback to conservative default thresholds
            mean = values.mean()
            std = values.std()
            
            # Use conservative 4-sigma for unknown tags
            upper_limit = mean + 4 * std
            lower_limit = mean - 4 * std
            
            anomalies = ((values < lower_limit) | (values > upper_limit))
            anomaly_count = anomalies.sum()
            confidence = "MEDIUM (Default Conservative)"
            
        anomaly_rate = anomaly_count / len(values) * 100
        
        # Additional validation - check if anomalies are clustered (sensor issue)
        if anomaly_count > 0:
            anomaly_indices = anomalies[anomalies].index
            
            # Check for consecutive anomalies (possible sensor malfunction)
            consecutive_count = 0
            max_consecutive = 0
            
            for i in range(1, len(anomaly_indices)):
                if anomaly_indices[i] - anomaly_indices[i-1] == 1:
                    consecutive_count += 1
                else:
                    max_consecutive = max(max_consecutive, consecutive_count)
                    consecutive_count = 0
            max_consecutive = max(max_consecutive, consecutive_count)
            
            # If more than 50% of anomalies are consecutive, likely sensor issue
            if max_consecutive > anomaly_count * 0.5:
                confidence = "LOW (Possible Sensor Issue)"
                
        # Get anomaly timestamps for reporting
        anomaly_timestamps = []
        if 'time' in tag_df.columns and anomaly_count > 0:
            anomaly_times = tag_df[anomalies]['time'].tolist()
            anomaly_timestamps = [t.isoformat() if hasattr(t, 'isoformat') else str(t) 
                                 for t in anomaly_times[:10]]  # Limit to first 10
            
        return {
            'tag': tag,
            'anomaly_count': int(anomaly_count),
            'total_records': len(values),
            'anomaly_rate': float(anomaly_rate),
            'confidence': confidence,
            'thresholds_used': {
                'upper_limit': float(upper_limit),
                'lower_limit': float(lower_limit)
            },
            'baseline_tuned': tag_config is not None,
            'anomaly_timestamps': anomaly_timestamps,
            'value_statistics': {
                'mean': float(values.mean()),
                'std': float(values.std()),
                'min': float(values.min()),
                'max': float(values.max())
            }
        }
    
    def compare_detection_methods(self, unit: str, hours_back: int = 24) -> dict:
        """
        Compare tuned vs untuned anomaly detection to show improvement
        """
        print(f"ANOMALY DETECTION COMPARISON - {unit}")
        print("=" * 80)
        print("Comparing baseline-tuned vs original detection methods...")
        print()
        
        # Get recent data
        df = self.db.get_unit_data(unit)
        if df.empty:
            return {'error': 'No data available'}
            
        # Filter to recent time period
        if 'time' in df.columns:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()
            
        if df.empty:
            return {'error': f'No data in last {hours_back} hours'}
            
        unique_tags = df['tag'].unique()
        
        # Run both detection methods
        tuned_results = {}
        original_results = {}
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            values = tag_df['value'].dropna()
            
            if len(values) < 10:
                continue
                
            # Method 1: Tuned detection
            tuned = self._detect_tag_anomalies_tuned(tag, tag_df)
            if tuned:
                tuned_results[tag] = tuned
                
            # Method 2: Original (3-sigma) detection
            mean = values.mean()
            std = values.std()
            original_anomalies = ((values < mean - 3*std) | (values > mean + 3*std)).sum()
            original_rate = original_anomalies / len(values) * 100
            
            original_results[tag] = {
                'anomaly_count': int(original_anomalies),
                'anomaly_rate': float(original_rate)
            }
        
        # Calculate comparison statistics
        tuned_total = sum(r['anomaly_count'] for r in tuned_results.values())
        original_total = sum(r['anomaly_count'] for r in original_results.values())
        
        total_records = sum(len(df[df['tag'] == tag]) for tag in unique_tags)
        
        tuned_rate = tuned_total / total_records * 100 if total_records > 0 else 0
        original_rate = original_total / total_records * 100 if total_records > 0 else 0
        
        reduction = ((original_total - tuned_total) / original_total * 100) if original_total > 0 else 0
        
        print("COMPARISON RESULTS:")
        print("=" * 50)
        print(f"Original (3-sigma) method:")
        print(f"  Total anomalies: {original_total:,}")
        print(f"  Anomaly rate: {original_rate:.2f}%")
        print()
        print(f"Tuned (baseline-calibrated) method:")
        print(f"  Total anomalies: {tuned_total:,}")
        print(f"  Anomaly rate: {tuned_rate:.2f}%")
        print()
        print(f"IMPROVEMENT:")
        print(f"  False positive reduction: {reduction:.1f}%")
        print(f"  Anomaly count reduction: {original_total - tuned_total:,}")
        print()
        
        if reduction > 50:
            print("[EXCELLENT] Significant false positive reduction achieved")
        elif reduction > 25:
            print("[GOOD] Meaningful false positive reduction")
        elif reduction > 0:
            print("[MODEST] Some false positive reduction")
        else:
            print("[WARNING] No reduction - baseline may need refinement")
            
        return {
            'original_method': {
                'total_anomalies': original_total,
                'anomaly_rate': original_rate
            },
            'tuned_method': {
                'total_anomalies': tuned_total,
                'anomaly_rate': tuned_rate
            },
            'improvement': {
                'false_positive_reduction_percent': reduction,
                'anomaly_count_reduction': original_total - tuned_total
            }
        }


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process-Aware Anomaly Detector')
    parser.add_argument('--unit', default='K-31-01', help='Unit to analyze (default: K-31-01)')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to analyze (default: 24)')
    parser.add_argument('--config', help='Path to baseline configuration file')
    parser.add_argument('--compare', action='store_true', help='Compare tuned vs original detection')
    
    args = parser.parse_args()
    
    # Look for baseline config file if not specified
    config_path = args.config
    if not config_path:
        config_path = f"baseline_config_{args.unit}.json"
        
    detector = ProcessAwareAnomalyDetector(config_path)
    
    if args.compare:
        results = detector.compare_detection_methods(args.unit, args.hours)
    else:
        results = detector.detect_anomalies_tuned(args.unit, args.hours)
    
    print("\nDetection complete.")


if __name__ == "__main__":
    main()