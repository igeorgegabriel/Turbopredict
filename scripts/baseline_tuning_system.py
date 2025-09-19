#!/usr/bin/env python3
"""
3-Month Baseline Anomaly Detection Tuning System for TURBOPREDICT X PROTEAN
Establishes proper baselines from historical data to eliminate false positives
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.covariance import EllipticEnvelope
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase


class BaselineAnomalyTuner:
    """3-month baseline analysis and anomaly detection tuning system"""
    
    def __init__(self):
        self.db = ParquetDatabase()
        self.baseline_config = {}
        self.tag_profiles = {}
        
    def analyze_3month_baseline(self, unit: str, save_config: bool = True):
        """
        Analyze 3 months of historical data to establish proper baselines
        
        Args:
            unit: Unit to analyze
            save_config: Whether to save the tuning configuration
        """
        print(f"3-MONTH BASELINE ANALYSIS - {unit}")
        print("=" * 80)
        print("Analyzing historical data to establish process-aware baselines...")
        print()
        
        # Load unit data
        df = self.db.get_unit_data(unit)
        if df.empty:
            print("ERROR: No data available for this unit")
            return None
            
        # Ensure time column is datetime
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')
            
        # Filter to last 3 months
        cutoff_date = datetime.now() - timedelta(days=90)
        baseline_df = df[df['time'] >= cutoff_date].copy()
        
        if baseline_df.empty:
            print("ERROR: No data in last 3 months")
            return None
            
        print(f"Baseline period: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
        print(f"Total baseline records: {len(baseline_df):,}")
        print()
        
        # Get unique tags
        unique_tags = baseline_df['tag'].unique()
        print(f"Analyzing {len(unique_tags)} unique PI tags...")
        print()
        
        # Analyze each tag
        tag_baselines = {}
        
        for i, tag in enumerate(unique_tags):
            if i % 50 == 0:
                print(f"Processing tag {i+1}/{len(unique_tags)}: {tag}")
                
            tag_data = baseline_df[baseline_df['tag'] == tag].copy()
            baseline = self._analyze_tag_baseline(tag, tag_data)
            
            if baseline:
                tag_baselines[tag] = baseline
                
        print(f"\nSuccessfully analyzed {len(tag_baselines)} tags")
        
        # Generate tuning configuration
        config = self._generate_tuning_config(unit, tag_baselines)
        
        # Save configuration
        if save_config:
            config_path = f"baseline_config_{unit}.json"
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2, default=str)
            print(f"Saved baseline configuration to: {config_path}")
            
        # Generate summary report
        self._generate_baseline_report(unit, tag_baselines, config)
        
        return config
    
    def _analyze_tag_baseline(self, tag: str, tag_data: pd.DataFrame) -> dict:
        """Analyze individual tag to establish baseline characteristics"""
        
        values = tag_data['value'].dropna()
        if len(values) < 100:  # Need sufficient data
            return None
            
        # Basic statistics
        mean = values.mean()
        std = values.std()
        median = values.median()
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1
        
        # Process stability analysis
        cv = std / mean * 100 if mean != 0 else 0
        
        # Determine stability category
        if cv < 2:
            stability = "VERY_STABLE"
            outlier_threshold = 4.0  # 4-sigma for very stable processes
        elif cv < 5:
            stability = "STABLE"
            outlier_threshold = 3.5  # 3.5-sigma for stable processes
        elif cv < 10:
            stability = "MODERATELY_STABLE"
            outlier_threshold = 3.0  # 3-sigma for moderately stable
        else:
            stability = "VARIABLE"
            outlier_threshold = 2.5  # 2.5-sigma for variable processes
            
        # Engineering limits based on tag type
        tag_type = self._classify_tag_type(tag)
        eng_limits = self._get_engineering_limits(tag_type, values)
        
        # Temporal analysis
        temporal_patterns = self._analyze_temporal_patterns(tag_data)
        
        # Calculate recommended thresholds
        # Use multiple methods and take the most conservative
        
        # Method 1: Statistical (process-aware sigma)
        stat_upper = mean + outlier_threshold * std
        stat_lower = mean - outlier_threshold * std
        
        # Method 2: IQR-based (more robust)
        iqr_factor = 2.0 if stability in ["VERY_STABLE", "STABLE"] else 1.5
        iqr_upper = q3 + iqr_factor * iqr
        iqr_lower = q1 - iqr_factor * iqr
        
        # Method 3: Engineering limits
        eng_upper = eng_limits.get('max_reasonable', float('inf'))
        eng_lower = eng_limits.get('min_reasonable', float('-inf'))
        
        # Take most conservative (tightest reasonable) limits
        final_upper = min(stat_upper, iqr_upper, eng_upper)
        final_lower = max(stat_lower, iqr_lower, eng_lower)
        
        # Validate against historical data
        historical_outliers = ((values < final_lower) | (values > final_upper)).sum()
        historical_rate = historical_outliers / len(values) * 100
        
        # Adjust if too many historical "outliers" (indicates bad thresholds)
        if historical_rate > 5:  # More than 5% shouldn't be outliers
            # Relax thresholds
            adjustment_factor = 1.2
            final_upper = min(stat_upper * adjustment_factor, iqr_upper * adjustment_factor, eng_upper)
            final_lower = max(stat_lower / adjustment_factor, iqr_lower / adjustment_factor, eng_lower)
            
        return {
            'tag': tag,
            'tag_type': tag_type,
            'stability': stability,
            'baseline_stats': {
                'mean': mean,
                'std': std,
                'median': median,
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
                'cv': cv,
                'count': len(values)
            },
            'thresholds': {
                'upper_limit': final_upper,
                'lower_limit': final_lower,
                'outlier_sigma': outlier_threshold
            },
            'engineering_limits': eng_limits,
            'temporal_patterns': temporal_patterns,
            'validation': {
                'historical_outlier_rate': historical_rate,
                'recommended_sensitivity': self._get_sensitivity_level(stability, historical_rate)
            }
        }
    
    def _classify_tag_type(self, tag: str) -> str:
        """Classify PI tag type from tag name"""
        tag_upper = tag.upper()
        
        if any(x in tag_upper for x in ['TI', 'TIA', 'TE', 'TEMP']):
            return 'TEMPERATURE'
        elif any(x in tag_upper for x in ['PI', 'PIA', 'PE', 'PRESS']):
            return 'PRESSURE'
        elif any(x in tag_upper for x in ['FI', 'FIA', 'FE', 'FLOW']):
            return 'FLOW'
        elif any(x in tag_upper for x in ['LI', 'LIA', 'LE', 'LEVEL']):
            return 'LEVEL'
        elif any(x in tag_upper for x in ['SI', 'SIA', 'SPEED', 'RPM']):
            return 'SPEED'
        elif any(x in tag_upper for x in ['XI', 'XIA', 'VIBR', 'VIB']):
            return 'VIBRATION'
        elif any(x in tag_upper for x in ['ZI', 'ZIA', 'VALVE', 'POS']):
            return 'POSITION'
        elif 'PERFORMANCE' in tag_upper:
            return 'PERFORMANCE'
        else:
            return 'UNKNOWN'
    
    def _get_engineering_limits(self, tag_type: str, values: pd.Series) -> dict:
        """Get reasonable engineering limits based on tag type and observed data"""
        
        data_min = values.min()
        data_max = values.max()
        data_range = data_max - data_min
        
        if tag_type == 'TEMPERATURE':
            # Temperature limits (assuming Celsius)
            return {
                'min_physical': -273,  # Absolute zero
                'max_physical': 2000,  # Very high industrial temp
                'min_reasonable': max(-50, data_min - data_range * 0.5),
                'max_reasonable': min(500, data_max + data_range * 0.5)
            }
        elif tag_type == 'PRESSURE':
            # Pressure limits
            return {
                'min_physical': 0,  # Usually no negative pressure
                'max_physical': 1000,  # Very high industrial pressure
                'min_reasonable': max(0, data_min - data_range * 0.3),
                'max_reasonable': min(200, data_max + data_range * 0.5)
            }
        elif tag_type == 'FLOW':
            # Flow limits
            return {
                'min_physical': 0,  # Usually no negative flow
                'max_physical': 50000,
                'min_reasonable': max(0, data_min - data_range * 0.2),
                'max_reasonable': data_max + data_range * 0.8
            }
        elif tag_type == 'LEVEL':
            # Level limits (usually percentage)
            return {
                'min_physical': 0,
                'max_physical': 100,
                'min_reasonable': max(0, data_min - 5),
                'max_reasonable': min(100, data_max + 5)
            }
        elif tag_type == 'SPEED':
            # Speed/RPM limits
            return {
                'min_physical': 0,
                'max_physical': 20000,
                'min_reasonable': max(0, data_min - data_range * 0.3),
                'max_reasonable': data_max + data_range * 0.5
            }
        else:
            # Generic limits
            return {
                'min_physical': float('-inf'),
                'max_physical': float('inf'),
                'min_reasonable': data_min - data_range * 0.5,
                'max_reasonable': data_max + data_range * 0.5
            }
    
    def _analyze_temporal_patterns(self, tag_data: pd.DataFrame) -> dict:
        """Analyze temporal patterns to detect sensor issues"""
        
        if len(tag_data) < 10:
            return {'insufficient_data': True}
            
        values = tag_data['value'].dropna()
        
        # Check for stuck values
        stuck_threshold = 0.001
        consecutive_same = 0
        max_consecutive = 0
        current_consecutive = 1
        
        for i in range(1, len(values)):
            if abs(values.iloc[i] - values.iloc[i-1]) < stuck_threshold:
                current_consecutive += 1
            else:
                max_consecutive = max(max_consecutive, current_consecutive)
                current_consecutive = 1
        max_consecutive = max(max_consecutive, current_consecutive)
        
        stuck_rate = max_consecutive / len(values) * 100
        
        # Check for excessive noise
        if len(values) > 1:
            diffs = np.diff(values)
            noise_level = np.std(diffs)
            signal_level = np.std(values)
            noise_ratio = noise_level / signal_level if signal_level > 0 else 0
        else:
            noise_ratio = 0
            
        return {
            'stuck_rate': stuck_rate,
            'noise_ratio': noise_ratio,
            'max_consecutive_same': max_consecutive,
            'data_quality': 'GOOD' if stuck_rate < 10 and noise_ratio < 1 else 'QUESTIONABLE'
        }
    
    def _get_sensitivity_level(self, stability: str, historical_rate: float) -> str:
        """Determine appropriate sensitivity level"""
        
        if stability in ["VERY_STABLE", "STABLE"] and historical_rate < 2:
            return "LOW_SENSITIVITY"  # Fewer false positives
        elif stability == "MODERATELY_STABLE" and historical_rate < 3:
            return "MEDIUM_SENSITIVITY"
        else:
            return "HIGH_SENSITIVITY"  # More responsive but more false positives
    
    def _generate_tuning_config(self, unit: str, tag_baselines: dict) -> dict:
        """Generate comprehensive tuning configuration"""
        
        # Categorize tags by stability
        stability_groups = {
            'VERY_STABLE': [],
            'STABLE': [],
            'MODERATELY_STABLE': [],
            'VARIABLE': []
        }
        
        tag_configs = {}
        
        for tag, baseline in tag_baselines.items():
            stability = baseline['stability']
            stability_groups[stability].append(tag)
            
            # Individual tag configuration
            tag_configs[tag] = {
                'thresholds': baseline['thresholds'],
                'sensitivity': baseline['validation']['recommended_sensitivity'],
                'tag_type': baseline['tag_type'],
                'baseline_period': '3_months',
                'last_updated': datetime.now().isoformat()
            }
        
        # Overall configuration
        config = {
            'unit': unit,
            'baseline_period_days': 90,
            'generated_date': datetime.now().isoformat(),
            'total_tags_analyzed': len(tag_baselines),
            'stability_distribution': {k: len(v) for k, v in stability_groups.items()},
            'tag_configurations': tag_configs,
            'global_settings': {
                'default_sigma_threshold': 3.0,
                'min_data_points_required': 100,
                'outlier_rate_tolerance': 5.0,  # Max 5% historical outliers acceptable
                'sensitivity_mapping': {
                    'LOW_SENSITIVITY': {'sigma': 4.0, 'contamination': 0.01},
                    'MEDIUM_SENSITIVITY': {'sigma': 3.0, 'contamination': 0.03},
                    'HIGH_SENSITIVITY': {'sigma': 2.5, 'contamination': 0.05}
                }
            }
        }
        
        return config
    
    def _generate_baseline_report(self, unit: str, tag_baselines: dict, config: dict):
        """Generate comprehensive baseline analysis report"""
        
        print("\n" + "=" * 80)
        print("BASELINE ANALYSIS SUMMARY")
        print("=" * 80)
        
        # Stability distribution
        stability_dist = config['stability_distribution']
        print(f"STABILITY DISTRIBUTION:")
        print(f"  Very Stable:      {stability_dist.get('VERY_STABLE', 0):3d} tags")
        print(f"  Stable:           {stability_dist.get('STABLE', 0):3d} tags") 
        print(f"  Moderately Stable:{stability_dist.get('MODERATELY_STABLE', 0):3d} tags")
        print(f"  Variable:         {stability_dist.get('VARIABLE', 0):3d} tags")
        print()
        
        # Tag type distribution
        tag_types = {}
        for baseline in tag_baselines.values():
            tag_type = baseline['tag_type']
            tag_types[tag_type] = tag_types.get(tag_type, 0) + 1
            
        print("TAG TYPE DISTRIBUTION:")
        for tag_type, count in sorted(tag_types.items()):
            print(f"  {tag_type:15s}: {count:3d} tags")
        print()
        
        # Sensitivity recommendations
        sensitivity_counts = {}
        for baseline in tag_baselines.values():
            sens = baseline['validation']['recommended_sensitivity']
            sensitivity_counts[sens] = sensitivity_counts.get(sens, 0) + 1
            
        print("RECOMMENDED SENSITIVITY LEVELS:")
        for sens, count in sorted(sensitivity_counts.items()):
            print(f"  {sens:20s}: {count:3d} tags")
        print()
        
        # Quality assessment
        good_quality = sum(1 for b in tag_baselines.values() 
                          if b['temporal_patterns'].get('data_quality') == 'GOOD')
        
        print("DATA QUALITY ASSESSMENT:")
        print(f"  Good Quality:     {good_quality:3d} tags")
        print(f"  Questionable:     {len(tag_baselines) - good_quality:3d} tags")
        print()
        
        print("TUNING RECOMMENDATIONS:")
        print("=" * 50)
        
        # Very stable tags
        very_stable = [tag for tag, b in tag_baselines.items() 
                      if b['stability'] == 'VERY_STABLE']
        if very_stable:
            print(f"[OK] {len(very_stable)} VERY STABLE tags - Use 4-sigma thresholds")
            print("  Example tags:", ", ".join(very_stable[:3]))
            print()
            
        # Variable tags  
        variable = [tag for tag, b in tag_baselines.items()
                   if b['stability'] == 'VARIABLE']
        if variable:
            print(f"[WARN] {len(variable)} VARIABLE tags - May need process investigation")
            print("  Example tags:", ", ".join(variable[:3]))
            print()
            
        # Poor quality tags
        poor_quality = [tag for tag, b in tag_baselines.items()
                       if b['temporal_patterns'].get('data_quality') == 'QUESTIONABLE']
        if poor_quality:
            print(f"[ISSUE] {len(poor_quality)} QUESTIONABLE QUALITY tags - Check sensors")
            print("  Example tags:", ", ".join(poor_quality[:3]))
            print()
            
        print("NEXT STEPS:")
        print("-" * 30)
        print("1. Review variable tags for process optimization opportunities")
        print("2. Investigate questionable quality tags for sensor maintenance")
        print("3. Implement tuned thresholds in anomaly detection system")
        print("4. Monitor false positive rates and adjust as needed")
        print()
        print("Configuration saved. Ready for deployment.")
        print("=" * 80)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='3-Month Baseline Anomaly Detection Tuner')
    parser.add_argument('--unit', default='K-31-01', help='Unit to analyze (default: K-31-01)')
    parser.add_argument('--no-save', action='store_true', help='Do not save configuration file')
    
    args = parser.parse_args()
    
    tuner = BaselineAnomalyTuner()
    config = tuner.analyze_3month_baseline(args.unit, save_config=not args.no_save)
    
    if config:
        print(f"\nBaseline tuning complete for {args.unit}")
        print(f"Analyzed {config['total_tags_analyzed']} tags over 90-day baseline period")
    else:
        print("ERROR: Baseline tuning failed")


if __name__ == "__main__":
    main()