#!/usr/bin/env python3
"""
Anomaly Detection Backtesting Framework for TURBOPREDICT X PROTEAN
Comprehensive testing of tuned vs original anomaly detection methods
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import matplotlib.pyplot as plt
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.tuned_anomaly_detection import TunedAnomalyDetector
from scripts.process_aware_anomaly_detector import ProcessAwareAnomalyDetector


class AnomalyBacktester:
    """Comprehensive backtesting framework for anomaly detection methods"""
    
    def __init__(self):
        self.db = ParquetDatabase()
        self.results_history = []
        
    def run_comprehensive_backtest(self, 
                                 unit: str,
                                 test_periods: List[Dict] = None,
                                 save_results: bool = True) -> Dict[str, Any]:
        """
        Run comprehensive backtest across multiple time periods
        
        Args:
            unit: Unit to test (e.g., 'K-31-01')
            test_periods: List of test periods with start/end dates
            save_results: Whether to save detailed results
            
        Returns:
            Complete backtest results
        """
        print(f"ANOMALY DETECTION BACKTEST - {unit}")
        print("=" * 80)
        print("Comprehensive testing of detection methods across multiple periods...")
        print()
        
        # Default test periods if none provided (last 30 days in weekly chunks)
        if test_periods is None:
            test_periods = self._generate_default_test_periods()
            
        # Load baseline configuration
        baseline_config_path = f"baseline_config_{unit}.json"
        if not Path(baseline_config_path).exists():
            print(f"ERROR: Baseline configuration not found: {baseline_config_path}")
            print("Run baseline analysis first: python scripts/baseline_tuning_system.py")
            return {}
            
        # Initialize detectors
        tuned_detector = ProcessAwareAnomalyDetector(baseline_config_path)
        
        # Load unit data
        df = self.db.get_unit_data(unit)
        if df.empty:
            print("ERROR: No data available for unit")
            return {}
            
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')
            
        print(f"Total available data: {len(df):,} records")
        print(f"Date range: {df['time'].min()} to {df['time'].max()}")
        print()
        
        # Run tests for each period
        period_results = []
        
        for i, period in enumerate(test_periods):
            print(f"[{i+1}/{len(test_periods)}] Testing period: {period['name']}")
            print(f"  Date range: {period['start_date']} to {period['end_date']}")
            
            # Filter data for this period
            period_df = df[
                (df['time'] >= period['start_date']) & 
                (df['time'] <= period['end_date'])
            ].copy()
            
            if period_df.empty:
                print("  SKIPPED: No data for this period")
                continue
                
            print(f"  Records: {len(period_df):,}")
            
            # Test different detection methods
            methods_results = self._test_multiple_methods(period_df, unit)
            
            period_result = {
                'period': period,
                'data_points': len(period_df),
                'methods': methods_results,
                'summary': self._calculate_period_summary(methods_results)
            }
            
            period_results.append(period_result)
            self._print_period_summary(period, methods_results)
            print()
            
        # Generate overall analysis
        overall_analysis = self._analyze_overall_performance(period_results, unit)
        
        # Save results if requested
        if save_results:
            results_file = f"backtest_results_{unit}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self._save_backtest_results(overall_analysis, results_file)
            print(f"Detailed results saved to: {results_file}")
            
        return overall_analysis
        
    def _generate_default_test_periods(self) -> List[Dict]:
        """Generate default test periods (last 4 weeks)"""
        end_date = datetime.now()
        periods = []
        
        for i in range(4):
            week_end = end_date - timedelta(days=i*7)
            week_start = week_end - timedelta(days=7)
            
            periods.append({
                'name': f'Week_{i+1}_back',
                'start_date': week_start,
                'end_date': week_end,
                'description': f'Week starting {week_start.strftime("%Y-%m-%d")}'
            })
            
        return periods
        
    def _test_multiple_methods(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Test multiple anomaly detection methods on the same data"""
        
        unique_tags = df['tag'].unique()
        methods = {}
        
        # Method 1: Original 3-sigma
        methods['original_3sigma'] = self._test_original_method(df, unique_tags)
        
        # Method 2: Conservative 4-sigma
        methods['conservative_4sigma'] = self._test_conservative_method(df, unique_tags)
        
        # Method 3: Baseline-tuned
        methods['baseline_tuned'] = self._test_tuned_method(df, unit)
        
        # Method 4: Isolation Forest (if enough data)
        if len(df) > 1000:
            methods['isolation_forest'] = self._test_isolation_forest(df, unique_tags)
            
        return methods
        
    def _test_original_method(self, df: pd.DataFrame, unique_tags: np.ndarray) -> Dict[str, Any]:
        """Test original 3-sigma method"""
        
        total_anomalies = 0
        tag_results = {}
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            values = tag_df['value'].dropna()
            
            if len(values) < 10:
                continue
                
            mean = values.mean()
            std = values.std()
            
            anomalies = ((values < mean - 3*std) | (values > mean + 3*std)).sum()
            
            if anomalies > 0:
                tag_results[tag] = {
                    'anomalies': int(anomalies),
                    'rate': float(anomalies / len(values)),
                    'thresholds': {'upper': mean + 3*std, 'lower': mean - 3*std}
                }
                total_anomalies += anomalies
                
        return {
            'method': 'original_3sigma',
            'total_anomalies': total_anomalies,
            'anomaly_rate': total_anomalies / len(df) if len(df) > 0 else 0,
            'tags_with_anomalies': len(tag_results),
            'tag_results': tag_results
        }
        
    def _test_conservative_method(self, df: pd.DataFrame, unique_tags: np.ndarray) -> Dict[str, Any]:
        """Test conservative 4-sigma method"""
        
        total_anomalies = 0
        tag_results = {}
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            values = tag_df['value'].dropna()
            
            if len(values) < 10:
                continue
                
            mean = values.mean()
            std = values.std()
            
            anomalies = ((values < mean - 4*std) | (values > mean + 4*std)).sum()
            
            if anomalies > 0:
                tag_results[tag] = {
                    'anomalies': int(anomalies),
                    'rate': float(anomalies / len(values)),
                    'thresholds': {'upper': mean + 4*std, 'lower': mean - 4*std}
                }
                total_anomalies += anomalies
                
        return {
            'method': 'conservative_4sigma',
            'total_anomalies': total_anomalies,
            'anomaly_rate': total_anomalies / len(df) if len(df) > 0 else 0,
            'tags_with_anomalies': len(tag_results),
            'tag_results': tag_results
        }
        
    def _test_tuned_method(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Test baseline-tuned method"""
        
        try:
            # Load baseline configuration
            config_path = f"baseline_config_{unit}.json"
            if not Path(config_path).exists():
                return {'method': 'baseline_tuned', 'error': 'No baseline config'}
                
            with open(config_path, 'r') as f:
                baseline_config = json.load(f)
                
            unique_tags = df['tag'].unique()
            total_anomalies = 0
            tag_results = {}
            
            for tag in unique_tags:
                tag_df = df[df['tag'] == tag].copy()
                values = tag_df['value'].dropna()
                
                if len(values) < 5:
                    continue
                    
                # Get tag-specific configuration
                tag_config = baseline_config.get('tag_configurations', {}).get(tag)
                
                if tag_config:
                    # Use baseline thresholds
                    thresholds = tag_config['thresholds']
                    upper_limit = thresholds['upper_limit']
                    lower_limit = thresholds['lower_limit']
                else:
                    # Conservative fallback
                    mean = values.mean()
                    std = values.std()
                    upper_limit = mean + 4 * std
                    lower_limit = mean - 4 * std
                    
                anomalies = ((values < lower_limit) | (values > upper_limit)).sum()
                
                if anomalies > 0:
                    tag_results[tag] = {
                        'anomalies': int(anomalies),
                        'rate': float(anomalies / len(values)),
                        'thresholds': {'upper': upper_limit, 'lower': lower_limit},
                        'baseline_tuned': tag_config is not None
                    }
                    total_anomalies += anomalies
                    
            return {
                'method': 'baseline_tuned',
                'total_anomalies': total_anomalies,
                'anomaly_rate': total_anomalies / len(df) if len(df) > 0 else 0,
                'tags_with_anomalies': len(tag_results),
                'tag_results': tag_results
            }
            
        except Exception as e:
            return {'method': 'baseline_tuned', 'error': str(e)}
            
    def _test_isolation_forest(self, df: pd.DataFrame, unique_tags: np.ndarray) -> Dict[str, Any]:
        """Test Isolation Forest method"""
        
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.preprocessing import StandardScaler
            
            # Prepare data for Isolation Forest
            tag_data = []
            for tag in unique_tags[:20]:  # Limit to top 20 tags for performance
                tag_values = df[df['tag'] == tag]['value'].dropna()
                if len(tag_values) > 0:
                    tag_data.append(tag_values.mean())
                    
            if len(tag_data) < 5:
                return {'method': 'isolation_forest', 'error': 'Insufficient data'}
                
            # Reshape for sklearn
            X = np.array(tag_data).reshape(-1, 1)
            
            # Scale data
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Run Isolation Forest
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            anomaly_labels = iso_forest.fit_predict(X_scaled)
            
            anomalies = np.sum(anomaly_labels == -1)
            
            return {
                'method': 'isolation_forest',
                'total_anomalies': int(anomalies),
                'anomaly_rate': float(anomalies / len(X_scaled)),
                'tags_analyzed': len(tag_data)
            }
            
        except Exception as e:
            return {'method': 'isolation_forest', 'error': str(e)}
            
    def _calculate_period_summary(self, methods_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics for a test period"""
        
        summary = {
            'methods_tested': len([m for m in methods_results.values() if 'error' not in m]),
            'anomaly_rates': {},
            'consistency_score': 0.0
        }
        
        rates = []
        for method, result in methods_results.items():
            if 'error' not in result:
                rate = result.get('anomaly_rate', 0)
                summary['anomaly_rates'][method] = rate
                rates.append(rate)
                
        # Calculate consistency (lower std deviation = more consistent)
        if len(rates) > 1:
            summary['consistency_score'] = 1.0 / (1.0 + np.std(rates))
        else:
            summary['consistency_score'] = 1.0
            
        return summary
        
    def _print_period_summary(self, period: Dict, methods_results: Dict[str, Any]):
        """Print summary for a single test period"""
        
        print("  RESULTS:")
        for method, result in methods_results.items():
            if 'error' in result:
                print(f"    {method:20s}: ERROR - {result['error']}")
            else:
                anomalies = result.get('total_anomalies', 0)
                rate = result.get('anomaly_rate', 0) * 100
                print(f"    {method:20s}: {anomalies:6,} anomalies ({rate:5.2f}%)")
                
    def _analyze_overall_performance(self, period_results: List[Dict], unit: str) -> Dict[str, Any]:
        """Analyze overall performance across all test periods"""
        
        print("=" * 80)
        print("OVERALL BACKTEST ANALYSIS")
        print("=" * 80)
        
        # Collect all method results
        method_performance = {}
        
        for period_result in period_results:
            for method, result in period_result['methods'].items():
                if 'error' not in result:
                    if method not in method_performance:
                        method_performance[method] = {
                            'anomaly_rates': [],
                            'total_anomalies': [],
                            'periods_tested': 0
                        }
                    
                    method_performance[method]['anomaly_rates'].append(result.get('anomaly_rate', 0))
                    method_performance[method]['total_anomalies'].append(result.get('total_anomalies', 0))
                    method_performance[method]['periods_tested'] += 1
                    
        # Calculate statistics for each method
        method_stats = {}
        for method, perf in method_performance.items():
            rates = perf['anomaly_rates']
            totals = perf['total_anomalies']
            
            method_stats[method] = {
                'avg_anomaly_rate': np.mean(rates) * 100,
                'std_anomaly_rate': np.std(rates) * 100,
                'min_anomaly_rate': np.min(rates) * 100,
                'max_anomaly_rate': np.max(rates) * 100,
                'avg_total_anomalies': np.mean(totals),
                'consistency_score': 1.0 / (1.0 + np.std(rates)) if np.std(rates) > 0 else 1.0,
                'periods_tested': perf['periods_tested']
            }
            
        # Print performance comparison
        print("METHOD PERFORMANCE COMPARISON:")
        print("-" * 60)
        print(f"{'Method':<20} {'Avg Rate %':<10} {'Consistency':<12} {'Periods':<8}")
        print("-" * 60)
        
        for method, stats in method_stats.items():
            avg_rate = stats['avg_anomaly_rate']
            consistency = stats['consistency_score']
            periods = stats['periods_tested']
            
            print(f"{method:<20} {avg_rate:>9.2f} {consistency:>11.3f} {periods:>7}")
            
        print()
        
        # Determine best method
        best_method = self._determine_best_method(method_stats)
        print(f"RECOMMENDED METHOD: {best_method['method']}")
        print(f"REASON: {best_method['reason']}")
        print()
        
        # Generate recommendations
        recommendations = self._generate_recommendations(method_stats, period_results)
        
        print("RECOMMENDATIONS:")
        print("-" * 40)
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        print()
        
        return {
            'unit': unit,
            'test_summary': {
                'periods_tested': len(period_results),
                'methods_compared': len(method_stats),
                'total_data_points': sum(p['data_points'] for p in period_results)
            },
            'method_performance': method_stats,
            'best_method': best_method,
            'recommendations': recommendations,
            'period_results': period_results,
            'test_timestamp': datetime.now().isoformat()
        }
        
    def _determine_best_method(self, method_stats: Dict[str, Any]) -> Dict[str, str]:
        """Determine the best performing method"""
        
        # Scoring criteria:
        # 1. Lower anomaly rate is better (fewer false positives)
        # 2. Higher consistency is better
        # 3. Baseline-tuned gets bonus points
        
        best_score = -1
        best_method = None
        best_reason = ""
        
        for method, stats in method_stats.items():
            # Normalize scores (0-1 scale)
            anomaly_score = max(0, 1 - stats['avg_anomaly_rate'] / 10)  # Lower is better
            consistency_score = stats['consistency_score']  # Higher is better
            
            # Bonus for baseline-tuned method
            baseline_bonus = 0.2 if 'baseline' in method else 0
            
            # Combined score
            total_score = (anomaly_score * 0.4 + consistency_score * 0.4 + baseline_bonus)
            
            if total_score > best_score:
                best_score = total_score
                best_method = method
                
                # Generate reason
                if 'baseline' in method:
                    best_reason = f"Process-aware calibration with {stats['avg_anomaly_rate']:.2f}% avg anomaly rate"
                elif stats['avg_anomaly_rate'] < 1:
                    best_reason = f"Low false positive rate ({stats['avg_anomaly_rate']:.2f}%)"
                else:
                    best_reason = f"Best consistency score ({stats['consistency_score']:.3f})"
                    
        return {
            'method': best_method or 'baseline_tuned',
            'reason': best_reason or 'Default recommendation',
            'score': best_score
        }
        
    def _generate_recommendations(self, method_stats: Dict[str, Any], period_results: List[Dict]) -> List[str]:
        """Generate actionable recommendations"""
        
        recommendations = []
        
        # Check if baseline tuning is available and performing well
        if 'baseline_tuned' in method_stats:
            baseline_stats = method_stats['baseline_tuned']
            if baseline_stats['avg_anomaly_rate'] < 2:
                recommendations.append("Continue using baseline-tuned detection - excellent performance")
            else:
                recommendations.append("Review baseline configuration - anomaly rate higher than expected")
        else:
            recommendations.append("Generate baseline configuration using baseline_tuning_system.py")
            
        # Check for high variability
        high_variability_methods = [
            method for method, stats in method_stats.items()
            if stats['consistency_score'] < 0.5
        ]
        
        if high_variability_methods:
            recommendations.append(f"Investigate variability in: {', '.join(high_variability_methods)}")
            
        # Check for very high anomaly rates
        high_rate_methods = [
            method for method, stats in method_stats.items()
            if stats['avg_anomaly_rate'] > 10
        ]
        
        if high_rate_methods:
            recommendations.append(f"Methods with high false positive rates: {', '.join(high_rate_methods)}")
            
        # Data quality recommendations
        recommendations.append("Schedule monthly baseline recalibration")
        recommendations.append("Monitor detection performance against operator feedback")
        
        return recommendations
        
    def _save_backtest_results(self, results: Dict[str, Any], filename: str):
        """Save detailed backtest results to JSON file"""
        
        # Convert numpy types and remove circular references
        def convert_numpy(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, datetime):
                return obj.isoformat()
            return obj
            
        # Create a clean copy without circular references
        clean_results = {
            'unit': results.get('unit'),
            'test_summary': results.get('test_summary'),
            'method_performance': results.get('method_performance'),
            'best_method': results.get('best_method'),
            'recommendations': results.get('recommendations'),
            'test_timestamp': results.get('test_timestamp')
            # Skip period_results to avoid circular references
        }
            
        # Deep convert the results
        import json
        json_str = json.dumps(clean_results, default=convert_numpy, indent=2)
        
        with open(filename, 'w') as f:
            f.write(json_str)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Anomaly Detection Backtesting Framework')
    parser.add_argument('--unit', default='K-31-01', help='Unit to test (default: K-31-01)')
    parser.add_argument('--weeks', type=int, default=4, help='Number of weeks to test (default: 4)')
    parser.add_argument('--no-save', action='store_true', help='Do not save detailed results')
    
    args = parser.parse_args()
    
    backtester = AnomalyBacktester()
    
    # Generate test periods
    test_periods = []
    end_date = datetime.now()
    
    for i in range(args.weeks):
        week_end = end_date - timedelta(days=i*7)
        week_start = week_end - timedelta(days=7)
        
        test_periods.append({
            'name': f'Week_{i+1}',
            'start_date': week_start,
            'end_date': week_end,
            'description': f'Week {i+1} back from {week_end.strftime("%Y-%m-%d")}'
        })
    
    # Run backtest
    results = backtester.run_comprehensive_backtest(
        unit=args.unit,
        test_periods=test_periods,
        save_results=not args.no_save
    )
    
    if results:
        print("Backtest completed successfully!")
        print(f"Best method: {results['best_method']['method']}")
        print(f"Tested {results['test_summary']['periods_tested']} periods")
        print(f"Analyzed {results['test_summary']['total_data_points']:,} data points")
    else:
        print("Backtest failed!")


if __name__ == "__main__":
    main()