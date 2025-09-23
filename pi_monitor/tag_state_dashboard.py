#!/usr/bin/env python3
"""
Tag State Dashboard - Comprehensive tag status and health monitoring
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
from pathlib import Path

from .parquet_database import ParquetDatabase
from .smart_anomaly_detection import smart_anomaly_detection


class TagStateDashboard:
    """Comprehensive tag state monitoring and dashboard"""

    def __init__(self):
        self.db = ParquetDatabase()

    def get_comprehensive_tag_states(self, unit: str, hours_back: int = 24) -> Dict[str, Any]:
        """Get comprehensive state information for all tags in a unit

        Args:
            unit: Unit identifier
            hours_back: Hours of recent data to analyze

        Returns:
            Comprehensive tag state information
        """

        print(f"TAG STATE DASHBOARD - {unit}")
        print("=" * 60)

        # Load unit data
        df = self.db.get_unit_data(unit)
        if df.empty:
            return {'error': 'No data available for unit'}

        # Filter to recent period
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        if 'time' in df.columns:
            df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()

        if df.empty:
            return {'error': f'No data in last {hours_back} hours'}

        # Get unit freshness info
        freshness_info = self.db.get_data_freshness_info(unit)

        # Run anomaly detection
        anomaly_results = smart_anomaly_detection(df, unit)

        # Analyze each tag
        unique_tags = df['tag'].unique()
        tag_states = {}

        for tag in unique_tags:
            tag_data = df[df['tag'] == tag].copy()
            if tag_data.empty:
                continue

            # Tag health metrics
            tag_state = self._analyze_tag_health(tag, tag_data, anomaly_results)
            tag_states[tag] = tag_state

        # Summary statistics
        total_tags = len(tag_states)
        healthy_tags = sum(1 for state in tag_states.values() if state['health_status'] == 'HEALTHY')
        warning_tags = sum(1 for state in tag_states.values() if state['health_status'] == 'WARNING')
        critical_tags = sum(1 for state in tag_states.values() if state['health_status'] == 'CRITICAL')

        return {
            'unit': unit,
            'analysis_period_hours': hours_back,
            'timestamp': datetime.now().isoformat(),
            'unit_freshness': freshness_info,
            'unit_status': anomaly_results.get('unit_status', {}),
            'summary': {
                'total_tags': total_tags,
                'healthy_tags': healthy_tags,
                'warning_tags': warning_tags,
                'critical_tags': critical_tags,
                'health_percentage': (healthy_tags / total_tags * 100) if total_tags > 0 else 0
            },
            'tag_states': tag_states,
            'anomaly_summary': {
                'total_anomalies': anomaly_results.get('total_anomalies', 0),
                'anomaly_rate': anomaly_results.get('anomaly_rate', 0) * 100,
                'detection_method': anomaly_results.get('method', 'Unknown')
            }
        }

    def _analyze_tag_health(self, tag: str, tag_data: pd.DataFrame, anomaly_results: Dict) -> Dict[str, Any]:
        """Analyze individual tag health and state"""

        # Basic statistics
        values = tag_data['value'].dropna()
        if len(values) == 0:
            return {
                'health_status': 'NO_DATA',
                'message': 'No valid data points',
                'data_points': 0
            }

        # Data quality metrics
        data_points = len(values)
        missing_points = len(tag_data) - data_points
        missing_rate = (missing_points / len(tag_data) * 100) if len(tag_data) > 0 else 0

        # Value statistics
        mean_val = float(values.mean())
        std_val = float(values.std())
        min_val = float(values.min())
        max_val = float(values.max())

        # Detect flat lining (same value for extended periods)
        is_flatlining = self._detect_flatlining(values)

        # Detect value jumps/spikes
        has_spikes = self._detect_spikes(values)

        # Get anomaly information for this tag
        by_tag = anomaly_results.get('by_tag', {})
        tag_anomalies = by_tag.get(tag, {})
        anomaly_count = tag_anomalies.get('count', 0)
        anomaly_rate = tag_anomalies.get('rate', 0) * 100

        # Data freshness
        latest_time = pd.to_datetime(tag_data['time']).max()
        data_age_hours = (datetime.now() - latest_time).total_seconds() / 3600

        # Determine overall health status
        health_status, health_message = self._determine_tag_health(
            missing_rate, is_flatlining, has_spikes, anomaly_rate, data_age_hours
        )

        return {
            'tag': tag,
            'health_status': health_status,
            'message': health_message,
            'data_quality': {
                'data_points': data_points,
                'missing_points': missing_points,
                'missing_rate': round(missing_rate, 2),
                'data_age_hours': round(data_age_hours, 2)
            },
            'value_statistics': {
                'mean': round(mean_val, 3),
                'std': round(std_val, 3),
                'min': round(min_val, 3),
                'max': round(max_val, 3),
                'range': round(max_val - min_val, 3)
            },
            'anomaly_info': {
                'anomaly_count': anomaly_count,
                'anomaly_rate': round(anomaly_rate, 2),
                'method': tag_anomalies.get('method', 'None')
            },
            'behavioral_flags': {
                'is_flatlining': is_flatlining,
                'has_spikes': has_spikes,
                'high_variance': std_val > (abs(mean_val) * 0.3) if abs(mean_val) > 0.01 else False
            }
        }

    def _detect_flatlining(self, values: pd.Series, threshold_hours: float = 4.0) -> bool:
        """Detect if tag is flat-lining (same value for extended period)"""
        if len(values) < 10:
            return False

        # Check if most recent values are identical
        recent_values = values.tail(min(50, len(values)))
        unique_recent = recent_values.nunique()

        # If less than 3 unique values in recent data, likely flat-lining
        return unique_recent <= 2

    def _detect_spikes(self, values: pd.Series, sigma_threshold: float = 3.0) -> bool:
        """Detect significant value spikes"""
        if len(values) < 10:
            return False

        mean_val = values.mean()
        std_val = values.std()

        if std_val <= 0:
            return False

        # Z-score based spike detection
        z_scores = np.abs((values - mean_val) / std_val)
        spike_count = (z_scores > sigma_threshold).sum()

        # If more than 5% of points are spikes, flag as having spikes
        return (spike_count / len(values)) > 0.05

    def _determine_tag_health(self, missing_rate: float, is_flatlining: bool,
                             has_spikes: bool, anomaly_rate: float, data_age_hours: float) -> tuple:
        """Determine overall tag health status"""

        issues = []

        # Check data age
        if data_age_hours > 24:
            issues.append(f"Data age: {data_age_hours:.1f}h")

        # Check missing data
        if missing_rate > 50:
            issues.append(f"Missing data: {missing_rate:.1f}%")

        # Check behavioral issues
        if is_flatlining:
            issues.append("Flat-lining detected")

        if has_spikes:
            issues.append("Value spikes detected")

        # Check anomaly rate
        if anomaly_rate > 10:
            issues.append(f"High anomaly rate: {anomaly_rate:.1f}%")
        elif anomaly_rate > 5:
            issues.append(f"Elevated anomaly rate: {anomaly_rate:.1f}%")

        # Determine status
        if not issues:
            return 'HEALTHY', 'Tag operating normally'
        elif len(issues) == 1 and anomaly_rate < 10:
            return 'WARNING', f"Minor issues: {', '.join(issues)}"
        else:
            return 'CRITICAL', f"Multiple issues: {', '.join(issues)}"

    def print_tag_state_report(self, unit: str, hours_back: int = 24) -> None:
        """Print comprehensive tag state report"""

        states = self.get_comprehensive_tag_states(unit, hours_back)

        if 'error' in states:
            print(f"Error: {states['error']}")
            return

        # Print summary
        summary = states['summary']
        print(f"\\nSUMMARY ({hours_back}h analysis):")
        print(f"  Total Tags: {summary['total_tags']}")
        print(f"  Healthy: {summary['healthy_tags']} ({summary['health_percentage']:.1f}%)")
        print(f"  Warning: {summary['warning_tags']}")
        print(f"  Critical: {summary['critical_tags']}")

        # Print unit status
        unit_status = states['unit_status']
        print(f"\\nUNIT STATUS:")
        print(f"  Status: {unit_status.get('status', 'UNKNOWN')}")
        print(f"  Message: {unit_status.get('message', 'N/A')}")

        # Print tag details
        print(f"\\nTAG HEALTH DETAILS:")
        print(f"{'Tag':<30} {'Status':<10} {'Anomalies':<12} {'Missing%':<10} {'Issues'}")
        print("-" * 100)

        tag_states = states['tag_states']
        for tag, state in sorted(tag_states.items()):
            status = state['health_status']
            anomaly_count = state['anomaly_info']['anomaly_count']
            anomaly_rate = state['anomaly_info']['anomaly_rate']
            missing_rate = state['data_quality']['missing_rate']

            # Truncate long tag names
            display_tag = tag[:29] if len(tag) <= 29 else tag[:26] + "..."

            # Color status
            status_display = status
            if status == 'HEALTHY':
                status_display = f"✓ {status}"
            elif status == 'WARNING':
                status_display = f"⚠ {status}"
            elif status == 'CRITICAL':
                status_display = f"✗ {status}"

            anomaly_display = f"{anomaly_count} ({anomaly_rate:.1f}%)"

            print(f"{display_tag:<30} {status_display:<10} {anomaly_display:<12} {missing_rate:<10.1f} {state['message'][:40]}")


def main():
    """CLI entry point for tag state dashboard"""
    import sys

    if len(sys.argv) > 1:
        unit = sys.argv[1]
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
    else:
        unit = "K-31-01"  # Default unit
        hours = 24

    dashboard = TagStateDashboard()
    dashboard.print_tag_state_report(unit, hours)


if __name__ == "__main__":
    main()