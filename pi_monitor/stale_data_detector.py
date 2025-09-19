#!/usr/bin/env python3
"""
Stale Data Detection Module for TURBOPREDICT X PROTEAN
Identifies tags with stale data and provides visual warnings in plots
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class StaleDataDetector:
    """Detects and reports stale data in PI tags"""

    def __init__(self, max_age_hours: float = 24.0):
        """
        Initialize stale data detector

        Args:
            max_age_hours: Hours after which data is considered stale (default: 24)
        """
        self.max_age_hours = max_age_hours
        self.max_age_delta = timedelta(hours=max_age_hours)

    def analyze_tag_freshness(self, df: pd.DataFrame, current_time: datetime = None) -> Dict[str, Any]:
        """
        Analyze data freshness for all tags in dataframe

        Args:
            df: DataFrame with 'tag', 'time', 'value' columns
            current_time: Reference time (defaults to now)

        Returns:
            Dict with freshness analysis results
        """
        if current_time is None:
            current_time = datetime.now()

        if df.empty or 'tag' not in df.columns or 'time' not in df.columns:
            return {'error': 'Invalid dataframe structure'}

        # Ensure time column is datetime
        df = df.copy()
        df['time'] = pd.to_datetime(df['time'])

        results = {
            'analysis_time': current_time,
            'max_age_hours': self.max_age_hours,
            'tags_analyzed': 0,
            'stale_tags': [],
            'fresh_tags': [],
            'tag_details': {},
            'summary': {}
        }

        try:
            # Group by tag and analyze each
            for tag, tag_data in df.groupby('tag'):
                if tag_data.empty:
                    continue

                results['tags_analyzed'] += 1

                # Get latest timestamp for this tag
                latest_time = tag_data['time'].max()
                age_delta = current_time - latest_time.replace(tzinfo=None)
                age_hours = age_delta.total_seconds() / 3600

                # Calculate data statistics
                data_span = tag_data['time'].max() - tag_data['time'].min()
                data_span_days = data_span.total_seconds() / 86400

                tag_info = {
                    'tag': str(tag),
                    'latest_time': latest_time,
                    'age_hours': age_hours,
                    'age_days': age_hours / 24,
                    'is_stale': age_hours > self.max_age_hours,
                    'record_count': len(tag_data),
                    'data_span_days': data_span_days,
                    'earliest_time': tag_data['time'].min(),
                    'staleness_level': self._get_staleness_level(age_hours)
                }

                results['tag_details'][str(tag)] = tag_info

                if tag_info['is_stale']:
                    results['stale_tags'].append(tag_info)
                else:
                    results['fresh_tags'].append(tag_info)

            # Generate summary statistics
            results['summary'] = self._generate_summary(results)

        except Exception as e:
            logger.error(f"Error analyzing tag freshness: {e}")
            results['error'] = str(e)

        return results

    def _get_staleness_level(self, age_hours: float) -> str:
        """Categorize staleness level"""
        if age_hours <= self.max_age_hours:
            return 'FRESH'
        elif age_hours <= 48:
            return 'MILDLY_STALE'
        elif age_hours <= 168:  # 1 week
            return 'STALE'
        else:
            return 'SEVERELY_STALE'

    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics"""
        total_tags = results['tags_analyzed']
        stale_count = len(results['stale_tags'])
        fresh_count = len(results['fresh_tags'])

        summary = {
            'total_tags': total_tags,
            'fresh_tags': fresh_count,
            'stale_tags': stale_count,
            'freshness_percentage': (fresh_count / total_tags * 100) if total_tags > 0 else 0,
            'staleness_distribution': {}
        }

        # Count by staleness level
        staleness_levels = ['FRESH', 'MILDLY_STALE', 'STALE', 'SEVERELY_STALE']
        for level in staleness_levels:
            count = sum(1 for tag in results['tag_details'].values()
                       if tag.get('staleness_level') == level)
            summary['staleness_distribution'][level] = count

        return summary

    def get_stale_tags_for_unit(self, df: pd.DataFrame, unit: str) -> List[Dict[str, Any]]:
        """
        Get stale tags specifically for a unit

        Args:
            df: DataFrame with unit data
            unit: Unit identifier

        Returns:
            List of stale tag information
        """
        analysis = self.analyze_tag_freshness(df)
        return analysis.get('stale_tags', [])

    def generate_stale_data_report(self, df: pd.DataFrame, unit: str = None) -> str:
        """
        Generate a text report of stale data issues

        Args:
            df: DataFrame to analyze
            unit: Optional unit identifier for report title

        Returns:
            Formatted text report
        """
        analysis = self.analyze_tag_freshness(df)

        if 'error' in analysis:
            return f"Error generating report: {analysis['error']}"

        report_lines = []

        # Header
        unit_text = f" for Unit {unit}" if unit else ""
        report_lines.append(f"STALE DATA DETECTION REPORT{unit_text}")
        report_lines.append("=" * 60)
        report_lines.append(f"Analysis Time: {analysis['analysis_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Staleness Threshold: {self.max_age_hours} hours")
        report_lines.append("")

        # Summary
        summary = analysis['summary']
        report_lines.append("SUMMARY:")
        report_lines.append(f"  Total Tags Analyzed: {summary['total_tags']}")
        report_lines.append(f"  Fresh Tags: {summary['fresh_tags']} ({summary['freshness_percentage']:.1f}%)")
        report_lines.append(f"  Stale Tags: {summary['stale_tags']} ({100-summary['freshness_percentage']:.1f}%)")
        report_lines.append("")

        # Staleness distribution
        report_lines.append("STALENESS DISTRIBUTION:")
        for level, count in summary['staleness_distribution'].items():
            if count > 0:
                report_lines.append(f"  {level}: {count} tags")
        report_lines.append("")

        # Stale tags details
        if analysis['stale_tags']:
            report_lines.append("STALE TAGS DETAILS:")
            report_lines.append("-" * 40)

            # Sort by staleness (most stale first)
            stale_tags = sorted(analysis['stale_tags'],
                              key=lambda x: x['age_hours'], reverse=True)

            for tag_info in stale_tags:
                age_days = tag_info['age_days']
                level = tag_info['staleness_level']
                report_lines.append(f"  Tag: {tag_info['tag']}")
                report_lines.append(f"    Last Data: {tag_info['latest_time']}")
                report_lines.append(f"    Age: {age_days:.1f} days ({level})")
                report_lines.append(f"    Records: {tag_info['record_count']:,}")
                report_lines.append("")
        else:
            report_lines.append("✅ No stale tags found - all data is fresh!")

        return "\n".join(report_lines)


def add_stale_data_warnings_to_plot(ax, tag_info: Dict[str, Any],
                                   detector: StaleDataDetector = None) -> None:
    """
    Add visual warnings to plot for stale data

    Args:
        ax: Matplotlib axes object
        tag_info: Tag information including freshness data
        detector: StaleDataDetector instance
    """
    if detector is None:
        detector = StaleDataDetector()

    try:
        # Check if tag has staleness information
        staleness_level = tag_info.get('staleness_level', 'UNKNOWN')
        age_hours = tag_info.get('age_hours', 0)
        latest_time = tag_info.get('latest_time')

        if staleness_level in ['MILDLY_STALE', 'STALE', 'SEVERELY_STALE']:
            # Add warning annotation
            warning_colors = {
                'MILDLY_STALE': 'orange',
                'STALE': 'red',
                'SEVERELY_STALE': 'darkred'
            }

            warning_messages = {
                'MILDLY_STALE': f'⚠️ MILDLY STALE\nLast data: {age_hours:.1f}h ago',
                'STALE': f'🚨 STALE DATA\nLast data: {age_hours/24:.1f} days ago',
                'SEVERELY_STALE': f'🔴 SEVERELY STALE\nLast data: {age_hours/24:.0f} days ago'
            }

            color = warning_colors.get(staleness_level, 'red')
            message = warning_messages.get(staleness_level, f'Stale: {age_hours:.1f}h')

            # Add warning box to top-left of plot
            ax.text(0.02, 0.98, message, transform=ax.transAxes,
                   fontsize=10, fontweight='bold', color='white',
                   verticalalignment='top', horizontalalignment='left',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor=color, alpha=0.8))

            # Add vertical line at latest data point if we have the time
            if latest_time:
                ax.axvline(x=pd.to_datetime(latest_time), color=color,
                          linestyle='--', linewidth=2, alpha=0.7,
                          label=f'Last Data ({staleness_level})')
        else:
            # Fresh data - add small green indicator
            ax.text(0.02, 0.98, '✅ FRESH', transform=ax.transAxes,
                   fontsize=9, color='white', fontweight='bold',
                   verticalalignment='top', horizontalalignment='left',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='green', alpha=0.7))

    except Exception as e:
        logger.warning(f"Could not add stale data warning to plot: {e}")


def detect_stale_tags_in_dataset(df: pd.DataFrame, max_age_hours: float = 24.0) -> StaleDataDetector:
    """
    Convenience function to detect stale tags in a dataset

    Args:
        df: DataFrame with PI data
        max_age_hours: Maximum age for fresh data

    Returns:
        StaleDataDetector instance with analysis results
    """
    detector = StaleDataDetector(max_age_hours=max_age_hours)
    detector.last_analysis = detector.analyze_tag_freshness(df)
    return detector


# Example usage and testing
if __name__ == "__main__":
    # Example usage
    print("Testing Stale Data Detector...")

    # Create test data
    current_time = datetime.now()
    test_data = pd.DataFrame({
        'tag': ['TAG_A'] * 100 + ['TAG_B'] * 50 + ['TAG_C'] * 30,
        'time': (
            # Fresh tag - recent data
            pd.date_range(current_time - timedelta(hours=2), current_time, periods=100).tolist() +
            # Stale tag - 3 days old
            pd.date_range(current_time - timedelta(days=4), current_time - timedelta(days=3), periods=50).tolist() +
            # Severely stale tag - 1 week old
            pd.date_range(current_time - timedelta(days=8), current_time - timedelta(days=7), periods=30).tolist()
        ),
        'value': np.random.normal(100, 10, 180)
    })

    # Test the detector
    detector = StaleDataDetector(max_age_hours=24.0)
    results = detector.analyze_tag_freshness(test_data)

    print("\n" + "="*60)
    print("STALE DATA DETECTION TEST RESULTS")
    print("="*60)
    print(f"Tags analyzed: {results['tags_analyzed']}")
    print(f"Fresh tags: {len(results['fresh_tags'])}")
    print(f"Stale tags: {len(results['stale_tags'])}")

    if results['stale_tags']:
        print("\nStale tags found:")
        for tag in results['stale_tags']:
            print(f"  {tag['tag']}: {tag['age_days']:.1f} days old ({tag['staleness_level']})")

    # Generate full report
    print("\n" + detector.generate_stale_data_report(test_data, "TEST_UNIT"))