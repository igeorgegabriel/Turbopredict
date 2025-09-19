#!/usr/bin/env python3
"""
Unit Status Detection - Identify if units are running or shutdown
Based on speed sensor readings to avoid anomaly analysis on offline units
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase


class UnitStatusDetector:
    """Detect if industrial units are running or shutdown based on speed sensors"""
    
    def __init__(self):
        self.db = ParquetDatabase()
        
    def check_unit_status(self, unit: str, hours_back: int = 2) -> dict:
        """
        Check if unit is running or shutdown based on speed sensors
        
        Args:
            unit: Unit identifier (e.g., 'K-31-01')
            hours_back: Hours of recent data to check
            
        Returns:
            Dict with unit status information
        """
        print(f"UNIT STATUS CHECK - {unit}")
        print("=" * 50)
        
        # Load unit data
        df = self.db.get_unit_data(unit)
        if df.empty:
            return {'status': 'NO_DATA', 'message': 'No data available'}
            
        # Filter to recent period
        if 'time' in df.columns:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()
            
        if df.empty:
            return {'status': 'NO_RECENT_DATA', 'message': f'No data in last {hours_back} hours'}
            
        # Find speed sensors
        speed_tags = self._find_speed_sensors(df)
        
        if not speed_tags:
            return {
                'status': 'NO_SPEED_SENSOR', 
                'message': 'No speed sensors found - cannot determine unit status',
                'proceed_with_analysis': True  # Default to proceed if no speed sensor
            }
            
        print(f"Found {len(speed_tags)} speed sensor(s):")
        for tag in speed_tags:
            print(f"  - {tag}")
        print()
        
        # Analyze speed sensor data
        status_results = {}
        overall_status = 'UNKNOWN'
        
        for tag in speed_tags:
            tag_data = df[df['tag'] == tag]['value'].dropna()
            
            if len(tag_data) == 0:
                continue
                
            status_results[tag] = self._analyze_speed_data(tag, tag_data)
            
        # Determine overall unit status
        if status_results:
            overall_status = self._determine_overall_status(status_results)
            
        # Generate recommendation
        proceed_with_analysis = overall_status in ['RUNNING', 'STARTUP', 'UNKNOWN']
        
        result = {
            'unit': unit,
            'status': overall_status,
            'speed_sensors': status_results,
            'proceed_with_analysis': proceed_with_analysis,
            'analysis_period_hours': hours_back,
            'timestamp': datetime.now().isoformat()
        }
        
        # Print status summary
        self._print_status_summary(result)
        
        return result
        
    def _find_speed_sensors(self, df: pd.DataFrame) -> list:
        """Find speed sensor tags in the data"""
        
        speed_patterns = [
            'SI',     # Speed Indicator
            'SIA',    # Speed Indicator with Alarm
            'SPEED',  # Explicit speed tag
            'RPM',    # RPM measurement
            'SE'      # Speed Element
        ]
        
        unique_tags = df['tag'].unique()
        speed_tags = []
        
        for tag in unique_tags:
            tag_upper = tag.upper()
            
            # Check for speed patterns
            for pattern in speed_patterns:
                if pattern in tag_upper:
                    # Additional validation - exclude obvious non-speed tags
                    if not any(exclude in tag_upper for exclude in ['TEMP', 'PRESS', 'FLOW', 'LEVEL', 'VIB']):
                        speed_tags.append(tag)
                        break
                        
        return speed_tags
        
    def _analyze_speed_data(self, tag: str, speed_data: pd.Series) -> dict:
        """Analyze speed sensor data to determine status"""
        
        # Basic statistics
        mean_speed = speed_data.mean()
        max_speed = speed_data.max()
        min_speed = speed_data.min()
        std_speed = speed_data.std()
        recent_speed = speed_data.iloc[-10:].mean() if len(speed_data) >= 10 else mean_speed
        
        # Count zero/near-zero readings
        zero_threshold = 10  # RPM - very low speed threshold
        near_zero_count = (speed_data <= zero_threshold).sum()
        zero_percentage = near_zero_count / len(speed_data) * 100
        
        # Determine status based on speed patterns
        if zero_percentage > 90:
            status = 'SHUTDOWN'
            message = f'Unit stopped - {zero_percentage:.1f}% readings ≤ {zero_threshold} RPM'
        elif zero_percentage > 50:
            status = 'INTERMITTENT'
            message = f'Intermittent operation - {zero_percentage:.1f}% low speed readings'
        elif mean_speed < 100:
            status = 'LOW_SPEED'
            message = f'Very low speed operation - average {mean_speed:.1f} RPM'
        elif std_speed > mean_speed * 0.5:  # High variability
            status = 'UNSTABLE'
            message = f'Unstable operation - high speed variation (σ={std_speed:.1f})'
        elif recent_speed < mean_speed * 0.5:
            status = 'SHUTDOWN_RECENT'
            message = f'Recently stopped - recent speed {recent_speed:.1f} vs avg {mean_speed:.1f} RPM'
        elif recent_speed > mean_speed * 1.2 and mean_speed < 1000:
            status = 'STARTUP'
            message = f'Starting up - recent speed {recent_speed:.1f} vs avg {mean_speed:.1f} RPM'
        else:
            status = 'RUNNING'
            message = f'Normal operation - average {mean_speed:.1f} RPM'
            
        return {
            'tag': tag,
            'status': status,
            'message': message,
            'statistics': {
                'mean_speed': float(mean_speed),
                'max_speed': float(max_speed),
                'min_speed': float(min_speed),
                'std_speed': float(std_speed),
                'recent_speed': float(recent_speed),
                'zero_percentage': float(zero_percentage),
                'data_points': len(speed_data)
            }
        }
        
    def _determine_overall_status(self, status_results: dict) -> str:
        """Determine overall unit status from multiple speed sensors"""
        
        statuses = [result['status'] for result in status_results.values()]
        
        # Priority order for determining overall status
        if 'SHUTDOWN' in statuses or 'SHUTDOWN_RECENT' in statuses:
            return 'SHUTDOWN'
        elif 'STARTUP' in statuses:
            return 'STARTUP'
        elif 'LOW_SPEED' in statuses:
            return 'LOW_SPEED'
        elif 'UNSTABLE' in statuses:
            return 'UNSTABLE'
        elif 'INTERMITTENT' in statuses:
            return 'INTERMITTENT'
        elif 'RUNNING' in statuses:
            return 'RUNNING'
        else:
            return 'UNKNOWN'
            
    def _print_status_summary(self, result: dict):
        """Print unit status summary"""
        
        status = result['status']
        proceed = result['proceed_with_analysis']
        
        print("UNIT STATUS SUMMARY:")
        print("-" * 30)
        print(f"Overall Status: {status}")
        print(f"Anomaly Analysis: {'PROCEED' if proceed else 'SKIP - UNIT OFFLINE'}")
        
        if result['speed_sensors']:
            print("\nSpeed Sensor Details:")
            for tag, sensor_data in result['speed_sensors'].items():
                stats = sensor_data['statistics']
                print(f"  {tag}:")
                print(f"    Status: {sensor_data['status']}")
                print(f"    Average Speed: {stats['mean_speed']:.1f} RPM")
                print(f"    Recent Speed: {stats['recent_speed']:.1f} RPM")
                print(f"    Zero readings: {stats['zero_percentage']:.1f}%")
                
        print()
        
        # Recommendation
        if not proceed:
            print("RECOMMENDATION: SKIP anomaly detection - unit is offline")
            print("Wait for unit restart before running analysis")
        elif status in ['STARTUP', 'UNSTABLE']:
            print("RECOMMENDATION: Use conservative anomaly thresholds")
            print("Unit in transitional state - expect higher variation")
        else:
            print("RECOMMENDATION: Proceed with normal anomaly detection")
            
        print("=" * 50)


def check_all_units():
    """Check status of all units"""
    
    print("CHECKING STATUS OF ALL UNITS")
    print("=" * 80)
    
    detector = UnitStatusDetector()
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    
    results = {}
    
    for unit in units:
        try:
            result = detector.check_unit_status(unit, hours_back=2)
            results[unit] = result
            print()
        except Exception as e:
            print(f"ERROR checking {unit}: {e}")
            print()
            
    # Summary table
    print("UNIT STATUS SUMMARY TABLE")
    print("=" * 80)
    print(f"{'Unit':<10} {'Status':<15} {'Anomaly Analysis':<20} {'Speed (RPM)':<15}")
    print("-" * 80)
    
    for unit, result in results.items():
        status = result.get('status', 'ERROR')
        proceed = 'PROCEED' if result.get('proceed_with_analysis', False) else 'SKIP'
        
        # Get average speed from speed sensors
        avg_speed = 'N/A'
        if result.get('speed_sensors'):
            speeds = [s['statistics']['mean_speed'] for s in result['speed_sensors'].values()]
            if speeds:
                avg_speed = f"{np.mean(speeds):.1f}"
                
        print(f"{unit:<10} {status:<15} {proceed:<20} {avg_speed:<15}")
        
    print("=" * 80)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unit Status Detector')
    parser.add_argument('--unit', help='Unit to check (default: check all units)')
    parser.add_argument('--hours', type=int, default=2, help='Hours of recent data to check (default: 2)')
    
    args = parser.parse_args()
    
    if args.unit:
        detector = UnitStatusDetector()
        detector.check_unit_status(args.unit, args.hours)
    else:
        check_all_units()


if __name__ == "__main__":
    main()