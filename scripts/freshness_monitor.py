#!/usr/bin/env python3
"""
Zero-Tolerance Freshness Monitor for TURBOPREDICT X PROTEAN
Critical system for anomaly detection reliability
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner


class ZeroToleranceFreshnessMonitor:
    """Zero tolerance freshness monitoring for critical anomaly detection"""
    
    def __init__(self, max_age_minutes=60, critical_age_minutes=30):
        """
        Initialize monitor with strict thresholds
        
        Args:
            max_age_minutes: Maximum data age before CRITICAL alert (default: 60)
            critical_age_minutes: Age threshold for WARNING alert (default: 30)
        """
        self.max_age_hours = max_age_minutes / 60.0
        self.critical_age_hours = critical_age_minutes / 60.0
        self.scanner = ParquetAutoScanner()
        
        # Create monitoring directory
        self.monitor_dir = project_root / 'monitoring'
        self.monitor_dir.mkdir(exist_ok=True)
        
        # Status file for external monitoring
        self.status_file = self.monitor_dir / 'freshness_status.json'
        
    def check_freshness_status(self):
        """Perform comprehensive freshness check with zero tolerance"""
        check_time = datetime.now()
        
        # Scan all units
        results = self.scanner.scan_all_units(max_age_hours=self.max_age_hours)
        
        status = {
            'timestamp': check_time.isoformat(),
            'check_type': 'zero_tolerance_freshness',
            'max_age_hours': self.max_age_hours,
            'critical_age_hours': self.critical_age_hours,
            'units': {},
            'summary': {
                'total_units': 0,
                'fresh_units': 0,
                'warning_units': 0,
                'critical_units': 0,
                'overall_status': 'UNKNOWN'
            },
            'alerts': []
        }
        
        # Analyze each unit
        for unit_info in results['units_scanned']:
            unit = unit_info['unit']
            age_hours = unit_info['data_age_hours'] or 0
            latest_time = unit_info['latest_timestamp']
            records = unit_info['total_records']
            
            # Determine unit status
            if age_hours > self.max_age_hours:
                unit_status = 'CRITICAL'
                status['summary']['critical_units'] += 1
                status['alerts'].append({
                    'level': 'CRITICAL',
                    'unit': unit,
                    'message': f'Data age {age_hours:.1f}h exceeds {self.max_age_hours:.1f}h limit',
                    'age_hours': age_hours,
                    'impact': 'ANOMALY DETECTION COMPROMISED'
                })
            elif age_hours > self.critical_age_hours:
                unit_status = 'WARNING'
                status['summary']['warning_units'] += 1
                status['alerts'].append({
                    'level': 'WARNING',
                    'unit': unit,
                    'message': f'Data age {age_hours:.1f}h approaching limit',
                    'age_hours': age_hours,
                    'impact': 'ANOMALY DETECTION AT RISK'
                })
            else:
                unit_status = 'FRESH'
                status['summary']['fresh_units'] += 1
            
            # Store unit details
            status['units'][unit] = {
                'age_hours': round(age_hours, 2),
                'status': unit_status,
                'latest_timestamp': latest_time.isoformat() if latest_time else None,
                'records': records,
                'is_anomaly_ready': unit_status == 'FRESH'
            }
            
            status['summary']['total_units'] += 1
        
        # Determine overall system status
        if status['summary']['critical_units'] > 0:
            status['summary']['overall_status'] = 'CRITICAL'
        elif status['summary']['warning_units'] > 0:
            status['summary']['overall_status'] = 'WARNING'
        else:
            status['summary']['overall_status'] = 'FRESH'
        
        # Calculate system health metrics
        total = status['summary']['total_units']
        fresh = status['summary']['fresh_units']
        status['summary']['freshness_rate'] = (fresh / total) if total > 0 else 0
        status['summary']['anomaly_detection_ready'] = (status['summary']['overall_status'] == 'FRESH')
        
        return status
    
    def save_status(self, status):
        """Save status to monitoring file"""
        with open(self.status_file, 'w') as f:
            json.dump(status, f, indent=2)
    
    def print_status_report(self, status):
        """Print comprehensive status report"""
        print("=" * 80)
        print("ZERO TOLERANCE FRESHNESS MONITOR")
        print("Critical System for Anomaly Detection")
        print("=" * 80)
        print(f"Check Time: {status['timestamp']}")
        print(f"Freshness Limit: {status['max_age_hours']:.1f} hours")
        print()
        
        # Overall status
        overall = status['summary']['overall_status']
        freshness_rate = status['summary']['freshness_rate'] * 100
        
        print(f"OVERALL STATUS: {overall}")
        print(f"Freshness Rate: {freshness_rate:.1f}%")
        print(f"Anomaly Detection Ready: {status['summary']['anomaly_detection_ready']}")
        print()
        
        # Unit details
        print("UNIT STATUS BREAKDOWN:")
        print("-" * 60)
        print(f"{'Unit':<10} {'Age (hrs)':<10} {'Status':<10} {'Records':<12}")
        print("-" * 60)
        
        for unit, details in status['units'].items():
            age = details['age_hours']
            unit_status = details['status']
            records = details['records']
            
            print(f"{unit:<10} {age:<10.1f} {unit_status:<10} {records:<12,}")
        
        print("-" * 60)
        print(f"Fresh: {status['summary']['fresh_units']}, "
              f"Warning: {status['summary']['warning_units']}, "
              f"Critical: {status['summary']['critical_units']}")
        
        # Alerts
        if status['alerts']:
            print()
            print("ACTIVE ALERTS:")
            print("-" * 40)
            for alert in status['alerts']:
                level = alert['level']
                unit = alert['unit']
                message = alert['message']
                impact = alert['impact']
                
                print(f"[{level}] {unit}: {message}")
                print(f"         Impact: {impact}")
        
        print("=" * 80)
        
        # Return exit code based on status
        if overall == 'CRITICAL':
            return 2  # Critical exit code
        elif overall == 'WARNING':
            return 1  # Warning exit code
        else:
            return 0  # Success exit code
    
    def run_check(self):
        """Run single freshness check"""
        status = self.check_freshness_status()
        self.save_status(status)
        return self.print_status_report(status)
    
    def continuous_monitor(self, check_interval_minutes=5):
        """Run continuous monitoring"""
        print(f"Starting continuous freshness monitoring...")
        print(f"Check interval: {check_interval_minutes} minutes")
        print(f"Critical threshold: {self.critical_age_hours:.1f} hours")
        print(f"Maximum threshold: {self.max_age_hours:.1f} hours")
        print()
        
        try:
            while True:
                status = self.check_freshness_status()
                self.save_status(status)
                
                # Quick status update
                overall = status['summary']['overall_status']
                fresh_count = status['summary']['fresh_units']
                total_count = status['summary']['total_units']
                timestamp = status['timestamp']
                
                print(f"[{timestamp}] Status: {overall} ({fresh_count}/{total_count} fresh)")
                
                # Alert if not all fresh
                if overall != 'FRESH':
                    print(f"  ALERT: {len(status['alerts'])} active alerts")
                    for alert in status['alerts']:
                        print(f"    [{alert['level']}] {alert['unit']}: {alert['message']}")
                
                time.sleep(check_interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\\nMonitoring stopped by user")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Zero Tolerance Freshness Monitor')
    parser.add_argument('--max-age', type=int, default=60,
                      help='Maximum data age in minutes (default: 60)')
    parser.add_argument('--critical-age', type=int, default=30,
                      help='Critical age threshold in minutes (default: 30)')
    parser.add_argument('--continuous', action='store_true',
                      help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=5,
                      help='Check interval for continuous mode in minutes (default: 5)')
    
    args = parser.parse_args()
    
    monitor = ZeroToleranceFreshnessMonitor(
        max_age_minutes=args.max_age,
        critical_age_minutes=args.critical_age
    )
    
    if args.continuous:
        monitor.continuous_monitor(args.interval)
    else:
        exit_code = monitor.run_check()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()