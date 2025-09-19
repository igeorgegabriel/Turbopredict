#!/usr/bin/env python3
"""
PI Tag Anomaly Incident Reporter for TURBOPREDICT X PROTEAN
Critical incident tracking: WHO-WHAT-WHEN-WHERE for each anomaly
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner


class PIAnomalyIncidentReporter:
    """Generate detailed incident reports for each PI tag anomaly"""
    
    def __init__(self):
        self.scanner = ParquetAutoScanner()
        
    def generate_incident_report(self, unit: str, hours_back: int = 24, top_tags: int = 10):
        """
        Generate detailed incident report with WHO-WHAT-WHEN-WHERE for each anomaly
        
        Args:
            unit: Unit to analyze
            hours_back: Hours of historical data to analyze
            top_tags: Number of top anomalous tags to detail
        """
        print(f"PI TAG ANOMALY INCIDENT REPORT - {unit}")
        print("=" * 100)
        print(f"Analysis Period: Last {hours_back} hours")
        print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Load unit data
        df = self.scanner.db.get_unit_data(unit)
        if df.empty:
            print("ERROR: No data available for this unit")
            return
            
        # Filter to recent time period
        if 'time' in df.columns:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()
            
        if df.empty:
            print(f"ERROR: No data in last {hours_back} hours")
            return
            
        # Get anomaly analysis
        analysis = self.scanner.analyze_unit_data(unit, run_anomaly_detection=True)
        if 'anomalies' not in analysis:
            print("ERROR: Anomaly detection failed")
            return
            
        anomaly_data = analysis['anomalies']
        tag_anomalies = anomaly_data.get('by_tag', {})
        
        # Sort tags by anomaly count
        sorted_tags = sorted(tag_anomalies.items(), 
                           key=lambda x: x[1].get('count', 0), 
                           reverse=True)
        
        print("TOP ANOMALOUS TAGS - INCIDENT DETAILS")
        print("=" * 100)
        
        incidents = []
        
        for i, (tag, anomaly_info) in enumerate(sorted_tags[:top_tags]):
            if anomaly_info.get('count', 0) == 0:
                continue
                
            print(f"[INCIDENT #{i+1}] {tag}")
            print("-" * 100)
            
            # Extract tag information
            tag_info = self._parse_tag_info(tag)
            equipment_info = self._identify_equipment(unit, tag)
            
            # Get tag-specific data with anomalies
            tag_df = df[df['tag'] == tag].copy()
            if tag_df.empty:
                print("  ERROR: No data for this tag in analysis period")
                continue
                
            # Identify specific anomaly incidents
            anomaly_incidents = self._identify_anomaly_incidents(tag_df, tag)
            
            # Print incident details
            print(f"WHO (Equipment): {equipment_info['equipment_name']}")
            print(f"WHAT (Tag Type): {tag_info['tag_type']} - {tag_info['description']}")
            print(f"WHERE (Location): {equipment_info['area']} / {equipment_info['system']}")
            print(f"TOTAL INCIDENTS: {len(anomaly_incidents)}")
            print()
            
            if anomaly_incidents:
                print("WHEN (Incident Timeline):")
                print(f"{'#':<3} {'Date/Time':<20} {'Value':<12} {'Severity':<10} {'Duration':<10} {'Notes'}")
                print("-" * 80)
                
                for idx, incident in enumerate(anomaly_incidents[:10]):  # Show top 10 incidents
                    incident_time = incident['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                    value = f"{incident['value']:.2f}"
                    severity = incident['severity']
                    duration = f"{incident['duration_min']:.0f}min"
                    notes = incident['notes']
                    
                    print(f"{idx+1:<3} {incident_time:<20} {value:<12} {severity:<10} {duration:<10} {notes}")
                
                if len(anomaly_incidents) > 10:
                    print(f"... and {len(anomaly_incidents) - 10} more incidents")
                    
            else:
                print("WHEN: No specific incidents identified in time period")
                
            # Add to incidents list
            incidents.append({
                'tag': tag,
                'equipment': equipment_info,
                'tag_info': tag_info,
                'incidents': anomaly_incidents,
                'total_anomalies': anomaly_info.get('count', 0),
                'anomaly_rate': anomaly_info.get('rate', 0) * 100
            })
            
            print()
            print("RECOMMENDED ACTION:")
            action = self._get_recommended_action(tag_info, equipment_info, anomaly_incidents)
            print(f"  {action}")
            print()
            print("=" * 100)
            print()
            
        # Generate summary report
        self._generate_summary_report(unit, incidents, hours_back)
        
        return incidents
    
    def _parse_tag_info(self, tag: str) -> dict:
        """Parse PI tag to extract type and description information"""
        tag_upper = tag.upper()
        
        # Extract unit and tag components
        parts = tag.split('_')
        if len(parts) >= 3:
            plant = parts[0] if len(parts) > 0 else 'Unknown'
            unit = parts[1] if len(parts) > 1 else 'Unknown'
            tag_id = parts[2] if len(parts) > 2 else 'Unknown'
            suffix = parts[3] if len(parts) > 3 else 'PV'
        else:
            plant = 'Unknown'
            unit = 'Unknown'
            tag_id = tag
            suffix = 'PV'
            
        # Determine tag type and description
        if 'TI' in tag_id or 'TIA' in tag_id:
            tag_type = 'Temperature Indicator'
            description = 'Process temperature measurement'
        elif 'PI' in tag_id or 'PIA' in tag_id:
            tag_type = 'Pressure Indicator'
            description = 'Process pressure measurement'
        elif 'LI' in tag_id or 'LIA' in tag_id:
            tag_type = 'Level Indicator'
            description = 'Process level measurement'
        elif 'FI' in tag_id or 'FIA' in tag_id:
            tag_type = 'Flow Indicator'
            description = 'Process flow measurement'
        elif 'SI' in tag_id or 'SIA' in tag_id:
            tag_type = 'Speed Indicator'
            description = 'Rotational speed measurement'
        elif 'XI' in tag_id or 'XIA' in tag_id:
            tag_type = 'Vibration Indicator'
            description = 'Mechanical vibration measurement'
        elif 'ZI' in tag_id or 'ZIA' in tag_id:
            tag_type = 'Position Indicator'
            description = 'Valve/actuator position'
        elif 'PY' in tag_id:
            tag_type = 'Pressure Controller'
            description = 'Process pressure control output'
        elif 'PV' in tag_id:
            tag_type = 'Process Variable'
            description = 'General process measurement'
        elif 'PERFORMANCE' in tag_upper:
            tag_type = 'Performance Calculation'
            description = 'Equipment performance metric'
        else:
            tag_type = 'Unknown'
            description = 'Unclassified measurement'
            
        return {
            'plant': plant,
            'unit': unit,
            'tag_id': tag_id,
            'suffix': suffix,
            'tag_type': tag_type,
            'description': description,
            'full_tag': tag
        }
    
    def _identify_equipment(self, unit: str, tag: str) -> dict:
        """Identify equipment and location from unit and tag information"""
        
        # Extract equipment number from tag
        equipment_num = 'Unknown'
        if 'K-31-01' in unit:
            equipment_num = 'K-31-01'
        elif 'K-12-01' in unit:
            equipment_num = 'K-12-01'
        elif 'K-16-01' in unit:
            equipment_num = 'K-16-01'
        elif 'K-19-01' in unit:
            equipment_num = 'K-19-01'
            
        # Map equipment to systems (customize based on your plant)
        equipment_mapping = {
            'K-31-01': {
                'equipment_name': 'Compressor K-31-01',
                'equipment_type': 'Centrifugal Compressor',
                'area': 'Compression Station',
                'system': 'Process Gas Compression',
                'criticality': 'High'
            },
            'K-12-01': {
                'equipment_name': 'Compressor K-12-01',
                'equipment_type': 'Centrifugal Compressor', 
                'area': 'Compression Station',
                'system': 'Process Gas Compression',
                'criticality': 'High'
            },
            'K-16-01': {
                'equipment_name': 'Compressor K-16-01',
                'equipment_type': 'Centrifugal Compressor',
                'area': 'Compression Station', 
                'system': 'Process Gas Compression',
                'criticality': 'High'
            },
            'K-19-01': {
                'equipment_name': 'Compressor K-19-01',
                'equipment_type': 'Centrifugal Compressor',
                'area': 'Compression Station',
                'system': 'Process Gas Compression', 
                'criticality': 'High'
            }
        }
        
        return equipment_mapping.get(equipment_num, {
            'equipment_name': f'Equipment {equipment_num}',
            'equipment_type': 'Unknown',
            'area': 'Unknown Area',
            'system': 'Unknown System',
            'criticality': 'Unknown'
        })
    
    def _identify_anomaly_incidents(self, tag_df: pd.DataFrame, tag: str) -> list:
        """Identify specific anomaly incidents with timestamps"""
        
        if len(tag_df) < 10:
            return []
            
        # Ensure time column is datetime
        if 'time' in tag_df.columns:
            tag_df['time'] = pd.to_datetime(tag_df['time'])
            tag_df = tag_df.sort_values('time')
        
        values = tag_df['value'].dropna()
        times = tag_df['time']
        
        # Use multiple methods to identify anomalies
        incidents = []
        
        # Method 1: Statistical outliers (Z-score > 3)
        if len(values) > 10:
            mean_val = values.mean()
            std_val = values.std()
            
            if std_val > 0:
                z_scores = np.abs((values - mean_val) / std_val)
                outlier_mask = z_scores > 3
                
                for idx, is_outlier in enumerate(outlier_mask):
                    if is_outlier and idx < len(times):
                        severity = 'HIGH' if z_scores.iloc[idx] > 5 else 'MEDIUM'
                        
                        incidents.append({
                            'timestamp': times.iloc[idx],
                            'value': values.iloc[idx],
                            'z_score': z_scores.iloc[idx],
                            'severity': severity,
                            'duration_min': 1,  # Assume 1 minute duration
                            'notes': f'Statistical outlier (Z={z_scores.iloc[idx]:.1f})',
                            'method': 'z_score'
                        })
        
        # Method 2: Sudden jumps
        if len(values) > 1:
            diffs = values.diff().abs()
            jump_threshold = diffs.quantile(0.95)  # 95th percentile
            
            large_jumps = diffs > jump_threshold
            
            for idx, is_jump in enumerate(large_jumps):
                if is_jump and idx < len(times) and idx > 0:
                    jump_size = diffs.iloc[idx]
                    severity = 'HIGH' if jump_size > 2 * jump_threshold else 'MEDIUM'
                    
                    incidents.append({
                        'timestamp': times.iloc[idx],
                        'value': values.iloc[idx],
                        'jump_size': jump_size,
                        'severity': severity,
                        'duration_min': 1,
                        'notes': f'Sudden jump ({jump_size:.2f} change)',
                        'method': 'jump_detection'
                    })
        
        # Remove duplicates and sort by time
        seen_times = set()
        unique_incidents = []
        
        for incident in incidents:
            timestamp_key = incident['timestamp'].strftime('%Y-%m-%d %H:%M')
            if timestamp_key not in seen_times:
                seen_times.add(timestamp_key)
                unique_incidents.append(incident)
                
        # Sort by timestamp (most recent first)
        unique_incidents.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return unique_incidents
    
    def _get_recommended_action(self, tag_info: dict, equipment_info: dict, incidents: list) -> str:
        """Get specific recommended action based on tag type and incidents"""
        
        tag_type = tag_info['tag_type']
        equipment = equipment_info['equipment_name']
        
        if not incidents:
            return f"Monitor {tag_type.lower()} on {equipment} - no recent incidents"
            
        incident_count = len(incidents)
        
        if 'Temperature' in tag_type:
            return f"URGENT: Check thermal conditions on {equipment} - {incident_count} temperature anomalies detected. Verify cooling/heating systems."
        elif 'Pressure' in tag_type:
            return f"URGENT: Investigate pressure system on {equipment} - {incident_count} pressure anomalies. Check for leaks, blockages, or control issues."
        elif 'Level' in tag_type:
            return f"CHECK: Verify level control on {equipment} - {incident_count} level anomalies. Inspect level transmitter and control valve."
        elif 'Flow' in tag_type:
            return f"CHECK: Investigate flow conditions on {equipment} - {incident_count} flow anomalies. Verify flow elements and control."
        elif 'Speed' in tag_type:
            return f"URGENT: Check rotational equipment on {equipment} - {incident_count} speed anomalies. Inspect coupling, bearings, drive system."
        elif 'Vibration' in tag_type:
            return f"URGENT: Mechanical inspection required on {equipment} - {incident_count} vibration anomalies. Check alignment, balance, bearings."
        elif 'Position' in tag_type:
            return f"CHECK: Verify actuator/valve on {equipment} - {incident_count} position anomalies. Inspect control valve and positioner."
        elif 'Performance' in tag_type:
            return f"ANALYZE: Performance degradation on {equipment} - {incident_count} performance anomalies. Review operational parameters."
        else:
            return f"INVESTIGATE: {incident_count} anomalies detected on {equipment}. Detailed analysis required."
    
    def _generate_summary_report(self, unit: str, incidents: list, hours_back: int):
        """Generate executive summary of all incidents"""
        
        print("EXECUTIVE SUMMARY")
        print("=" * 100)
        
        total_incidents = sum(len(inc['incidents']) for inc in incidents)
        total_tags = len([inc for inc in incidents if inc['incidents']])
        
        print(f"Unit: {unit}")
        print(f"Analysis Period: {hours_back} hours")
        print(f"Total Anomalous Tags: {total_tags}")
        print(f"Total Incidents: {total_incidents}")
        print()
        
        if incidents:
            print("PRIORITY ACTIONS REQUIRED:")
            print("-" * 50)
            
            # Sort by criticality (number of incidents)
            priority_incidents = sorted(incidents, 
                                      key=lambda x: len(x['incidents']), 
                                      reverse=True)
            
            for i, incident in enumerate(priority_incidents[:5]):
                tag = incident['tag']
                equipment = incident['equipment']['equipment_name']
                incident_count = len(incident['incidents'])
                tag_type = incident['tag_info']['tag_type']
                
                print(f"{i+1}. {equipment} - {tag_type}")
                print(f"   Tag: {tag}")
                print(f"   Incidents: {incident_count}")
                print(f"   Last Incident: {incident['incidents'][0]['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if incident['incidents'] else 'N/A'}")
                print()
        
        print("Report complete. Take immediate action on priority items.")
        print("=" * 100)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PI Tag Anomaly Incident Reporter')
    parser.add_argument('--unit', default='K-31-01', help='Unit to analyze (default: K-31-01)')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to analyze (default: 24)')
    parser.add_argument('--top-tags', type=int, default=10, help='Number of top tags to detail (default: 10)')
    
    args = parser.parse_args()
    
    reporter = PIAnomalyIncidentReporter()
    reporter.generate_incident_report(args.unit, args.hours, args.top_tags)


if __name__ == "__main__":
    main()