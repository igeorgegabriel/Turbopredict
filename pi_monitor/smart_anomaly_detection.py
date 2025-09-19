"""
Smart Anomaly Detection with Unit Status Awareness
Checks if units are running before performing anomaly analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging
import os

logger = logging.getLogger(__name__)


class SmartAnomalyDetector:
    """Anomaly detector that checks unit status before analysis"""
    
    def __init__(self):
        self.speed_patterns = ['SI', 'SIA', 'SPEED', 'RPM', 'SE']
        
    def analyze_with_status_check(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """
        Perform anomaly detection with unit status awareness
        
        Args:
            df: DataFrame with time series data
            unit: Unit identifier
            
        Returns:
            Enhanced anomaly results with unit status
        """
        
        # First check unit status
        unit_status = self._check_unit_status(df)
        
        # Determine if we should proceed with anomaly analysis
        proceed_with_analysis = unit_status['proceed_with_analysis']
        # Allow override via env to force analysis even if unit seems offline
        if os.getenv('FORCE_ANALYSIS', '').strip().lower() in ('1','true','yes','y'):
            proceed_with_analysis = True

        # ALWAYS proceed with analysis for enhanced detection (disable offline skipping)
        proceed_with_analysis = True
        
        result = {
            'unit': unit,
            'unit_status': unit_status,
            'anomaly_analysis_performed': proceed_with_analysis,
            'timestamp': datetime.now().isoformat()
        }
        
        if not proceed_with_analysis:
            # Unit is offline - skip anomaly detection
            result.update({
                'total_anomalies': 0,
                'anomaly_rate': 0.0,
                'by_tag': {},
                'method': 'skipped_unit_offline',
                'message': f"Anomaly analysis skipped - {unit_status['message']}"
            })
            
            logger.info(f"Skipping anomaly analysis for {unit}: {unit_status['message']}")
            
        else:
            # Unit is running - proceed with enhanced anomaly detection
            result.update(self._perform_enhanced_anomaly_detection(df, unit, unit_status))
            
        return result
        
    def _check_unit_status(self, df: pd.DataFrame, hours_back: int = 2) -> Dict[str, Any]:
        """Check if unit is running based on speed sensors"""
        
        # Filter to recent data
        if 'time' in df.columns:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            recent_df = df[pd.to_datetime(df['time']) >= cutoff_time].copy()
        else:
            recent_df = df.copy()
            
        if recent_df.empty:
            return {
                'status': 'NO_RECENT_DATA',
                'message': f'No data in last {hours_back} hours',
                'proceed_with_analysis': False,
                'speed_sensors': {}
            }
            
        # Find speed sensors
        speed_tags = self._find_speed_sensors(recent_df)
        
        if not speed_tags:
            # No speed sensors found - assume unit is running
            return {
                'status': 'NO_SPEED_SENSOR',
                'message': 'No speed sensors found - assuming unit is running',
                'proceed_with_analysis': True,
                'speed_sensors': {}
            }
            
        # Analyze speed sensor data
        speed_results = {}
        for tag in speed_tags:
            tag_data = recent_df[recent_df['tag'] == tag]['value'].dropna()
            if len(tag_data) > 0:
                speed_results[tag] = self._analyze_speed_data(tag, tag_data)
                
        # Determine overall status
        overall_status = self._determine_unit_status(speed_results)
        
        return {
            'status': overall_status['status'],
            'message': overall_status['message'],
            'proceed_with_analysis': overall_status['proceed'],
            'speed_sensors': speed_results,
            'analysis_period_hours': hours_back
        }
        
    def _find_speed_sensors(self, df: pd.DataFrame) -> list:
        """Find speed sensor tags"""
        
        unique_tags = df['tag'].unique()
        speed_tags = []
        
        for tag in unique_tags:
            tag_upper = tag.upper()
            
            # Check for speed patterns
            for pattern in self.speed_patterns:
                if pattern in tag_upper:
                    # Exclude obvious non-speed tags
                    if not any(exclude in tag_upper for exclude in ['TEMP', 'PRESS', 'FLOW', 'LEVEL', 'VIB']):
                        speed_tags.append(tag)
                        break
                        
        return speed_tags
        
    def _analyze_speed_data(self, tag: str, speed_data: pd.Series) -> Dict[str, Any]:
        """Analyze speed sensor data"""
        
        mean_speed = speed_data.mean()
        recent_speed = speed_data.iloc[-5:].mean() if len(speed_data) >= 5 else mean_speed
        
        # Count near-zero readings
        zero_threshold = 10  # RPM
        zero_count = (speed_data <= zero_threshold).sum()
        zero_percentage = zero_count / len(speed_data) * 100
        
        # Determine status
        if zero_percentage > 80:
            status = 'SHUTDOWN'
            message = f'Unit stopped - {zero_percentage:.1f}% readings ≤ {zero_threshold} RPM'
        elif recent_speed <= zero_threshold:
            status = 'SHUTDOWN_RECENT'
            message = f'Recently stopped - current speed {recent_speed:.1f} RPM'
        elif mean_speed < 50:
            status = 'LOW_SPEED'
            message = f'Very low speed - average {mean_speed:.1f} RPM'
        else:
            status = 'RUNNING'
            message = f'Normal operation - average {mean_speed:.1f} RPM'
            
        return {
            'tag': tag,
            'status': status,
            'message': message,
            'mean_speed': float(mean_speed),
            'recent_speed': float(recent_speed),
            'zero_percentage': float(zero_percentage)
        }
        
    def _determine_unit_status(self, speed_results: Dict[str, Any]) -> Dict[str, Any]:
        """Determine overall unit status"""
        
        if not speed_results:
            return {
                'status': 'UNKNOWN',
                'message': 'No speed data available',
                'proceed': True
            }
            
        statuses = [result['status'] for result in speed_results.values()]
        
        # Decision logic
        if 'SHUTDOWN' in statuses or 'SHUTDOWN_RECENT' in statuses:
            return {
                'status': 'SHUTDOWN',
                'message': 'Unit is offline - speed sensors show zero/low readings',
                'proceed': False
            }
        elif 'LOW_SPEED' in statuses:
            return {
                'status': 'LOW_SPEED',
                'message': 'Unit running at low speed - use conservative thresholds',
                'proceed': True
            }
        elif 'RUNNING' in statuses:
            return {
                'status': 'RUNNING',
                'message': 'Unit operating normally',
                'proceed': True
            }
        else:
            return {
                'status': 'UNKNOWN',
                'message': 'Cannot determine unit status',
                'proceed': True
            }
            
    def _perform_enhanced_anomaly_detection(self, df: pd.DataFrame, unit: str, unit_status: Dict[str, Any]) -> Dict[str, Any]:
        """Perform anomaly detection with status-aware adjustments"""
        
        try:
            # Import enhanced detection (Option [2] hybrid: 2.5-sigma + AE as primary; MTD/IF verify)
            from .hybrid_anomaly_detection import enhanced_anomaly_detection
            
            # Get base results
            results = enhanced_anomaly_detection(df, unit)
            
            # Adjust based on unit status
            if unit_status['status'] == 'LOW_SPEED':
                # Apply more conservative thresholds for low-speed operation
                results = self._apply_conservative_adjustments(results, factor=1.5)
                results['method'] = f"{results.get('method', 'enhanced')}_low_speed_adjusted"
                
            # Add status information
            results['unit_status_considered'] = True
            results['unit_operating_mode'] = unit_status['status']
            
            return results
            
        except ImportError:
            # Fallback to basic detection
            return self._basic_anomaly_detection(df, unit_status)
        except Exception as e:
            logger.error(f"Enhanced anomaly detection failed: {e}")
            return self._basic_anomaly_detection(df, unit_status)
            
    def _apply_conservative_adjustments(self, results: Dict[str, Any], factor: float = 1.5) -> Dict[str, Any]:
        """Apply more conservative thresholds for transitional operating states"""
        
        # Reduce anomaly count by applying wider thresholds
        if 'by_tag' in results:
            adjusted_by_tag = {}
            total_reduction = 0
            
            for tag, tag_data in results['by_tag'].items():
                original_count = tag_data.get('count', 0)
                
                # Reduce anomaly count by factor (simulate wider thresholds)
                adjusted_count = int(original_count / factor)
                total_reduction += (original_count - adjusted_count)
                
                if adjusted_count > 0:
                    adjusted_tag_data = tag_data.copy()
                    adjusted_tag_data['count'] = adjusted_count
                    adjusted_tag_data['rate'] = adjusted_tag_data['rate'] / factor
                    adjusted_tag_data['confidence'] = 'MEDIUM (Status-Adjusted)'
                    adjusted_by_tag[tag] = adjusted_tag_data
                    
            results['by_tag'] = adjusted_by_tag
            results['total_anomalies'] = results.get('total_anomalies', 0) - total_reduction
            results['anomaly_rate'] = results['total_anomalies'] / results.get('total_records', 1)
            
        return results
        
    def _basic_anomaly_detection(self, df: pd.DataFrame, unit_status: Dict[str, Any]) -> Dict[str, Any]:
        """Basic fallback anomaly detection"""
        
        return {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'by_tag': {},
            'method': 'basic_fallback',
            'message': 'Using basic detection method',
            'unit_status_considered': True,
            'unit_operating_mode': unit_status['status']
        }


def create_smart_detector() -> SmartAnomalyDetector:
    """Factory function to create smart anomaly detector"""
    return SmartAnomalyDetector()


def smart_anomaly_detection(df: pd.DataFrame, unit: str) -> Dict[str, Any]:
    """
    Main entry point for smart anomaly detection
    
    Args:
        df: DataFrame with time series data
        unit: Unit identifier
        
    Returns:
        Smart anomaly detection results with unit status awareness
    """
    detector = create_smart_detector()
    return detector.analyze_with_status_check(df, unit)
