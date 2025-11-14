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
        
        unique_tags = df['tag'].unique() if 'tag' in df.columns else []
        speed_tags = []
        
        for tag in unique_tags:
            # Guard: some datasets may carry missing or non-string tag ids
            if not isinstance(tag, str):
                try:
                    tag_upper = str(tag).upper()
                except Exception:
                    continue
            else:
                tag_upper = tag.upper()
            
            # Check for speed patterns
            for pattern in self.speed_patterns:
                if pattern in tag_upper:
                    # Exclude obvious non-speed tags
                    if not any(exclude in tag_upper for exclude in ['TEMP', 'PRESS', 'FLOW', 'LEVEL', 'VIB']):
                        speed_tags.append(str(tag))
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


def _sigma_only_detection(df: pd.DataFrame, unit: str, *, z_thresh: float = 2.5, min_consecutive: int = 6) -> Dict[str, Any]:
    """Sigma-only anomaly detection with consecutive-run gate (no MTD/IF).

    A tag is abnormal if there exists a run of >= `min_consecutive` consecutive
    points with |z| >= `z_thresh` within the last 24 hours.
    """
    results: Dict[str, Any] = {
        'unit': unit,
        'method': 'sigma_only',
        'total_anomalies': 0,
        'by_tag': {},
        'total_records': int(len(df) if df is not None else 0),
    }
    if df is None or len(df) == 0:
        return results

    cols = [c for c in ['time', 'value', 'tag'] if c in df.columns]
    d = df[cols].copy()
    d['time'] = pd.to_datetime(d['time'], errors='coerce')
    d = d.dropna(subset=['time', 'value'])
    if 'tag' not in d.columns:
        d['tag'] = 'series'

    # Sigma baseline and recency gate: use 90 days for baseline, last 24h for actionability
    cutoff = datetime.now() - timedelta(hours=24)
    by_tag: Dict[str, Any] = {}
    for tag, g in d.groupby('tag'):
        gg = g.sort_values('time')
        try:
            ts = gg.set_index('time')['value']
            # 90-day rolling baseline for mean/std
            roll_mean = ts.rolling('90D', min_periods=20).mean()
            roll_std = ts.rolling('90D', min_periods=20).std()
            z = (ts - roll_mean) / roll_std
            sigma_flag = (z.abs() >= z_thresh) & roll_std.notna()
        except Exception:
            continue

        last_24 = sigma_flag[sigma_flag.index >= cutoff]
        last_24_count = int(last_24.sum())

        run = 0
        max_run = 0
        for v in last_24.astype(int).values.tolist():
            if v == 1:
                run += 1
                if run > max_run:
                    max_run = run
            else:
                run = 0

        if max_run >= min_consecutive and last_24_count > 0:
            # Get current (most recent) value and its baseline mean
            current_value = float(ts.iloc[-1]) if len(ts) > 0 else 0.0
            baseline_mean = float(roll_mean.iloc[-1]) if len(roll_mean) > 0 and pd.notna(roll_mean.iloc[-1]) else current_value

            # Calculate percentage deviation from baseline mean
            # This is the key metric for severity and bubble sizing
            if baseline_mean != 0:
                deviation_pct = abs((current_value - baseline_mean) / baseline_mean) * 100
            else:
                deviation_pct = 0.0

            # Clamp deviation percentage to reasonable range (0-300%)
            # Removed 100% cap - tags can have >100% deviation (e.g., value doubles = 100% deviation)
            # Cap at 300% to prevent extreme outliers from skewing visualization
            deviation_pct = min(300.0, max(0.0, deviation_pct))

            # Dynamic confidence score calculation (20-100 scale)
            # Base: 20 points for any detected anomaly
            base_score = 20
            # Recent anomalies (0-35 points): more recent activity = higher severity
            recent_score = min(35, last_24_count * 2.5)
            # Consecutive run (0-30 points): longer runs = more persistent issue
            consecutive_score = min(30, max_run * 3)
            # Overall anomaly rate (0-15 points): higher rate = more problematic
            rate_score = min(15, sigma_flag.mean() * 100)

            # Calculate final score and clamp to 20-100 range
            dynamic_confidence = int(base_score + recent_score + consecutive_score + rate_score)
            dynamic_confidence = min(100, max(20, dynamic_confidence))

            # Assign confidence level based on score
            if dynamic_confidence >= 80:
                confidence_level = 'HIGH'
                priority_level = 'HIGH'
            elif dynamic_confidence >= 50:
                confidence_level = 'MEDIUM'
                priority_level = 'MEDIUM'
            else:
                confidence_level = 'LOW'
                priority_level = 'LOW'

            tag_info = {
                'count': int(sigma_flag.sum()),
                'rate': float(sigma_flag.mean()),
                'method': 'sigma_only',
                'sigma_2_5_count': int(sigma_flag.sum()),
                'sigma_consecutive_ge_n': int(max_run),
                'recency_breakdown': {'last_24h': last_24_count},
                # New classification signals: 24h exceedances threshold
                'recent_exceedances_24h': int(last_24_count),
                'is_anomalous_24h_min5': bool(last_24_count >= 5),
                'mtd_count': 0,
                'isolation_forest_count': 0,
                'confidence': confidence_level,
                'confidence_score': dynamic_confidence,
                'priority': priority_level,
                # Add current value, baseline mean, and deviation percentage
                'current_value': round(current_value, 2),
                'baseline_mean': round(baseline_mean, 2),
                'deviation_percentage': round(deviation_pct, 1),
            }
            by_tag[str(tag)] = tag_info

    results['by_tag'] = by_tag
    results['total_anomalies'] = sum(v.get('count', 0) for v in by_tag.values())
    # Ensure consistent contract with callers: always provide anomaly_rate
    total_records = max(1, int(results.get('total_records', 0) or 0))
    results['anomaly_rate'] = results['total_anomalies'] / total_records
    return results

def smart_anomaly_detection(df: pd.DataFrame, unit: str, auto_plot_anomalies: bool = True) -> Dict[str, Any]:
    """
    Main entry point for smart anomaly detection with automatic anomaly plotting

    Args:
        df: DataFrame with time series data
        unit: Unit identifier
        auto_plot_anomalies: If True, automatically generate 3-month plots for verified anomalies

    Returns:
        Smart anomaly detection results with unit status awareness
    """
    # Default: sigma-only detection with 90d baseline and >=6 consecutive 2.5σ in last 24h.
    # To revert to the original hybrid pipeline (2.5σ + MTD + IF), set HYBRID_DETECTION=1.
    _hybrid_env = os.getenv('HYBRID_DETECTION', '').strip().lower()
    if _hybrid_env not in ('1','true','yes','y'):
        logger.info(f"Using sigma-only detection for unit {unit} (2.5σ, min 6 consecutive in last 24h)")
        results = _sigma_only_detection(df, unit, z_thresh=2.5, min_consecutive=6)

        # Auto-plot using sigma-only gate
        if auto_plot_anomalies and results.get('by_tag'):
            try:
                from .anomaly_triggered_plots import generate_anomaly_plots
                by_tag = results.get('by_tag', {})
                verified_count = 0
                for _tag, tag_info in by_tag.items():
                    recent = tag_info.get('recency_breakdown', {}).get('last_24h', 0)
                    max_run = tag_info.get('sigma_consecutive_ge_n', 0)
                    if recent and max_run and int(max_run) >= 6:
                        verified_count += 1
                if verified_count > 0:
                    detection_results = {unit: results}
                    plot_session_dir = generate_anomaly_plots(detection_results)
                    results['anomaly_plots_generated'] = True
                    results['plot_session_dir'] = str(plot_session_dir)
                    results['verified_anomalies_count'] = verified_count
                else:
                    results['anomaly_plots_generated'] = False
                    results['verified_anomalies_count'] = 0
            except Exception as e:
                logger.error(f"Error in automatic anomaly plotting (sigma-only): {e}")
                results['anomaly_plots_generated'] = False
                results['plot_error'] = str(e)
        return results
    # Sigma-only early-exit (env SIGMA_ONLY=1): 2.5σ with ≥6 consecutive
    # points in the last 24h triggers plotting of 90-day context.
    _sigma_only_env = os.getenv('SIGMA_ONLY', '').strip().lower()
    if _sigma_only_env in ('1','true','yes','y'):
        logger.info(f"Using sigma-only detection for unit {unit} (2.5σ, min 6 consecutive in last 24h)")
        results = _sigma_only_detection(df, unit, z_thresh=2.5, min_consecutive=6)

        # Auto-plot using sigma-only gate
        if auto_plot_anomalies and results.get('by_tag'):
            try:
                from .anomaly_triggered_plots import generate_anomaly_plots
                by_tag = results.get('by_tag', {})
                verified_count = 0
                for _tag, tag_info in by_tag.items():
                    recent = tag_info.get('recency_breakdown', {}).get('last_24h', 0)
                    max_run = tag_info.get('sigma_consecutive_ge_n', 0)
                    if recent and max_run and int(max_run) >= 6:
                        verified_count += 1
                if verified_count > 0:
                    detection_results = {unit: results}
                    plot_session_dir = generate_anomaly_plots(detection_results)
                    results['anomaly_plots_generated'] = True
                    results['plot_session_dir'] = str(plot_session_dir)
                    results['verified_anomalies_count'] = verified_count
                else:
                    results['anomaly_plots_generated'] = False
                    results['verified_anomalies_count'] = 0
            except Exception as e:
                logger.error(f"Error in automatic anomaly plotting (sigma-only): {e}")
                results['anomaly_plots_generated'] = False
                results['plot_error'] = str(e)
        return results
    # SIGMA-ONLY MODE: Use pure 2.5-sigma detection without MTD/IF verification
    # Disabled hybrid detection (MTD + IF) for reliability and simplicity
    # Pure sigma with 90-day rolling baseline + 6-consecutive-point gate
    logger.info(f"Using sigma-only anomaly detection (2.5σ, pure) for unit {unit}")

    # Force sigma-only: pure 2.5-sigma with 90-day rolling baseline
    results = _sigma_only_detection(df, unit, z_thresh=2.5, min_consecutive=6)

    # Auto-trigger plotting for verified anomalies
    if auto_plot_anomalies and results.get('by_tag'):
        try:
            from .anomaly_triggered_plots import generate_anomaly_plots

            # Check if any anomalies are verified (hybrid) or meet sigma-only rule
            verified_count = 0
            by_tag = results.get('by_tag', {})

            _sigma_only_env = os.getenv('SIGMA_ONLY', '').strip().lower()
            if _sigma_only_env in ('1','true','yes','y'):
                for _tag, tag_info in by_tag.items():
                    recent = tag_info.get('recency_breakdown', {}).get('last_24h', 0)
                    max_run = tag_info.get('sigma_consecutive_ge_n', 0)
                    if recent and max_run and int(max_run) >= 6:
                        verified_count += 1
            else:
                for _tag, tag_info in by_tag.items():
                    sigma_count = tag_info.get('sigma_2_5_count', 0)
                    ae_count = tag_info.get('autoencoder_count', 0)
                    mtd_count = tag_info.get('mtd_count', 0)
                    iso_count = tag_info.get('isolation_forest_count', 0)
                    confidence = tag_info.get('confidence', 'LOW')

                    primary_detected = sigma_count > 0 or ae_count > 0
                    verification_detected = mtd_count > 0 or iso_count > 0
                    high_confidence = confidence in ['HIGH', 'MEDIUM']

                    if primary_detected and verification_detected and high_confidence:
                        verified_count += 1

            # Trigger plotting if verified anomalies found
            if verified_count > 0:
                logger.info(f"Detected {verified_count} verified anomalies in {unit} - triggering 3-month diagnostic plots")

                # Prepare detection results in the format expected by the plotter
                detection_results = {unit: results}

                # Generate anomaly-triggered plots
                plot_session_dir = generate_anomaly_plots(detection_results)

                # Add plotting info to results
                results['anomaly_plots_generated'] = True
                results['plot_session_dir'] = str(plot_session_dir)
                results['verified_anomalies_count'] = verified_count

                logger.info(f"Anomaly diagnostic plots generated: {plot_session_dir}")
            else:
                results['anomaly_plots_generated'] = False
                results['verified_anomalies_count'] = 0

        except Exception as e:
            logger.error(f"Error in automatic anomaly plotting: {e}")
            results['anomaly_plots_generated'] = False
            results['plot_error'] = str(e)

    return results
