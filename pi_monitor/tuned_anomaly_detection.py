"""
Tuned Anomaly Detection Module for TURBOPREDICT X PROTEAN
Integrates 3-month baseline-calibrated detection with existing system
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging

try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)


class TunedAnomalyDetector:
    """Process-aware anomaly detector with baseline calibration"""
    
    def __init__(self, baseline_config_path: Optional[str] = None):
        self.baseline_config = None
        self.config_loaded = False
        self.if_config = {'contamination': 0.02, 'n_estimators': 100}
        
        if baseline_config_path and Path(baseline_config_path).exists():
            try:
                with open(baseline_config_path, 'r') as f:
                    self.baseline_config = json.load(f)
                self.config_loaded = True
                logger.info(f"Loaded baseline configuration from: {baseline_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load baseline config: {e}")
                
    def load_unit_config(self, unit: str) -> bool:
        """Load baseline configuration for specific unit"""
        config_path = f"baseline_config_{unit}.json"
        
        if Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    self.baseline_config = json.load(f)
                self.config_loaded = True
                # Load IF config if available
                if isinstance(self.baseline_config, dict) and 'if_config' in self.baseline_config:
                    try:
                        cfg = self.baseline_config['if_config']
                        for k in ['contamination', 'n_estimators']:
                            if k in cfg:
                                self.if_config[k] = cfg[k]
                    except Exception:
                        pass
                logger.info(f"Loaded baseline configuration for {unit}")
                return True
            except Exception as e:
                logger.warning(f"Failed to load config for {unit}: {e}")
        # Also attempt to load standalone IF config if present (does NOT set config_loaded)
        try:
            if_cfg_path = Path(f"if_config_{unit}.json")
            if if_cfg_path.exists():
                with open(if_cfg_path, 'r') as f:
                    cfg = json.load(f)
                for k in ['contamination', 'n_estimators']:
                    if k in cfg:
                        self.if_config[k] = cfg[k]
                logger.info(f"Loaded standalone IF config for {unit}")
        except Exception as e:
            logger.warning(f"Failed to load standalone IF config for {unit}: {e}")
        return False
        
    def detect_anomalies_with_tuning(self, df: pd.DataFrame, unit: str = None) -> Dict[str, Any]:
        """
        Enhanced anomaly detection using baseline tuning
        
        Args:
            df: DataFrame with 'tag', 'value', 'time' columns
            unit: Unit identifier for baseline configuration
            
        Returns:
            Dict with tuned anomaly results
        """
        # Try to load unit-specific config if not already loaded
        if not self.config_loaded and unit:
            self.load_unit_config(unit)
            
        # Fallback to original detection if no config
        if not self.config_loaded:
            return self._fallback_detection(df)
            
        return self._tuned_detection(df)
        
    def _tuned_detection(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Process-aware anomaly detection using baseline configuration"""
        
        if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
            return self._empty_result()
            
        unique_tags = df['tag'].unique()
        tag_anomalies = {}
        total_anomalies = 0
        total_records = len(df)
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            values = tag_df['value'].dropna()
            
            if len(values) < 5:  # Need minimum data points
                continue
                
            tag_result = self._detect_tag_anomalies(tag, values, tag_df)
            
            if tag_result and tag_result['count'] > 0:
                tag_anomalies[tag] = tag_result
                total_anomalies += tag_result['count']
                
        overall_rate = total_anomalies / total_records if total_records > 0 else 0
        
        return {
            'method': 'baseline_tuned',
            'total_anomalies': total_anomalies,
            'anomaly_rate': overall_rate,
            'by_tag': tag_anomalies,
            'config_loaded': True,
            'tags_analyzed': len(unique_tags),
            'tags_with_anomalies': len(tag_anomalies)
        }
        
    def _detect_tag_anomalies(self, tag: str, values: pd.Series, tag_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Detect anomalies using both MTD and Isolation Forest methods"""
        
        # Get baseline configuration for this tag
        tag_config = None
        if self.baseline_config and 'tag_configurations' in self.baseline_config:
            tag_config = self.baseline_config['tag_configurations'].get(tag)
        
        # Run both MTD and Isolation Forest
        mtd_results = self._run_mtd_detection(tag, values, tag_df, tag_config)
        isolation_results = self._run_isolation_forest_detection(tag, values, tag_df)
        
        # Combine results and track detection methods
        combined_results = self._combine_detection_results(mtd_results, isolation_results, tag_config)
        
        return combined_results if combined_results and combined_results['count'] > 0 else None
        
    def _run_mtd_detection(self, tag: str, values: pd.Series, tag_df: pd.DataFrame, tag_config: Dict = None) -> Dict[str, Any]:
        """Run Mahalanobis-Taguchi Distance (MTD) detection"""
        
        if tag_config:
            # Use tuned thresholds from baseline analysis
            thresholds = tag_config['thresholds']
            upper_limit = thresholds['upper_limit']
            lower_limit = thresholds['lower_limit']
            confidence = 'HIGH'
        else:
            # Conservative fallback for unconfigured tags
            mean = values.mean()
            std = values.std()
            
            # Use 4-sigma for conservative detection
            upper_limit = mean + 4 * std
            lower_limit = mean - 4 * std
            confidence = 'MEDIUM'
            
        # Detect MTD anomalies
        mtd_anomalies = ((values < lower_limit) | (values > upper_limit))
        mtd_count = mtd_anomalies.sum()
        
        # Get MTD anomaly details
        mtd_data = []
        if 'time' in tag_df.columns and mtd_count > 0:
            anomaly_mask = tag_df['value'].dropna().index[mtd_anomalies]
            for idx in anomaly_mask[:20]:  # Limit to first 20
                try:
                    time_val = tag_df.loc[idx, 'time']
                    value_val = tag_df.loc[idx, 'value']
                    mtd_data.append({
                        'timestamp': time_val.isoformat() if hasattr(time_val, 'isoformat') else str(time_val),
                        'value': float(value_val),
                        'method': 'MTD',
                        'severity': 'HIGH' if abs(value_val - values.mean()) > 3 * values.std() else 'MEDIUM'
                    })
                except:
                    continue
        
        return {
            'count': int(mtd_count),
            'rate': float(mtd_count / len(values)) if len(values) > 0 else 0.0,
            'confidence': confidence,
            'method': 'MTD',
            'thresholds': {
                'upper': float(upper_limit),
                'lower': float(lower_limit)
            },
            'anomalies': mtd_data,
            'baseline_tuned': tag_config is not None
        }
        
    def _run_isolation_forest_detection(self, tag: str, values: pd.Series, tag_df: pd.DataFrame) -> Dict[str, Any]:
        """Run Isolation Forest detection"""
        
        if not SKLEARN_AVAILABLE or len(values) < 10:
            return {
                'count': 0,
                'rate': 0.0,
                'confidence': 'N/A',
                'method': 'Isolation Forest',
                'anomalies': [],
                'available': False
            }
        
        try:
            # Prepare data for Isolation Forest
            X = values.values.reshape(-1, 1)
            
            # Configure Isolation Forest
            contamination = float(self.if_config.get('contamination', 0.02) or 0.02)
            n_estimators = int(self.if_config.get('n_estimators', 100) or 100)
            iso_forest = IsolationForest(contamination=contamination, random_state=42, n_estimators=n_estimators)
            
            # Fit and predict
            outlier_labels = iso_forest.fit_predict(X)
            outlier_scores = iso_forest.score_samples(X)
            
            # Get anomalies (outlier_labels = -1 for anomalies)
            iso_anomalies = (outlier_labels == -1)
            iso_count = iso_anomalies.sum()
            
            # Get Isolation Forest anomaly details
            iso_data = []
            if 'time' in tag_df.columns and iso_count > 0:
                anomaly_indices = np.where(iso_anomalies)[0]
                for i, idx in enumerate(anomaly_indices[:20]):  # Limit to first 20
                    try:
                        time_val = tag_df.iloc[idx]['time']
                        value_val = values.iloc[idx]
                        score = outlier_scores[idx]
                        
                        iso_data.append({
                            'timestamp': time_val.isoformat() if hasattr(time_val, 'isoformat') else str(time_val),
                            'value': float(value_val),
                            'method': 'Isolation Forest',
                            'anomaly_score': float(score),
                            'severity': 'HIGH' if score < -0.1 else 'MEDIUM'
                        })
                    except:
                        continue
            
            return {
                'count': int(iso_count),
                'rate': float(iso_count / len(values)) if len(values) > 0 else 0.0,
                'confidence': 'HIGH',
                'method': 'Isolation Forest',
                'anomalies': iso_data,
                'available': True
            }
            
        except Exception as e:
            logger.warning(f"Isolation Forest failed for tag {tag}: {e}")
            return {
                'count': 0,
                'rate': 0.0,
                'confidence': 'N/A',
                'method': 'Isolation Forest',
                'anomalies': [],
                'available': False,
                'error': str(e)
            }
            
    def _combine_detection_results(self, mtd_results: Dict[str, Any], iso_results: Dict[str, Any], tag_config: Dict = None) -> Dict[str, Any]:
        """Combine MTD and Isolation Forest results"""
        
        # Combine anomaly counts
        total_mtd = mtd_results.get('count', 0)
        total_iso = iso_results.get('count', 0)
        
        # Combine anomaly data and track methods
        combined_anomalies = []
        combined_anomalies.extend(mtd_results.get('anomalies', []))
        combined_anomalies.extend(iso_results.get('anomalies', []))
        
        # Sort by timestamp if available
        if combined_anomalies and 'timestamp' in combined_anomalies[0]:
            try:
                combined_anomalies.sort(key=lambda x: x['timestamp'])
            except:
                pass
        
        # Determine primary method and confidence
        if total_mtd > 0 and total_iso > 0:
            primary_method = f"MTD({total_mtd}) + Isolation Forest({total_iso})"
            confidence = 'VERY HIGH'
        elif total_mtd > 0:
            primary_method = f"MTD({total_mtd})"
            confidence = mtd_results.get('confidence', 'HIGH')
        elif total_iso > 0:
            primary_method = f"Isolation Forest({total_iso})"
            confidence = iso_results.get('confidence', 'HIGH')
        else:
            primary_method = "None"
            confidence = 'N/A'
        
        total_anomalies = total_mtd + total_iso
        total_rate = (mtd_results.get('rate', 0) + iso_results.get('rate', 0))
        
        return {
            'count': total_anomalies,
            'rate': total_rate,
            'confidence': confidence,
            'method': primary_method,
            'mtd_count': total_mtd,
            'isolation_forest_count': total_iso,
            'detection_breakdown': {
                'mtd': {
                    'count': total_mtd,
                    'rate': mtd_results.get('rate', 0),
                    'available': True
                },
                'isolation_forest': {
                    'count': total_iso, 
                    'rate': iso_results.get('rate', 0),
                    'available': iso_results.get('available', False)
                }
            },
            'thresholds': mtd_results.get('thresholds', {}),
            'anomalies': combined_anomalies[:30],  # Limit combined results
            'baseline_tuned': tag_config is not None
        }
        
    def _fallback_detection(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Conservative fallback when no baseline config is available"""
        
        if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
            return self._empty_result()
            
        unique_tags = df['tag'].unique()
        tag_anomalies = {}
        total_anomalies = 0
        
        for tag in unique_tags:
            tag_df = df[df['tag'] == tag].copy()
            values = tag_df['value'].dropna()
            
            if len(values) < 10:
                continue
                
            # Conservative 4-sigma detection
            mean = values.mean()
            std = values.std()
            
            upper_limit = mean + 4 * std
            lower_limit = mean - 4 * std
            
            anomalies = ((values < lower_limit) | (values > upper_limit))
            anomaly_count = anomalies.sum()
            
            if anomaly_count > 0:
                tag_anomalies[tag] = {
                    'count': int(anomaly_count),
                    'rate': float(anomaly_count / len(values)),
                    'confidence': 'LOW',
                    'method': 'fallback_4sigma'
                }
                total_anomalies += anomaly_count
                
        return {
            'method': 'conservative_fallback',
            'total_anomalies': total_anomalies,
            'anomaly_rate': total_anomalies / len(df) if len(df) > 0 else 0,
            'by_tag': tag_anomalies,
            'config_loaded': False,
            'warning': 'No baseline configuration loaded - using conservative detection'
        }
        
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'method': 'none',
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'by_tag': {},
            'config_loaded': self.config_loaded
        }
        
    def get_detection_summary(self, results: Dict[str, Any]) -> str:
        """Generate human-readable summary of detection results"""
        
        if results['total_anomalies'] == 0:
            return "No anomalies detected using tuned thresholds"
            
        method = results.get('method', 'unknown')
        total = results['total_anomalies']
        rate = results['anomaly_rate'] * 100
        tag_count = results.get('tags_with_anomalies', 0)
        
        if method == 'baseline_tuned':
            confidence_msg = "HIGH CONFIDENCE (Baseline-calibrated)"
        elif method == 'conservative_fallback':
            confidence_msg = "MEDIUM CONFIDENCE (Conservative thresholds)"
        else:
            confidence_msg = "LOW CONFIDENCE (Uncalibrated)"
            
        return (
            f"Detected {total:,} anomalies ({rate:.2f}%) across {tag_count} tags\n"
            f"Detection method: {method}\n"
            f"Confidence level: {confidence_msg}"
        )


# Factory function for easy integration
def create_tuned_detector(unit: str = None) -> TunedAnomalyDetector:
    """Create tuned anomaly detector instance"""
    detector = TunedAnomalyDetector()
    
    if unit:
        detector.load_unit_config(unit)
        
    return detector


# Integration function for existing pipeline
def enhanced_anomaly_detection(df: pd.DataFrame, unit: str = None) -> Dict[str, Any]:
    """
    Enhanced anomaly detection function for integration with existing pipeline
    
    Args:
        df: DataFrame with time series data
        unit: Unit identifier for baseline configuration
        
    Returns:
        Enhanced anomaly results with tuning
    """
    detector = create_tuned_detector(unit)
    return detector.detect_anomalies_with_tuning(df, unit)
