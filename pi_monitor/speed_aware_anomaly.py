"""
Speed-aware anomaly detection for TURBOPREDICT X PROTEAN
Integrates speed compensation with advanced anomaly detection
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
import warnings

from .speed_compensator import SpeedAwareCompensator, SpeedCompensationResult
from .anomaly import add_anomalies

logger = logging.getLogger(__name__)


@dataclass
class SpeedAwareAnomalyResult:
    """Result of speed-aware anomaly detection"""
    original_anomalies: pd.DataFrame
    compensated_anomalies: pd.DataFrame
    speed_compensation_result: SpeedCompensationResult
    anomaly_reduction_factor: float
    speed_correlated_anomalies: pd.DataFrame
    confidence_score: float
    method_used: str
    metadata: Dict[str, Any]


class SpeedAwareAnomalyDetector:
    """Advanced anomaly detection with speed compensation and correlation analysis"""

    def __init__(self, speed_compensator: Optional[SpeedAwareCompensator] = None):
        """Initialize speed-aware anomaly detector

        Args:
            speed_compensator: Speed compensator instance (creates new if None)
        """
        self.speed_compensator = speed_compensator or SpeedAwareCompensator()
        self.anomaly_cache = {}
        self.speed_correlation_threshold = 0.7
        self.min_anomaly_duration = timedelta(minutes=5)

        logger.info("Speed-aware anomaly detector initialized")

    def detect_speed_aware_anomalies(self,
                                   data: pd.DataFrame,
                                   plant: str,
                                   unit: str,
                                   *,
                                   target_tags: Optional[List[str]] = None,
                                   anomaly_method: str = "2_5_sigma_verified",
                                   speed_correlation_analysis: bool = True,
                                   adaptive_thresholds: bool = True) -> SpeedAwareAnomalyResult:
        """Detect anomalies with speed awareness and compensation

        Args:
            data: Input data containing measurements
            plant: Plant name
            unit: Unit name
            target_tags: Specific tags to analyze (if None, analyzes all non-speed tags)
            anomaly_method: Method for anomaly detection ('isolation_forest', 'statistical', 'hybrid')
            speed_correlation_analysis: Whether to analyze speed correlation
            adaptive_thresholds: Whether to use speed-adaptive thresholds

        Returns:
            SpeedAwareAnomalyResult with comprehensive analysis
        """
        metadata = {
            "plant": plant,
            "unit": unit,
            "start_time": datetime.now().isoformat(),
            "data_points": len(data),
            "method": anomaly_method
        }

        # Step 1: Detect anomalies in original data
        logger.info(f"Detecting original anomalies for {plant}.{unit}")
        original_anomalies = self._detect_anomalies(data, method=anomaly_method, tag_filter=target_tags)

        # Step 2: Apply speed compensation
        logger.info(f"Applying speed compensation for {plant}.{unit}")
        compensation_result = self.speed_compensator.compensate_data(data, plant, unit, target_tags)

        # Step 3: Detect anomalies in compensated data
        logger.info(f"Detecting anomalies in compensated data for {plant}.{unit}")
        compensated_anomalies = self._detect_anomalies(
            compensation_result.compensated_data,
            method=anomaly_method,
            tag_filter=target_tags,
            adaptive_thresholds=adaptive_thresholds,
            speed_data=compensation_result.speed_data
        )

        # Step 4: Analyze speed correlation if requested
        speed_correlated_anomalies = pd.DataFrame()
        if speed_correlation_analysis and not compensation_result.speed_data.empty:
            speed_correlated_anomalies = self._analyze_speed_correlation(
                original_anomalies, compensation_result.speed_data
            )

        # Step 5: Calculate metrics
        anomaly_reduction_factor = self._calculate_anomaly_reduction(
            original_anomalies, compensated_anomalies
        )

        confidence_score = self._calculate_detection_confidence(
            compensation_result, original_anomalies, compensated_anomalies
        )

        # Step 6: Add metadata
        metadata.update({
            "original_anomaly_count": len(original_anomalies),
            "compensated_anomaly_count": len(compensated_anomalies),
            "speed_correlated_count": len(speed_correlated_anomalies),
            "anomaly_reduction": anomaly_reduction_factor,
            "confidence": confidence_score,
            "compensation_factor": compensation_result.compensation_factor,
            "compensation_confidence": compensation_result.confidence,
            "end_time": datetime.now().isoformat()
        })

        return SpeedAwareAnomalyResult(
            original_anomalies=original_anomalies,
            compensated_anomalies=compensated_anomalies,
            speed_compensation_result=compensation_result,
            anomaly_reduction_factor=anomaly_reduction_factor,
            speed_correlated_anomalies=speed_correlated_anomalies,
            confidence_score=confidence_score,
            method_used=anomaly_method,
            metadata=metadata
        )

    def _detect_anomalies(self,
                         data: pd.DataFrame,
                         method: str = "2_5_sigma_verified",
                         tag_filter: Optional[List[str]] = None,
                         adaptive_thresholds: bool = False,
                         speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Detect anomalies using specified method"""

        if data.empty:
            return pd.DataFrame()

        # Filter data if tag_filter is provided
        if tag_filter:
            filtered_data = data[data['tag'].isin(tag_filter)].copy()
        else:
            filtered_data = data.copy()

        if filtered_data.empty:
            return pd.DataFrame()

        try:
            if method == "2_5_sigma_verified":
                return self._2_5_sigma_verified_anomalies(filtered_data, adaptive_thresholds, speed_data)
            elif method == "isolation_forest":
                return self._isolation_forest_anomalies(filtered_data, adaptive_thresholds, speed_data)
            elif method == "statistical":
                return self._statistical_anomalies(filtered_data, adaptive_thresholds, speed_data)
            elif method == "hybrid":
                return self._hybrid_anomalies(filtered_data, adaptive_thresholds, speed_data)
            else:
                # Fallback to 2.5-sigma verified method
                logger.warning(f"Unknown method {method}, using 2.5-sigma verified fallback")
                return self._2_5_sigma_verified_anomalies(filtered_data, adaptive_thresholds, speed_data)

        except Exception as e:
            logger.error(f"Error in anomaly detection ({method}): {e}")
            # Fallback to original method
            return add_anomalies(filtered_data)

    def _2_5_sigma_verified_anomalies(self,
                                     data: pd.DataFrame,
                                     adaptive_thresholds: bool = False,
                                     speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Primary 2.5-sigma detection verified by MTD and Isolation Forest"""

        anomalies_list = []

        for tag in data['tag'].unique():
            tag_data = data[data['tag'] == tag].copy()

            if len(tag_data) < 20:  # Need minimum data points for reliable detection
                continue

            # Step 1: Primary 2.5-sigma detection
            primary_anomalies = self._primary_2_5_sigma_detection(tag_data, adaptive_thresholds, speed_data)

            if primary_anomalies.empty:
                continue

            # Step 2: Verify with MTD (Modified Thompson Tau)
            mtd_verified = self._verify_with_mtd(primary_anomalies, tag_data)

            # Step 3: Verify with Isolation Forest
            if_verified = self._verify_with_isolation_forest(mtd_verified, tag_data)

            if not if_verified.empty:
                if_verified['anomaly_method'] = '2.5sigma_mtd_if_verified'
                if_verified['verification_stages'] = 'primary+mtd+isolation_forest'
                anomalies_list.append(if_verified)

        if anomalies_list:
            return pd.concat(anomalies_list, ignore_index=True)
        else:
            return pd.DataFrame()

    def _primary_2_5_sigma_detection(self,
                                    tag_data: pd.DataFrame,
                                    adaptive_thresholds: bool = False,
                                    speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Primary 2.5-sigma anomaly detection"""

        # Calculate statistical measures
        mean_val = tag_data['value'].mean()
        std_val = tag_data['value'].std()

        if std_val == 0:
            return pd.DataFrame()

        # 2.5-sigma threshold (configurable)
        sigma_threshold = float(os.environ.get('PRIMARY_SIGMA_THRESHOLD', '2.5'))

        # Adaptive threshold adjustment based on speed if available
        if adaptive_thresholds and speed_data is not None and not speed_data.empty:
            # Adjust threshold based on speed variability
            speed_cv = speed_data['value'].std() / speed_data['value'].mean() if speed_data['value'].mean() != 0 else 0
            if speed_cv > 0.2:  # High speed variability
                sigma_threshold = max(2.0, sigma_threshold - 0.3)
            elif speed_cv < 0.05:  # Very stable speed
                sigma_threshold = min(3.0, sigma_threshold + 0.2)

        # Calculate z-scores and find anomalies
        z_scores = np.abs((tag_data['value'] - mean_val) / std_val)
        anomaly_mask = z_scores > sigma_threshold

        if not anomaly_mask.any():
            return pd.DataFrame()

        anomalies = tag_data[anomaly_mask].copy()
        anomalies['anomaly_score'] = z_scores[anomaly_mask]
        anomalies['sigma_threshold_used'] = sigma_threshold
        anomalies['detection_stage'] = 'primary_2.5sigma'

        return anomalies

    def _verify_with_mtd(self, primary_anomalies: pd.DataFrame, tag_data: pd.DataFrame) -> pd.DataFrame:
        """Verify anomalies using Modified Thompson Tau (MTD) method"""

        if primary_anomalies.empty:
            return pd.DataFrame()

        verified_anomalies = []

        # MTD verification for each anomaly
        for _, anomaly in primary_anomalies.iterrows():
            anomaly_value = anomaly['value']
            anomaly_time = anomaly['time']

            # Get surrounding data window (±30 data points or ±1 hour, whichever is smaller)
            time_window = timedelta(hours=1)
            window_data = tag_data[
                (tag_data['time'] >= anomaly_time - time_window) &
                (tag_data['time'] <= anomaly_time + time_window)
            ].copy()

            if len(window_data) < 10:
                # Not enough data for MTD verification, use broader window
                window_data = tag_data.copy()

            # Calculate Thompson Tau statistic
            n = len(window_data)
            if n < 3:
                continue

            # Modified Thompson Tau critical value (approximation)
            if n <= 10:
                tau_critical = 1.15  # Conservative for small samples
            elif n <= 50:
                tau_critical = 1.4
            else:
                tau_critical = 1.5

            # Calculate tau statistic for the anomaly value
            mean_window = window_data['value'].mean()
            std_window = window_data['value'].std()

            if std_window == 0:
                continue

            tau_stat = abs(anomaly_value - mean_window) / std_window

            # Verify if anomaly passes MTD test
            if tau_stat > tau_critical:
                anomaly_copy = anomaly.copy()
                anomaly_copy['mtd_tau_statistic'] = tau_stat
                anomaly_copy['mtd_tau_critical'] = tau_critical
                anomaly_copy['detection_stage'] = 'primary_2.5sigma+mtd_verified'
                verified_anomalies.append(anomaly_copy)

        if verified_anomalies:
            return pd.DataFrame(verified_anomalies)
        else:
            return pd.DataFrame()

    def _verify_with_isolation_forest(self, mtd_verified: pd.DataFrame, tag_data: pd.DataFrame) -> pd.DataFrame:
        """Final verification using Isolation Forest"""

        if mtd_verified.empty:
            return pd.DataFrame()

        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.preprocessing import StandardScaler

            # Prepare features for isolation forest
            tag_data_sorted = tag_data.sort_values('time')
            tag_data_sorted['value_lag1'] = tag_data_sorted['value'].shift(1)
            tag_data_sorted['value_diff'] = tag_data_sorted['value'].diff()
            tag_data_sorted['value_rolling_mean'] = tag_data_sorted['value'].rolling(window=5, min_periods=1).mean()
            tag_data_sorted['value_rolling_std'] = tag_data_sorted['value'].rolling(window=5, min_periods=1).std()

            # Create feature matrix
            features = ['value', 'value_lag1', 'value_diff', 'value_rolling_mean', 'value_rolling_std']
            feature_data = tag_data_sorted[features].fillna(0)

            if len(feature_data) < 10:
                # Not enough data for IF, return MTD verified anomalies
                mtd_verified_copy = mtd_verified.copy()
                mtd_verified_copy['detection_stage'] = 'primary_2.5sigma+mtd_verified+if_insufficient_data'
                return mtd_verified_copy

            # Scale features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(feature_data)

            # Apply Isolation Forest with conservative contamination
            contamination = min(0.1, len(mtd_verified) / len(tag_data_sorted))  # Conservative contamination
            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            anomaly_labels = iso_forest.fit_predict(scaled_features)

            # Check which MTD verified anomalies are also detected by IF
            verified_final = []

            for _, anomaly in mtd_verified.iterrows():
                anomaly_time = anomaly['time']

                # Find closest data point in sorted data
                closest_idx = (tag_data_sorted['time'] - anomaly_time).abs().idxmin()
                closest_position = tag_data_sorted.index.get_loc(closest_idx)

                # Check if IF also detected this as anomaly
                if closest_position < len(anomaly_labels) and anomaly_labels[closest_position] == -1:
                    anomaly_copy = anomaly.copy()
                    anomaly_copy['if_anomaly_score'] = iso_forest.score_samples(scaled_features[closest_position:closest_position+1])[0]
                    anomaly_copy['detection_stage'] = 'primary_2.5sigma+mtd_verified+if_verified'
                    verified_final.append(anomaly_copy)

            if verified_final:
                return pd.DataFrame(verified_final)
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.warning(f"Isolation Forest verification failed: {e}")
            # Return MTD verified anomalies if IF fails
            mtd_verified_copy = mtd_verified.copy()
            mtd_verified_copy['detection_stage'] = 'primary_2.5sigma+mtd_verified+if_error'
            return mtd_verified_copy

    def _isolation_forest_anomalies(self,
                                   data: pd.DataFrame,
                                   adaptive_thresholds: bool = False,
                                   speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Detect anomalies using Isolation Forest"""

        anomalies_list = []

        for tag in data['tag'].unique():
            tag_data = data[data['tag'] == tag].copy()

            if len(tag_data) < 10:  # Need minimum data points
                continue

            # Prepare features for isolation forest
            tag_data = tag_data.sort_values('time')
            tag_data['value_lag1'] = tag_data['value'].shift(1)
            tag_data['value_diff'] = tag_data['value'].diff()
            tag_data['value_rolling_mean'] = tag_data['value'].rolling(window=5, min_periods=1).mean()
            tag_data['value_rolling_std'] = tag_data['value'].rolling(window=5, min_periods=1).std()

            # Create feature matrix
            features = ['value', 'value_lag1', 'value_diff', 'value_rolling_mean', 'value_rolling_std']
            feature_data = tag_data[features].fillna(0)

            # Add speed features if available and adaptive thresholds enabled
            if adaptive_thresholds and speed_data is not None and not speed_data.empty:
                # Align speed data with tag data by time
                merged_data = pd.merge_asof(
                    tag_data.sort_values('time'),
                    speed_data.sort_values('time')[['time', 'value']].rename(columns={'value': 'speed_value'}),
                    on='time',
                    direction='nearest',
                    tolerance=pd.Timedelta('10 minutes')
                )
                if 'speed_value' in merged_data.columns:
                    feature_data['speed_value'] = merged_data['speed_value'].fillna(0)

            # Scale features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(feature_data)

            # Apply Isolation Forest
            contamination = 0.1 if not adaptive_thresholds else 0.05  # More conservative with speed awareness
            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            anomaly_labels = iso_forest.fit_predict(scaled_features)

            # Get anomalies
            anomaly_mask = anomaly_labels == -1
            tag_anomalies = tag_data[anomaly_mask].copy()

            if not tag_anomalies.empty:
                tag_anomalies['anomaly_score'] = iso_forest.score_samples(scaled_features)[anomaly_mask]
                tag_anomalies['anomaly_method'] = 'isolation_forest'
                anomalies_list.append(tag_anomalies)

        if anomalies_list:
            return pd.concat(anomalies_list, ignore_index=True)
        else:
            return pd.DataFrame()

    def _statistical_anomalies(self,
                              data: pd.DataFrame,
                              adaptive_thresholds: bool = False,
                              speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Detect anomalies using statistical methods (Z-score, IQR)"""

        anomalies_list = []

        for tag in data['tag'].unique():
            tag_data = data[data['tag'] == tag].copy()

            if len(tag_data) < 10:
                continue

            # Calculate statistical measures
            mean_val = tag_data['value'].mean()
            std_val = tag_data['value'].std()
            q1 = tag_data['value'].quantile(0.25)
            q3 = tag_data['value'].quantile(0.75)
            iqr = q3 - q1

            # Adaptive thresholds based on speed if available
            z_threshold = 3.0
            iqr_multiplier = 1.5

            if adaptive_thresholds and speed_data is not None and not speed_data.empty:
                # Adjust thresholds based on speed variability
                speed_cv = speed_data['value'].std() / speed_data['value'].mean() if speed_data['value'].mean() != 0 else 0
                if speed_cv > 0.2:  # High speed variability
                    z_threshold = 2.5
                    iqr_multiplier = 1.2

            # Z-score anomalies
            if std_val > 0:
                z_scores = np.abs((tag_data['value'] - mean_val) / std_val)
                z_anomalies = tag_data[z_scores > z_threshold].copy()
                z_anomalies['anomaly_score'] = z_scores[z_scores > z_threshold]
                z_anomalies['anomaly_method'] = 'z_score'
            else:
                z_anomalies = pd.DataFrame()

            # IQR anomalies
            iqr_lower = q1 - iqr_multiplier * iqr
            iqr_upper = q3 + iqr_multiplier * iqr
            iqr_mask = (tag_data['value'] < iqr_lower) | (tag_data['value'] > iqr_upper)
            iqr_anomalies = tag_data[iqr_mask].copy()
            if not iqr_anomalies.empty:
                iqr_anomalies['anomaly_score'] = np.abs(iqr_anomalies['value'] - mean_val) / std_val if std_val > 0 else 1.0
                iqr_anomalies['anomaly_method'] = 'iqr'

            # Combine anomalies
            tag_anomalies_list = [df for df in [z_anomalies, iqr_anomalies] if not df.empty]
            if tag_anomalies_list:
                tag_anomalies = pd.concat(tag_anomalies_list, ignore_index=True)
                tag_anomalies = tag_anomalies.drop_duplicates(subset=['time', 'tag', 'value'])
                anomalies_list.append(tag_anomalies)

        if anomalies_list:
            return pd.concat(anomalies_list, ignore_index=True)
        else:
            return pd.DataFrame()

    def _hybrid_anomalies(self,
                         data: pd.DataFrame,
                         adaptive_thresholds: bool = False,
                         speed_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Detect anomalies using hybrid approach (combination of methods)"""

        # Get anomalies from both methods
        iso_anomalies = self._isolation_forest_anomalies(data, adaptive_thresholds, speed_data)
        stat_anomalies = self._statistical_anomalies(data, adaptive_thresholds, speed_data)

        # Combine and deduplicate
        if not iso_anomalies.empty and not stat_anomalies.empty:
            combined_anomalies = pd.concat([iso_anomalies, stat_anomalies], ignore_index=True)
            combined_anomalies = combined_anomalies.drop_duplicates(subset=['time', 'tag', 'value'])
            combined_anomalies['anomaly_method'] = 'hybrid'
            return combined_anomalies
        elif not iso_anomalies.empty:
            iso_anomalies['anomaly_method'] = 'hybrid_iso'
            return iso_anomalies
        elif not stat_anomalies.empty:
            stat_anomalies['anomaly_method'] = 'hybrid_stat'
            return stat_anomalies
        else:
            return pd.DataFrame()

    def _analyze_speed_correlation(self,
                                 anomalies: pd.DataFrame,
                                 speed_data: pd.DataFrame) -> pd.DataFrame:
        """Analyze correlation between anomalies and speed variations"""

        if anomalies.empty or speed_data.empty:
            return pd.DataFrame()

        speed_correlated = []

        for _, anomaly in anomalies.iterrows():
            anomaly_time = anomaly['time']

            # Find speed data around anomaly time (±30 minutes)
            time_window = timedelta(minutes=30)
            speed_window = speed_data[
                (speed_data['time'] >= anomaly_time - time_window) &
                (speed_data['time'] <= anomaly_time + time_window)
            ]

            if len(speed_window) < 3:
                continue

            # Calculate speed variability in window
            speed_std = speed_window['value'].std()
            speed_mean = speed_window['value'].mean()

            if speed_mean > 0:
                speed_cv = speed_std / speed_mean

                # Check if anomaly coincides with speed variation
                if speed_cv > 0.1:  # 10% coefficient of variation threshold
                    anomaly_copy = anomaly.copy()
                    anomaly_copy['speed_correlation'] = speed_cv
                    anomaly_copy['speed_mean_window'] = speed_mean
                    anomaly_copy['speed_std_window'] = speed_std
                    speed_correlated.append(anomaly_copy)

        if speed_correlated:
            result = pd.DataFrame(speed_correlated)
            return result
        else:
            return pd.DataFrame()

    def _calculate_anomaly_reduction(self,
                                   original_anomalies: pd.DataFrame,
                                   compensated_anomalies: pd.DataFrame) -> float:
        """Calculate the reduction in anomalies after speed compensation"""

        if original_anomalies.empty:
            return 0.0

        original_count = len(original_anomalies)
        compensated_count = len(compensated_anomalies)

        reduction = (original_count - compensated_count) / original_count
        return max(0.0, reduction)

    def _calculate_detection_confidence(self,
                                      compensation_result: SpeedCompensationResult,
                                      original_anomalies: pd.DataFrame,
                                      compensated_anomalies: pd.DataFrame) -> float:
        """Calculate overall confidence in the speed-aware detection"""

        # Base confidence from speed compensation
        base_confidence = compensation_result.confidence

        # Factor in anomaly reduction quality
        if not original_anomalies.empty:
            reduction_factor = self._calculate_anomaly_reduction(original_anomalies, compensated_anomalies)
            # Higher reduction generally indicates better speed compensation
            reduction_bonus = min(0.3, reduction_factor * 0.5)
        else:
            reduction_bonus = 0.0

        # Factor in data quality
        data_quality_factor = 1.0
        if len(compensation_result.speed_data) < 10:
            data_quality_factor = 0.8

        # Factor in warnings
        warning_penalty = len(compensation_result.warnings) * 0.05

        final_confidence = (base_confidence + reduction_bonus) * data_quality_factor - warning_penalty
        return max(0.0, min(1.0, final_confidence))

    def batch_analyze_units(self,
                           data: pd.DataFrame,
                           unit_list: Optional[List[Tuple[str, str]]] = None) -> Dict[str, SpeedAwareAnomalyResult]:
        """Analyze multiple units in batch"""

        if unit_list is None:
            unit_list = self.speed_compensator._get_all_units()

        results = {}

        for plant, unit in unit_list:
            try:
                # Filter data for this unit
                unit_data = self.speed_compensator._filter_data_for_unit(data, plant, unit)

                if unit_data.empty:
                    logger.warning(f"No data found for {plant}.{unit}")
                    continue

                # Analyze with speed awareness
                result = self.detect_speed_aware_anomalies(unit_data, plant, unit)
                results[f"{plant}.{unit}"] = result

                logger.info(f"Analyzed {plant}.{unit}: "
                           f"original_anomalies={len(result.original_anomalies)}, "
                           f"compensated_anomalies={len(result.compensated_anomalies)}, "
                           f"reduction={result.anomaly_reduction_factor:.2f}")

            except Exception as e:
                logger.error(f"Error analyzing {plant}.{unit}: {e}")
                continue

        return results

    def generate_anomaly_report(self, results: Dict[str, SpeedAwareAnomalyResult]) -> pd.DataFrame:
        """Generate comprehensive anomaly analysis report"""

        report_data = []

        for unit_key, result in results.items():
            report_data.append({
                'unit': unit_key,
                'original_anomalies': len(result.original_anomalies),
                'compensated_anomalies': len(result.compensated_anomalies),
                'speed_correlated_anomalies': len(result.speed_correlated_anomalies),
                'anomaly_reduction_factor': result.anomaly_reduction_factor,
                'confidence_score': result.confidence_score,
                'compensation_factor': result.speed_compensation_result.compensation_factor,
                'compensation_confidence': result.speed_compensation_result.confidence,
                'method_used': result.method_used,
                'warnings_count': len(result.speed_compensation_result.warnings),
                'data_points': result.metadata.get('data_points', 0),
                'analysis_timestamp': result.metadata.get('end_time', '')
            })

        return pd.DataFrame(report_data)


def create_speed_aware_detector(speed_compensator: Optional[SpeedAwareCompensator] = None) -> SpeedAwareAnomalyDetector:
    """Factory function to create speed-aware anomaly detector"""
    return SpeedAwareAnomalyDetector(speed_compensator)


# Example usage
if __name__ == "__main__":
    # Initialize detector
    detector = create_speed_aware_detector()

    # Example: Analyze a unit (requires actual data)
    # result = detector.detect_speed_aware_anomalies(data, "PCFS", "K-12-01")
    # print(f"Analysis complete: {result.metadata}")

    print("Speed-aware anomaly detector initialized successfully")