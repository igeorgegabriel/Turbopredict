"""
Speed-Aware Anomaly Detection System
Integrates comprehensive speed compensation with hybrid anomaly detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Set, List
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class SpeedAwareAnomalyDetector:
    """Enhanced anomaly detector with speed compensation integration"""

    def __init__(self):
        self.speed_config = self._load_speed_configuration()

    def _load_speed_configuration(self) -> Dict[str, Any]:
        """Load comprehensive speed compensation configuration"""
        config_files = [
            'config/final_complete_speed_compensation.json',
            'config/multi_plant_speed_compensation.json',
            'config/speed_compensation.json'
        ]

        for config_path in config_files:
            try:
                if Path(config_path).exists():
                    with open(config_path, 'r') as f:
                        config = json.load(f)
                    logger.info(f"Loaded speed configuration from {config_path}")
                    return config
            except Exception as e:
                logger.debug(f"Failed to load {config_path}: {e}")

        logger.warning("No speed configuration found - using basic detection")
        return {}

    def get_unit_speed_config(self, unit: str) -> Optional[Dict[str, Any]]:
        """Get speed compensation configuration for specific unit"""
        if not self.speed_config:
            return None

        # Check different config structures - try multiple approaches

        # Method 1: Check speed_compensation_by_plant
        speed_comp = self.speed_config.get('speed_compensation_by_plant', {})
        for plant, plant_data in speed_comp.items():
            units = plant_data.get('units', {})
            if unit in units:
                return {
                    'plant': plant,
                    'unit': unit,
                    'config': units[unit],
                    'compensation_method': plant_data.get('compensation_method')
                }

        # Method 2: Check speed_compensation_formulas for unit baselines
        formulas = self.speed_config.get('speed_compensation_formulas', {})
        rpm_units = formulas.get('rpm_based_units', {}).get('applicable_units', [])
        control_units = formulas.get('control_percentage_units', {}).get('applicable_units', [])

        # Search in applicable units lists
        for unit_desc in rpm_units + control_units:
            if unit in unit_desc:
                # Extract baseline from description like "PCFS K-31-01 (baseline: 3400)"
                try:
                    baseline_str = unit_desc.split('baseline: ')[1].rstrip(')')
                    baseline = float(baseline_str)
                    plant = unit_desc.split(' ')[0]

                    # Determine primary speed tag based on known patterns
                    primary_tag = self._infer_primary_speed_tag(unit, plant)

                    return {
                        'plant': plant,
                        'unit': unit,
                        'config': {
                            'baseline_speed': baseline,
                            'primary_speed_tag': primary_tag,
                            'compensation_type': 'rpm_based' if unit_desc in rpm_units else 'control_percentage'
                        },
                        'compensation_method': 'formula_based'
                    }
                except (IndexError, ValueError):
                    continue

        # Method 3: Check complete_speed_tag_inventory patterns
        inventory = self.speed_config.get('complete_speed_tag_inventory', {})
        for plant, tags in inventory.items():
            if isinstance(tags, list):
                for tag_desc in tags:
                    if unit in str(tag_desc):
                        # Found unit in speed tag inventory
                        return {
                            'plant': plant.replace('_speed_tags', ''),
                            'unit': unit,
                            'config': {'primary_speed_tag': tag_desc.split(' ')[0]},
                            'compensation_method': 'inventory_based'
                        }

        return None

    def _infer_primary_speed_tag(self, unit: str, plant: str) -> str:
        """Infer primary speed tag based on unit and plant patterns"""
        # Known speed tag patterns from configuration
        speed_tag_patterns = {
            'PCFS': {
                'K-12-01': 'PCFS_K-12-01_12SI-401B_PV',
                'K-16-01': 'PCFS_K-16-01_16SI-501B_PV',
                'K-19-01': 'PCFS_K-19-01_19SI-601B_PV',
                'K-31-01': 'PCFS_K-31-01_31SI-301B_PV'
            },
            'PCMSB': {
                'C-104': 'PCM_C-104_SIALH-1451_PV',
                'C-201': 'PCM_C-201_SI-2151_PV',
                'C-202': 'PCM_C-202_SIALH-2251_PV',
                'C-1301': 'PCM_C-1301_SIC13201OFB_PV',
                'C-1302': 'PCM_C-1302_SIC13401OFB_PV',
                'C-02001': 'PCM_C-02001_020SI6601_PV',
                'C-13001': 'PCM_C-13001_130SI4409_PV'
            },
            'ABF': {
                '07-MT001': 'ABF_07-MT001_SI-07002D_new_PV'
            },
            'XT': {
                '07002': 'PCM_XT-07002_070SI8101_PV'
            }
        }

        return speed_tag_patterns.get(plant, {}).get(unit, f"{plant}_{unit}_SI_PV")

    def get_speed_tags_for_unit(self, unit: str) -> List[str]:
        """Get speed indicator tags for a specific unit"""
        unit_config = self.get_unit_speed_config(unit)
        if not unit_config:
            return []

        config = unit_config.get('config', {})
        speed_tags = []

        # Primary speed tag
        if 'primary_speed_tag' in config:
            speed_tags.append(config['primary_speed_tag'])

        # Secondary speed tags
        if 'secondary_speed_tags' in config:
            speed_tags.extend(config['secondary_speed_tags'])

        if 'secondary_speed_tag' in config:
            speed_tags.append(config['secondary_speed_tag'])

        return speed_tags

    def calculate_speed_compensation_factor(self, unit: str, current_speed: float) -> float:
        """Calculate speed compensation factor for current conditions"""
        unit_config = self.get_unit_speed_config(unit)
        if not unit_config:
            return 1.0  # No compensation

        config = unit_config.get('config', {})
        baseline_speed = config.get('baseline_speed', config.get('baseline_value'))

        if not baseline_speed or current_speed <= 0:
            return 1.0

        # Speed compensation: normalized_value = actual_value * (baseline_speed / current_speed)
        compensation_factor = float(baseline_speed) / float(current_speed)

        # Limit extreme compensation factors
        return max(0.1, min(10.0, compensation_factor))

    def apply_speed_compensation(self, df: pd.DataFrame, unit: str) -> pd.DataFrame:
        """Apply speed compensation to sensor data"""
        if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
            return df

        # Get speed tags for this unit
        speed_tags = self.get_speed_tags_for_unit(unit)
        if not speed_tags:
            logger.info(f"No speed tags found for {unit} - using uncompensated data")
            return df

        # Find primary speed tag data in the dataframe
        primary_speed_tag = speed_tags[0] if speed_tags else None
        if not primary_speed_tag:
            return df

        # Get speed data
        speed_data = df[df['tag'] == primary_speed_tag]['value'].dropna()
        if speed_data.empty:
            logger.warning(f"No speed data found for {primary_speed_tag} - using uncompensated data")
            return df

        # Calculate current average speed
        current_speed = float(speed_data.mean())

        # Get compensation factor
        compensation_factor = self.calculate_speed_compensation_factor(unit, current_speed)

        if abs(compensation_factor - 1.0) < 0.01:  # No significant compensation needed
            logger.info(f"Unit {unit} speed compensation factor {compensation_factor:.3f} - minimal compensation")
            return df

        # Apply compensation to non-speed tags
        compensated_df = df.copy()

        for _, row in compensated_df.iterrows():
            tag = row['tag']
            # Don't compensate speed tags themselves
            if tag not in speed_tags:
                original_value = row['value']
                if pd.notna(original_value):
                    compensated_value = float(original_value) * compensation_factor
                    compensated_df.loc[compensated_df.index == row.name, 'value'] = compensated_value

        logger.info(f"Applied speed compensation to {unit}: factor={compensation_factor:.3f}, "
                   f"baseline_speed={self.get_unit_speed_config(unit).get('config', {}).get('baseline_speed', 'unknown')}, "
                   f"current_speed={current_speed:.1f}")

        return compensated_df

    def enhanced_anomaly_detection_with_speed_compensation(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Perform anomaly detection with speed compensation applied"""
        try:
            # Step 1: Apply speed compensation
            compensated_df = self.apply_speed_compensation(df, unit)

            # Step 2: Run enhanced anomaly detection on compensated data
            from .hybrid_anomaly_detection import enhanced_anomaly_detection

            results = enhanced_anomaly_detection(compensated_df, unit)

            # Step 3: Add speed compensation metadata
            speed_config = self.get_unit_speed_config(unit)
            speed_tags = self.get_speed_tags_for_unit(unit)

            # Calculate current speed if available
            current_speed = None
            compensation_factor = 1.0
            if speed_tags:
                primary_speed_tag = speed_tags[0]
                speed_data = df[df['tag'] == primary_speed_tag]['value'].dropna()
                if not speed_data.empty:
                    current_speed = float(speed_data.mean())
                    compensation_factor = self.calculate_speed_compensation_factor(unit, current_speed)

            # Enhance results with speed compensation info
            results['speed_compensation'] = {
                'enabled': bool(speed_config),
                'unit': unit,
                'speed_tags': speed_tags,
                'current_speed': current_speed,
                'compensation_factor': compensation_factor,
                'baseline_speed': speed_config.get('config', {}).get('baseline_speed') if speed_config else None,
                'compensation_method': speed_config.get('compensation_method') if speed_config else None,
                'plant': speed_config.get('plant') if speed_config else None
            }

            # Update method name to indicate speed compensation
            if speed_config:
                original_method = results.get('method', 'unknown')
                results['method'] = f"{original_method}_speed_compensated"

            return results

        except Exception as e:
            logger.error(f"Speed-aware anomaly detection failed for {unit}: {e}")
            # Fallback to basic detection without speed compensation
            try:
                from .hybrid_anomaly_detection import enhanced_anomaly_detection
                results = enhanced_anomaly_detection(df, unit)
                results['speed_compensation'] = {
                    'enabled': False,
                    'error': str(e),
                    'fallback_to_basic': True
                }
                return results
            except Exception as fallback_error:
                return {
                    'method': 'speed_aware_error',
                    'total_anomalies': 0,
                    'anomaly_rate': 0.0,
                    'by_tag': {},
                    'speed_compensation': {
                        'enabled': False,
                        'error': str(e),
                        'fallback_error': str(fallback_error)
                    },
                    'error': f"Both speed-aware and fallback detection failed: {e}"
                }


def create_speed_aware_detector() -> SpeedAwareAnomalyDetector:
    """Factory function to create speed-aware anomaly detector"""
    return SpeedAwareAnomalyDetector()


def speed_aware_anomaly_detection(df: pd.DataFrame, unit: str, auto_plot_anomalies: bool = True) -> Dict[str, Any]:
    """
    Main entry point for speed-aware anomaly detection with automatic anomaly plotting

    Args:
        df: DataFrame with time series data
        unit: Unit identifier
        auto_plot_anomalies: If True, automatically generate 3-month plots for verified anomalies

    Returns:
        Speed-aware anomaly detection results with compensation metadata
    """
    detector = create_speed_aware_detector()
    results = detector.enhanced_anomaly_detection_with_speed_compensation(df, unit)

    # Auto-trigger plotting for verified anomalies (same logic as smart_anomaly_detection)
    if auto_plot_anomalies and results.get('by_tag'):
        try:
            from .anomaly_triggered_plots import generate_anomaly_plots

            # Check if any anomalies are verified (meet the detection pipeline criteria)
            verified_count = 0
            by_tag = results.get('by_tag', {})

            for tag, tag_info in by_tag.items():
                # Check verification criteria (same as in anomaly_triggered_plots.py)
                sigma_count = tag_info.get('sigma_2_5_count', 0)
                ae_count = tag_info.get('autoencoder_count', 0)
                mtd_count = tag_info.get('mtd_count', 0)
                iso_count = tag_info.get('isolation_forest_count', 0)
                confidence = tag_info.get('confidence', 'LOW')

                # Primary detection + verification + confidence
                primary_detected = sigma_count > 0 or ae_count > 0
                verification_detected = mtd_count > 0 or iso_count > 0
                high_confidence = confidence in ['HIGH', 'MEDIUM']

                if primary_detected and verification_detected and high_confidence:
                    verified_count += 1

            # Trigger plotting if verified anomalies found
            if verified_count > 0:
                logger.info(f"Detected {verified_count} verified anomalies in {unit} (speed-compensated) - triggering 3-month diagnostic plots")

                # Prepare detection results in the format expected by the plotter
                detection_results = {unit: results}

                # Generate anomaly-triggered plots
                plot_session_dir = generate_anomaly_plots(detection_results)

                # Add plotting info to results
                results['anomaly_plots_generated'] = True
                results['plot_session_dir'] = str(plot_session_dir)
                results['verified_anomalies_count'] = verified_count

                logger.info(f"Speed-compensated anomaly diagnostic plots generated: {plot_session_dir}")
            else:
                results['anomaly_plots_generated'] = False
                results['verified_anomalies_count'] = 0

        except Exception as e:
            logger.error(f"Error in automatic anomaly plotting for speed-compensated detection: {e}")
            results['anomaly_plots_generated'] = False
            results['plot_error'] = str(e)

    return results