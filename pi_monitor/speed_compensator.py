"""
Speed-aware compensation system for TURBOPREDICT X PROTEAN
Implements speed compensation across all plants (PCFS, ABF, PCMSB)
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
import warnings

logger = logging.getLogger(__name__)


@dataclass
class SpeedCompensationResult:
    """Result of speed compensation operation"""
    original_data: pd.DataFrame
    compensated_data: pd.DataFrame
    speed_data: pd.DataFrame
    compensation_factor: float
    method_used: str
    confidence: float
    warnings: List[str]
    metadata: Dict[str, Any]


class SpeedAwareCompensator:
    """Speed-aware compensation system for industrial equipment monitoring"""

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize speed compensator with configuration.

        Args:
            config_path: Path to speed-aware configuration file
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "speed_aware_config.json"

        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.compensation_cache = {}

        logger.info(f"Speed compensator initialized with {len(self._get_all_units())} units across {len(self.config['plants'])} plants")

    def _load_config(self) -> Dict[str, Any]:
        """Load speed-aware configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded speed configuration: {config['config_type']} v{config['version']}")
            return config
        except Exception as e:
            logger.error(f"Failed to load speed config from {self.config_path}: {e}")
            raise

    def _get_all_units(self) -> List[Tuple[str, str]]:
        """Get all plant-unit combinations"""
        units = []
        for plant_name, plant_config in self.config['plants'].items():
            for unit_name in plant_config['units'].keys():
                units.append((plant_name, unit_name))
        return units

    def get_speed_tags_for_unit(self, plant: str, unit: str) -> List[str]:
        """Get speed tags for a specific unit"""
        try:
            unit_config = self.config['plants'][plant]['units'][unit]
            speed_tags = []

            # Handle different tag configurations
            if 'speed_tag' in unit_config:
                speed_tags.append(unit_config['speed_tag'])
            if 'primary_speed_tag' in unit_config:
                speed_tags.append(unit_config['primary_speed_tag'])
            if 'secondary_speed_tag' in unit_config:
                speed_tags.append(unit_config['secondary_speed_tag'])

            return speed_tags
        except KeyError as e:
            logger.warning(f"Unit {plant}.{unit} not found in configuration: {e}")
            return []

    def is_speed_aware_enabled(self, plant: str, unit: str) -> bool:
        """Check if speed awareness is enabled for a unit"""
        try:
            return (plant in self.config['plants'] and
                   unit in self.config['plants'][plant]['units'] and
                   self.config['global_settings']['enable_speed_compensation'])
        except KeyError:
            return False

    def get_compensation_method(self, plant: str, unit: str) -> str:
        """Get compensation method for a unit"""
        try:
            unit_config = self.config['plants'][plant]['units'][unit]
            return unit_config.get('compensation_method',
                                 self.config['global_settings']['default_compensation_method'])
        except KeyError:
            return self.config['global_settings']['default_compensation_method']

    def extract_speed_data(self, data: pd.DataFrame, plant: str, unit: str) -> Optional[pd.DataFrame]:
        """Extract speed data for a unit from the dataset"""
        speed_tags = self.get_speed_tags_for_unit(plant, unit)
        if not speed_tags:
            return None

        # Filter data for speed tags
        speed_data = data[data['tag'].isin(speed_tags)].copy()

        if speed_data.empty:
            logger.warning(f"No speed data found for {plant}.{unit} with tags {speed_tags}")
            return None

        # Sort by time for proper processing
        speed_data = speed_data.sort_values('time')
        return speed_data

    def calculate_compensation_factor(self, speed_data: pd.DataFrame, plant: str, unit: str) -> Tuple[float, float, List[str]]:
        """Calculate compensation factor for a unit

        Returns:
            Tuple of (compensation_factor, confidence, warnings)
        """
        warnings_list = []

        try:
            unit_config = self.config['plants'][plant]['units'][unit]
            baseline_speed = unit_config['baseline_speed']
            method = self.get_compensation_method(plant, unit)

            # Handle different compensation methods
            if method == 'rpm_normalized':
                current_speed = speed_data['value'].mean()
                if current_speed == 0:
                    warnings_list.append("Zero speed detected, using baseline")
                    return 1.0, 0.0, warnings_list

                compensation_factor = baseline_speed / current_speed
                confidence = self._calculate_confidence(speed_data, unit_config)

            elif method == 'percentage_normalized':
                current_percentage = speed_data['value'].mean()
                if current_percentage == 0:
                    warnings_list.append("Zero percentage detected, using baseline")
                    return 1.0, 0.0, warnings_list

                compensation_factor = baseline_speed / current_percentage
                confidence = self._calculate_confidence(speed_data, unit_config)

            elif method == 'dual_rpm_averaged':
                # Handle dual speed indicators
                speed_tags = self.get_speed_tags_for_unit(plant, unit)
                if len(speed_tags) >= 2:
                    primary_data = speed_data[speed_data['tag'] == speed_tags[0]]
                    secondary_data = speed_data[speed_data['tag'] == speed_tags[1]]

                    if not primary_data.empty and not secondary_data.empty:
                        avg_speed = (primary_data['value'].mean() + secondary_data['value'].mean()) / 2
                        compensation_factor = baseline_speed / avg_speed if avg_speed != 0 else 1.0
                        confidence = self._calculate_dual_confidence(primary_data, secondary_data, unit_config)
                    else:
                        warnings_list.append("Incomplete dual speed data, using single speed")
                        current_speed = speed_data['value'].mean()
                        compensation_factor = baseline_speed / current_speed if current_speed != 0 else 1.0
                        confidence = 0.5
                else:
                    warnings_list.append("Dual method requested but insufficient speed tags")
                    compensation_factor = 1.0
                    confidence = 0.0
            else:
                warnings_list.append(f"Unknown compensation method: {method}")
                compensation_factor = 1.0
                confidence = 0.0

            # Check if compensation factor is reasonable
            if compensation_factor < 0.1 or compensation_factor > 10.0:
                warnings_list.append(f"Extreme compensation factor: {compensation_factor:.2f}")
                confidence *= 0.5

            return compensation_factor, confidence, warnings_list

        except Exception as e:
            logger.error(f"Error calculating compensation factor for {plant}.{unit}: {e}")
            return 1.0, 0.0, [f"Calculation error: {str(e)}"]

    def _calculate_confidence(self, speed_data: pd.DataFrame, unit_config: Dict[str, Any]) -> float:
        """Calculate confidence score for speed compensation"""
        try:
            # Base confidence
            confidence = 1.0

            # Reduce confidence for insufficient data
            min_points = self.config['global_settings']['minimum_speed_data_points']
            if len(speed_data) < min_points:
                confidence *= len(speed_data) / min_points

            # Reduce confidence for high variability
            speed_std = speed_data['value'].std()
            stability_threshold = unit_config.get('speed_stability_threshold', 100)
            if speed_std > stability_threshold:
                confidence *= stability_threshold / speed_std

            # Check operating range
            speed_mean = speed_data['value'].mean()
            operating_range = unit_config.get('operating_range', [0, float('inf')])
            if not (operating_range[0] <= speed_mean <= operating_range[1]):
                confidence *= 0.7

            return max(0.0, min(1.0, confidence))

        except Exception as e:
            logger.warning(f"Error calculating confidence: {e}")
            return 0.5

    def _calculate_dual_confidence(self, primary_data: pd.DataFrame, secondary_data: pd.DataFrame,
                                 unit_config: Dict[str, Any]) -> float:
        """Calculate confidence for dual speed indicators with cross-validation"""
        try:
            # Calculate individual confidences
            primary_conf = self._calculate_confidence(primary_data, unit_config)
            secondary_conf = self._calculate_confidence(secondary_data, unit_config)

            # Cross-validation: check agreement between sensors
            primary_mean = primary_data['value'].mean()
            secondary_mean = secondary_data['value'].mean()

            if primary_mean != 0 and secondary_mean != 0:
                agreement = 1.0 - abs(primary_mean - secondary_mean) / max(primary_mean, secondary_mean)
                cross_validation_factor = max(0.5, agreement)
            else:
                cross_validation_factor = 0.5

            # Combined confidence
            base_confidence = (primary_conf + secondary_conf) / 2
            return base_confidence * cross_validation_factor

        except Exception as e:
            logger.warning(f"Error calculating dual confidence: {e}")
            return 0.5

    def compensate_data(self, data: pd.DataFrame, plant: str, unit: str,
                       target_tags: Optional[List[str]] = None) -> SpeedCompensationResult:
        """Apply speed compensation to data for a specific unit

        Args:
            data: Input data containing both speed and target measurements
            plant: Plant name
            unit: Unit name
            target_tags: Specific tags to compensate (if None, compensates all non-speed tags)

        Returns:
            SpeedCompensationResult with original and compensated data
        """
        warnings_list = []

        # Check if speed awareness is enabled
        if not self.is_speed_aware_enabled(plant, unit):
            warnings_list.append(f"Speed awareness not enabled for {plant}.{unit}")
            return SpeedCompensationResult(
                original_data=data,
                compensated_data=data.copy(),
                speed_data=pd.DataFrame(),
                compensation_factor=1.0,
                method_used="none",
                confidence=0.0,
                warnings=warnings_list,
                metadata={"reason": "speed_awareness_disabled"}
            )

        # Extract speed data
        speed_data = self.extract_speed_data(data, plant, unit)
        if speed_data is None or speed_data.empty:
            warnings_list.append(f"No speed data available for {plant}.{unit}")
            return SpeedCompensationResult(
                original_data=data,
                compensated_data=data.copy(),
                speed_data=pd.DataFrame(),
                compensation_factor=1.0,
                method_used="none",
                confidence=0.0,
                warnings=warnings_list,
                metadata={"reason": "no_speed_data"}
            )

        # Calculate compensation factor
        compensation_factor, confidence, comp_warnings = self.calculate_compensation_factor(
            speed_data, plant, unit)
        warnings_list.extend(comp_warnings)

        # Check confidence threshold
        min_confidence = self.config['global_settings']['compensation_confidence_threshold']
        if confidence < min_confidence:
            warnings_list.append(f"Low confidence ({confidence:.2f}) below threshold ({min_confidence})")

        # Determine target tags to compensate
        speed_tags = self.get_speed_tags_for_unit(plant, unit)
        if target_tags is None:
            # Compensate all non-speed tags
            all_tags = data['tag'].unique()
            target_tags = [tag for tag in all_tags if tag not in speed_tags]

        # Apply compensation
        compensated_data = data.copy()
        method_used = self.get_compensation_method(plant, unit)

        # Only apply compensation if confidence is sufficient and factor is reasonable
        if confidence >= min_confidence and 0.1 <= compensation_factor <= 10.0:
            mask = compensated_data['tag'].isin(target_tags)
            compensated_data.loc[mask, 'value'] = compensated_data.loc[mask, 'value'] * compensation_factor

            # Add compensation metadata
            compensated_data.loc[mask, 'compensated'] = True
            compensated_data.loc[mask, 'compensation_factor'] = compensation_factor
        else:
            warnings_list.append("Compensation not applied due to low confidence or extreme factor")
            compensation_factor = 1.0
            method_used = "none"

        return SpeedCompensationResult(
            original_data=data,
            compensated_data=compensated_data,
            speed_data=speed_data,
            compensation_factor=compensation_factor,
            method_used=method_used,
            confidence=confidence,
            warnings=warnings_list,
            metadata={
                "plant": plant,
                "unit": unit,
                "target_tags": target_tags,
                "speed_tags": speed_tags,
                "timestamp": datetime.now().isoformat()
            }
        )

    def batch_compensate(self, data: pd.DataFrame, unit_filter: Optional[List[Tuple[str, str]]] = None) -> Dict[str, SpeedCompensationResult]:
        """Apply speed compensation to multiple units in batch

        Args:
            data: Input data containing measurements from multiple units
            unit_filter: List of (plant, unit) tuples to process (if None, processes all configured units)

        Returns:
            Dictionary mapping unit keys to compensation results
        """
        if unit_filter is None:
            unit_filter = self._get_all_units()

        results = {}

        for plant, unit in unit_filter:
            try:
                # Filter data for this unit
                unit_data = self._filter_data_for_unit(data, plant, unit)
                if unit_data.empty:
                    logger.warning(f"No data found for {plant}.{unit}")
                    continue

                # Apply compensation
                result = self.compensate_data(unit_data, plant, unit)
                results[f"{plant}.{unit}"] = result

                logger.info(f"Compensated {plant}.{unit}: factor={result.compensation_factor:.3f}, "
                           f"confidence={result.confidence:.3f}, method={result.method_used}")

            except Exception as e:
                logger.error(f"Error compensating {plant}.{unit}: {e}")
                continue

        return results

    def _filter_data_for_unit(self, data: pd.DataFrame, plant: str, unit: str) -> pd.DataFrame:
        """Filter data for a specific unit based on tag naming conventions"""
        # Implementation depends on tag naming convention
        # For now, use simple string matching
        unit_patterns = [
            f"{plant}_{unit}_",  # PCFS pattern
            f"{plant}.{unit}.",  # ABF/PCMSB pattern
        ]

        mask = data['tag'].str.contains('|'.join(unit_patterns), case=False, na=False)
        return data[mask].copy()

    def get_compensation_summary(self, results: Dict[str, SpeedCompensationResult]) -> pd.DataFrame:
        """Generate summary of compensation results"""
        summary_data = []

        for unit_key, result in results.items():
            summary_data.append({
                'unit': unit_key,
                'method': result.method_used,
                'compensation_factor': result.compensation_factor,
                'confidence': result.confidence,
                'warnings_count': len(result.warnings),
                'speed_data_points': len(result.speed_data),
                'compensated_tags': len(result.metadata.get('target_tags', [])),
                'timestamp': result.metadata.get('timestamp', '')
            })

        return pd.DataFrame(summary_data)

    def export_speed_config(self, output_path: Path) -> None:
        """Export current speed configuration to file"""
        with open(output_path, 'w') as f:
            json.dump(self.config, f, indent=2)
        logger.info(f"Speed configuration exported to {output_path}")

    def validate_configuration(self) -> Dict[str, Any]:
        """Validate speed configuration and return status"""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }

        try:
            # Check required sections
            required_sections = ['plants', 'compensation_algorithms', 'global_settings']
            for section in required_sections:
                if section not in self.config:
                    validation_result['errors'].append(f"Missing required section: {section}")
                    validation_result['valid'] = False

            # Validate plant configurations
            total_units = 0
            total_tags = 0

            for plant_name, plant_config in self.config['plants'].items():
                if 'units' not in plant_config:
                    validation_result['errors'].append(f"Plant {plant_name} missing units configuration")
                    continue

                for unit_name, unit_config in plant_config['units'].items():
                    total_units += 1

                    # Count speed tags
                    speed_tags = self.get_speed_tags_for_unit(plant_name, unit_name)
                    total_tags += len(speed_tags)

                    # Check required fields
                    required_fields = ['baseline_speed', 'operating_range', 'compensation_method']
                    for field in required_fields:
                        if field not in unit_config:
                            validation_result['warnings'].append(
                                f"Unit {plant_name}.{unit_name} missing {field}")

            validation_result['summary'] = {
                'total_plants': len(self.config['plants']),
                'total_units': total_units,
                'total_speed_tags': total_tags,
                'config_version': self.config.get('version', 'unknown')
            }

        except Exception as e:
            validation_result['errors'].append(f"Validation error: {str(e)}")
            validation_result['valid'] = False

        return validation_result


def create_speed_compensator(config_path: Optional[Path] = None) -> SpeedAwareCompensator:
    """Factory function to create speed compensator instance"""
    return SpeedAwareCompensator(config_path)


# Example usage and testing
if __name__ == "__main__":
    # Initialize compensator
    compensator = create_speed_compensator()

    # Validate configuration
    validation = compensator.validate_configuration()
    print("Configuration validation:", validation)

    # Example: Get speed tags for a unit
    speed_tags = compensator.get_speed_tags_for_unit("PCFS", "K-12-01")
    print(f"Speed tags for PCFS.K-12-01: {speed_tags}")