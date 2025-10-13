#!/usr/bin/env python3
"""
Pure Speed Tag Scanner for TurboPredict

Identifies ONLY speed-related tags (RPM, motor speed, drive frequency, turbine speed)
that can be used for speed compensation in abnormal behavior detection.

Focus: Precise speed tags for option [2] analysis compensation.
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
import re
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PureSpeedScanner:
    """Scanner focused exclusively on speed-related industrial tags"""

    def __init__(self):
        # Pure speed indicators only
        self.speed_patterns = [
            r'.*speed.*',           # Direct speed indicators
            r'.*rpm.*',             # Revolutions per minute
            r'.*freq.*',            # Frequency (drive frequency)
            r'.*hz.*',              # Hertz (motor frequency)
            r'.*motor.*speed.*',    # Motor speed
            r'.*drive.*speed.*',    # Drive speed
            r'.*turbine.*speed.*',  # Turbine speed
            r'.*pump.*speed.*',     # Pump speed
            r'.*compressor.*speed.*', # Compressor speed
            r'.*fan.*speed.*',      # Fan speed
            r'.*rotation.*',        # Rotation rate
            r'.*rps.*',             # Revolutions per second
            r'.*velocity.*',        # Velocity
            r'.*rate.*rpm.*',       # Rate in RPM
            r'.*motor.*freq.*',     # Motor frequency
            r'.*drive.*freq.*',     # Drive frequency
            r'.*vfd.*',             # Variable Frequency Drive
            r'.*inverter.*freq.*',  # Inverter frequency
        ]

        self.unit_speed_tags = defaultdict(list)

    def analyze_all_units(self):
        """Analyze all units for pure speed tags"""

        dataset_dir = Path("data/processed/dataset")
        if not dataset_dir.exists():
            logger.error(f"Dataset directory not found: {dataset_dir}")
            return {}

        results = {
            "scan_type": "pure_speed_tags_only",
            "purpose": "speed_compensation_for_abnormal_behavior_detection",
            "target_analysis": "option_2_precise_analysis",
            "units": {}
        }

        for plant_dir in dataset_dir.iterdir():
            if plant_dir.is_dir() and plant_dir.name.startswith('plant='):
                for unit_dir in plant_dir.iterdir():
                    if unit_dir.is_dir() and unit_dir.name.startswith('unit='):
                        unit_name = unit_dir.name.replace('unit=', '')

                        logger.info(f"Scanning {unit_name} for speed tags...")
                        speed_tags = self.scan_unit_for_speed_tags(unit_dir, unit_name)

                        if speed_tags:
                            results["units"][unit_name] = speed_tags

        return results

    def scan_unit_for_speed_tags(self, unit_dir, unit_name):
        """Scan specific unit for pure speed tags"""

        speed_tags = {
            "unit": unit_name,
            "speed_tags": [],
            "tag_analysis": {},
            "compensation_ready": False
        }

        tag_count = 0
        for tag_dir in unit_dir.iterdir():
            if tag_dir.is_dir() and tag_dir.name.startswith('tag='):
                tag_name = tag_dir.name.replace('tag=', '')
                tag_count += 1

                # Check if this is a pure speed tag
                if self.is_pure_speed_tag(tag_name):
                    logger.info(f"Found speed tag in {unit_name}: {tag_name}")

                    # Analyze the tag data
                    tag_analysis = self.analyze_speed_tag_data(tag_dir, tag_name)

                    speed_tags["speed_tags"].append(tag_name)
                    speed_tags["tag_analysis"][tag_name] = tag_analysis

        # Mark as compensation ready if we have speed tags
        if speed_tags["speed_tags"]:
            speed_tags["compensation_ready"] = True
            speed_tags["primary_speed_tag"] = speed_tags["speed_tags"][0]  # First found as primary

        logger.info(f"{unit_name}: {len(speed_tags['speed_tags'])} speed tags found out of {tag_count} total tags")

        return speed_tags if speed_tags["speed_tags"] else None

    def is_pure_speed_tag(self, tag_name):
        """Check if tag is a pure speed indicator"""
        tag_lower = tag_name.lower()

        # Check against speed patterns
        for pattern in self.speed_patterns:
            if re.match(pattern, tag_lower):
                return True

        # Additional specific checks
        speed_keywords = ['speed', 'rpm', 'freq', 'hz', 'rps', 'vfd']
        for keyword in speed_keywords:
            if keyword in tag_lower:
                return True

        return False

    def analyze_speed_tag_data(self, tag_dir, tag_name):
        """Analyze speed tag data characteristics"""

        analysis = {
            "tag_name": tag_name,
            "data_available": False,
            "sample_count": 0,
            "value_range": {},
            "speed_characteristics": {},
            "compensation_suitability": "unknown"
        }

        try:
            # Find parquet files in this tag directory
            parquet_files = list(tag_dir.glob("**/*.parquet"))

            if parquet_files:
                # Sample from first file
                sample_file = parquet_files[0]
                df = pd.read_parquet(sample_file)

                if not df.empty and 'value' in df.columns:
                    values = df['value'].dropna()

                    if len(values) > 0:
                        analysis.update({
                            "data_available": True,
                            "sample_count": len(values),
                            "value_range": {
                                "min": float(values.min()),
                                "max": float(values.max()),
                                "mean": float(values.mean()),
                                "std": float(values.std()) if len(values) > 1 else 0
                            }
                        })

                        # Determine speed characteristics
                        analysis["speed_characteristics"] = self.classify_speed_data(values, tag_name)
                        analysis["compensation_suitability"] = self.assess_compensation_suitability(values, tag_name)

        except Exception as e:
            logger.warning(f"Could not analyze {tag_name}: {e}")
            analysis["error"] = str(e)

        return analysis

    def classify_speed_data(self, values, tag_name):
        """Classify the type of speed data"""

        min_val, max_val, mean_val = values.min(), values.max(), values.mean()
        tag_lower = tag_name.lower()

        characteristics = {
            "speed_type": "unknown",
            "units_likely": "unknown",
            "operating_range": f"{min_val:.1f} - {max_val:.1f}",
            "typical_value": f"{mean_val:.1f}"
        }

        # Classify based on value ranges and tag name
        if 'rpm' in tag_lower or ('motor' in tag_lower and max_val > 100):
            characteristics["speed_type"] = "motor_rpm"
            characteristics["units_likely"] = "RPM"

            if max_val > 1000:
                characteristics["motor_type"] = "high_speed_motor"
            else:
                characteristics["motor_type"] = "low_speed_motor"

        elif 'freq' in tag_lower or 'hz' in tag_lower:
            characteristics["speed_type"] = "drive_frequency"
            characteristics["units_likely"] = "Hz"

            if max_val <= 100:
                characteristics["drive_type"] = "variable_frequency_drive"
            else:
                characteristics["drive_type"] = "high_frequency_drive"

        elif 'pump' in tag_lower:
            characteristics["speed_type"] = "pump_speed"
            characteristics["units_likely"] = "RPM" if max_val > 100 else "Hz"

        elif 'turbine' in tag_lower:
            characteristics["speed_type"] = "turbine_speed"
            characteristics["units_likely"] = "RPM"

        elif 'compressor' in tag_lower:
            characteristics["speed_type"] = "compressor_speed"
            characteristics["units_likely"] = "RPM" if max_val > 100 else "Hz"

        return characteristics

    def assess_compensation_suitability(self, values, tag_name):
        """Assess how suitable this tag is for speed compensation"""

        if len(values) < 10:
            return "insufficient_data"

        # Calculate variability
        cv = values.std() / values.mean() if values.mean() != 0 else float('inf')

        # Check for reasonable speed values
        min_val, max_val = values.min(), values.max()

        if cv < 0.05:  # Very low variability
            return "low_variability"
        elif cv > 2.0:  # Very high variability
            return "high_variability"
        elif min_val < 0:  # Negative speeds don't make sense
            return "invalid_range"
        elif max_val == 0:  # No movement
            return "zero_speed"
        else:
            return "excellent"  # Good for compensation

    def generate_speed_compensation_config(self, scan_results):
        """Generate configuration for speed compensation in option [2] analysis"""

        config = {
            "config_type": "speed_compensation_for_abnormal_behavior",
            "generated_at": pd.Timestamp.now().isoformat(),
            "purpose": "Compensate for speed variations in abnormal behavior detection",
            "target_analysis": "option_2_precise_analysis",
            "units_with_speed_compensation": {},
            "speed_compensation_strategy": {},
            "analysis_recommendations": {}
        }

        for unit_name, unit_data in scan_results.get("units", {}).items():
            if unit_data["compensation_ready"]:

                # Find best speed tag for compensation
                best_tag = self.select_best_compensation_tag(unit_data)

                config["units_with_speed_compensation"][unit_name] = {
                    "primary_speed_tag": best_tag["tag_name"],
                    "compensation_type": best_tag["compensation_type"],
                    "expected_range": best_tag["expected_range"],
                    "compensation_factor": best_tag["compensation_factor"],
                    "backup_tags": [tag for tag in unit_data["speed_tags"] if tag != best_tag["tag_name"]]
                }

                # Strategy for this unit
                config["speed_compensation_strategy"][unit_name] = {
                    "method": "speed_normalized_analysis",
                    "baseline_speed": best_tag["baseline_speed"],
                    "compensation_formula": f"normalized_value = actual_value * (baseline_speed / current_speed)",
                    "abnormal_threshold_adjustment": "dynamic_based_on_speed_deviation"
                }

                # Analysis recommendations
                config["analysis_recommendations"][unit_name] = {
                    "enable_speed_compensation": True,
                    "monitor_speed_stability": True,
                    "speed_change_threshold": 0.1,  # 10% change triggers compensation
                    "compensation_confidence": best_tag["confidence"]
                }

        return config

    def select_best_compensation_tag(self, unit_data):
        """Select the best speed tag for compensation purposes"""

        best_tag = {
            "tag_name": unit_data["primary_speed_tag"],
            "compensation_type": "basic",
            "expected_range": "unknown",
            "compensation_factor": 1.0,
            "baseline_speed": 0,
            "confidence": "medium"
        }

        # Analyze all speed tags to find the best one
        best_suitability = "unknown"

        for tag_name, analysis in unit_data["tag_analysis"].items():
            suitability = analysis.get("compensation_suitability", "unknown")

            if suitability == "excellent" and best_suitability != "excellent":
                best_tag.update({
                    "tag_name": tag_name,
                    "compensation_type": "advanced",
                    "confidence": "high"
                })
                best_suitability = suitability

                # Set compensation parameters
                if analysis.get("value_range"):
                    range_data = analysis["value_range"]
                    best_tag.update({
                        "expected_range": f"{range_data['min']:.1f} - {range_data['max']:.1f}",
                        "baseline_speed": range_data["mean"],
                        "compensation_factor": 1.0 / range_data["mean"] if range_data["mean"] != 0 else 1.0
                    })

        return best_tag


def main():
    scanner = PureSpeedScanner()

    print("[SPEED] Scanning for pure speed tags across all units...")
    print("[PURPOSE] Speed compensation for abnormal behavior detection (Option 2)")

    # Scan all units
    scan_results = scanner.analyze_all_units()

    # Generate compensation configuration
    compensation_config = scanner.generate_speed_compensation_config(scan_results)

    # Save results
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)

    # Save detailed scan results
    with open(config_dir / "speed_tags_scan.json", 'w') as f:
        json.dump(scan_results, f, indent=2)

    # Save compensation configuration
    with open(config_dir / "speed_compensation.json", 'w') as f:
        json.dump(compensation_config, f, indent=2)

    # Print summary
    print(f"\n[RESULTS] Speed Tag Analysis Complete")
    print(f"{'='*50}")

    units_with_speed = len(scan_results.get("units", {}))
    compensatable_units = len(compensation_config.get("units_with_speed_compensation", {}))

    print(f"Units scanned: 4")
    print(f"Units with speed tags: {units_with_speed}")
    print(f"Units ready for compensation: {compensatable_units}")

    if scan_results.get("units"):
        print(f"\n[SPEED TAGS] Found by Unit:")
        for unit, data in scan_results["units"].items():
            print(f"  {unit}: {len(data['speed_tags'])} speed tags")
            for tag in data["speed_tags"]:
                suitability = data["tag_analysis"][tag].get("compensation_suitability", "unknown")
                print(f"    - {tag} ({suitability})")

    print(f"\n[CONFIG] Files saved:")
    print(f"  - config/speed_tags_scan.json (detailed analysis)")
    print(f"  - config/speed_compensation.json (compensation settings)")

    if compensatable_units > 0:
        print(f"\n[OPTION 2] Speed compensation ENABLED for {compensatable_units} units")
        print(f"[ANALYSIS] Abnormal behavior detection will use speed-normalized analysis")
    else:
        print(f"\n[WARNING] No suitable speed tags found for compensation")
        print(f"[ANALYSIS] Standard analysis will be used (no speed compensation)")


if __name__ == "__main__":
    main()