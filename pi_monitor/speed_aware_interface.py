"""
Speed-aware interface integration for TURBOPREDICT X PROTEAN
Provides speed compensation interface and reporting capabilities
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
import json

from .speed_compensator import SpeedAwareCompensator, SpeedCompensationResult
from .speed_aware_anomaly import SpeedAwareAnomalyDetector, SpeedAwareAnomalyResult
from .parquet_database import ParquetDatabase
from .config import Config

logger = logging.getLogger(__name__)


class SpeedAwareInterface:
    """Interface layer for speed-aware functionality in TurboPredict"""

    def __init__(self, config: Optional[Config] = None):
        """Initialize speed-aware interface

        Args:
            config: Configuration object (creates new if None)
        """
        self.config = config or Config()
        self.compensator = SpeedAwareCompensator()
        self.anomaly_detector = SpeedAwareAnomalyDetector(self.compensator)
        self.db = ParquetDatabase()

        logger.info("Speed-aware interface initialized")

    def get_speed_aware_menu(self) -> str:
        """Get speed-aware menu options for TurboPredict interface"""
        return """
+================================================================+
|                    SPEED-AWARE ANALYSIS SUITE                 |
+================================================================+
| S1. SPEED COMPENSATION   - Apply speed normalization to data  |
| S2. SPEED-AWARE ANOMALY  - Detect anomalies with speed comp   |
| S3. SPEED TAG MONITOR    - Monitor all speed indicators       |
| S4. SPEED CORRELATION    - Analyze speed-anomaly correlation  |
| S5. SPEED CONFIG STATUS  - View speed configuration status    |
| S6. BATCH SPEED ANALYSIS - Analyze all units with speed comp  |
| S7. SPEED REPORT EXPORT  - Export comprehensive speed report  |
+----------------------------------------------------------------+
| S8. SPEED CONFIG MGMT    - Manage speed configurations        |
| S9. SPEED CALIBRATION    - Calibrate speed baselines          |
+================================================================+
"""

    def handle_speed_aware_command(self, command: str, unit: Optional[str] = None) -> str:
        """Handle speed-aware commands from TurboPredict interface

        Args:
            command: Command identifier (S1, S2, etc.)
            unit: Optional unit specification

        Returns:
            Result message or status
        """
        try:
            if command.upper() == "S1":
                return self._handle_speed_compensation(unit)
            elif command.upper() == "S2":
                return self._handle_speed_aware_anomaly(unit)
            elif command.upper() == "S3":
                return self._handle_speed_tag_monitor()
            elif command.upper() == "S4":
                return self._handle_speed_correlation(unit)
            elif command.upper() == "S5":
                return self._handle_speed_config_status()
            elif command.upper() == "S6":
                return self._handle_batch_speed_analysis()
            elif command.upper() == "S7":
                return self._handle_speed_report_export()
            elif command.upper() == "S8":
                return self._handle_speed_config_mgmt()
            elif command.upper() == "S9":
                return self._handle_speed_calibration()
            else:
                return f"Unknown speed-aware command: {command}"

        except Exception as e:
            logger.error(f"Error handling speed-aware command {command}: {e}")
            return f"Error executing speed-aware command: {str(e)}"

    def _handle_speed_compensation(self, unit: Optional[str] = None) -> str:
        """Handle speed compensation command"""
        if unit is None:
            return "Available units for speed compensation:\n" + self._get_available_units_summary()

        try:
            # Parse unit (format: "PLANT.UNIT")
            if "." in unit:
                plant, unit_name = unit.split(".", 1)
            else:
                return f"Invalid unit format. Use PLANT.UNIT (e.g., PCFS.K-12-01)"

            # Get recent data for the unit
            unit_data = self._get_unit_data(plant, unit_name)
            if unit_data.empty:
                return f"No data found for {plant}.{unit_name}"

            # Apply speed compensation
            result = self.compensator.compensate_data(unit_data, plant, unit_name)

            # Generate summary
            summary = f"""
SPEED COMPENSATION RESULTS - {plant}.{unit_name}
{'='*60}
Compensation Factor: {result.compensation_factor:.3f}
Method Used: {result.method_used}
Confidence: {result.confidence:.2f}
Speed Data Points: {len(result.speed_data)}
Warnings: {len(result.warnings)}

Original Data Points: {len(result.original_data)}
Compensated Data Points: {len(result.compensated_data)}
"""
            if result.warnings:
                summary += f"\nWarnings:\n" + "\n".join(f"- {w}" for w in result.warnings)

            return summary

        except Exception as e:
            return f"Error in speed compensation: {str(e)}"

    def _handle_speed_aware_anomaly(self, unit: Optional[str] = None) -> str:
        """Handle speed-aware anomaly detection command"""
        if unit is None:
            return "Available units for speed-aware anomaly detection:\n" + self._get_available_units_summary()

        try:
            # Parse unit
            if "." in unit:
                plant, unit_name = unit.split(".", 1)
            else:
                return f"Invalid unit format. Use PLANT.UNIT"

            # Get recent data
            unit_data = self._get_unit_data(plant, unit_name)
            if unit_data.empty:
                return f"No data found for {plant}.{unit_name}"

            # Perform speed-aware anomaly detection
            result = self.anomaly_detector.detect_speed_aware_anomalies(unit_data, plant, unit_name)

            # Generate summary
            summary = f"""
SPEED-AWARE ANOMALY DETECTION - {plant}.{unit_name}
{'='*60}
Original Anomalies: {len(result.original_anomalies)}
Compensated Anomalies: {len(result.compensated_anomalies)}
Speed-Correlated Anomalies: {len(result.speed_correlated_anomalies)}
Anomaly Reduction: {result.anomaly_reduction_factor:.1%}
Detection Confidence: {result.confidence_score:.2f}
Method Used: {result.method_used}

Compensation Factor: {result.speed_compensation_result.compensation_factor:.3f}
Compensation Confidence: {result.speed_compensation_result.confidence:.2f}
"""

            if result.anomaly_reduction_factor > 0.1:
                summary += f"\n✓ Speed compensation reduced anomalies by {result.anomaly_reduction_factor:.1%}"

            if len(result.speed_correlated_anomalies) > 0:
                summary += f"\n⚠ {len(result.speed_correlated_anomalies)} anomalies correlate with speed changes"

            return summary

        except Exception as e:
            return f"Error in speed-aware anomaly detection: {str(e)}"

    def _handle_speed_tag_monitor(self) -> str:
        """Handle speed tag monitoring command"""
        try:
            # Get all configured units
            all_units = self.compensator._get_all_units()

            summary = "SPEED TAG MONITORING STATUS\n" + "="*60 + "\n"

            for plant, unit in all_units:
                speed_tags = self.compensator.get_speed_tags_for_unit(plant, unit)
                enabled = self.compensator.is_speed_aware_enabled(plant, unit)
                method = self.compensator.get_compensation_method(plant, unit)

                summary += f"\n{plant}.{unit}:\n"
                summary += f"  Speed Tags: {', '.join(speed_tags) if speed_tags else 'None'}\n"
                summary += f"  Enabled: {'Yes' if enabled else 'No'}\n"
                summary += f"  Method: {method}\n"

                # Try to get recent speed data
                try:
                    unit_data = self._get_unit_data(plant, unit, hours=1)  # Last hour
                    speed_data = self.compensator.extract_speed_data(unit_data, plant, unit)
                    if speed_data is not None and not speed_data.empty:
                        latest_speed = speed_data['value'].iloc[-1] if len(speed_data) > 0 else 0
                        summary += f"  Latest Speed: {latest_speed:.1f}\n"
                    else:
                        summary += f"  Latest Speed: No data\n"
                except:
                    summary += f"  Latest Speed: Error\n"

            return summary

        except Exception as e:
            return f"Error in speed tag monitoring: {str(e)}"

    def _handle_speed_correlation(self, unit: Optional[str] = None) -> str:
        """Handle speed-correlation analysis command"""
        if unit is None:
            return "Specify unit for speed correlation analysis (format: PLANT.UNIT)"

        try:
            # Parse unit
            if "." in unit:
                plant, unit_name = unit.split(".", 1)
            else:
                return f"Invalid unit format. Use PLANT.UNIT"

            # Get extended data for correlation analysis
            unit_data = self._get_unit_data(plant, unit_name, hours=24)  # Last 24 hours
            if unit_data.empty:
                return f"No data found for {plant}.{unit_name}"

            # Perform analysis
            result = self.anomaly_detector.detect_speed_aware_anomalies(
                unit_data, plant, unit_name, speed_correlation_analysis=True
            )

            # Generate correlation summary
            summary = f"""
SPEED-ANOMALY CORRELATION ANALYSIS - {plant}.{unit_name}
{'='*60}
Total Anomalies: {len(result.original_anomalies)}
Speed-Correlated: {len(result.speed_correlated_anomalies)}
Correlation Rate: {len(result.speed_correlated_anomalies) / max(1, len(result.original_anomalies)):.1%}
"""

            if not result.speed_correlated_anomalies.empty:
                avg_correlation = result.speed_correlated_anomalies['speed_correlation'].mean()
                summary += f"Average Speed Correlation: {avg_correlation:.3f}\n"
                summary += f"Speed Variability Range: {result.speed_correlated_anomalies['speed_correlation'].min():.3f} - {result.speed_correlated_anomalies['speed_correlation'].max():.3f}\n"

                # Show top correlated anomalies
                top_correlated = result.speed_correlated_anomalies.nlargest(5, 'speed_correlation')
                summary += f"\nTop Speed-Correlated Anomalies:\n"
                for _, row in top_correlated.iterrows():
                    summary += f"  {row['tag']} @ {row['time']}: correlation={row['speed_correlation']:.3f}\n"

            return summary

        except Exception as e:
            return f"Error in speed correlation analysis: {str(e)}"

    def _handle_speed_config_status(self) -> str:
        """Handle speed configuration status command"""
        try:
            validation = self.compensator.validate_configuration()

            summary = f"""
SPEED CONFIGURATION STATUS
{'='*60}
Configuration Valid: {'Yes' if validation['valid'] else 'No'}
Total Plants: {validation['summary'].get('total_plants', 0)}
Total Units: {validation['summary'].get('total_units', 0)}
Total Speed Tags: {validation['summary'].get('total_speed_tags', 0)}
Config Version: {validation['summary'].get('config_version', 'unknown')}
"""

            if validation['errors']:
                summary += f"\nErrors ({len(validation['errors'])}):\n"
                for error in validation['errors']:
                    summary += f"  ✗ {error}\n"

            if validation['warnings']:
                summary += f"\nWarnings ({len(validation['warnings'])}):\n"
                for warning in validation['warnings'][:5]:  # Show first 5
                    summary += f"  ⚠ {warning}\n"
                if len(validation['warnings']) > 5:
                    summary += f"  ... and {len(validation['warnings']) - 5} more warnings\n"

            return summary

        except Exception as e:
            return f"Error checking speed configuration: {str(e)}"

    def _handle_batch_speed_analysis(self) -> str:
        """Handle batch speed analysis command"""
        try:
            # Get recent data for all units
            all_data = self._get_recent_data_all_units()
            if all_data.empty:
                return "No recent data available for batch analysis"

            # Perform batch analysis
            results = self.anomaly_detector.batch_analyze_units(all_data)

            if not results:
                return "No units could be analyzed"

            # Generate summary report
            report_df = self.anomaly_detector.generate_anomaly_report(results)

            summary = f"""
BATCH SPEED-AWARE ANALYSIS RESULTS
{'='*60}
Units Analyzed: {len(results)}
Total Original Anomalies: {report_df['original_anomalies'].sum()}
Total Compensated Anomalies: {report_df['compensated_anomalies'].sum()}
Average Reduction: {report_df['anomaly_reduction_factor'].mean():.1%}
Average Confidence: {report_df['confidence_score'].mean():.2f}

Top Performing Units (by anomaly reduction):
"""
            # Show top 5 units by anomaly reduction
            top_units = report_df.nlargest(5, 'anomaly_reduction_factor')
            for _, row in top_units.iterrows():
                summary += f"  {row['unit']}: {row['anomaly_reduction_factor']:.1%} reduction (confidence: {row['confidence_score']:.2f})\n"

            return summary

        except Exception as e:
            return f"Error in batch speed analysis: {str(e)}"

    def _handle_speed_report_export(self) -> str:
        """Handle speed report export command"""
        try:
            # Get recent data and perform comprehensive analysis
            all_data = self._get_recent_data_all_units()
            if all_data.empty:
                return "No data available for report generation"

            results = self.anomaly_detector.batch_analyze_units(all_data)
            if not results:
                return "No analysis results available for export"

            # Generate comprehensive report
            report_df = self.anomaly_detector.generate_anomaly_report(results)

            # Export to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = Path(f"reports/speed_aware_analysis_{timestamp}.csv")
            report_path.parent.mkdir(exist_ok=True)

            report_df.to_csv(report_path, index=False)

            # Also export JSON with detailed results
            json_path = Path(f"reports/speed_aware_detailed_{timestamp}.json")
            detailed_results = {}
            for unit_key, result in results.items():
                detailed_results[unit_key] = {
                    'metadata': result.metadata,
                    'compensation_factor': result.speed_compensation_result.compensation_factor,
                    'confidence': result.confidence_score,
                    'anomaly_reduction': result.anomaly_reduction_factor,
                    'warnings': result.speed_compensation_result.warnings
                }

            with open(json_path, 'w') as f:
                json.dump(detailed_results, f, indent=2, default=str)

            return f"""
SPEED-AWARE REPORT EXPORTED
{'='*60}
CSV Report: {report_path}
JSON Details: {json_path}
Units Analyzed: {len(results)}
Total Data Points: {sum(r.metadata.get('data_points', 0) for r in results.values())}
Export Timestamp: {timestamp}
"""

        except Exception as e:
            return f"Error exporting speed report: {str(e)}"

    def _handle_speed_config_mgmt(self) -> str:
        """Handle speed configuration management command"""
        try:
            config_path = self.compensator.config_path
            backup_path = config_path.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

            # Create backup
            self.compensator.export_speed_config(backup_path)

            summary = f"""
SPEED CONFIGURATION MANAGEMENT
{'='*60}
Current Config: {config_path}
Backup Created: {backup_path}
Configuration Status: {'Valid' if self.compensator.validate_configuration()['valid'] else 'Invalid'}

Available Operations:
  - Backup created automatically
  - Manual validation completed
  - Configuration export available
"""
            return summary

        except Exception as e:
            return f"Error in speed configuration management: {str(e)}"

    def _handle_speed_calibration(self) -> str:
        """Handle speed calibration command"""
        return """
SPEED CALIBRATION SYSTEM
{'='*60}
Speed calibration requires historical data analysis and is typically performed during system setup.

Current baseline speeds are configured in the speed-aware configuration file.
For recalibration, please:
1. Ensure sufficient historical data (minimum 1 week)
2. Verify equipment operating in normal conditions
3. Update baseline speeds in configuration
4. Validate new configuration

Contact system administrator for baseline recalibration procedures.
"""

    def _get_available_units_summary(self) -> str:
        """Get summary of available units for speed analysis"""
        try:
            all_units = self.compensator._get_all_units()
            summary = ""

            current_plant = None
            for plant, unit in all_units:
                if plant != current_plant:
                    summary += f"\n{plant} Plant:\n"
                    current_plant = plant

                enabled = "✓" if self.compensator.is_speed_aware_enabled(plant, unit) else "✗"
                speed_tags = self.compensator.get_speed_tags_for_unit(plant, unit)
                tag_count = len(speed_tags)

                summary += f"  {enabled} {plant}.{unit} ({tag_count} speed tag{'s' if tag_count != 1 else ''})\n"

            return summary

        except Exception as e:
            return f"Error getting units summary: {str(e)}"

    def _get_unit_data(self, plant: str, unit: str, hours: int = 24) -> pd.DataFrame:
        """Get recent data for a specific unit"""
        try:
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)

            # Get data from database
            unit_data = self.db.get_unit_data(f"{plant}-{unit}", start_time, end_time)

            # If no data with hyphen format, try period format
            if unit_data.empty:
                unit_data = self.db.get_unit_data(f"{plant}.{unit}", start_time, end_time)

            return unit_data

        except Exception as e:
            logger.warning(f"Error getting data for {plant}.{unit}: {e}")
            return pd.DataFrame()

    def _get_recent_data_all_units(self, hours: int = 24) -> pd.DataFrame:
        """Get recent data for all configured units"""
        try:
            all_data = []
            all_units = self.compensator._get_all_units()

            for plant, unit in all_units:
                unit_data = self._get_unit_data(plant, unit, hours)
                if not unit_data.empty:
                    all_data.append(unit_data)

            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error getting recent data for all units: {e}")
            return pd.DataFrame()

    def get_speed_status_summary(self) -> Dict[str, Any]:
        """Get overall speed system status for dashboard display"""
        try:
            validation = self.compensator.validate_configuration()
            all_units = self.compensator._get_all_units()

            enabled_units = [
                (plant, unit) for plant, unit in all_units
                if self.compensator.is_speed_aware_enabled(plant, unit)
            ]

            return {
                'total_units': len(all_units),
                'enabled_units': len(enabled_units),
                'total_speed_tags': validation['summary'].get('total_speed_tags', 0),
                'config_valid': validation['valid'],
                'system_ready': validation['valid'] and len(enabled_units) > 0,
                'last_check': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting speed status summary: {e}")
            return {
                'total_units': 0,
                'enabled_units': 0,
                'total_speed_tags': 0,
                'config_valid': False,
                'system_ready': False,
                'error': str(e)
            }


def create_speed_aware_interface(config: Optional[Config] = None) -> SpeedAwareInterface:
    """Factory function to create speed-aware interface"""
    return SpeedAwareInterface(config)


# Example usage
if __name__ == "__main__":
    interface = create_speed_aware_interface()
    print("Speed-aware interface initialized")
    print(interface.get_speed_aware_menu())