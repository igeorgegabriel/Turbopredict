#!/usr/bin/env python3
"""
Anomaly-Triggered Plotting System for TURBOPREDICT X PROTEAN
Generates 3-month historical plots only for verified anomalous tags
Integrates with: 2.5-sigma + Autoencoder -> MTD + Isolation Forest verification
"""

from __future__ import annotations
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logger = logging.getLogger(__name__)


class AnomalyTriggeredPlotter:
    """Generates plots only when anomalies are detected and verified."""

    def __init__(self, reports_dir: Path = None):
        """Initialize the anomaly-triggered plotter.

        Args:
            reports_dir: Base directory for reports (default: reports/)
        """
        # Use shared reports root; specific session folder is created per run
        from .plot_controls import build_scan_root_dir
        base = reports_dir or Path("reports")
        # Defer actual folder creation to processing time
        self.reports_dir = Path(base)
        self._build_scan_root_dir = build_scan_root_dir

        # Plot settings for 90-day historical context (performance optimized)
        # Using 90 days instead of full dataset reduces processing from hours to minutes
        self.historical_days = 90
        self.historical_months = 3  # Kept for backward compatibility
        self.plot_width = 16
        self.plot_height = 10

        # Detection pipeline status
        self.detection_pipeline = {
            'primary_detectors': ['2.5_sigma', 'autoencoder'],
            'verification_detectors': ['mtd', 'isolation_forest'],
            'require_verification': True
        }

    def process_anomaly_detection_results(self, detection_results: Dict[str, Any]) -> Path:
        """Process anomaly detection results and generate plots for verified anomalies.

        Args:
            detection_results: Results from the anomaly detection pipeline

        Returns:
            Path to the generated anomaly report directory
        """
        import time

        # Create master scan folder using day/time naming (shared with other plotters)
        session_dir = self._build_scan_root_dir(self.reports_dir)
        session_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing anomaly detection results for plotting")

        # Extract verified anomalies from detection results
        verified_anomalies = self._extract_verified_anomalies(detection_results)

        if not verified_anomalies:
            logger.info("No verified anomalies found - no plots generated")
            self._create_no_anomalies_report(session_dir, detection_results)
            return session_dir

        logger.info(f"Found {len(verified_anomalies)} verified anomalous tags")

        # Generate plots for each verified anomalous tag with timing
        plots_generated = 0
        total_plot_time = 0.0
        plot_timing_by_unit = {}

        for tag_info in verified_anomalies:
            try:
                plot_start = time.time()
                success = self._generate_anomaly_plot(tag_info, session_dir)
                plot_time = time.time() - plot_start

                if success:
                    plots_generated += 1
                    total_plot_time += plot_time

                    # Track timing by unit
                    unit = tag_info['unit']
                    if unit not in plot_timing_by_unit:
                        plot_timing_by_unit[unit] = {'count': 0, 'time': 0.0}
                    plot_timing_by_unit[unit]['count'] += 1
                    plot_timing_by_unit[unit]['time'] += plot_time

                    print(f"   [TIMING] Plot {tag_info['tag']}: {plot_time:.2f}s")
            except Exception as e:
                logger.error(f"Failed to generate plot for {tag_info['tag']}: {e}")

        # Print plotting summary
        if plots_generated > 0:
            print(f"\n   [TIMING] Plotting Summary:")
            print(f"   Total plots generated: {plots_generated}")
            print(f"   Total plotting time: {total_plot_time:.2f}s ({total_plot_time/60:.2f}min)")
            print(f"   Average time per plot: {total_plot_time/plots_generated:.2f}s")

            # Per-unit breakdown
            print(f"\n   By unit:")
            for unit, timing in sorted(plot_timing_by_unit.items()):
                avg_time = timing['time'] / timing['count'] if timing['count'] > 0 else 0
                print(f"   - {unit}: {timing['count']} plots, {timing['time']:.2f}s ({avg_time:.2f}s/plot)")

        # Store timing in detection results for overall summary
        for unit, timing in plot_timing_by_unit.items():
            if unit in detection_results and isinstance(detection_results[unit], dict):
                if 'timing' not in detection_results[unit]:
                    detection_results[unit]['timing'] = {}
                detection_results[unit]['timing']['plotting_seconds'] = round(timing['time'], 2)
                detection_results[unit]['timing']['plots_generated'] = timing['count']

        # Create session summary
        self._create_session_summary(session_dir, detection_results, verified_anomalies, plots_generated)

        # Generate consolidated PDF report with all plots
        if plots_generated > 0:
            try:
                pdf_start = time.time()
                pdf_path = self._generate_consolidated_pdf(session_dir)
                pdf_time = time.time() - pdf_start

                if pdf_path:
                    logger.info(f"Consolidated PDF report generated: {pdf_path}")
                    print(f"\n[PDF] Consolidated anomaly report: {pdf_path}")
                    print(f"[TIMING] PDF generation: {pdf_time:.2f}s")

                    # Send PDF via email
                    try:
                        from .email_sender import send_pdf_report
                        print(f"\n[EMAIL] Sending PDF report to george.gabrielujai@petronas.com.my...")
                        email_sent = send_pdf_report(pdf_path)
                        if email_sent:
                            print(f"[EMAIL] ✓ Report sent successfully!")
                        else:
                            print(f"[EMAIL] ✗ Failed to send email - check logs")
                    except Exception as email_err:
                        logger.warning(f"Failed to send email: {email_err}")
                        print(f"[EMAIL] ✗ Email sending failed: {email_err}")
                else:
                    logger.warning("PDF generation returned None")
                    print(f"\n[WARNING] PDF generation failed - check logs")
            except Exception as e:
                logger.error(f"Failed to generate consolidated PDF: {e}")
                print(f"\n[ERROR] PDF generation failed: {e}")
                import traceback
                traceback.print_exc()

        logger.info(f"Anomaly-triggered plotting completed: {plots_generated} plots generated")
        return session_dir

    def _extract_verified_anomalies(self, detection_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract tags that have been verified as anomalous by the detection pipeline."""
        verified_anomalies = []

        # Check each unit's results
        for unit, unit_results in detection_results.items():
            if not isinstance(unit_results, dict) or 'by_tag' not in unit_results:
                continue

            by_tag = unit_results['by_tag']

            for tag, tag_info in by_tag.items():
                # Check if anomaly was verified by the pipeline
                if self._is_anomaly_verified(tag_info):
                    verified_anomalies.append({
                        'unit': unit,
                        'tag': tag,
                        'tag_info': tag_info,
                        'verification_details': self._get_verification_details(tag_info)
                    })

        # Sort by anomaly severity (most severe first)
        verified_anomalies.sort(key=lambda x: self._calculate_anomaly_severity(x['tag_info']), reverse=True)

        return verified_anomalies

    def _is_anomaly_verified(self, tag_info: Dict[str, Any]) -> bool:
        """Check if an anomaly has been verified by the detection pipeline.

        CRITICAL REQUIREMENT: Only consider anomalies detected in last 24 hours as ACTIONABLE.
        Anomalies older than 24 hours are historical and should not trigger plots/alerts.
        """
        # RECENCY FILTER: Anomalies must be NEW (within last 24 hours)
        recency_breakdown = tag_info.get('recency_breakdown', {})
        last_24h_count = recency_breakdown.get('last_24h', 0)

        if last_24h_count == 0:
            # No recent anomalies - this is historical data, skip plotting
            return False

        # WEIGHTED CONFIDENCE FILTERING: Use priority-based confidence thresholds
        # Higher priority = more lenient threshold (catch more potential issues)
        # Lower priority = stricter threshold (reduce noise)
        priority = tag_info.get('priority', 'LOW')
        confidence_score = tag_info.get('confidence_score', 0)

        # Priority-based confidence thresholds (0-100 scale)
        # These thresholds balance false positives vs false negatives per priority level
        if priority == 'CRITICAL':
            confidence_threshold = 50  # Lenient: Catch all critical issues, tolerate some false positives
        elif priority == 'HIGH':
            confidence_threshold = 60  # Standard: Balanced detection
        elif priority == 'MEDIUM':
            confidence_threshold = 70  # Strict: Only high-confidence medium priority
        else:
            confidence_threshold = 80  # Very strict: LOW priority must be extremely confident

        # Check if confidence score meets threshold
        if confidence_score < confidence_threshold:
            return False

        # Additional safety check: Must have primary detection
        # (Confidence score could theoretically be non-zero from verification alone)
        sigma_count = tag_info.get('sigma_2_5_count', 0)
        ae_count = tag_info.get('autoencoder_count', 0)
        primary_detected = sigma_count > 0 or ae_count > 0

        if not primary_detected:
            return False

        # Additional safety check: Must have verification
        # (Pipeline requires both primary + verification layers)
        mtd_count = tag_info.get('mtd_count', 0)
        iso_count = tag_info.get('isolation_forest_count', 0)
        verification_detected = mtd_count > 0 or iso_count > 0

        return verification_detected

    def _get_verification_details(self, tag_info: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed verification information for the anomaly.

        Enhanced with priority and time-weighted scoring information.
        """
        return {
            'primary_detections': {
                '2.5_sigma': tag_info.get('sigma_2_5_count', 0),
                'autoencoder': tag_info.get('autoencoder_count', 0)
            },
            'verification_detections': {
                'mtd': tag_info.get('mtd_count', 0),
                'isolation_forest': tag_info.get('isolation_forest_count', 0)
            },
            'confidence': tag_info.get('confidence', 'UNKNOWN'),
            'total_anomalies': tag_info.get('count', 0),
            'anomaly_rate': tag_info.get('rate', 0),
            # NEW: Priority and time-weighted information
            'priority': tag_info.get('priority', 'LOW'),
            'weighted_score': tag_info.get('weighted_score', 0),
            'recency_breakdown': tag_info.get('recency_breakdown', {
                'last_24h': 0, 'last_7d': 0, 'last_30d': 0, 'older': 0
            })
        }

    def _calculate_anomaly_severity(self, tag_info: Dict[str, Any]) -> float:
        """Calculate severity score for prioritizing anomalies.

        Enhanced with time-weighted scoring for better prioritization.
        """
        # Use new weighted score if available (prioritizes recent anomalies)
        weighted_score = tag_info.get('weighted_score', 0)
        if weighted_score > 0:
            # Priority multipliers: CRITICAL >> HIGH >> MEDIUM
            priority = tag_info.get('priority', 'LOW')
            priority_multiplier = {
                'CRITICAL': 1000.0,  # Plot CRITICAL first
                'HIGH': 100.0,       # Then HIGH
                'MEDIUM': 10.0,      # Then MEDIUM
                'LOW': 1.0
            }.get(priority, 1.0)

            return weighted_score * priority_multiplier

        # Fallback to legacy severity calculation if weighted score not available
        count = tag_info.get('count', 0)
        rate = tag_info.get('rate', 0)
        confidence = tag_info.get('confidence', 'LOW')

        # Base severity from count and rate
        severity = count * 10 + rate * 100

        # Confidence multipliers
        if confidence == 'HIGH':
            severity *= 2.0
        elif confidence == 'MEDIUM':
            severity *= 1.5

        return severity

    def _generate_anomaly_plot(self, anomaly_info: Dict[str, Any], session_dir: Path) -> bool:
        """Generate a 3-month historical plot for a verified anomalous tag."""
        unit = anomaly_info['unit']
        tag = anomaly_info['tag']
        tag_info = anomaly_info['tag_info']
        verification = anomaly_info['verification_details']

        logger.info(f"Generating 3-month historical plot for {unit}/{tag}")

        try:
            # Load historical data for the tag (3 months)
            historical_data = self._load_tag_historical_data(unit, tag)

            if historical_data.empty:
                logger.warning(f"No historical data found for {unit}/{tag}")
                return False

            # Create the anomaly diagnostic plot
            plot_path = self._create_anomaly_diagnostic_plot(
                historical_data, unit, tag, tag_info, verification, session_dir
            )

            logger.info(f"Plot generated: {plot_path.name}")
            return True

        except Exception as e:
            # Provide richer diagnostics to pinpoint dataframe/column issues
            try:
                cols = historical_data.columns.tolist() if isinstance(historical_data, pd.DataFrame) else None
                nrows, ncols = (historical_data.shape if hasattr(historical_data, 'shape') else (None, None))
            except Exception:
                cols, nrows, ncols = None, None, None
            logger.exception(
                f"Error generating plot for {unit}/{tag}: {e}. "
                f"DataFrame shape={nrows}x{ncols}, columns={cols}"
            )
            return False

    def _load_tag_historical_data(self, unit: str, tag: str) -> pd.DataFrame:
        """Load 90 days of historical data for a specific tag (performance optimized).

        Also marks a 'running' boolean column by detecting unit running periods
        from available speed-like tags, so plotting stats can exclude
        startup/shutdown windows.

        Note: Using 90-day window reduces memory usage and processing time
        from hours to minutes on large datasets (8M+ records).
        """
        # Calculate 90-day lookback period (performance optimized)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.historical_days)

        try:
            # Import parquet database to load historical data
            from .parquet_database import ParquetDatabase
            db = ParquetDatabase()

            # Invalidate DuckDB cache to ensure fresh data after Excel refresh
            try:
                db.invalidate_cache()
            except Exception as cache_err:
                logger.warning(f"Cache invalidation failed (non-fatal): {cache_err}")

            # Load full unit data (needed to infer running periods)
            unit_df = db.get_unit_data(unit)
            if unit_df.empty:
                return pd.DataFrame()

            # Normalize and window for efficiency
            unit_df['time'] = pd.to_datetime(unit_df['time'])
            unit_window = unit_df[(unit_df['time'] >= start_date) & (unit_df['time'] <= end_date)].copy()
            if unit_window.empty:
                return pd.DataFrame()

            # SHUTDOWN DETECTION DISABLED - Was causing false positives and chart pollution
            # Old code attempted to detect running vs stopped periods but was unreliable
            # running_times = self._derive_running_timestamps(unit_window)

            # Extract the requested tag within the window
            tag_data = unit_window[unit_window['tag'] == tag].copy()
            if tag_data.empty:
                return pd.DataFrame()

            # Mark all data as "running" - no shutdown filtering
            tag_data['running'] = True

            historical_data = tag_data.sort_values('time')
            logger.info(f"Loaded {len(historical_data):,} historical records for {unit}/{tag}")

            return historical_data

        except Exception as e:
            logger.error(f"Error loading historical data for {unit}/{tag}: {e}")
            return pd.DataFrame()

    def _create_anomaly_diagnostic_plot(self, data: pd.DataFrame, unit: str, tag: str,
                                      tag_info: Dict, verification: Dict, session_dir: Path) -> Path:
        """Create a comprehensive diagnostic plot for the anomalous tag."""

        # Analyze data quality and characteristics
        staleness_warning = ""
        instrumentation_issue = False
        data_quality_notes = []

        if 'time' in data.columns and len(data) > 0:
            latest_time = pd.to_datetime(data['time']).max()
            earliest_time = pd.to_datetime(data['time']).min()

            # Check Parquet file freshness instead of data timestamps
            # Data timestamps may be old (historical) but file was just refreshed
            try:
                from pathlib import Path
                import os
                # Try to find the Parquet file for this unit
                parquet_files = list(Path('data/processed').glob(f'{unit}_*.parquet'))
                if parquet_files:
                    # Use the most recently modified file
                    latest_parquet = max(parquet_files, key=lambda p: p.stat().st_mtime)
                    file_age_seconds = datetime.now().timestamp() - latest_parquet.stat().st_mtime
                    hours_stale = file_age_seconds / 3600
                else:
                    # Fallback to data timestamp if no file found
                    hours_stale = (datetime.now() - latest_time).total_seconds() / 3600
            except Exception:
                # Fallback to data timestamp on error
                hours_stale = (datetime.now() - latest_time).total_seconds() / 3600

            data_span_days = (latest_time - earliest_time).total_seconds() / 86400

            # Staleness threshold: 24 hours for instrumentation issue warning
            # (1-hour threshold is used for refresh triggers, but plots should only warn on serious issues)
            staleness_threshold_hours = 24.0
            if hours_stale > staleness_threshold_hours:
                instrumentation_issue = True
                staleness_warning = f" ⚠️ INSTRUMENTATION ISSUE: Data stale ({hours_stale:.1f}h old)"

            # Detect sparse data
            if len(data) < 100:
                data_quality_notes.append(f"⚠️ Sparse data ({len(data)} points)")

            # Detect short history
            if data_span_days < 30:
                data_quality_notes.append(f"ℹ️ Short history ({data_span_days:.0f} days)")

            # Calculate data frequency
            if len(data) > 1:
                time_diffs = pd.to_datetime(data['time']).diff().dropna()
                median_interval_min = time_diffs.median().total_seconds() / 60
                if median_interval_min > 30:
                    data_quality_notes.append(f"ℹ️ Low frequency ({median_interval_min:.0f}min intervals)")

        # Create figure with subplots
        fig, axes = plt.subplots(3, 1, figsize=(self.plot_width, self.plot_height))

        # Title with staleness warning, priority badge, and data quality notes
        title_color = 'red' if instrumentation_issue else 'black'
        quality_suffix = f" | {' | '.join(data_quality_notes)}" if data_quality_notes else ""

        # Add priority badge to title
        priority = verification.get('priority', 'UNKNOWN')
        priority_emoji = {
            'CRITICAL': '🚨',
            'HIGH': '⚠️',
            'MEDIUM': '🔸',
            'LOW': '📊',
            'UNKNOWN': ''
        }.get(priority, '')

        # Add recency info
        recency = verification.get('recency_breakdown', {})
        recent_24h = recency.get('last_24h', 0)
        recent_7d = recency.get('last_7d', 0)
        recency_text = ''
        if recent_24h > 0:
            recency_text = f' | {recent_24h} anomalies in last 24h'
        elif recent_7d > 0:
            recency_text = f' | {recent_7d} anomalies in last 7d'

        fig.suptitle(f'{priority_emoji} ANOMALY DIAGNOSTIC REPORT [{priority}]\n{unit} | {tag}\n'
                    f'3-Month Historical Context{staleness_warning}{quality_suffix}{recency_text}',
                    fontsize=16, fontweight='bold', color=title_color)

        # Calculate robust statistics for sigma bounds
        # Ensure 'running' mask is boolean to avoid label-indexing errors
        if 'running' in data.columns:
            try:
                run_mask = pd.Series(data['running']).astype(bool)
            except Exception:
                # Robust fallback for odd encodings/values
                run_mask = pd.Series(
                    [str(x).strip().lower() in ('1', 'true', 't', 'yes', 'y') for x in data['running']],
                    index=data.index,
                    dtype=bool,
                )
        else:
            run_mask = pd.Series([True] * len(data), index=data.index, dtype=bool)
        vals = pd.to_numeric(data.loc[run_mask, 'value'], errors='coerce').dropna()

        try:
            if len(vals) >= 10:
                # Robust statistics (median + MAD-based sigma)
                med = float(vals.median())
                mad = float((vals - med).abs().median())
                robust_sigma = 1.4826 * mad if mad > 0 else float(vals.std(ddof=0))
                center_val = med
                lower_bound = med - 2.5 * robust_sigma
                upper_bound = med + 2.5 * robust_sigma
                bounds_label = 'Median/Robust 2.5σ'
            else:
                raise ValueError('insufficient data')
        except Exception:
            # Fallback to mean/std
            center_val = float(data['value'].mean())
            std_val = float(data['value'].std(ddof=0))
            lower_bound = center_val - 2.5 * std_val
            upper_bound = center_val + 2.5 * std_val
            bounds_label = 'Mean/2.5σ'

        # Identify anomalous points (breach ±2.5σ)
        data['anomaly'] = (data['value'] < lower_bound) | (data['value'] > upper_bound)

        # Speed-aware classification (if running status available)
        has_running_status = 'running' in data.columns

        # Main time series plot
        ax1 = axes[0]

        # Forward fill gaps for continuous visualization
        # Create a complete time range with regular intervals
        data_sorted = data.sort_values('time').copy()
        time_series = pd.to_datetime(data_sorted['time'])

        # Validate required columns exist
        required_cols = ['time', 'value']
        if not all(col in data_sorted.columns for col in required_cols):
            logger.warning(f"Missing required columns for forward fill: {required_cols}")
            data_filled = data_sorted
            original_data = data_sorted
            filled_data = pd.DataFrame()
        # Detect time interval (median difference between consecutive timestamps)
        elif len(time_series) > 1:
            time_diffs = time_series.diff().dropna()
            median_interval = time_diffs.median()

            # Create complete time index with forward fill
            time_min = time_series.min()
            time_max = time_series.max()
            complete_time_range = pd.date_range(start=time_min, end=time_max, freq=median_interval)

            # Reindex with forward fill (max 12 periods = 2 hours at 10min intervals)
            data_filled = data_sorted.set_index('time').reindex(complete_time_range, method='ffill', limit=12)
            data_filled['is_filled'] = ~data_filled.index.isin(data_sorted['time'])
            data_filled = data_filled.reset_index().rename(columns={'index': 'time'})

            # Validate is_filled column exists after reset_index
            if 'is_filled' not in data_filled.columns:
                logger.warning("is_filled column missing after reindex - skipping forward fill separation")
                original_data = data_filled
                filled_data = pd.DataFrame()
            else:
                # Separate original and filled data
                original_data = data_filled[data_filled['is_filled'] == False].copy()
                filled_data = data_filled[data_filled['is_filled'] == True].copy()
        else:
            # Not enough data for forward fill
            data_filled = data_sorted
            original_data = data_sorted
            filled_data = pd.DataFrame()

        # Plot full continuous line with color change when crossing bounds
        if 'time' in data_filled.columns and 'value' in data_filled.columns:
            times = data_filled['time'].values
            values = data_filled['value'].values
            normal_labeled = False
            anomalous_labeled = False

            # Plot segment by segment with color change at boundary crossings
            for i in range(len(values) - 1):
                # Check if current value is within bounds
                if lower_bound <= values[i] <= upper_bound:
                    color = 'blue'
                    label = 'Value (Normal)' if not normal_labeled else ''
                    normal_labeled = True
                else:
                    color = 'red'
                    label = 'Value (Anomaly)' if not anomalous_labeled else ''
                    anomalous_labeled = True

                ax1.plot([times[i], times[i+1]], [values[i], values[i+1]],
                        color=color, linewidth=1.2, alpha=0.7, label=label, zorder=1)
        else:
            logger.warning(f"Cannot plot - missing columns. Available: {data_filled.columns.tolist()}")

        # Overlay filled gaps with lighter blue dots
        if not filled_data.empty and 'time' in filled_data.columns and 'value' in filled_data.columns:
            ax1.scatter(filled_data['time'], filled_data['value'],
                       color='lightblue', s=8, alpha=0.4, marker='.',
                       label='Forward Filled', zorder=2)

        # SHUTDOWN OVERLAY DISABLED - was causing false positives and messy charts
        # Keeping the logic for backward compatibility but not plotting stopped periods
        if False:  # Disabled shutdown detection overlay
            if has_running_status:
                if 'running' in original_data.columns:
                    run_mask_orig = pd.Series(original_data['running']).astype(bool)
                    stopped_data = original_data.loc[~run_mask_orig]
                else:
                    stopped_data = pd.DataFrame()
                if not stopped_data.empty and 'time' in stopped_data.columns and 'value' in stopped_data.columns:
                    ax1.scatter(stopped_data['time'], stopped_data['value'],
                               color='gray', s=20, alpha=0.7, marker='s',
                               label='Unit Stopped/Shutdown', zorder=3, edgecolors='darkgray', linewidths=0.5)

        # Overlay anomalous points with red markers (only on REAL data when running)
        if has_running_status:
            if 'running' in original_data.columns and 'anomaly' in original_data.columns:
                run_mask_orig = pd.Series(original_data['running']).astype(bool)
                anom_mask_orig = pd.Series(original_data['anomaly']).astype(bool)
                running_anomalous = original_data.loc[run_mask_orig & anom_mask_orig]
            else:
                running_anomalous = pd.DataFrame()
        else:
            if 'anomaly' in original_data.columns:
                anom_mask_orig = pd.Series(original_data['anomaly']).astype(bool)
                running_anomalous = original_data.loc[anom_mask_orig]
            else:
                running_anomalous = pd.DataFrame()

        if not running_anomalous.empty and 'time' in running_anomalous.columns and 'value' in running_anomalous.columns:
            ax1.scatter(running_anomalous['time'], running_anomalous['value'],
                       color='red', s=50, alpha=0.9, marker='o',
                       label='Anomaly (Running)', zorder=5, edgecolors='darkred', linewidths=1.5)

        # Add center line (mean or median)
        baseline_label = f"{bounds_label.split('/')[0]} (Running Only)" if has_running_status else bounds_label.split('/')[0]
        ax1.axhline(center_val, color='green', linestyle='-', linewidth=1.5, alpha=0.8, label=baseline_label)

        # Add ±2.5σ bounds
        ax1.axhline(upper_bound, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='+2.5σ Upper')
        ax1.axhline(lower_bound, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='-2.5σ Lower')

        # Highlight recent anomalous period (last 7 days)
        recent_cutoff = datetime.now() - timedelta(days=7)
        recent_data = data[data['time'] >= recent_cutoff]
        if not recent_data.empty:
            ax1.axvspan(recent_data['time'].min(), recent_data['time'].max(),
                       alpha=0.15, color='pink', label='Recent Period (7d)', zorder=0)

        # Add instrumentation issue warning annotation if stale
        if instrumentation_issue:
            # Mark the end of data with a vertical red line
            latest_time = pd.to_datetime(data_sorted['time']).max()
            ax1.axvline(latest_time, color='red', linestyle=':', linewidth=2, alpha=0.8, label='Data Stopped', zorder=6)

            # Add prominent warning box - positioned outside plot to right margin
            fig.text(0.92, 0.75,
                    f'⚠️ INSTRUMENTATION ISSUE\nData stopped {hours_stale:.1f}h ago\nLatest: {latest_time.strftime("%Y-%m-%d %H:%M")}',
                    fontsize=9, fontweight='bold',
                    verticalalignment='center', horizontalalignment='left',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.8, edgecolor='darkred', linewidth=2),
                    color='white')

        # Add speed-aware annotation if applicable
        title_suffix = ' (Speed-Aware Baseline)' if has_running_status else ''
        ax1.set_title(f'3-Month Time Series with ±2.5σ Bounds{title_suffix}')
        ax1.set_ylabel('Value')
        ax1.legend(loc='best', fontsize=8, ncol=2)
        ax1.grid(True, alpha=0.3)

        # MTD Score & Isolation Forest Anomaly Score Visualization
        ax2 = axes[1]

        # Compute MTD-style anomaly scores (Mahalanobis distance)
        try:
            # Use most recent 7-day window from the data (not from current time)
            # This ensures we have data even if the plot is for historical 3-month window
            if 'time' in data.columns and len(data) > 0:
                latest_time = pd.to_datetime(data['time']).max()
                recent_cutoff = latest_time - timedelta(days=7)
            else:
                recent_cutoff = datetime.now() - timedelta(days=7)

            recent_window = data[pd.to_datetime(data['time']) >= recent_cutoff].copy()
            # Ensure running flag is boolean in this window to avoid indexer errors
            if 'running' in recent_window.columns:
                try:
                    recent_window['running'] = recent_window['running'].astype(bool)
                except Exception:
                    recent_window['running'] = recent_window['running'].apply(lambda x: str(x).strip().lower() in ('1','true','t','yes','y'))

            # Lower threshold to ensure we have enough data for meaningful analysis
            min_points_required = 10  # Reduced from 20
            if len(recent_window) >= min_points_required:
                # Calculate rolling statistics for MTD-style scoring
                window_size = min(24, len(recent_window) // 4)  # 24-hour or adaptive
                recent_window['rolling_mean'] = recent_window['value'].rolling(window=window_size, center=False).mean()
                recent_window['rolling_std'] = recent_window['value'].rolling(window=window_size, center=False).std()

                # MTD-style score: normalized deviation from rolling baseline
                recent_window['mtd_score'] = (
                    (recent_window['value'] - recent_window['rolling_mean']).abs() /
                    (recent_window['rolling_std'] + 1e-6)
                )

                # Isolation Forest Score (simulate using deviation from global stats)
                global_mean = data['value'].mean()
                global_std = data['value'].std()
                recent_window['if_score'] = -1 * (  # Negative = anomalous in IF convention
                    (recent_window['value'] - global_mean).abs() / (global_std + 1e-6)
                )

                # Plot MTD scores - SHUTDOWN DETECTION DISABLED for cleaner charts
                mtd_threshold = 2.5  # Typical threshold for MTD

                # Always plot all data without filtering by running status
                if False:  # Disabled running status filtering
                    # Split by running status
                    run_mask_recent = pd.Series(recent_window['running']).astype(bool)
                    running_mtd = recent_window.loc[run_mask_recent]
                    stopped_mtd = recent_window.loc[~run_mask_recent]

                    # Plot running periods with color change at threshold
                    if not running_mtd.empty:
                        times_run = running_mtd['time'].values
                        scores_run = running_mtd['mtd_score'].values
                        normal_labeled = False
                        anomalous_labeled = False

                        for i in range(len(scores_run) - 1):
                            if scores_run[i] < mtd_threshold:
                                color = 'blue'
                                label = 'MTD Normal (Running)' if not normal_labeled else ''
                                normal_labeled = True
                            else:
                                color = 'red'
                                label = 'MTD Anomaly (Running)' if not anomalous_labeled else ''
                                anomalous_labeled = True
                            ax2.plot([times_run[i], times_run[i+1]], [scores_run[i], scores_run[i+1]],
                                    color=color, linewidth=1.5, alpha=0.8, label=label)

                    # Plot stopped periods in gray - use line for continuity
                    if not stopped_mtd.empty:
                        ax2.plot(stopped_mtd['time'], stopped_mtd['mtd_score'],
                                'gray', linewidth=1.0, alpha=0.4, linestyle='-',
                                label='MTD (Stopped)', zorder=1)

                    # Count anomalous regions
                    anomalous_mtd = running_mtd[running_mtd['mtd_score'] > mtd_threshold] if not running_mtd.empty else pd.DataFrame()
                    title_suffix = ' - Speed-Aware'

                # Use all data without shutdown filtering
                if True:
                    # No running status - plot all data with color change at threshold
                    times = recent_window['time'].values
                    scores = recent_window['mtd_score'].values
                    normal_labeled = False
                    anomalous_labeled = False

                    for i in range(len(scores) - 1):
                        if scores[i] < mtd_threshold:
                            color = 'blue'
                            label = 'MTD Normal' if not normal_labeled else ''
                            normal_labeled = True
                        else:
                            color = 'red'
                            label = 'MTD Anomaly' if not anomalous_labeled else ''
                            anomalous_labeled = True
                        ax2.plot([times[i], times[i+1]], [scores[i], scores[i+1]],
                                color=color, linewidth=1.5, alpha=0.8, label=label)

                    anomalous_mtd = recent_window[recent_window['mtd_score'] > mtd_threshold]
                    title_suffix = ''

                ax2.axhline(mtd_threshold, color='red', linestyle='--', linewidth=2,
                           alpha=0.7, label=f'MTD Threshold ({mtd_threshold})')

                # Mark anomalies
                if not anomalous_mtd.empty:
                    ax2.scatter(anomalous_mtd['time'], anomalous_mtd['mtd_score'],
                              color='red', s=40, alpha=0.8, marker='x',
                              label='MTD Anomaly (Running)', zorder=5)

                ax2.set_title(f'MTD Anomaly Score (Last 7 Days){title_suffix}')
                ax2.set_ylabel('MTD Score (σ units)')
                ax2.set_xlabel('Time')
                ax2.legend(loc='best', fontsize=9)
                ax2.grid(True, alpha=0.3)

                # Add latest value status - positioned outside plot to right margin
                if len(recent_window) > 0:
                    # Use column-first then iloc to avoid any chance of label confusion
                    latest_mtd = recent_window['mtd_score'].iloc[-1]
                    latest_status = 'ANOMALOUS' if latest_mtd > mtd_threshold else 'NORMAL'
                    status_color = 'red' if latest_mtd > mtd_threshold else 'green'
                    fig.text(0.92, 0.50, f'Latest MTD: {latest_mtd:.2f}\nStatus: {latest_status}',
                            fontsize=9, fontweight='bold',
                            verticalalignment='center', horizontalalignment='left',
                            bbox=dict(boxstyle='round,pad=0.4', facecolor=status_color, alpha=0.6))
            else:
                actual_points = len(recent_window)
                ax2.text(0.5, 0.5, f'Insufficient recent data for MTD analysis\n(have {actual_points} points, need ≥{min_points_required} in last 7 days)',
                        transform=ax2.transAxes, ha='center', va='center', fontsize=11)
                ax2.set_title('MTD Anomaly Score - Insufficient Data')
        except Exception as e:
            ax2.text(0.5, 0.5, f'MTD computation failed:\n{str(e)[:80]}',
                    transform=ax2.transAxes, ha='center', va='center', fontsize=10)
            ax2.set_title('MTD Anomaly Score - Error')

        # Isolation Forest Anomaly Score Visualization
        ax3 = axes[2]

        try:
            # Use same recent window as MTD
            if len(recent_window) >= min_points_required:
                # Isolation Forest scores using sklearn if available
                try:
                    from sklearn.ensemble import IsolationForest

                    # Prepare features for IF (value + time-based features)
                    recent_window['hour'] = pd.to_datetime(recent_window['time']).dt.hour
                    recent_window['dayofweek'] = pd.to_datetime(recent_window['time']).dt.dayofweek
                    X = recent_window[['value', 'hour', 'dayofweek']].values

                    # Train Isolation Forest
                    iso_forest = IsolationForest(contamination=0.1, random_state=42, n_estimators=100)
                    recent_window['if_anomaly_score'] = iso_forest.fit_predict(X)  # -1 = anomaly, 1 = normal
                    recent_window['if_decision_score'] = iso_forest.decision_function(X)  # Lower = more anomalous

                    # Plot IF decision scores with color transition at threshold crossings
                    # Use segment-by-segment plotting to change color when crossing threshold
                    times = recent_window['time'].values
                    scores = recent_window['if_decision_score'].values

                    # Track if we've added labels
                    normal_labeled = False
                    anomalous_labeled = False

                    # Plot line segments, changing color at threshold crossings
                    for i in range(len(scores) - 1):
                        # Determine color based on current score
                        if scores[i] >= 0:
                            color = 'purple'
                            label = 'IF Score (Normal)' if not normal_labeled else ''
                            normal_labeled = True
                        else:
                            color = 'red'
                            label = 'IF Score (Anomalous)' if not anomalous_labeled else ''
                            anomalous_labeled = True

                        # Plot segment
                        ax3.plot([times[i], times[i+1]], [scores[i], scores[i+1]],
                                color=color, linewidth=1.5, alpha=0.8, label=label)

                    ax3.axhline(0, color='orange', linestyle='--', linewidth=2,
                               alpha=0.7, label='IF Decision Boundary')

                    # Count anomalies for label
                    if_anomalies = recent_window[recent_window['if_anomaly_score'] == -1]
                    if not if_anomalies.empty:
                        # Add annotation instead of scatter markers
                        ax3.text(0.02, 0.95, f'IF Anomaly: {len(if_anomalies)} points',
                                transform=ax3.transAxes, fontsize=9, fontweight='bold',
                                verticalalignment='top', color='red',
                                bbox=dict(boxstyle='round,pad=0.3', facecolor='pink', alpha=0.5))

                    ax3.set_title('Isolation Forest Anomaly Detection (Last 7 Days) - Verification Layer')
                    ax3.set_ylabel('IF Decision Score')
                    ax3.set_xlabel('Time')
                    ax3.legend(loc='best', fontsize=9)
                    ax3.grid(True, alpha=0.3)

                    # Add latest value status - positioned outside plot to right margin
                    if len(recent_window) > 0:
                        # Column-first then iloc for robustness
                        latest_if = recent_window['if_anomaly_score'].iloc[-1]
                        latest_score = recent_window['if_decision_score'].iloc[-1]
                        latest_status = 'ANOMALOUS' if latest_if == -1 else 'NORMAL'
                        status_color = 'red' if latest_if == -1 else 'green'
                        fig.text(0.92, 0.25, f'Latest IF: {latest_score:.3f}\nStatus: {latest_status}',
                                fontsize=9, fontweight='bold',
                                verticalalignment='center', horizontalalignment='left',
                                bbox=dict(boxstyle='round,pad=0.4', facecolor=status_color, alpha=0.6))

                    # Add detection summary box - positioned outside plot to right margin
                    instrumentation_status = "⚠️ STALE" if instrumentation_issue else "✓ Fresh"
                    summary_text = f"""DETECTION SUMMARY:
Primary: {verification['primary_detections']['2.5_sigma']} σ-anom
Verified: MTD={verification['verification_detections']['mtd']}, IF={verification['verification_detections']['isolation_forest']}
Total: {verification['total_anomalies']} | Rate: {verification['anomaly_rate']*100:.1f}%
Confidence: {verification['confidence']}
Instrumentation: {instrumentation_status}"""

                    summary_bg_color = 'lightcoral' if instrumentation_issue else 'lightyellow'
                    fig.text(0.92, 0.08, summary_text,
                            fontsize=8, fontfamily='monospace', verticalalignment='bottom', horizontalalignment='left',
                            bbox=dict(boxstyle='round,pad=0.3', facecolor=summary_bg_color, alpha=0.7,
                                     edgecolor='darkred' if instrumentation_issue else 'gray', linewidth=2 if instrumentation_issue else 1))

                except ImportError:
                    # Fallback to simple IF-style score without sklearn
                    global_mean = data['value'].mean()
                    global_std = data['value'].std()
                    recent_window['if_simple_score'] = -1 * (
                        (recent_window['value'] - global_mean).abs() / (global_std + 1e-6)
                    )

                    ax3.plot(recent_window['time'], recent_window['if_simple_score'],
                            'purple', linewidth=1.5, alpha=0.8, label='IF-Style Score')
                    ax3.axhline(-2.5, color='orange', linestyle='--', linewidth=2,
                               alpha=0.7, label='Anomaly Threshold (-2.5σ)')

                    ax3.set_title('Isolation Forest-Style Score (sklearn not available)')
                    ax3.set_ylabel('Normalized Score')
                    ax3.set_xlabel('Time')
                    ax3.legend(loc='best', fontsize=9)
                    ax3.grid(True, alpha=0.3)

            else:
                actual_points = len(recent_window) if 'recent_window' in locals() else 0
                ax3.text(0.5, 0.5, f'Insufficient recent data for IF analysis\n(have {actual_points} points, need ≥{min_points_required} in last 7 days)',
                        transform=ax3.transAxes, ha='center', va='center', fontsize=11)
                ax3.set_title('Isolation Forest - Insufficient Data')

        except Exception as e:
            ax3.text(0.5, 0.5, f'IF computation failed:\n{str(e)[:80]}',
                    transform=ax3.transAxes, ha='center', va='center', fontsize=10)
            ax3.set_title('Isolation Forest - Error')

        # Adjust layout to make room for right margin text boxes
        plt.tight_layout(rect=[0, 0, 0.88, 1])  # Leave 12% space on right for info boxes

        # Save plot with descriptive filename inside a unit subfolder
        from .plot_controls import ensure_unit_dir
        unit_dir = ensure_unit_dir(session_dir, unit)
        safe_tag = "".join(c for c in tag if c.isalnum() or c in "._-")[:50]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"ANOMALY_{unit}_{safe_tag}_{timestamp}.png"
        plot_path = unit_dir / filename

        plt.savefig(plot_path, dpi=150, bbox_inches='tight', facecolor='white', pad_inches=0.1)
        plt.close()

        return plot_path

    # -- helpers -------------------------------------------------------------
    def _derive_running_timestamps(self, unit_window: pd.DataFrame) -> set | None:
        """Return set of timestamps when unit appears to be running (>10 RPM).

        Detect speed-like tags (SI/SIA/SPEED/RPM/SE, excluding TEMP/PRESS/FLOW/LEVEL/VIB),
        average their values across tags per timestamp, and mark times with mean>10 as running.
        """
        try:
            if unit_window.empty or 'tag' not in unit_window.columns:
                return None
            tags = pd.Series(unit_window['tag'].unique()).astype(str).str.upper()
            include = ['SI', 'SIA', 'SPEED', 'RPM', 'SE']
            exclude = ['TEMP', 'PRESS', 'FLOW', 'LEVEL', 'VIB']

            def _is_speed(t: str) -> bool:
                return any(p in t for p in include) and not any(x in t for x in exclude)

            speed_tags = [t for t in tags if _is_speed(t)]
            if not speed_tags:
                return None

            df = unit_window[unit_window['tag'].str.upper().isin(speed_tags)][['time', 'value']].copy()
            if df.empty:
                return None
            df['time'] = pd.to_datetime(df['time'])
            sp = df.groupby('time', as_index=False)['value'].mean()
            return set(pd.to_datetime(sp.loc[sp['value'] > 10.0, 'time']))
        except Exception:
            return None

    def _create_session_summary(self, session_dir: Path, detection_results: Dict,
                              verified_anomalies: List, plots_generated: int):
        """Create a summary report for the anomaly detection session."""
        summary_file = session_dir / "ANOMALY_SESSION_SUMMARY.txt"

        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("TURBOPREDICT X PROTEAN - ANOMALY DETECTION SESSION REPORT\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Session Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Detection Pipeline: 2.5-Sigma + Autoencoder → MTD + Isolation Forest\n\n")

            f.write("DETECTION PIPELINE CONFIGURATION:\n")
            f.write(f"  Primary Detectors: {', '.join(self.detection_pipeline['primary_detectors'])}\n")
            f.write(f"  Verification Layer: {', '.join(self.detection_pipeline['verification_detectors'])}\n")
            f.write(f"  Verification Required: {self.detection_pipeline['require_verification']}\n")
            f.write(f"  Recency Filter: ENABLED (Only NEW anomalies <24 hours old)\n")
            f.write(f"  Confidence System: WEIGHTED SCORING (0-100 scale)\n")
            f.write(f"    - Primary detectors: 70 points (2.5σ=40, AE=30)\n")
            f.write(f"    - Verification layer: 30 points (MTD=20, IF=10)\n")
            f.write(f"    - Thresholds: CRITICAL≥50, HIGH≥60, MEDIUM≥70, LOW≥80\n\n")

            f.write("SESSION RESULTS:\n")
            f.write(f"  Units Analyzed: {len(detection_results)}\n")
            f.write(f"  Verified Anomalies: {len(verified_anomalies)}\n")
            f.write(f"  Plots Generated: {plots_generated}\n")
            f.write(f"  Historical Context: {self.historical_months} months\n\n")

            if verified_anomalies:
                f.write("VERIFIED ANOMALOUS TAGS (RECENT <24H ONLY):\n")
                for i, anomaly in enumerate(verified_anomalies, 1):
                    verification = anomaly['verification_details']
                    tag_info = anomaly['tag_info']
                    recency = tag_info.get('recency_breakdown', {})
                    priority = tag_info.get('priority', 'UNKNOWN')

                    confidence_score = tag_info.get('confidence_score', 0)
                    confidence_breakdown = tag_info.get('confidence_breakdown', {})

                    f.write(f"  {i:2d}. {anomaly['unit']} | {anomaly['tag']}\n")
                    f.write(f"      Priority: {priority}\n")
                    f.write(f"      Confidence Score: {confidence_score:.1f}/100\n")
                    f.write(f"        └─ 2.5σ={confidence_breakdown.get('sigma_contribution', 0):.1f}, "
                           f"AE={confidence_breakdown.get('ae_contribution', 0):.1f}, "
                           f"MTD={confidence_breakdown.get('mtd_contribution', 0):.1f}, "
                           f"IF={confidence_breakdown.get('if_contribution', 0):.1f}\n")
                    f.write(f"      Recent Anomalies (24h): {recency.get('last_24h', 0)}\n")
                    f.write(f"      Total Anomalies: {verification['total_anomalies']} ({verification['anomaly_rate']*100:.1f}%)\n")
                    f.write(f"      Pipeline: 2.5σ({verification['primary_detections']['2.5_sigma']}) + "
                           f"AE({verification['primary_detections']['autoencoder']}) → "
                           f"MTD({verification['verification_detections']['mtd']}) + "
                           f"IS({verification['verification_detections']['isolation_forest']})\n\n")
            else:
                f.write("No recent (<24h) verified anomalies detected.\n")
                f.write("All systems operating normally or anomalies are historical (>24h old).\n")

    def _create_no_anomalies_report(self, session_dir: Path, detection_results: Dict):
        """Create a report when no anomalies are verified."""
        summary_file = session_dir / "NO_ANOMALIES_DETECTED.txt"

        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("TURBOPREDICT X PROTEAN - ANOMALY DETECTION REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Session Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("Detection Result: NO VERIFIED ANOMALIES\n\n")
            f.write("All monitored tags are operating within normal parameters.\n")
            f.write("No historical diagnostic plots were generated.\n\n")
            f.write("Detection Pipeline Status: OPERATIONAL\n")
            f.write("System Status: NORMAL OPERATION\n")

    def _generate_consolidated_pdf(self, session_dir: Path) -> Path:
        """Generate a single PDF file containing all anomaly plots from the session.

        Args:
            session_dir: Session directory containing PNG plot files

        Returns:
            Path to generated PDF file
        """
        try:
            from matplotlib.backends.backend_pdf import PdfPages
            import glob

            # Find all PNG plot files in the session directory
            png_files = []
            for unit_dir in session_dir.iterdir():
                if unit_dir.is_dir():
                    png_files.extend(list(unit_dir.glob("ANOMALY_*.png")))

            if not png_files:
                logger.warning("No PNG files found to compile into PDF")
                return None

            # Sort by filename for consistent ordering
            png_files = sorted(png_files)

            # Create PDF file
            pdf_filename = f"ANOMALY_REPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            pdf_path = session_dir / pdf_filename

            logger.info(f"Generating consolidated PDF with {len(png_files)} plots...")

            with PdfPages(pdf_path) as pdf:
                for png_file in png_files:
                    try:
                        # Read PNG image
                        from PIL import Image
                        img = Image.open(png_file)

                        # Create a new figure and add image
                        fig = plt.figure(figsize=(16, 10))
                        ax = fig.add_subplot(111)
                        ax.imshow(img)
                        ax.axis('off')

                        # Add to PDF
                        pdf.savefig(fig, bbox_inches='tight', pad_inches=0.1)
                        plt.close(fig)

                    except Exception as e:
                        logger.error(f"Failed to add {png_file.name} to PDF: {e}")
                        continue

            logger.info(f"Successfully generated consolidated PDF: {pdf_path}")
            return pdf_path

        except ImportError as e:
            logger.error(f"Missing required library for PDF generation: {e}")
            logger.info("Install with: pip install pillow")
            return None
        except Exception as e:
            logger.error(f"PDF generation failed: {e}")
            return None


# Convenience function for integration
def generate_anomaly_plots(detection_results: Dict[str, Any], reports_dir: Path = None) -> Path:
    """Generate anomaly-triggered plots from detection results.

    Args:
        detection_results: Results from anomaly detection pipeline
        reports_dir: Directory for reports (optional)

    Returns:
        Path to generated report directory
    """
    plotter = AnomalyTriggeredPlotter(reports_dir)
    return plotter.process_anomaly_detection_results(detection_results)
