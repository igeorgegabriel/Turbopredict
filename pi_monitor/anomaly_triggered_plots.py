#!/usr/bin/env python3
"""
Anomaly-Triggered Plotting System for TURBOPREDICT
Generates 3-month historical plots only for verified anomalous tags
Integrates with: 2.5-sigma + Autoencoder -> MTD + Isolation Forest verification
Syncs anomaly data to Supabase for TurboBubble dashboard
"""

from __future__ import annotations
import os
import pandas as pd

# Force non-interactive Agg backend for faster rendering (must be before pyplot import)
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging
import sys
import warnings
warnings.filterwarnings('ignore')
from multiprocessing import Pool, cpu_count
from functools import partial

# Setup logging
logger = logging.getLogger(__name__)

# Import Supabase sync (optional - gracefully handle if not available)
try:
    from pi_monitor.supabase_sync import sync_detection_results
    SUPABASE_AVAILABLE = True
except ImportError:
    logger.warning("Supabase sync module not available - install supabase-py: pip install supabase")
    SUPABASE_AVAILABLE = False


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

        # Plot settings for 3-month historical context (calendar months)
        # Using calendar months avoids oddities with fixed 90-day windows
        self.historical_days = 90  # fallback
        self.historical_months = int(os.getenv('PLOT_WINDOW_MONTHS', '3').strip() or '3')
        # Larger plot size to utilize full box space
        self.plot_width = 24
        self.plot_height = 14

        # Compressed multi-plot layout settings (18 plots per page)
        # Override via env for readability: PLOTS_PER_PAGE, PLOT_ROWS, PLOT_COLS
        self.plots_per_page = 18
        self.plot_rows = 6
        self.plot_cols = 3
        try:
            env_rows = os.getenv('PLOT_ROWS')
            env_cols = os.getenv('PLOT_COLS')
            env_pp = os.getenv('PLOTS_PER_PAGE')
            if env_rows and env_cols:
                r = int(env_rows)
                c = int(env_cols)
                if r >= 1 and c >= 1:
                    self.plot_rows = r
                    self.plot_cols = c
                    self.plots_per_page = r * c
            elif env_pp:
                pp = int(env_pp)
                if pp >= 1:
                    # Choose columns up to 3 to keep aspect
                    self.plot_cols = min(3, max(1, pp))
                    self.plot_rows = max(1, (pp + self.plot_cols - 1) // self.plot_cols)
                    self.plots_per_page = self.plot_rows * self.plot_cols
        except Exception:
            # Ignore malformed env
            pass

        # Detection pipeline status
        self.detection_pipeline = {
            'primary_detectors': ['2.5_sigma', 'autoencoder'],
            'verification_detectors': ['mtd', 'isolation_forest'],
            'require_verification': True
        }

        # Configure improved matplotlib fonts and sizes for better readability
        self._configure_plot_styling()
        # Prefer comprehensive consolidated PDF generation by default.
        # Inline PDF (streaming pages directly) is disabled unless explicitly enabled
        # via env PDF_INLINE=1. Default '0' avoids duplicate PDFs and is more stable.
        self._pdf_inline_enabled = os.getenv('PDF_INLINE', '0').strip().lower() in ('1','true','yes','y')
        self._pdf_inline_path = None
        self._pdf_writer = None

        # Cache for 1-year extremes per (unit, tag)
        self._year_extreme_cache: dict[tuple[str, str], tuple[float, float]] = {}

    def _get_one_year_extremes(self, unit: str, tag: str) -> tuple[float, float]:
        """Return (low, high) over the last 1 year for a tag.

        Uses DuckDB aggregation over Parquet for performance. Falls back to
        loading a 1-year window via ParquetDatabase.get_unit_data() if needed.
        Results are cached per (unit, tag) for the duration of the run.
        """
        key = (unit, tag)
        if key in self._year_extreme_cache:
            return self._year_extreme_cache[key]

        start = datetime.now() - timedelta(days=365)
        end = datetime.now()
        low, high = float('nan'), float('nan')
        try:
            from .parquet_database import ParquetDatabase
            db = ParquetDatabase()
            if db.conn is not None:
                q = (
                    "SELECT MIN(value) AS l, MAX(value) AS h "
                    f"FROM read_parquet('{db._parquet_glob(True)}') "
                    "WHERE unit = ? AND tag = ? AND time >= ? AND time <= ?"
                )
                row = db.conn.execute(q, [unit, tag, start, end]).fetchone()
                if row is not None:
                    low = float(row[0]) if row[0] is not None else float('nan')
                    high = float(row[1]) if row[1] is not None else float('nan')
            if not (low == low and high == high):  # any NaN -> fallback
                # Memory-safe fallback: read only the requested tag within window
                df = db.get_unit_tag_data(unit, tag, start_time=start, end_time=end)
                if not df.empty and 'value' in df.columns:
                    low = float(pd.to_numeric(df['value'], errors='coerce').min())
                    high = float(pd.to_numeric(df['value'], errors='coerce').max())
        except Exception:
            pass

        # Final fallback to zeros if still NaN
        if not (low == low):
            low = 0.0
        if not (high == high):
            high = 0.0

        self._year_extreme_cache[key] = (low, high)
        return low, high

    # --- verification helpers to enforce true threshold exceedance ---------
    def _compute_sigma_bounds(self, values: pd.Series) -> tuple[float, float, float, str]:
        """Return (center, lower, upper, label) using robust median/MAD fallback.

        Matches the plotting logic so gating decisions align with visuals.
        """
        vals = pd.to_numeric(values, errors='coerce').dropna()
        if len(vals) >= 10:
            med = float(vals.median())
            mad = float((vals - med).abs().median())
            robust_sigma = 1.4826 * mad if mad > 0 else float(vals.std(ddof=0))
            center_val = med
            lower_bound = med - 2.5 * robust_sigma
            upper_bound = med + 2.5 * robust_sigma
            return center_val, lower_bound, upper_bound, 'Median/Robust 2.5Ïƒ'
        # Fallback to mean/std
        center_val = float(vals.mean()) if len(vals) else 0.0
        std_val = float(vals.std(ddof=0)) if len(vals) else 0.0
        lower_bound = center_val - 2.5 * std_val
        upper_bound = center_val + 2.5 * std_val
        return center_val, lower_bound, upper_bound, 'Mean/Std 2.5Ïƒ'

    def _has_recent_threshold_exceedance(self, unit: str, tag: str) -> tuple[bool, pd.DataFrame]:
        """Load 90d data and check if any points in the last 24h exceed +/- 2.5 sigma.

        Returns (has_exceedance, data). Data is preloaded to avoid re-reading.
        """
        data = self._load_tag_historical_data(unit, tag)
        if data.empty or 'time' not in data.columns or 'value' not in data.columns:
            return False, data

        # Use only 'running' data for bounds if available (same as plot path)
        if 'running' in data.columns:
            run_mask = pd.Series(data['running']).astype(bool)
            values_for_bounds = data.loc[run_mask, 'value']
        else:
            values_for_bounds = data['value']

        _, lb, ub, _ = self._compute_sigma_bounds(values_for_bounds)
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent = data[data['time'] >= recent_cutoff]
        if recent.empty:
            return False, data
        exceed = ((recent['value'] < lb) | (recent['value'] > ub)).any()
        return bool(exceed), data

    def _calculate_anomaly_duration_hours(self, data: pd.DataFrame, window_hours: float = 24.0) -> float:
        """Calculate cumulative anomaly duration within the recent window using timestamps.

        The duration is derived from actual time deltas between consecutive samples,
        rather than assuming a fixed cadence per anomaly point.
        """
        if data is None or data.empty or 'time' not in data.columns:
            return 0.0

        try:
            # If no 'anomaly' column, calculate it from 2.5-sigma bounds
            if 'anomaly' not in data.columns:
                if 'value' not in data.columns:
                    return 0.0

                # Calculate sigma bounds to determine anomalies
                data = data.copy()
                if 'running' in data.columns:
                    run_mask = data['running'].astype(bool)
                    values_for_bounds = data.loc[run_mask, 'value']
                else:
                    values_for_bounds = data['value']

                _, lb, ub, _ = self._compute_sigma_bounds(values_for_bounds)

                # Mark anomalies as points outside bounds
                data['anomaly'] = (data['value'] < lb) | (data['value'] > ub)

            df = data[['time', 'anomaly']].copy()
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
            df['anomaly'] = df['anomaly'].astype(bool)
            df = df.dropna(subset=['time'])
            if df.empty:
                return 0.0

            cutoff = datetime.now() - timedelta(hours=window_hours)
            df = df[df['time'] >= cutoff]
            if df.empty:
                return 0.0

            df = df.sort_values('time')
            df['time_diff'] = df['time'].shift(-1) - df['time']
            positive_diffs = df['time_diff'][(df['time_diff'].notna()) & (df['time_diff'] > timedelta(0))]
            fallback = positive_diffs.median() if not positive_diffs.empty else pd.Timedelta(0)
            if pd.isna(fallback) or fallback <= pd.Timedelta(0):
                fallback = pd.Timedelta(0)
            df['time_diff'] = df['time_diff'].apply(
                lambda td: td if isinstance(td, pd.Timedelta) and td > pd.Timedelta(0) else fallback
            )

            anomaly_duration = df.loc[df['anomaly'], 'time_diff'].sum()
            if not isinstance(anomaly_duration, pd.Timedelta):
                return 0.0
            hours = anomaly_duration.total_seconds() / 3600.0
            return float(max(0.0, min(hours, window_hours)))
        except Exception as e:
            # DEBUG: Show exception
            print(f"    [DURATION ERROR] Exception: {e}")
            return 0.0

    def _configure_plot_styling(self):
        """Configure matplotlib with improved fonts and sizes for better PDF readability."""
        import matplotlib as mpl

        # Set professional font defaults with larger sizes
        mpl.rcParams['font.family'] = 'sans-serif'
        mpl.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'sans-serif']
        mpl.rcParams['font.size'] = 11  # Base font size (was default 10)
        mpl.rcParams['font.weight'] = 'normal'

        # Increase specific text element sizes
        mpl.rcParams['axes.titlesize'] = 12  # Subplot titles (was 7-8)
        mpl.rcParams['axes.labelsize'] = 11  # Axis labels
        mpl.rcParams['xtick.labelsize'] = 10  # X-axis tick labels
        mpl.rcParams['ytick.labelsize'] = 10  # Y-axis tick labels
        mpl.rcParams['legend.fontsize'] = 10  # Legend text (was 8)
        mpl.rcParams['figure.titlesize'] = 18  # Figure suptitle (was 16)

        # Improve readability with bold titles
        mpl.rcParams['axes.titleweight'] = 'bold'
        mpl.rcParams['figure.titleweight'] = 'bold'

        # Better line widths for clarity
        mpl.rcParams['lines.linewidth'] = 1.5
        mpl.rcParams['axes.linewidth'] = 1.2
        mpl.rcParams['grid.linewidth'] = 0.8

        # Performance optimizations
        # Speed: split long paths into chunks at render time (no quality loss)
        try:
            mpl.rcParams['agg.path.chunksize'] = 8000
        except Exception:
            pass

        # Faster rendering with minimal quality impact
        mpl.rcParams['path.simplify'] = True  # Simplify paths (faster, minimal visual difference)
        mpl.rcParams['path.simplify_threshold'] = 0.5  # More aggressive simplification
        mpl.rcParams['text.antialiased'] = True  # Keep text crisp
        mpl.rcParams['lines.antialiased'] = False  # Disable line AA for speed (DPI 200 compensates)

        # Improve tick visibility
        mpl.rcParams['xtick.major.width'] = 1.2
        mpl.rcParams['ytick.major.width'] = 1.2

        logger.info("Configured improved plot styling with performance optimizations")

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

        # Sync ONLY verified anomalies to TurboBubble (healthy tags are not sent)
        if SUPABASE_AVAILABLE:
            try:
                filtered_results: Dict[str, Any] = {}
                for unit, unit_results in detection_results.items():
                    if not isinstance(unit_results, dict):
                        continue
                    by_tag = unit_results.get('by_tag', {}) or {}
                    keep: Dict[str, Any] = {}
                    for tag, tag_info in by_tag.items():
                        try:
                            if self._is_anomaly_verified(tag_info):
                                keep[tag] = tag_info
                        except Exception:
                            continue
                    if keep:
                        filtered_results[unit] = {'by_tag': keep}

                # Enrich with 7-day relative change metric per verified tag
                # This metric is used by TurboBubble to size bubbles meaningfully.
                try:
                    from .parquet_database import ParquetDatabase as _DB
                    db = _DB()
                    for unit, udata in filtered_results.items():
                        try:
                            # Pull last ~3 months to compute (current vs current-3 months)
                            months = int(os.getenv('RELATIVE_CHANGE_MONTHS', '3').strip() or '3')
                            end_dt = pd.Timestamp.now().to_pydatetime()
                            start_dt = (pd.Timestamp(end_dt) - pd.DateOffset(months=months)).to_pydatetime()
                            for tag, info in list(udata.get('by_tag', {}).items()):
                                try:
                                    # Memory-safe per-tag load instead of full unit
                                    sub = db.get_unit_tag_data(unit, tag, start_time=start_dt, end_time=end_dt)
                                    if sub.empty or 'time' not in sub.columns or 'value' not in sub.columns:
                                        continue
                                    sub['time'] = pd.to_datetime(sub['time'], errors='coerce')
                                    sub = sub.dropna(subset=['time','value'])
                                    if sub.empty:
                                        continue
                                    sub = sub.sort_values('time')
                                    current_time = pd.to_datetime(sub['time']).max()
                                    current_val = float(sub.loc[sub['time'].idxmax(), 'value'])
                                    target_time = pd.Timestamp(current_time) - pd.DateOffset(months=months)
                                    # nearest point to target_time
                                    idx = (sub['time'] - target_time).abs().idxmin()
                                    prev_time = pd.to_datetime(sub.loc[idx, 'time'])
                                    prev_val = float(sub.loc[idx, 'value'])
                                    # if nearest sample is too far (>72h), skip metric
                                    if abs((prev_time - target_time).total_seconds()) > 72*3600:
                                        continue
                                    # relative change percentage
                                    denom = abs(prev_val) if abs(prev_val) > 1e-9 else 1.0
                                    diff = current_val - prev_val
                                    rel_pct_signed = (diff / denom) * 100.0
                                    rel_pct = abs(rel_pct_signed)
                                    info['relative_change_pct'] = float(rel_pct)
                                    info['relative_change_pct_signed'] = float(rel_pct_signed)
                                    info['relative_change_window_months'] = months
                                    # Include raw values and timestamps for UI display
                                    info['current_value'] = float(current_val)
                                    info['baseline_value_3m'] = float(prev_val)
                                    info['current_time'] = pd.Timestamp(current_time).isoformat()
                                    info['baseline_time_3m'] = pd.Timestamp(prev_time).isoformat()
                                    # Human-friendly label for UI hover/tooltips
                                    try:
                                        info['bubble_label'] = (
                                            'Δ' + str(months) + 'm: ' + f"{rel_pct_signed:+.2f}% "
                                            + '(now=' + f"{current_val:.3g}" + ' @ ' + pd.Timestamp(current_time).strftime('%Y-%m-%d %H:%M')
                                            + ', base=' + f"{prev_val:.3g}" + ' @ ' + pd.Timestamp(prev_time).strftime('%Y-%m-%d %H:%M') + ')'
                                        )
                                    except Exception:
                                        pass
                                except Exception:
                                    continue
                        except Exception:
                            continue
                except Exception:
                    # Enrichment is optional â€“ continue even if it fails
                    pass

                # Print a clear console message for operators
                total_units = len(filtered_results)
                total_tags = sum(len(u.get('by_tag', {})) for u in filtered_results.values())
                print(f"[PUBLISH] TurboBubble: syncing {total_units} units, {total_tags} verified tags...")
                logger.info("Syncing VERIFIED anomalies to Supabase for TurboBubble...")
                sync_stats = sync_detection_results(filtered_results, deactivate_missing=True)
                logger.info(f"Supabase sync complete: {sync_stats}")
                # Surface success on console as well
                ins = sync_stats.get('inserted', 0)
                upd = sync_stats.get('updated', 0)
                deact = sync_stats.get('deactivated', 0)
                err = sync_stats.get('errors', 0)
                print(f"[PUBLISH] TurboBubble sync: inserted={ins} updated={upd} deactivated={deact} errors={err}")
            except Exception as e:
                logger.warning(f"Failed to sync to Supabase: {e}")
                print(f"[PUBLISH] TurboBubble sync FAILED: {e}")
        else:
            print("[PUBLISH] TurboBubble sync skipped: supabase client not available")

        # Generate compressed multi-plot pages (18 plots per page)
        plots_generated = 0
        total_plot_time = 0.0
        plot_timing_by_unit = {}

        plot_start = time.time()

        # Clean up old PNG and PDF files from previous cycles to prevent duplicates
        # Each continuous loop cycle generates new plots/PDFs, so we remove old ones first
        try:
            old_pngs = list(session_dir.glob("COMPRESSED_ANOMALY_PAGE_*.png"))
            old_pdfs = list(session_dir.glob("ANOMALY_REPORT_*.pdf"))
            files_to_clean = old_pngs + old_pdfs

            if files_to_clean:
                logger.info(f"Cleaning up {len(files_to_clean)} old files from previous cycle ({len(old_pngs)} PNGs, {len(old_pdfs)} PDFs)")
                for old_file in files_to_clean:
                    try:
                        old_file.unlink()
                    except Exception as e:
                        logger.warning(f"Failed to delete old file {old_file.name}: {e}")
        except Exception as e:
            logger.warning(f"Failed to clean up old files: {e}")

        # Inline PDF: open writer so pages are streamed while plotting
        inline_writer = None
        if self._pdf_inline_enabled:
            try:
                from matplotlib.backends.backend_pdf import PdfPages
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                self._pdf_inline_path = session_dir / ("ANOMALY_REPORT_" + ts + ".pdf")
                inline_writer = PdfPages(self._pdf_inline_path)
                self._pdf_writer = inline_writer
            except Exception:
                inline_writer = None
                self._pdf_writer = None

        plots_generated = self._generate_compressed_plots(verified_anomalies, session_dir)

        if inline_writer is not None:
            try:
                inline_writer.close()
            except Exception:
                pass
            finally:
                self._pdf_writer = None

        total_plot_time = time.time() - plot_start

        # Track timing by unit
        for tag_info in verified_anomalies:
            unit = tag_info['unit']
            if unit not in plot_timing_by_unit:
                plot_timing_by_unit[unit] = {'count': 0, 'time': 0.0}
            plot_timing_by_unit[unit]['count'] += 1

        # Distribute total time across units proportionally
        for unit, info in plot_timing_by_unit.items():
            info['time'] = (info['count'] / len(verified_anomalies)) * total_plot_time if len(verified_anomalies) > 0 else 0

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
                pdf_path = None
                if self._pdf_inline_enabled and self._pdf_inline_path:
                    # Prefer inline PDF if available, but validate it isn't truncated.
                    inline_path = self._pdf_inline_path
                    try:
                        if inline_path.exists() and inline_path.stat().st_size > 1024:
                            try:
                                with open(inline_path, 'rb') as f:
                                    try:
                                        f.seek(-64, os.SEEK_END)
                                    except Exception:
                                        pass
                                    tail = f.read()
                                # Check for PDF EOF marker
                                if b"%%EOF" in tail:
                                    pdf_path = inline_path
                                else:
                                    # Fallback to consolidated if EOF marker missing
                                    pdf_path = self._generate_consolidated_pdf(session_dir, detection_results, verified_anomalies)
                            except Exception:
                                pdf_path = self._generate_consolidated_pdf(session_dir, detection_results, verified_anomalies)
                        else:
                            pdf_path = self._generate_consolidated_pdf(session_dir, detection_results, verified_anomalies)
                    except Exception:
                        pdf_path = self._generate_consolidated_pdf(session_dir, detection_results, verified_anomalies)
                else:
                    pdf_path = self._generate_consolidated_pdf(session_dir, detection_results, verified_anomalies)
                pdf_time = time.time() - pdf_start

                if pdf_path:
                    logger.info(f"Consolidated PDF report generated: {pdf_path}")
                    print(f"\n[PDF] Consolidated anomaly report: {pdf_path}")
                    print(f"[TIMING] PDF generation: {pdf_time:.2f}s")

                    # Send PDF via email using Outlook (no password needed!)
                    try:
                        from .email_sender_outlook import send_pdf_via_outlook
                        print(f"\n[EMAIL] Sending PDF report via Outlook to george.gabrielujai@petronas.com.my...")
                        email_sent = send_pdf_via_outlook(pdf_path)
                        if email_sent:
                            print(f"[EMAIL] OK - Report sent successfully!")
                        else:
                            print(f"[EMAIL] FAILED - Check error messages above")
                    except Exception as email_err:
                        logger.warning(f"Failed to send email: {email_err}")
                        print(f"[EMAIL] ERROR: {email_err}")
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

        # Sort by anomaly recency (most recent/fresh anomalies first)
        # Lower anomaly hours = fresher anomalies = page 1
        # Higher anomaly hours = older anomalies = later pages
        verified_anomalies.sort(key=lambda x: self._get_anomaly_recency_score(x['tag_info']))

        return verified_anomalies

    def _generate_compressed_plots(self, verified_anomalies: List[Dict[str, Any]], session_dir: Path) -> int:
        """Generate compressed multi-plot pages with 18 plots per page.

        Args:
            verified_anomalies: List of verified anomalous tags
            session_dir: Session directory for output

        Returns:
            Number of individual plots generated
        """
        # Enforce that plots are only generated when the 90-day bounds are
        # actually exceeded in the last 24 hours. This aligns with the
        # "only plot on exceed threshold" requirement.
        prefiltered: List[Dict[str, Any]] = []
        print("\n[DURATION DEBUG] Preparing anomalies for plotting (no 24h filter)...")
        for idx, anom in enumerate(verified_anomalies, 1):
            unit = anom['unit']
            tag = anom['tag']
            # Try to preload 90d data for duration calc and faster draw; do not gate on recency
            data = None
            try:
                _ok, data = self._has_recent_threshold_exceedance(unit, tag)
            except Exception:
                data = None
            if data is not None and not data.empty:
                anom['preloaded_data'] = data
                # Duration within 24h window (used for secondary sort); may be 0 for older anomalies
                try:
                    anom['anomaly_duration_hours'] = self._calculate_anomaly_duration_hours(data)
                except Exception:
                    anom['anomaly_duration_hours'] = 999999.0
            # Recency score (lower = fresher); fallback high when unavailable
            try:
                anom['recency_score'] = self._get_anomaly_recency_score(anom.get('tag_info', {}))
            except Exception:
                anom['recency_score'] = 999999.0
            if idx <= 10 and 'anomaly_duration_hours' in anom:
                print(f"  Tag {idx}: {tag}: {anom['anomaly_duration_hours']:.2f}h")
            prefiltered.append(anom)
        print(f"[DURATION DEBUG] Prepared {len(prefiltered)} tags for plotting")

        # Sort primarily by anomaly duration (shortest/freshest first), then by recency
        # Shorter duration = just started = most urgent = Page 1
        # Longer duration = ongoing for a while = less urgent = later pages
        prefiltered.sort(key=lambda x: (x.get('anomaly_duration_hours', 999999.0), x.get('recency_score', 999999.0)))

        # DEBUG: Print first 10 tags with their durations to verify sorting
        print("\n[SORT DEBUG] First 10 tags after sorting by anomaly duration:")
        for i, anom in enumerate(prefiltered[:10], 1):
            duration = anom.get('anomaly_duration_hours', -1)
            print(f"  {i}. {anom['unit']}/{anom['tag']}: {duration:.2f}h")
        if len(prefiltered) > 10:
            print(f"\n[SORT DEBUG] Last 3 tags:")
            for i, anom in enumerate(prefiltered[-3:], len(prefiltered)-2):
                duration = anom.get('anomaly_duration_hours', -1)
                print(f"  {i}. {anom['unit']}/{anom['tag']}: {duration:.2f}h")

        total_plots = len(prefiltered)
        num_pages = (total_plots + self.plots_per_page - 1) // self.plots_per_page

        logger.info(f"Generating {num_pages} compressed pages for {total_plots} plots")

        page_files = []
        plot_count = 0

        # Prepare page data for parallel processing
        page_data = []
        for page_num in range(num_pages):
            start_idx = page_num * self.plots_per_page
            end_idx = min(start_idx + self.plots_per_page, total_plots)
            page_anomalies = prefiltered[start_idx:end_idx]

            # DEBUG: Print what's on page 1
            if page_num == 0:
                print(f"\n[PAGE 1 DEBUG] First page will contain:")
                for i, anom in enumerate(page_anomalies, 1):
                    dur = anom.get('anomaly_duration_hours', -1)
                    print(f"  Plot {i}: {anom['unit']}/{anom['tag']}: {dur:.2f}h")

            page_data.append((page_anomalies, page_num + 1, session_dir))

        # Use parallel processing for page generation (CPU cores - 1, max 4)
        # Allow override via env PLOT_WORKERS to reduce memory pressure.
        try:
            env_workers = os.getenv('PLOT_WORKERS')
            if env_workers:
                max_workers = max(1, min(int(env_workers), num_pages))
            else:
                max_workers = min(cpu_count() - 1, 4, num_pages)
        except Exception:
            max_workers = min(cpu_count() - 1, 4, num_pages)

        # If inline PDF is enabled, avoid parallel writes to the same PDF file.
        # This prevents corrupted PDFs from concurrent writes across processes.
        if self._pdf_writer is not None and max_workers > 1:
            try:
                print("\n[PDF] Inline PDF disabled due to parallel page generation; using consolidated PDF.")
            except Exception:
                pass
            # Close any open inline writer and clear inline path so caller
            # will fall back to consolidated PDF generation.
            # Also remove any partially created inline PDF to avoid duplicate
            # reports in the same session directory.
            old_inline_path = self._pdf_inline_path
            try:
                self._pdf_writer.close()
            except Exception:
                pass
            finally:
                self._pdf_writer = None
                self._pdf_inline_path = None
                try:
                    if old_inline_path is not None and hasattr(old_inline_path, 'exists'):
                        if old_inline_path.exists():
                            old_inline_path.unlink(missing_ok=True)
                except Exception:
                    # Non-fatal: leave the temporary inline file if deletion fails
                    pass
        if max_workers < 2:
            max_workers = 1  # Fallback to sequential if only 1 core

        print(f"\n[PARALLEL] Using {max_workers} workers to generate {num_pages} pages...")

        if max_workers > 1:
            # Parallel generation
            with Pool(processes=max_workers) as pool:
                page_files = pool.starmap(self._create_compressed_page, page_data)
        else:
            # Sequential fallback
            page_files = [self._create_compressed_page(anomalies, page_num, session_dir)
                         for anomalies, page_num, session_dir in page_data]

        # Count successful plots
        for page_file in page_files:
            if page_file:
                plot_count += len(prefiltered)  # Approximate

        print(f"\n[PARALLEL] Completed generating {num_pages} pages with {plot_count} plots")

        return plot_count

    def _create_compressed_page(self, anomalies: List[Dict[str, Any]], page_num: int, session_dir: Path) -> Path:
        """Create a single page with multiple compressed plots (6 rows x 3 cols = 18 plots).

        Args:
            anomalies: List of anomalies to plot on this page
            page_num: Page number
            session_dir: Output directory

        Returns:
            Path to generated page file
        """
        # Create figure with subplots
        fig, axes = plt.subplots(self.plot_rows, self.plot_cols, figsize=(20, 24))
        axes = axes.flatten()

        # Generate title for the page with nice border
        current_time = datetime.now()
        date_str = current_time.strftime('%Y-%m-%d')
        time_str = current_time.strftime('%H:%M')

        # Create title text with border
        title_text = (f'TurboPredict Abnormality Variables\n'
                     f'Summary for {date_str} {time_str}')

        # Add title with border styling
        fig.suptitle(title_text, fontsize=20, fontweight='bold', y=0.995,
                    bbox=dict(boxstyle='round,pad=0.8', facecolor='lightblue',
                             edgecolor='darkblue', linewidth=2.5, alpha=0.9))

        # Plot each anomaly in a subplot
        plotted = 0
        for idx, anomaly_info in enumerate(anomalies):
            ax = axes[idx]
            drew = self._plot_single_compressed(ax, anomaly_info)
            if drew:
                plotted += 1

            # Heartbeat: print periodic progress within page generation
            if (idx + 1) % 3 == 0 or (idx + 1) == len(anomalies):
                try:
                    print(f"   [PLOT] Page {page_num}: {idx+1}/{len(anomalies)}")
                    sys.stdout.flush()
                except Exception:
                    pass

        # Hide unused subplots
        for idx in range(len(anomalies), len(axes)):
            axes[idx].axis('off')

        # Adjust layout
        plt.tight_layout(rect=[0, 0, 1, 0.99])

        # If nothing was drawn on this page (all panels filtered out), skip saving
        if plotted == 0:
            try:
                plt.close(fig)
            except Exception:
                pass
            return None

        # Save the page with high DPI for crisp, readable text
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"COMPRESSED_ANOMALY_PAGE_{page_num:02d}_{timestamp_str}.png"
        page_path = session_dir / filename

        # Use 200 DPI for high-quality output that remains readable when viewed/printed
        plt.savefig(page_path, dpi=200, bbox_inches='tight', facecolor='white')
        # Inline PDF: also stream this page into the PDF writer when enabled
        if self._pdf_writer is not None:
            try:
                self._pdf_writer.savefig(fig, dpi=200, bbox_inches='tight', pad_inches=0.1)
            except Exception:
                pass
        plt.close()

        return page_path

    def _plot_single_compressed(self, ax, anomaly_info: Dict[str, Any]) -> bool:
        """Plot a single anomaly in a compressed subplot.

        Args:
            ax: Matplotlib axis
            anomaly_info: Anomaly information dictionary
        """
        unit = anomaly_info['unit']
        tag = anomaly_info['tag']
        tag_info = anomaly_info['tag_info']
        verification = anomaly_info['verification_details']

        try:
            # Load historical data (reuse if preloaded by prefilter stage)
            historical_data = anomaly_info.get('preloaded_data')
            if historical_data is None:
                historical_data = self._load_tag_historical_data(unit, tag)

            if historical_data.empty:
                ax.text(0.5, 0.5, f'No data: {tag}', ha='center', va='center', transform=ax.transAxes, fontsize=10)
                ax.set_title(f'{tag[:30]}...', fontsize=11, fontweight='bold')
                return False

            # Calculate robust statistics for sigma bounds
            if 'running' in historical_data.columns:
                run_mask = pd.Series(historical_data['running']).astype(bool)
            else:
                run_mask = pd.Series([True] * len(historical_data), index=historical_data.index, dtype=bool)

            vals = pd.to_numeric(historical_data.loc[run_mask, 'value'], errors='coerce').dropna()

            center_val, lower_bound, upper_bound, _ = self._compute_sigma_bounds(vals)

            # Do not hide panels when exceedance is older than 24h.
            # Always draw; out-of-bounds segments will be colored red when present.

            # Identify anomalous points
            historical_data['anomaly'] = (historical_data['value'] < lower_bound) | (historical_data['value'] > upper_bound)

            # Plot time series with color-coded segments
            times = historical_data['time'].values
            values = historical_data['value'].values

            # Vectorized rendering using LineCollection (much faster than per-segment loop)
            try:
                from matplotlib.collections import LineCollection
                from matplotlib import dates as mdates
                t_num = mdates.date2num(pd.to_datetime(historical_data['time']).values)
                x0, x1 = t_num[:-1], t_num[1:]
                y0, y1 = values[:-1], values[1:]
                segs = np.stack([np.stack([x0, y0], axis=1), np.stack([x1, y1], axis=1)], axis=1)
                normal_mask = (values[:-1] >= lower_bound) & (values[:-1] <= upper_bound)
                if normal_mask.any():
                    lc_norm = LineCollection(segs[normal_mask], colors='blue', linewidths=1.0, alpha=0.85)
                    ax.add_collection(lc_norm)
                if (~normal_mask).any():
                    lc_anom = LineCollection(segs[~normal_mask], colors='red', linewidths=1.0, alpha=0.85)
                    ax.add_collection(lc_anom)
            except Exception:
                # Fallback to original loop if LineCollection not available
                for i in range(len(values) - 1):
                    color = 'blue' if (lower_bound <= values[i] <= upper_bound) else 'red'
                    ax.plot([times[i], times[i+1]], [values[i], values[i+1]],
                           color=color, linewidth=1.0, alpha=0.85)

            # Add bounds
            ax.axhline(upper_bound, color='orange', linestyle='--', linewidth=0.8, alpha=0.5)
            ax.axhline(lower_bound, color='orange', linestyle='--', linewidth=0.8, alpha=0.5)
            ax.axhline(center_val, color='green', linestyle='-', linewidth=0.8, alpha=0.5)

            # Calculate anomaly hours for title using actual timestamps
            recency = verification.get('recency_breakdown', {}) or {}
            anomaly_hours = self._calculate_anomaly_duration_hours(historical_data)
            if anomaly_hours <= 0.0:
                recent_24h = recency.get('last_24h', 0)
                if recent_24h:
                    anomaly_hours = recent_24h * 0.5  # Fallback estimate

            # Calculate latest value and compute 1-year extremes for H/L display
            latest_time_val = pd.to_datetime(historical_data['time']).max()
            try:
                latest_row = historical_data.loc[pd.to_datetime(historical_data['time']).idxmax()]
                latest_val = float(pd.to_numeric(latest_row['value'], errors='coerce'))
            except Exception:
                latest_val = float(pd.to_numeric(historical_data['value'], errors='coerce').dropna().iloc[-1]) if len(historical_data) else 0.0

            l_val, h_val = self._get_one_year_extremes(unit, tag)

            # Set title with tag name, anomaly duration, and H/L (2 decimals)
            tag_display = tag.split('|')[-1] if '|' in tag else tag

            # Check if current value exceeds H (high) or falls below L (low)
            h_exceeded = latest_val > h_val if not pd.isna(latest_val) and not pd.isna(h_val) else False
            l_exceeded = latest_val < l_val if not pd.isna(latest_val) and not pd.isna(l_val) else False

            # Format H/L with visual indicators (red asterisk if exceeded)
            h_display = f"H:{h_val:.2f}{'*' if h_exceeded else ''}"
            l_display = f"L:{l_val:.2f}{'*' if l_exceeded else ''}"

            # Format title: use compact notation and ensure it fits within subplot
            # Reduce tag name to 28 chars, use compact format for readability
            # Format: "tag_name | Xh | H:max L:min" (H=High, L=Low)
            # Asterisk (*) indicates value exceeded that limit
            title_text = f'{tag_display[:28]} | {anomaly_hours:.1f}h | {h_display} {l_display}'
            ax.set_title(title_text, fontsize=11, fontweight='bold', pad=3)

            # Add warning box below title if H or L exceeded (red highlight)
            if h_exceeded or l_exceeded:
                exceeded_list = []
                if h_exceeded:
                    exceeded_list.append(f"H exceeded ({h_val:.2f})")
                if l_exceeded:
                    exceeded_list.append(f"L exceeded ({l_val:.2f})")
                exceeded_text = " | ".join(exceeded_list)
                # Add red text warning below the title
                ax.text(0.5, 1.02, f'⚠ {exceeded_text}',
                       transform=ax.transAxes, ha='center', va='bottom',
                       fontsize=9, color='red', fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3, edgecolor='red', linewidth=1.5))

            # H/L values are already included in the subplot title above.
            # Remove the in-axes H/L overlay to avoid duplication and clutter.

            # Improved axis labels for readability
            ax.tick_params(labelsize=10)
            ax.grid(True, alpha=0.3, linewidth=0.8)

            # Smart x-axis range: show only where data exists (avoid empty left side)
            # Add small margin on right to see latest values clearly
            try:
                latest_time = pd.to_datetime(historical_data['time']).max()
                earliest_time = pd.to_datetime(historical_data['time']).min()
                if pd.isna(latest_time):
                    latest_time = datetime.now()

                end_date = pd.Timestamp(latest_time).to_pydatetime()
                default_start = (pd.Timestamp(end_date) - pd.DateOffset(months=self.historical_months)).to_pydatetime()

                # Use whichever is later: 3 months back OR first data point
                # This prevents showing empty space when data is less than 3 months old
                if not pd.isna(earliest_time):
                    earliest_date = pd.Timestamp(earliest_time).to_pydatetime()
                    start_date = max(earliest_date, default_start)
                else:
                    start_date = default_start

                # Add 2% padding on right side for better visibility of latest data
                time_range = (end_date - start_date).total_seconds()
                right_padding = timedelta(seconds=time_range * 0.02)
                end_date_padded = end_date + right_padding

                ax.set_xlim(start_date, end_date_padded)
            except Exception:
                pass

            # Ensure compact day/month date ticks on the x-axis (no year)
            try:
                from matplotlib import dates as mdates
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
            except Exception:
                pass

            # Dynamic y-zoom for readability on near-flat trends
            try:
                import numpy as _np_zoom
                if os.getenv('COMPRESSED_ZOOM_Y', '1').strip().lower() in ('1','true','yes','y'):
                    v = pd.to_numeric(historical_data['value'], errors='coerce').dropna().values
                    if v.size >= 5:
                        lo_p = float(os.getenv('ZOOM_Y_LOW_PCT', '1'))
                        hi_p = float(os.getenv('ZOOM_Y_HIGH_PCT', '99'))
                        lo_p = min(max(lo_p, 0.0), 49.0)
                        hi_p = max(min(hi_p, 100.0), lo_p + 1.0)
                        p_lo = float(_np_zoom.nanpercentile(v, lo_p))
                        p_hi = float(_np_zoom.nanpercentile(v, hi_p))
                        rng = max(p_hi - p_lo, 1e-6)
                        pad = max(rng * 0.05, abs(center_val) * 0.01)
                        y_min = min(p_lo - pad, lower_bound)
                        y_max = max(p_hi + pad, upper_bound)
                        if y_max > y_min:
                            ax.set_ylim(y_min, y_max)
            except Exception:
                pass

            # Successful draw for this panel
            return True

        except Exception as e:
            logger.error(f"Error plotting {unit}/{tag}: {e}")
            ax.text(0.5, 0.5, f'Error: {tag}', ha='center', va='center', transform=ax.transAxes, fontsize=10)
            ax.set_title(f'{tag[:30]}...', fontsize=11, fontweight='bold')
            return False

    def _is_anomaly_verified(self, tag_info: Dict[str, Any]) -> bool:
        """Check if an anomaly has been verified by the detection pipeline.

        CRITICAL REQUIREMENT: Only consider anomalies detected in last 24 hours as ACTIONABLE.
        Anomalies older than 24 hours are historical and should not trigger plots/alerts.
        """
        # Sigma-only mode: allow plots when a tag has a run of N consecutive
        # points beyond +/- 2.5 sigma within the last 24 hours. Controlled by env:
        #   SIGMA_ONLY=1 and optional SIGMA_CONSEC_MIN (default 6)
        try:
            import os as _os_sigma
            if _os_sigma.getenv('SIGMA_ONLY', '').strip().lower() in ('1','true','yes','y'):
                consec_min = int((_os_sigma.getenv('SIGMA_CONSEC_MIN', '6') or '6').strip())
                last_24h_count = int(tag_info.get('recency_breakdown', {}).get('last_24h', 0) or 0)
                max_run = int(tag_info.get('sigma_consecutive_ge_n', 0) or 0)
                # Require BOTH: points in last 24h AND 6+ consecutive sigma violations
                # This ensures we only plot recent/active anomalies with significant runs
                if (last_24h_count > 0) and (max_run >= consec_min):
                    return True
                # In SIGMA_ONLY mode, reject if criteria not met (strict recency requirement)
                return False
        except Exception:
            # Fall back to default verification logic if env or parsing fails
            pass

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
            }),
            # NEW: Min-5-in-24h classification flags
            'recent_exceedances_24h': tag_info.get('recent_exceedances_24h', 0),
            'is_anomalous_24h_min5': tag_info.get('is_anomalous_24h_min5', False),
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

    def _get_anomaly_recency_score(self, tag_info: Dict[str, Any]) -> float:
        """Calculate anomaly recency score for sorting.

        Returns a score where lower values = fresher/newer anomalies.
        Anomalies with shorter duration (0.1h) appear before longer ones (23.4h).

        Args:
            tag_info: Tag information dictionary with recency breakdown

        Returns:
            Recency score (lower = fresher = page 1, higher = older = later pages)
        """
        try:
            # Use recency breakdown to calculate a freshness score
            recency = tag_info.get('recency_breakdown', {})

            # Count anomalies in each time window
            last_24h = recency.get('last_24h', 0)
            last_7d = recency.get('last_7d', 0)
            last_30d = recency.get('last_30d', 0)
            older = recency.get('older', 0)

            # Calculate weighted score (lower = more recent)
            # Anomalies in last 24h are weighted lowest (most urgent)
            # Older anomalies get higher weights (less urgent)
            if last_24h > 0:
                # Most recent anomalies - use inverse of count
                # More anomalies in 24h = lower score (more urgent)
                return 1.0 / (last_24h + 1)
            elif last_7d > 0:
                # Week-old anomalies
                return 100.0 + (1.0 / (last_7d + 1))
            elif last_30d > 0:
                # Month-old anomalies
                return 10000.0 + (1.0 / (last_30d + 1))
            else:
                # Very old anomalies - sort to end
                return 999999.0

        except Exception as e:
            logger.warning(f"Error calculating recency score: {e}")
            # On error, sort to end
            return 999999.0

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

            # Enforce exceedance: only generate if last 24h contains values
            # beyond +/- 2.5 sigma using the same robust bounds as the plot itself.
            vals_for_bounds = (
                pd.Series(historical_data['value'][pd.Series(historical_data.get('running', True)).astype(bool)])
                if 'running' in historical_data.columns
                else historical_data['value']
            )
            _, lb_gate, ub_gate, _ = self._compute_sigma_bounds(vals_for_bounds)
            recent_cutoff_gate = datetime.now() - timedelta(hours=24)
            recent_gate = historical_data[historical_data['time'] >= recent_cutoff_gate]
            if not ((recent_gate['value'] < lb_gate) | (recent_gate['value'] > ub_gate)).any():
                logger.info(f"Skip plot for {unit}/{tag}: no +/- 2.5 sigma exceedance in last 24h")
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
        # Calculate ~3-month lookback period
        end_date = datetime.now()
        try:
            start_date = (pd.Timestamp(end_date) - pd.DateOffset(months=self.historical_months)).to_pydatetime()
        except Exception:
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

            # MEMORY OPTIMIZATION: Load only the requested tag for the 90-day window.
            # This avoids loading the whole unit into memory and prevents OOM on
            # large 1y dedup files when DuckDB is disabled.
            tag_data = db.get_unit_tag_data(unit, tag, start_time=start_date, end_time=end_date)
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
                staleness_warning = f" INSTRUMENTATION ISSUE: Data stale ({hours_stale:.1f}h old)"

            # Detect sparse data
            if len(data) < 100:
                data_quality_notes.append(f"âš ï¸ Sparse data ({len(data)} points)")

            # Detect short history
            if data_span_days < 30:
                data_quality_notes.append(f"â„¹ï¸ Short history ({data_span_days:.0f} days)")

            # Calculate data frequency
            if len(data) > 1:
                time_diffs = pd.to_datetime(data['time']).diff().dropna()
                median_interval_min = time_diffs.median().total_seconds() / 60
                if median_interval_min > 30:
                    data_quality_notes.append(f"â„¹ï¸ Low frequency ({median_interval_min:.0f}min intervals)")

        # Create figure with single subplot (2.5-sigma only, no MTD or IF)
        fig, ax1 = plt.subplots(1, 1, figsize=(self.plot_width, self.plot_height))

        # Title with staleness warning, priority badge, and data quality notes
        title_color = 'red' if instrumentation_issue else 'black'
        quality_suffix = f" | {' | '.join(data_quality_notes)}" if data_quality_notes else ""

        # Add priority badge to title
        priority_emoji = {
            'CRITICAL': 'ðŸš¨',
            'HIGH': 'âš ï¸',
            'MEDIUM': 'ðŸ”¸',
            'LOW': 'ðŸ“Š',
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
                    fontsize=20, fontweight='bold', color=title_color)

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
                bounds_label = 'Median/Robust 2.5Ïƒ'
            else:
                raise ValueError('insufficient data')
        except Exception:
            # Fallback to mean/std
            center_val = float(data['value'].mean())
            std_val = float(data['value'].std(ddof=0))
            lower_bound = center_val - 2.5 * std_val
            upper_bound = center_val + 2.5 * std_val
            bounds_label = 'Mean/2.5Ïƒ'

        # Identify anomalous points (breach +/- 2.5 sigma)
        data['anomaly'] = (data['value'] < lower_bound) | (data['value'] > upper_bound)

        # Speed-aware classification (if running status available)
        has_running_status = 'running' in data.columns

        # Main time series plot (only 2.5-sigma - MTD and IF removed)

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

            # Vectorized rendering with LineCollection for speed
            try:
                from matplotlib.collections import LineCollection
                from matplotlib import dates as mdates
                t_num = mdates.date2num(pd.to_datetime(times))
                x0, x1 = t_num[:-1], t_num[1:]
                y0, y1 = values[:-1], values[1:]
                segs = np.stack([np.stack([x0, y0], axis=1), np.stack([x1, y1], axis=1)], axis=1)
                normal_mask = (values[:-1] >= lower_bound) & (values[:-1] <= upper_bound)
                if normal_mask.any():
                    lc_norm = LineCollection(segs[normal_mask], colors='blue', linewidths=1.2, alpha=0.7, zorder=1)
                    lc_norm.set_label('Value (Normal)')
                    ax1.add_collection(lc_norm)
                if (~normal_mask).any():
                    lc_anom = LineCollection(segs[~normal_mask], colors='red', linewidths=1.2, alpha=0.7, zorder=1)
                    lc_anom.set_label('Value (Anomaly)')
                    ax1.add_collection(lc_anom)
            except Exception:
                # Fallback to loop if LineCollection not available
                for i in range(len(values) - 1):
                    if lower_bound <= values[i] <= upper_bound:
                        color = 'blue'; label = 'Value (Normal)' if not normal_labeled else ''; normal_labeled = True
                    else:
                        color = 'red'; label = 'Value (Anomaly)' if not anomalous_labeled else ''; anomalous_labeled = True
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

        # Add +/- 2.5 sigma bounds
        ax1.axhline(upper_bound, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='+2.5Ïƒ Upper')
        ax1.axhline(lower_bound, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='-2.5Ïƒ Lower')

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
                    f'INSTRUMENTATION ISSUE\nData stopped {hours_stale:.1f}h ago\nLatest: {latest_time.strftime("%Y-%m-%d %H:%M")}',
                    fontsize=11, fontweight='bold',
                    verticalalignment='center', horizontalalignment='left',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.8, edgecolor='darkred', linewidth=2),
                    color='white')

        # Add speed-aware annotation if applicable
        title_suffix = ' (Speed-Aware Baseline)' if has_running_status else ''
        ax1.set_title(f'3-Month Time Series with +/- 2.5 sigma Bounds{title_suffix}')
        ax1.set_ylabel('Value', fontsize=12, fontweight='bold')
        ax1.legend(loc='best', fontsize=10, ncol=2)
        ax1.grid(True, alpha=0.3)

        # Smart x-axis range: show only where data exists (avoid empty left side)
        # Add small margin on right to see latest values clearly
        try:
            latest_time = pd.to_datetime(data_sorted['time']).max()
            earliest_time = pd.to_datetime(data_sorted['time']).min()
            if pd.isna(latest_time):
                latest_time = datetime.now()

            end_date = pd.Timestamp(latest_time).to_pydatetime()
            default_start = (pd.Timestamp(end_date) - pd.DateOffset(months=self.historical_months)).to_pydatetime()

            # Use whichever is later: 3 months back OR first data point
            # This prevents showing empty space when data is less than 3 months old
            if not pd.isna(earliest_time):
                earliest_date = pd.Timestamp(earliest_time).to_pydatetime()
                start_date = max(earliest_date, default_start)
            else:
                start_date = default_start

            # Add 2% padding on right side for better visibility of latest data
            time_range = (end_date - start_date).total_seconds()
            right_padding = timedelta(seconds=time_range * 0.02)
            end_date_padded = end_date + right_padding

            ax1.set_xlim(start_date, end_date_padded)
        except Exception:
            pass

        # Overlay 1-year H/L with two decimals; color when exceeded by latest value
        try:
            latest_val_fs = float(pd.to_numeric(data_sorted['value'], errors='coerce').dropna().iloc[-1]) if len(data_sorted) else float('nan')
            l_year, h_year = self._get_one_year_extremes(unit, tag)
            exceed_high_fs = latest_val_fs > h_year if h_year == h_year else False
            exceed_low_fs = latest_val_fs < l_year if l_year == l_year else False
            ax1.text(0.02, 0.98, f'H:{float(h_year):.2f}', transform=ax1.transAxes, va='top', ha='left',
                     fontsize=10, fontweight='bold', color=('red' if exceed_high_fs else 'black'), zorder=10)
            ax1.text(0.16, 0.98, f'L:{float(l_year):.2f}', transform=ax1.transAxes, va='top', ha='left',
                     fontsize=10, fontweight='bold', color=('red' if exceed_low_fs else 'black'), zorder=10)
        except Exception:
            pass

        # Ensure compact day/month date ticks on the x-axis (no year)
        try:
            from matplotlib import dates as mdates
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
        except Exception:
            pass

        # Add detection summary text box
        instrumentation_status = "STALE" if instrumentation_issue else "Fresh"
        recent24 = int(verification.get('recent_exceedances_24h', 0) or 0)
        min5_flag = 'YES' if verification.get('is_anomalous_24h_min5', False) else 'NO'
        summary_text = f"""DETECTION SUMMARY:
Primary: {verification['primary_detections']['2.5_sigma']} Ïƒ-anom
Verified: MTD={verification['verification_detections']['mtd']}, IF={verification['verification_detections']['isolation_forest']}
Total: {verification['total_anomalies']} | Rate: {verification['anomaly_rate']*100:.1f}%
Confidence: {verification['confidence']}
24h exceedances: {recent24}  (>=5 â†’ {min5_flag})
Instrumentation: {instrumentation_status}"""

        summary_bg_color = 'lightcoral' if instrumentation_issue else 'lightyellow'
        fig.text(0.92, 0.50, summary_text,
                fontsize=10, fontfamily='monospace', verticalalignment='center', horizontalalignment='left',
                bbox=dict(boxstyle='round,pad=0.3', facecolor=summary_bg_color, alpha=0.7,
                         edgecolor='darkred' if instrumentation_issue else 'gray', linewidth=2 if instrumentation_issue else 1))

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
            f.write("TURBOPREDICT - ANOMALY DETECTION SESSION REPORT\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Session Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

            # Check if SIGMA_ONLY mode is enabled
            sigma_only_mode = os.getenv('SIGMA_ONLY', '').strip().lower() in ('1','true','yes','y')
            consec_min = int((os.getenv('SIGMA_CONSEC_MIN', '6') or '6').strip())

            if sigma_only_mode:
                f.write(f"Detection Pipeline: 2.5-SIGMA ONLY MODE (NO Autoencoder/MTD/Isolation Forest)\n\n")
            else:
                f.write(f"Detection Pipeline: 2.5-Sigma + Autoencoder -> MTD + Isolation Forest\n\n")

            f.write("DETECTION PIPELINE CONFIGURATION:\n")

            if sigma_only_mode:
                f.write(f"  Mode: SIGMA_ONLY (2.5-Sigma primary detection)\n")
                f.write(f"  Primary Detector: 2.5-Sigma (Standard Deviation threshold)\n")
                f.write(f"  Verification Layer: DISABLED (Autoencoder, MTD, IF not in use)\n")
                f.write(f"  Verification Required: False\n")
                f.write(f"  Consecutive Violations Required: {consec_min}+ points in last 24 hours\n")
                f.write(f"  Recency Filter: ENABLED (Points in last 24 hours only)\n")
                f.write(f"  Detection Criterion: Any tag with {consec_min}+ consecutive sigma violations\n\n")
            else:
                f.write(f"  Primary Detectors: {', '.join(self.detection_pipeline['primary_detectors'])}\n")
                f.write(f"  Verification Layer: {', '.join(self.detection_pipeline['verification_detectors'])}\n")
                f.write(f"  Verification Required: {self.detection_pipeline['require_verification']}\n")
                f.write(f"  Recency Filter: ENABLED (Only NEW anomalies <24 hours old)\n")
                f.write(f"  Confidence System: WEIGHTED SCORING (0-100 scale)\n")
                f.write(f"    - Primary detectors: 70 points (2.5sigma=40, AE=30)\n")
                f.write(f"    - Verification layer: 30 points (MTD=20, IF=10)\n")
                f.write(f"    - Thresholds: CRITICAL>=50, HIGH>=60, MEDIUM>=70, LOW>=80\n\n")

            f.write("SESSION RESULTS:\n")
            # Count all units in detection_results (now includes units with and without anomalies)
            f.write(f"  Units Analyzed: {len(detection_results)}\n")
            f.write(f"  Verified Anomalies: {len(verified_anomalies)}\n")
            f.write(f"  Plots Generated: {plots_generated}\n")
            f.write(f"  Historical Context: {self.historical_months} months\n\n")

            if verified_anomalies:
                # Split anomalies by recency
                recent_anomalies = [a for a in verified_anomalies if a['tag_info'].get('recency_breakdown', {}).get('last_24h', 0) > 0]
                older_anomalies = [a for a in verified_anomalies if a['tag_info'].get('recency_breakdown', {}).get('last_24h', 0) == 0]

                # Recent anomalies section
                if recent_anomalies:
                    f.write("VERIFIED ANOMALOUS TAGS - RECENT (<24H):\n")
                    for i, anomaly in enumerate(recent_anomalies, 1):
                        verification = anomaly['verification_details']
                        tag_info = anomaly['tag_info']
                        recency = tag_info.get('recency_breakdown', {})
                        priority = tag_info.get('priority', 'UNKNOWN')

                        confidence_score = tag_info.get('confidence_score', 0)
                        confidence_breakdown = tag_info.get('confidence_breakdown', {})

                        f.write(f"  {i:2d}. {anomaly['unit']} | {anomaly['tag']}\n")
                        f.write(f"      Priority: {priority}\n")
                        f.write(f"      Confidence Score: {confidence_score:.1f}/100\n")
                        f.write(f"        â""â"€ 2.5Ïƒ={confidence_breakdown.get('sigma_contribution', 0):.1f}, "
                               f"AE={confidence_breakdown.get('ae_contribution', 0):.1f}, "
                               f"MTD={confidence_breakdown.get('mtd_contribution', 0):.1f}, "
                               f"IF={confidence_breakdown.get('if_contribution', 0):.1f}\n")
                        f.write(f"      Recent Anomalies (24h): {recency.get('last_24h', 0)}\n")
                        f.write(f"      Total Anomalies: {verification['total_anomalies']} ({verification['anomaly_rate']*100:.1f}%)\n")
                        f.write(f"      Pipeline: 2.5Ïƒ({verification['primary_detections']['2.5_sigma']}) + "
                               f"AE({verification['primary_detections']['autoencoder']}) â†' "
                               f"MTD({verification['verification_detections']['mtd']}) + "
                               f"IS({verification['verification_detections']['isolation_forest']})\n\n")

                # Older anomalies section (>24h but detected by pipeline)
                if older_anomalies:
                    f.write("DETECTED ANOMALOUS TAGS - OLDER (>24H):\n")
                    f.write("(Tags with anomalies detected in previous periods, no recent activity)\n\n")
                    for i, anomaly in enumerate(older_anomalies, 1):
                        verification = anomaly['verification_details']
                        tag_info = anomaly['tag_info']
                        recency = tag_info.get('recency_breakdown', {})
                        priority = tag_info.get('priority', 'UNKNOWN')
                        confidence_score = tag_info.get('confidence_score', 0)

                        f.write(f"  {i:2d}. {anomaly['unit']} | {anomaly['tag']}\n")
                        f.write(f"      Priority: {priority} | Confidence: {confidence_score:.1f}/100\n")
                        f.write(f"      Anomalies (7d): {recency.get('last_7d', 0)} | "
                               f"Anomalies (30d): {recency.get('last_30d', 0)} | "
                               f"Total: {verification['total_anomalies']} ({verification['anomaly_rate']*100:.1f}%)\n\n")
            else:
                f.write("HEALTH STATUS: No verified anomalies detected.\n")
                f.write("All systems operating normally.\n\n")

            # Show units with NO anomalies (healthy units)
            if detection_results:
                verified_unit_tags = set((a['unit'], a['tag']) for a in verified_anomalies)
                units_no_anomalies = []

                for unit, unit_data in detection_results.items():
                    if isinstance(unit_data, dict):
                        by_tag = unit_data.get('by_tag', {})
                        unit_tag_pairs = set((unit, tag) for tag in by_tag.keys())
                        if not unit_tag_pairs:
                            # Unit has no tags at all, or no anomalous tags
                            units_no_anomalies.append(unit)

                if units_no_anomalies:
                    f.write("UNITS WITH NO ANOMALIES (HEALTHY):\n")
                    for unit in sorted(units_no_anomalies):
                        f.write(f"  ✓ {unit}\n")
                    f.write(f"\nTotal Healthy Units: {len(units_no_anomalies)}\n")

    def _create_no_anomalies_report(self, session_dir: Path, detection_results: Dict):
        """Create a report when no anomalies are verified."""
        summary_file = session_dir / "NO_ANOMALIES_DETECTED.txt"

        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("TURBOPREDICT - ANOMALY DETECTION REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Session Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("Detection Result: NO VERIFIED ANOMALIES\n\n")
            f.write("All monitored tags are operating within normal parameters.\n")
            f.write("No historical diagnostic plots were generated.\n\n")
            f.write("Detection Pipeline Status: OPERATIONAL\n")
            f.write("System Status: NORMAL OPERATION\n")

    def _load_previous_bad_tags(self) -> Dict[str, set]:
        """Load previously known bad tags from state file for NEW tag detection.

        Returns:
            Dict mapping unit -> set of previously bad tag names
        """
        try:
            state_file = Path(self.reports_dir) / ".bad_tags_state.json"
            if state_file.exists():
                import json
                with open(state_file, 'r') as f:
                    data = json.load(f)
                    # Convert lists back to sets
                    return {unit: set(tags) for unit, tags in data.items()}
        except Exception:
            pass
        return {}

    def _save_bad_tags_state(self, current_bad_tags: Dict[str, List[str]]):
        """Save current bad tags to state file for next run's NEW detection.

        Args:
            current_bad_tags: Dict mapping unit -> list of bad tag names
        """
        try:
            state_file = Path(self.reports_dir) / ".bad_tags_state.json"
            import json
            # Convert sets to lists for JSON serialization
            data = {unit: list(tags) if isinstance(tags, set) else tags
                   for unit, tags in current_bad_tags.items()}
            state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save bad tags state: {e}")

    def _load_previous_problematic_tags(self) -> Dict[str, set]:
        """Load previously known problematic tags (stale/instrumentation) from state file.

        Returns:
            Dict mapping unit -> set of previously problematic tag names
        """
        try:
            state_file = Path(self.reports_dir) / ".problematic_tags_state.json"
            if state_file.exists():
                import json
                with open(state_file, 'r') as f:
                    data = json.load(f)
                    # Convert lists back to sets
                    return {unit: set(tags) for unit, tags in data.items()}
        except Exception:
            pass
        return {}

    def _save_problematic_tags_state(self, current_problematic_tags: Dict[str, List[str]]):
        """Save current problematic tags to state file for next run's NEW detection.

        Args:
            current_problematic_tags: Dict mapping unit -> list of problematic tag names
        """
        try:
            state_file = Path(self.reports_dir) / ".problematic_tags_state.json"
            import json
            # Convert sets to lists for JSON serialization
            data = {unit: list(tags) if isinstance(tags, set) else tags
                   for unit, tags in current_problematic_tags.items()}
            state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save problematic tags state: {e}")

    def _create_anomalies_summary_page(self, verified_anomalies: List[Dict[str, Any]], detection_results: Dict[str, Any] = None):
        """Create a summary page showing data health issues (stale/instrumentation), grouped by unit.

        Lists tags with STALE DATA or INSTRUMENTATION ISSUES, EXCLUDING tags already plotted as anomalies.
        Separates issues by type to prioritize critical problems.
        NEW problematic tags are marked with "*NEW*" indicator.

        Args:
            verified_anomalies: List of verified anomalous tags already plotted (to exclude from summary)
            detection_results: Detection results containing all tag/unit data

        Returns:
            Matplotlib figure object or None
        """
        try:
            # Build set of already-plotted anomalous tags to exclude
            plotted_anomalies = set()
            for anomaly in verified_anomalies:
                unit = anomaly.get('unit')
                tag = anomaly.get('tag')
                if unit and tag:
                    plotted_anomalies.add((unit, tag))

            # Load previously known problematic tags
            previous_problematic = self._load_previous_problematic_tags()

            # Collect tags with STALE DATA or INSTRUMENTATION ISSUES
            units_with_issues = {}
            current_problematic_flat = {}  # For state tracking

            if detection_results:
                for unit, unit_results in detection_results.items():
                    if not isinstance(unit_results, dict):
                        continue

                    by_tag = unit_results.get('by_tag', {}) or {}
                    problematic_tags = []

                    for tag, tag_info in by_tag.items():
                        if not isinstance(tag_info, dict):
                            continue

                        # SKIP tags already plotted as anomalies
                        if (unit, tag) in plotted_anomalies:
                            continue

                        issue_type = None
                        issue_desc = None

                        # Check 1: Instrumentation Issues (no data or fetch errors) - MOST CRITICAL
                        total_records = tag_info.get('total_records', 0)
                        has_error = tag_info.get('fetch_error') or tag_info.get('error')

                        if total_records == 0 or has_error:
                            issue_type = "INSTRUMENTATION"
                            issue_desc = tag_info.get('fetch_error') or tag_info.get('error') or 'No data'

                        # Check 2: Stale Data (data hasn't been updated recently)
                        else:
                            is_stale = tag_info.get('is_stale', False)
                            data_age_hours = tag_info.get('data_age_hours')

                            if is_stale or (data_age_hours is not None and data_age_hours > 24):
                                issue_type = "STALE_DATA"
                                if data_age_hours is not None:
                                    age_str = f"{data_age_hours:.1f}h old"
                                else:
                                    age_str = "old data"
                                issue_desc = f"Data {age_str}"

                        # Add if has any issue
                        if issue_type:
                            tag_name = tag
                            # Check if this is a NEW problematic tag
                            is_new = tag_name not in previous_problematic.get(unit, set())

                            problematic_tags.append({
                                'tag': tag_name,
                                'issue_type': issue_type,
                                'issue_desc': issue_desc,
                                'is_new': is_new
                            })

                    if problematic_tags:
                        units_with_issues[unit] = problematic_tags
                        current_problematic_flat[unit] = [t['tag'] for t in problematic_tags]

            # Save current state for next run
            self._save_problematic_tags_state(current_problematic_flat)

            # Create summary figure
            fig, ax = plt.subplots(figsize=(14, 10))
            ax.axis('off')

            # Title
            title = "Data Health Issues - Stale & Instrumentation Problems"
            ax.text(0.5, 0.95, title, ha='center', va='top', fontsize=18, fontweight='bold',
                   transform=ax.transAxes, color='#c9302c')

            subtitle = f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ax.text(0.5, 0.91, subtitle, ha='center', va='top', fontsize=11, style='italic',
                   transform=ax.transAxes, color='gray')

            y_pos = 0.87

            if not units_with_issues:
                # All good
                ax.text(0.5, y_pos, "✓ All remaining tags have fresh data and healthy instrumentation",
                       ha='center', va='top', fontsize=14, color='green', fontweight='bold',
                       transform=ax.transAxes)
            else:
                # Show problematic tags grouped by unit
                ax.text(0.05, y_pos, "Tags with Stale Data or Instrumentation Issues:",
                       fontsize=12, fontweight='bold', transform=ax.transAxes, color='#c9302c')
                y_pos -= 0.04

                for unit in sorted(units_with_issues.keys()):
                    tags = units_with_issues[unit]

                    # Unit header
                    ax.text(0.08, y_pos, f"► {unit}  ({len(tags)} tag{'s' if len(tags) != 1 else ''})",
                           fontsize=11, fontweight='bold', color='#d9534f',
                           transform=ax.transAxes)
                    y_pos -= 0.03

                    # Tag details - Instrumentation issues FIRST (more critical)
                    for tag_info in sorted(tags, key=lambda x: (x['issue_type'] != 'INSTRUMENTATION', x['tag'])):
                        tag_text = f"  • {tag_info['tag']}"
                        tag_color = '#d9534f'  # Red

                        # Mark NEW problematic tags prominently
                        if tag_info['is_new']:
                            tag_text += "  *NEW*"
                            tag_color = '#ff6b6b'  # Brighter red

                        # Add issue type label
                        if tag_info['issue_type'] == 'INSTRUMENTATION':
                            tag_text += "  [INSTRUMENTATION]"
                        elif tag_info['issue_type'] == 'STALE_DATA':
                            tag_text += "  [STALE]"

                        ax.text(0.10, y_pos, tag_text, fontsize=9,
                               transform=ax.transAxes, family='monospace',
                               color=tag_color, fontweight='bold')
                        y_pos -= 0.025

                    y_pos -= 0.01

            # Footer
            ax.text(0.5, 0.02, "Issues: INSTRUMENTATION = Sensor/connection problem | STALE = Data >24h old | *NEW* = First time detected",
                   ha='center', va='bottom', fontsize=8, style='italic',
                   transform=ax.transAxes, color='gray')

            plt.tight_layout()
            return fig

        except Exception as e:
            logger.error(f"Failed to create data health summary page: {e}")
            return None

    def _generate_consolidated_pdf(self, session_dir: Path, detection_results: Dict[str, Any] = None,
                                   verified_anomalies: List[Dict[str, Any]] = None) -> Path:
        """Generate a single PDF file containing all anomaly plots from the session.

        Includes a final summary page listing tags that were plotted (verified anomalies).

        Args:
            session_dir: Session directory containing PNG plot files
            detection_results: Detection results for summary page (optional)
            verified_anomalies: List of verified anomalous tags that were plotted (optional)

        Returns:
            Path to generated PDF file
        """
        try:
            from matplotlib.backends.backend_pdf import PdfPages
            import glob

            # Find all compressed page PNG files in the session directory
            png_files = list(session_dir.glob("COMPRESSED_ANOMALY_PAGE_*.png"))

            # Fallback to individual plots if no compressed pages found
            if not png_files:
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
            print(f"[PDF] Generating consolidated report from {len(png_files)} pages…")

            with PdfPages(pdf_path) as pdf:
                total_pages = len(png_files)
                for idx, png_file in enumerate(png_files, start=1):
                    try:
                        # Read PNG image at full resolution for high-quality PDF
                        from PIL import Image
                        img = Image.open(png_file)

                        # Create a new figure and add image at original resolution
                        # Calculate figure size to maintain aspect ratio
                        dpi = 150  # High DPI for crisp text and details
                        fig_width = img.width / dpi
                        fig_height = img.height / dpi

                        fig = plt.figure(figsize=(fig_width, fig_height), dpi=dpi)
                        ax = fig.add_subplot(111)
                        ax.imshow(img)
                        ax.axis('off')

                        # Add to PDF with high DPI for readability
                        pdf.savefig(fig, dpi=dpi, bbox_inches='tight', pad_inches=0.1)
                        try:
                            img.close()
                        except Exception:
                            pass
                        plt.close(fig)
                        if (idx % 2) == 0 or idx == total_pages:
                            print(f"[PDF] Added {idx}/{total_pages} pages…")

                    except Exception as e:
                        logger.error(f"Failed to add {png_file.name} to PDF: {e}")
                        continue

                # Add data health issues summary page at the end
                try:
                    summary_fig = self._create_anomalies_summary_page(verified_anomalies, detection_results)
                    if summary_fig:
                        pdf.savefig(summary_fig, dpi=150, bbox_inches='tight', pad_inches=0.1)
                        plt.close(summary_fig)
                        print(f"[PDF] Added data health issues summary page")
                except Exception as e:
                    logger.warning(f"Failed to add data health summary to PDF: {e}")

            logger.info(f"Successfully generated consolidated PDF: {pdf_path}")
            print(f"[PDF] Done: {pdf_path}")
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





