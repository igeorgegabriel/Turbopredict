#!/usr/bin/env python3
"""
Plot Generation Controls for TURBOPREDICT X PROTEAN
Prevents excessive plot generation and manages report disk usage
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class PlotController:
    """Controls plot generation to prevent disk space exhaustion."""

    def __init__(self, max_plots_per_unit: int = 5, max_units_per_report: int = 8):
        """Initialize plot controller with sensible limits.

        Args:
            max_plots_per_unit: Maximum plots to generate per unit (default: 5)
            max_units_per_report: Maximum units to analyze per report (default: 8)
        """
        self.max_plots_per_unit = max_plots_per_unit
        self.max_units_per_report = max_units_per_report
        self.max_total_plots = max_plots_per_unit * max_units_per_report  # 40 plots max

        # Report cleanup thresholds
        self.max_report_dirs = 5  # Keep only 5 most recent report directories
        self.max_report_age_days = 7  # Delete reports older than 7 days
        self.max_total_report_size_mb = 200  # Alert if total reports > 200MB

    def should_create_plot(self, tag: str, tag_info: Dict[str, Any], priority: int = 0) -> bool:
        """Determine if a plot should be created for this tag.

        Args:
            tag: Tag name
            tag_info: Tag anomaly information
            priority: Tag priority (0=highest, higher numbers=lower priority)

        Returns:
            True if plot should be created
        """
        # Only plot high-priority, high-anomaly tags
        anomaly_count = tag_info.get('count', 0)
        anomaly_rate = tag_info.get('rate', 0)

        # Criteria for plotting:
        # 1. High anomaly count (>10 anomalies)
        # 2. High anomaly rate (>5%)
        # 3. High priority (top N tags)

        if priority >= self.max_plots_per_unit:
            return False

        if anomaly_count >= 10 or anomaly_rate >= 0.05:
            return True

        # Plot at least the top 2 problematic tags even if low counts
        if priority < 2 and anomaly_count > 0:
            return True

        return False

    def filter_units_for_analysis(self, all_units: List[str]) -> List[str]:
        """Filter units to analyze based on priority.

        Args:
            all_units: List of all available units

        Returns:
            Filtered list of units to analyze
        """
        # Priority units (most critical systems)
        priority_units = ['K-31-01', 'K-19-01', 'K-16-01', 'K-12-01']

        # Filter to priority units that exist in the data
        filtered_units = [unit for unit in priority_units if unit in all_units]

        # Add remaining units up to limit
        remaining_units = [unit for unit in all_units if unit not in priority_units]
        remaining_slots = self.max_units_per_report - len(filtered_units)

        if remaining_slots > 0:
            filtered_units.extend(remaining_units[:remaining_slots])

        logger.info(f"Unit filtering: {len(all_units)} available -> {len(filtered_units)} selected")
        return filtered_units

    def filter_tags_for_plotting(self, by_tag: Dict[str, Dict], unit: str) -> List[Tuple[str, Dict]]:
        """Filter and prioritize tags for plotting.

        Args:
            by_tag: Dictionary of tag anomaly information
            unit: Unit name for context

        Returns:
            List of (tag, tag_info) tuples to plot, in priority order
        """
        if not by_tag:
            return []

        # Sort by anomaly significance
        def tag_score(tag_info):
            count = tag_info.get('count', 0)
            rate = tag_info.get('rate', 0)
            confidence = tag_info.get('confidence', 'LOW')

            # Scoring system
            score = count * 10  # Base score from anomaly count
            score += rate * 1000  # Rate is very important

            # Confidence multipliers
            if confidence == 'HIGH':
                score *= 1.5
            elif confidence == 'MEDIUM':
                score *= 1.2

            return score

        # Sort by score descending
        sorted_tags = sorted(by_tag.items(), key=lambda x: tag_score(x[1]), reverse=True)

        # Filter based on plotting criteria
        filtered_tags = []
        for i, (tag, tag_info) in enumerate(sorted_tags):
            if self.should_create_plot(tag, tag_info, priority=i):
                filtered_tags.append((tag, tag_info))

        logger.info(f"Tag filtering for {unit}: {len(by_tag)} problematic -> {len(filtered_tags)} selected for plots")
        return filtered_tags

    def cleanup_old_reports(self, reports_dir: Path) -> Dict[str, Any]:
        """Clean up old report directories to prevent disk exhaustion.

        Args:
            reports_dir: Path to reports directory

        Returns:
            Dictionary with cleanup statistics
        """
        if not reports_dir.exists():
            return {"cleaned": 0, "space_reclaimed_mb": 0}

        stats = {"cleaned": 0, "space_reclaimed_mb": 0, "errors": []}

        # Get all report directories sorted by age
        report_dirs = [d for d in reports_dir.iterdir() if d.is_dir()]
        report_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)  # Newest first

        cutoff_date = datetime.now() - timedelta(days=self.max_report_age_days)

        # Keep only recent directories, remove old ones
        for i, report_dir in enumerate(report_dirs):
            try:
                should_delete = False
                dir_mtime = datetime.fromtimestamp(report_dir.stat().st_mtime)

                # Delete if too old
                if dir_mtime < cutoff_date:
                    should_delete = True
                    reason = f"older than {self.max_report_age_days} days"

                # Delete if beyond max count (keep newest)
                elif i >= self.max_report_dirs:
                    should_delete = True
                    reason = f"beyond max {self.max_report_dirs} directories"

                if should_delete:
                    # Calculate size before deletion
                    dir_size = sum(f.stat().st_size for f in report_dir.glob('**/*') if f.is_file())

                    # Remove directory
                    import shutil
                    shutil.rmtree(report_dir)

                    stats["cleaned"] += 1
                    stats["space_reclaimed_mb"] += dir_size / (1024 * 1024)

                    logger.info(f"Cleaned report directory: {report_dir.name} ({reason})")

            except Exception as e:
                error_msg = f"Failed to clean {report_dir.name}: {e}"
                stats["errors"].append(error_msg)
                logger.error(error_msg)

        return stats

    def check_disk_usage_alert(self, reports_dir: Path) -> bool:
        """Check if report disk usage exceeds thresholds.

        Args:
            reports_dir: Path to reports directory

        Returns:
            True if usage is excessive and cleanup is recommended
        """
        if not reports_dir.exists():
            return False

        try:
            # Calculate total size of reports
            total_size = sum(f.stat().st_size for f in reports_dir.glob('**/*') if f.is_file())
            total_size_mb = total_size / (1024 * 1024)

            # Count total files
            total_files = len([f for f in reports_dir.glob('**/*') if f.is_file()])

            logger.info(f"Report disk usage: {total_files} files, {total_size_mb:.1f}MB")

            if total_size_mb > self.max_total_report_size_mb:
                logger.warning(f"Report disk usage ({total_size_mb:.1f}MB) exceeds threshold ({self.max_total_report_size_mb}MB)")
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to check disk usage: {e}")
            return False


# Global instance for easy access
plot_controller = PlotController()


def create_controlled_report(analysis_function, reports_dir: Path, **kwargs) -> Path:
    """Create a report with automatic plot controls and cleanup.

    Args:
        analysis_function: Function that creates the analysis/plots
        reports_dir: Directory for reports
        **kwargs: Arguments to pass to analysis function

    Returns:
        Path to created report directory
    """
    # Pre-cleanup if needed
    if plot_controller.check_disk_usage_alert(reports_dir):
        logger.info("Excessive disk usage detected, running cleanup...")
        cleanup_stats = plot_controller.cleanup_old_reports(reports_dir)
        if cleanup_stats["cleaned"] > 0:
            logger.info(f"Cleaned {cleanup_stats['cleaned']} old reports, "
                       f"reclaimed {cleanup_stats['space_reclaimed_mb']:.1f}MB")

    # Run analysis with controls
    report_path = analysis_function(controller=plot_controller, **kwargs)

    # Post-cleanup
    plot_controller.cleanup_old_reports(reports_dir)

    return report_path


# ---- Shared helpers for report folder naming ---------------------------------
def _safe_unit_name(unit: str) -> str:
    """Return a filesystem-safe unit folder name."""
    keep = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    return "".join(c if c in keep else "_" for c in (unit or "")).strip("_") or "unit"


def build_scan_root_dir(base_reports_dir: Path | None = None, *, when: datetime | None = None) -> Path:
    """Return the master scan folder path using a day/session hierarchy.

    Requested structure:
      reports/DD-MM-YYYY/<SessionEndTime>
        <UNIT>/...

    - Date folder: "DD-MM-YYYY"
    - Session folder: hour-only in 12h with AM/PM (e.g., "2PM").

    Args:
        base_reports_dir: Base reports directory (defaults to "reports")
        when: Optional datetime for deterministic naming; defaults to now()

    Returns:
        Path to the session directory (not created): reports/<date>/<time>
    """
    base = Path(base_reports_dir) if base_reports_dir else Path("reports")
    dt = when or datetime.now()
    date_folder = dt.strftime("%d-%m-%Y")
    time_folder = dt.strftime("%I%p").lstrip("0")  # e.g., "2PM"
    return base / date_folder / time_folder


def ensure_unit_dir(root: Path, unit: str) -> Path:
    """Create and return a unit subdirectory under the given scan root."""
    u = _safe_unit_name(unit)
    path = root / u
    path.mkdir(parents=True, exist_ok=True)
    return path
