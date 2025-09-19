"""Auto-scan functionality for TURBOPREDICT X PROTEAN - intelligent PI data fetching."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
import logging
from datetime import datetime, timedelta

from .database import LocalDatabase
from .batch import build_unit_from_tags, read_tags_from_sheet
from .config import Config
from .ingest import load_latest_frame, write_parquet
from .anomaly import add_anomalies
from .plotting import plot_anomalies
from .emailer import send_email

logger = logging.getLogger(__name__)


class AutoScanner:
    """Intelligent auto-scanner for PI data with local database optimization."""
    
    def __init__(self, config: Config, db_path: Optional[Path] = None):
        """Initialize auto-scanner.
        
        Args:
            config: Configuration object
            db_path: Optional custom database path
        """
        self.config = config
        if db_path is None:
            db_path = config.paths.processed_dir / "turbopredict_local.sqlite"
        self.db = LocalDatabase(db_path)
        
    def auto_scan_tags(self, 
                      tags: List[str],
                      xlsx_path: Path,
                      plant: str,
                      unit: str,
                      *,
                      max_age_hours: float = 1.0,
                      server: str = "\\\\PTSG-1MMPDPdb01",
                      start: str = "-24h",
                      end: str = "*",
                      step: str = "-6min",
                      force_refresh: bool = False,
                      batch_size: int = 10) -> Dict[str, Any]:
        """Auto-scan tags: check local database freshness and fetch from PI if needed.
        
        Args:
            tags: List of PI tags to scan
            xlsx_path: Path to Excel workbook for PI DataLink
            plant: Plant identifier
            unit: Unit identifier
            max_age_hours: Maximum data age before fetching from PI
            server: PI server path
            start: Start time for PI fetch
            end: End time for PI fetch
            step: Time step for PI fetch
            force_refresh: Force refresh even if data is fresh
            batch_size: Number of tags to process in each batch
            
        Returns:
            Dictionary with scan results and statistics
        """
        results = {
            'total_tags': len(tags),
            'fetched_from_pi': [],
            'used_local_cache': [],
            'failed_tags': [],
            'processing_stats': {},
            'scan_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Starting auto-scan for {len(tags)} tags")
        
        # Analyze freshness for all tags
        freshness_info = {}
        tags_needing_fetch = []
        
        for tag in tags:
            info = self.db.get_data_freshness_info(tag, plant, unit)
            freshness_info[tag] = info
            
            should_fetch = force_refresh or self.db.should_fetch_from_pi(
                tag, plant, unit, max_age_hours
            )
            
            if should_fetch:
                tags_needing_fetch.append(tag)
            else:
                results['used_local_cache'].append(tag)
        
        logger.info(f"Found {len(tags_needing_fetch)} tags needing PI fetch")
        logger.info(f"Using local cache for {len(results['used_local_cache'])} tags")
        
        # Fetch data in batches for tags that need updates
        if tags_needing_fetch:
            results['fetched_from_pi'] = self._fetch_tags_in_batches(
                tags_needing_fetch, xlsx_path, plant, unit, server, 
                start, end, step, batch_size, results['failed_tags']
            )
        
        # Compile processing statistics
        results['processing_stats'] = {
            'total_processed': len(tags),
            'from_pi': len(results['fetched_from_pi']),
            'from_cache': len(results['used_local_cache']),
            'failed': len(results['failed_tags']),
            'success_rate': (len(tags) - len(results['failed_tags'])) / len(tags) if tags else 0,
            'freshness_info': {tag: info for tag, info in freshness_info.items() if info['is_stale']},
        }
        
        logger.info(f"Auto-scan complete: {results['processing_stats']['from_pi']} from PI, "
                   f"{results['processing_stats']['from_cache']} from cache, "
                   f"{results['processing_stats']['failed']} failed")
        
        return results
    
    def _fetch_tags_in_batches(self, tags: List[str], xlsx_path: Path, plant: str, unit: str,
                              server: str, start: str, end: str, step: str, 
                              batch_size: int, failed_tags: List[str]) -> List[str]:
        """Fetch tags from PI in batches to optimize Excel/DataLink operations."""
        fetched_tags = []
        
        for i in range(0, len(tags), batch_size):
            batch = tags[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} tags")
            
            try:
                # Use existing batch processing from batch.py
                temp_parquet = self.config.paths.processed_dir / f"temp_batch_{i//batch_size}.parquet"
                
                build_unit_from_tags(
                    xlsx_path, batch, temp_parquet,
                    plant=plant, unit=unit, server=server,
                    start=start, end=end, step=step,
                    settle_seconds=2.0, visible=False
                )
                
                # Load the fetched data and store in local database
                if temp_parquet.exists():
                    df = pd.read_parquet(temp_parquet)
                    if not df.empty:
                        # Store in local database for each tag
                        for tag in batch:
                            tag_data = df[df['tag'] == tag.replace('.', '_').replace(' ', '_')]
                            if not tag_data.empty:
                                self.db.store_dataframe(tag_data, tag, plant, unit)
                                fetched_tags.append(tag)
                                logger.debug(f"Stored {len(tag_data)} records for {tag}")
                    
                    # Clean up temp file
                    temp_parquet.unlink(missing_ok=True)
                
            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                failed_tags.extend(batch)
        
        return fetched_tags
    
    def get_combined_data(self, tag: str, plant: str | None = None, unit: str | None = None,
                         hours_back: int = 24) -> pd.DataFrame:
        """Get combined data from local database with optional recent PI fetch.
        
        Args:
            tag: PI tag name
            plant: Plant identifier
            unit: Unit identifier  
            hours_back: Hours of historical data to include
            
        Returns:
            Combined DataFrame with time series data
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Get local data
        df = self.db.get_local_data(tag, plant, unit, start_time, end_time)
        
        if df.empty:
            logger.warning(f"No local data found for {tag}")
        else:
            logger.info(f"Retrieved {len(df)} records from local database for {tag}")
        
        return df
    
    def auto_scan_and_analyze(self, 
                             tags: List[str],
                             xlsx_path: Path,
                             plant: str,
                             unit: str,
                             *,
                             max_age_hours: float = 1.0,
                             run_anomaly_detection: bool = True,
                             generate_plots: bool = True,
                             send_notifications: bool = True) -> Dict[str, Any]:
        """Complete auto-scan with analysis pipeline.
        
        Args:
            tags: List of PI tags to process
            xlsx_path: Excel workbook path
            plant: Plant identifier
            unit: Unit identifier
            max_age_hours: Maximum data age before PI fetch
            run_anomaly_detection: Whether to run anomaly detection
            generate_plots: Whether to generate plots
            send_notifications: Whether to send email notifications
            
        Returns:
            Complete analysis results
        """
        logger.info("Starting complete auto-scan and analysis pipeline")
        
        # Step 1: Auto-scan tags
        scan_results = self.auto_scan_tags(
            tags, xlsx_path, plant, unit, max_age_hours=max_age_hours
        )
        
        # Step 2: Analyze each tag
        analysis_results = {}
        all_alerts = []
        
        for tag in tags:
            if tag not in scan_results['failed_tags']:
                try:
                    # Get combined data
                    df = self.get_combined_data(tag, plant, unit)
                    
                    if not df.empty:
                        # Run anomaly detection if requested
                        if run_anomaly_detection:
                            df = add_anomalies(df, roll=self.config.roll, drop_pct=self.config.drop_pct)
                            alerts = df[df.get('alert', False) == True] if 'alert' in df.columns else pd.DataFrame()
                            all_alerts.append(alerts)
                        
                        analysis_results[tag] = {
                            'record_count': len(df),
                            'latest_timestamp': df['time'].max().isoformat() if not df.empty else None,
                            'alerts_count': len(alerts) if 'alerts' in locals() else 0,
                            'data_quality': 'good' if len(df) > 10 else 'sparse'
                        }
                        
                        # Generate plots if requested
                        if generate_plots:
                            plot_path = self.config.paths.reports_dir / f"{plant}_{unit}_{tag.replace('.', '_')}.png"
                            try:
                                plot_anomalies(df, save_to=plot_path, show=False)
                                analysis_results[tag]['plot_path'] = str(plot_path)
                            except Exception as e:
                                logger.warning(f"Failed to generate plot for {tag}: {e}")
                        
                except Exception as e:
                    logger.error(f"Failed to analyze {tag}: {e}")
                    scan_results['failed_tags'].append(tag)
        
        # Step 3: Send notifications if requested
        if send_notifications and any(analysis_results.values()):
            try:
                self._send_analysis_notification(scan_results, analysis_results, all_alerts)
            except Exception as e:
                logger.error(f"Failed to send notifications: {e}")
        
        # Combine results
        complete_results = {
            'scan_results': scan_results,
            'analysis_results': analysis_results,
            'total_alerts': sum(len(alerts) for alerts in all_alerts),
            'completion_time': datetime.now().isoformat()
        }
        
        logger.info(f"Auto-scan and analysis complete. Processed {len(analysis_results)} tags successfully")
        return complete_results
    
    def _send_analysis_notification(self, scan_results: Dict[str, Any], 
                                  analysis_results: Dict[str, Any],
                                  all_alerts: List[pd.DataFrame]) -> None:
        """Send email notification with analysis results."""
        total_alerts = sum(len(alerts) for alerts in all_alerts)
        successful_tags = len(analysis_results)
        
        subject = f"TURBOPREDICT X PROTEAN Auto-Scan: {successful_tags} tags, {total_alerts} alerts"
        
        body_lines = [
            "TURBOPREDICT X PROTEAN Auto-Scan Report",
            "=" * 50,
            "",
            f"Scan completed at: {scan_results['scan_timestamp']}",
            f"Total tags processed: {scan_results['total_tags']}",
            f"Fetched from PI: {len(scan_results['fetched_from_pi'])}",
            f"Used local cache: {len(scan_results['used_local_cache'])}",
            f"Failed tags: {len(scan_results['failed_tags'])}",
            f"Success rate: {scan_results['processing_stats']['success_rate']:.1%}",
            "",
            f"Analysis Results:",
            f"- Successfully analyzed: {successful_tags} tags",
            f"- Total alerts detected: {total_alerts}",
            "",
        ]
        
        if scan_results['failed_tags']:
            body_lines.extend([
                "Failed tags:",
                *[f"  - {tag}" for tag in scan_results['failed_tags'][:10]],
                f"  ... and {len(scan_results['failed_tags']) - 10} more" if len(scan_results['failed_tags']) > 10 else "",
                ""
            ])
        
        # Add alert summary
        if total_alerts > 0:
            body_lines.extend([
                "Alert Summary by Tag:",
                *[f"  - {tag}: {result.get('alerts_count', 0)} alerts" 
                  for tag, result in analysis_results.items() 
                  if result.get('alerts_count', 0) > 0],
                ""
            ])
        
        body = "\n".join(body_lines)
        
        # Collect attachments (plots)
        attachments = []
        for result in analysis_results.values():
            if 'plot_path' in result and Path(result['plot_path']).exists():
                attachments.append(Path(result['plot_path']))
        
        try:
            send_email(
                smtp_host=self.config.email.smtp_host,
                smtp_port=self.config.email.smtp_port,
                sender=self.config.email.sender,
                recipients=self.config.email.recipients,
                subject=subject,
                body=body,
                attachments=attachments,
                username=self.config.email.username,
                password=self.config.email.password,
                use_tls=self.config.email.use_tls,
            )
            logger.info(f"Sent notification email with {len(attachments)} attachments")
        except Exception as e:
            logger.error(f"Failed to send notification email: {e}")
    
    def get_database_status(self) -> Dict[str, Any]:
        """Get comprehensive database status information."""
        import sqlite3
        
        with sqlite3.connect(self.db.db_path) as conn:
            # Get table sizes
            data_count = conn.execute("SELECT COUNT(*) FROM pi_data").fetchone()[0]
            metadata_count = conn.execute("SELECT COUNT(*) FROM update_metadata").fetchone()[0]
            
            # Get date range
            date_range = conn.execute("""
                SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest 
                FROM pi_data
            """).fetchone()
            
            # Get tag statistics
            tag_stats = conn.execute("""
                SELECT COUNT(DISTINCT tag) as unique_tags,
                       COUNT(DISTINCT plant) as unique_plants,
                       COUNT(DISTINCT unit) as unique_units
                FROM pi_data
            """).fetchone()
            
            # Get recent activity
            recent_activity = conn.execute("""
                SELECT tag, MAX(timestamp) as latest_data, COUNT(*) as records
                FROM pi_data 
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY tag
                ORDER BY latest_data DESC
                LIMIT 10
            """).fetchall()
        
        return {
            'database_path': str(self.db.db_path),
            'total_records': data_count,
            'metadata_records': metadata_count,
            'earliest_data': date_range[0] if date_range[0] else None,
            'latest_data': date_range[1] if date_range[1] else None,
            'unique_tags': tag_stats[0],
            'unique_plants': tag_stats[1], 
            'unique_units': tag_stats[2],
            'recent_activity': [
                {'tag': row[0], 'latest_data': row[1], 'records': row[2]}
                for row in recent_activity
            ],
            'status_timestamp': datetime.now().isoformat()
        }