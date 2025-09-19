"""
Real-time Progress Tracker for TURBOPREDICT X PROTEAN
Shows incremental progress during long Excel refresh operations
"""

from __future__ import annotations

import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Real-time progress tracking for long-running operations"""
    
    def __init__(self, total_units: List[str]):
        """Initialize progress tracker"""
        self.total_units = total_units
        self.unit_status = {unit: "PENDING" for unit in total_units}
        self.unit_start_times = {}
        self.unit_end_times = {}
        self.current_unit = None
        self.overall_start_time = None
        self.lock = threading.RLock()
        self.progress_callbacks = []
        
    def start_overall_operation(self):
        """Mark start of overall operation"""
        with self.lock:
            self.overall_start_time = datetime.now()
            logger.info(f"Started operation with {len(self.total_units)} units")
    
    def start_unit(self, unit: str):
        """Mark start of unit processing"""
        with self.lock:
            self.current_unit = unit
            self.unit_status[unit] = "IN_PROGRESS"
            self.unit_start_times[unit] = datetime.now()
            self._notify_progress()
            
    def complete_unit(self, unit: str, success: bool = True):
        """Mark unit as completed"""
        with self.lock:
            self.unit_status[unit] = "COMPLETED" if success else "FAILED"
            self.unit_end_times[unit] = datetime.now()
            if self.current_unit == unit:
                self.current_unit = None
            self._notify_progress()
            
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get current progress summary"""
        with self.lock:
            completed = sum(1 for status in self.unit_status.values() if status in ["COMPLETED", "FAILED"])
            in_progress = sum(1 for status in self.unit_status.values() if status == "IN_PROGRESS")
            pending = sum(1 for status in self.unit_status.values() if status == "PENDING")
            
            # Calculate time estimates
            elapsed_time = None
            estimated_remaining = None
            if self.overall_start_time:
                elapsed_time = (datetime.now() - self.overall_start_time).total_seconds()
                
                if completed > 0:
                    avg_time_per_unit = elapsed_time / completed
                    estimated_remaining = avg_time_per_unit * (len(self.total_units) - completed)
            
            return {
                "total_units": len(self.total_units),
                "completed": completed,
                "in_progress": in_progress,
                "pending": pending,
                "current_unit": self.current_unit,
                "progress_percentage": (completed / len(self.total_units)) * 100 if self.total_units else 0,
                "elapsed_time_seconds": elapsed_time,
                "estimated_remaining_seconds": estimated_remaining,
                "unit_details": dict(self.unit_status),
                "unit_timings": self._get_unit_timings()
            }
    
    def _get_unit_timings(self) -> Dict[str, Any]:
        """Get timing details for each unit"""
        timings = {}
        for unit in self.total_units:
            start_time = self.unit_start_times.get(unit)
            end_time = self.unit_end_times.get(unit)
            
            if start_time:
                if end_time:
                    duration = (end_time - start_time).total_seconds()
                    timings[unit] = {
                        "status": "completed",
                        "duration_seconds": duration,
                        "started_at": start_time.isoformat(),
                        "completed_at": end_time.isoformat()
                    }
                else:
                    # Still in progress
                    duration = (datetime.now() - start_time).total_seconds()
                    timings[unit] = {
                        "status": "in_progress",
                        "duration_seconds": duration,
                        "started_at": start_time.isoformat()
                    }
            else:
                timings[unit] = {"status": "pending"}
                
        return timings
    
    def add_progress_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Add callback to be notified of progress updates"""
        self.progress_callbacks.append(callback)
    
    def _notify_progress(self):
        """Notify all callbacks of progress update"""
        summary = self.get_progress_summary()
        for callback in self.progress_callbacks:
            try:
                callback(summary)
            except Exception as e:
                logger.error(f"Progress callback failed: {e}")
    
    def print_progress_line(self, summary: Optional[Dict[str, Any]] = None):
        """Print a single line progress update"""
        if summary is None:
            summary = self.get_progress_summary()
        
        completed = summary["completed"]
        total = summary["total_units"]
        current = summary["current_unit"]
        percentage = summary["progress_percentage"]
        elapsed = summary["elapsed_time_seconds"]
        remaining = summary["estimated_remaining_seconds"]
        
        # Format time
        elapsed_str = f"{int(elapsed//60)}:{int(elapsed%60):02d}" if elapsed else "0:00"
        remaining_str = f"{int(remaining//60)}:{int(remaining%60):02d}" if remaining else "?"
        
        current_str = f"Processing {current}" if current else "Waiting..."
        
        print(f"\r[{completed}/{total}] {percentage:5.1f}% | {elapsed_str} elapsed | ~{remaining_str} remaining | {current_str}", end="", flush=True)


class InteractiveExcelRefresh:
    """Excel refresh with real-time progress tracking"""
    
    def __init__(self):
        """Initialize interactive Excel refresh"""
        self.tracker = None
        
    def refresh_with_progress(self, xlsx_path: Path, units: List[str]) -> Dict[str, Any]:
        """Refresh Excel with live progress tracking"""
        
        print(f"\nSTARTING INTERACTIVE EXCEL REFRESH")
        print(f"File: {xlsx_path}")
        print(f"Units: {len(units)} units to process")
        print("=" * 60)
        
        # Initialize progress tracker
        self.tracker = ProgressTracker(units)
        
        # Add console progress callback
        def print_progress(summary):
            self.tracker.print_progress_line(summary)
        
        self.tracker.add_progress_callback(print_progress)
        self.tracker.start_overall_operation()
        
        # Import Excel refresh functionality
        try:
            from .excel_refresh import refresh_excel_safe
            from .excel_file_manager import create_dummy_and_refresh
        except ImportError:
            return {"success": False, "error": "Excel modules not available"}
        
        overall_start = time.time()
        results = {}
        
        try:
            # Process each unit with progress tracking
            for i, unit in enumerate(units):
                self.tracker.start_unit(unit)
                
                try:
                    # Simulate unit-specific processing
                    unit_start = time.time()
                    
                    print(f"\n[{i+1}/{len(units)}] Starting {unit}...")
                    
                    # For now, do full Excel refresh
                    # TODO: Implement unit-specific refresh
                    if i == 0:  # Only refresh Excel once for all units
                        refresh_excel_safe(xlsx_path)
                    
                    unit_time = time.time() - unit_start
                    results[unit] = {"success": True, "time": unit_time}
                    
                    self.tracker.complete_unit(unit, success=True)
                    print(f"\n[{i+1}/{len(units)}] {unit} completed in {unit_time:.1f}s")
                    
                except Exception as e:
                    unit_time = time.time() - unit_start
                    results[unit] = {"success": False, "error": str(e), "time": unit_time}
                    
                    self.tracker.complete_unit(unit, success=False)
                    print(f"\n[{i+1}/{len(units)}] {unit} FAILED after {unit_time:.1f}s: {e}")
            
            total_time = time.time() - overall_start
            final_summary = self.tracker.get_progress_summary()
            
            print(f"\n\n" + "=" * 60)
            print(f"EXCEL REFRESH COMPLETED")
            print(f"Total time: {total_time:.1f} seconds")
            print(f"Successful units: {final_summary['completed'] - len([r for r in results.values() if not r['success']])}/{len(units)}")
            print(f"Failed units: {len([r for r in results.values() if not r['success']])}/{len(units)}")
            
            return {
                "success": True,
                "total_time": total_time,
                "results": results,
                "progress_summary": final_summary
            }
            
        except Exception as e:
            total_time = time.time() - overall_start
            print(f"\n\nEXCEL REFRESH FAILED after {total_time:.1f}s: {e}")
            
            return {
                "success": False,
                "error": str(e),
                "total_time": total_time,
                "results": results
            }


def create_progress_enhanced_refresh():
    """Create progress-enhanced refresh function"""
    
    def enhanced_refresh_with_status(xlsx_path: Path, units: List[str] = None):
        """Enhanced refresh with real-time status updates"""
        
        if units is None:
            # Auto-detect units from database
            try:
                from .parquet_database import ParquetDatabase
                db = ParquetDatabase()
                units = db.get_all_units()
            except:
                units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]  # Default fallback
        
        # Use interactive refresh
        interactive = InteractiveExcelRefresh()
        return interactive.refresh_with_progress(xlsx_path, units)
    
    return enhanced_refresh_with_status


# Export the enhanced function
interactive_excel_refresh = create_progress_enhanced_refresh()