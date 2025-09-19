"""
Instant Cache System for TURBOPREDICT X PROTEAN
Provides sub-second diagnostics for critical equipment monitoring
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import threading
import time
import json

logger = logging.getLogger(__name__)


class InstantCache:
    """In-memory cache for instant critical equipment diagnostics"""
    
    def __init__(self, cache_ttl_minutes: int = 30):
        """Initialize instant cache system"""
        self.cache = {}
        self.cache_metadata = {}
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.lock = threading.RLock()
        self._background_refresh_active = False
        
    def preload_critical_data(self, parquet_files: List[Path], units: List[str]) -> Dict[str, Any]:
        """Preload critical equipment data into memory for instant access"""
        
        start_time = time.time()
        loaded_units = []
        
        with self.lock:
            for unit in units:
                try:
                    # Find relevant files for this unit
                    unit_files = [f for f in parquet_files if unit in f.name]
                    
                    if not unit_files:
                        logger.warning(f"No files found for unit {unit}")
                        continue
                    
                    # Load latest data file (usually .dedup.parquet)
                    latest_file = max(unit_files, key=lambda f: f.stat().st_mtime)
                    
                    logger.info(f"Loading critical data for {unit} from {latest_file.name}")
                    df = pd.read_parquet(latest_file)
                    
                    # Cache raw data
                    cache_key = f"unit_data_{unit}"
                    self.cache[cache_key] = df
                    
                    # Cache pre-computed analytics for instant access
                    analytics = self._compute_instant_analytics(df, unit)
                    analytics_key = f"unit_analytics_{unit}"
                    self.cache[analytics_key] = analytics
                    
                    # Store metadata
                    self.cache_metadata[cache_key] = {
                        "loaded_at": datetime.now(),
                        "file_path": str(latest_file),
                        "file_size_mb": latest_file.stat().st_size / (1024 * 1024),
                        "records": len(df),
                        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024)
                    }
                    
                    loaded_units.append(unit)
                    
                except Exception as e:
                    logger.error(f"Failed to preload data for unit {unit}: {e}")
        
        load_time = time.time() - start_time
        
        logger.info(f"Preloaded {len(loaded_units)} units in {load_time:.2f} seconds")
        
        return {
            "success": True,
            "units_loaded": loaded_units,
            "load_time": load_time,
            "cache_size": len(self.cache),
            "total_memory_mb": sum(meta.get("memory_usage_mb", 0) 
                                 for meta in self.cache_metadata.values())
        }
    
    def _compute_instant_analytics(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Pre-compute analytics for instant access"""
        
        try:
            analytics = {
                "unit": unit,
                "total_records": len(df),
                "computed_at": datetime.now().isoformat(),
                "data_timespan": {},
                "critical_metrics": {},
                "alert_summary": {},
                "tag_summary": {}
            }
            
            if "time" in df.columns:
                analytics["data_timespan"] = {
                    "earliest": df["time"].min().isoformat() if not df["time"].isna().all() else None,
                    "latest": df["time"].max().isoformat() if not df["time"].isna().all() else None,
                    "hours_covered": ((df["time"].max() - df["time"].min()).total_seconds() / 3600) 
                                   if not df["time"].isna().all() else 0
                }
            
            if "value" in df.columns:
                values = df["value"].dropna()
                if len(values) > 0:
                    analytics["critical_metrics"] = {
                        "mean": float(values.mean()),
                        "std": float(values.std()),
                        "min": float(values.min()),
                        "max": float(values.max()),
                        "latest_value": float(values.iloc[-1]) if len(values) > 0 else None,
                        "value_range": float(values.max() - values.min()),
                        "anomaly_threshold": float(values.mean() + 3 * values.std())
                    }
            
            if "tag" in df.columns:
                tag_counts = df["tag"].value_counts()
                analytics["tag_summary"] = {
                    "total_tags": len(tag_counts),
                    "most_active_tags": tag_counts.head(10).to_dict(),
                    "tags_list": tag_counts.index.tolist()
                }
            
            # Compute alerts/anomalies
            if "value" in df.columns and len(df) > 0:
                mean_val = df["value"].mean()
                std_val = df["value"].std()
                if not pd.isna(mean_val) and not pd.isna(std_val):
                    threshold = mean_val + 3 * std_val
                    anomalies = df[df["value"] > threshold]
                    
                    analytics["alert_summary"] = {
                        "anomaly_count": len(anomalies),
                        "anomaly_percentage": (len(anomalies) / len(df)) * 100,
                        "latest_anomaly": anomalies["time"].max().isoformat() 
                                        if len(anomalies) > 0 and "time" in anomalies.columns 
                                        else None
                    }
            
            return analytics
            
        except Exception as e:
            logger.error(f"Failed to compute analytics for {unit}: {e}")
            return {"unit": unit, "error": str(e), "computed_at": datetime.now().isoformat()}
    
    def get_instant_unit_status(self, unit: str) -> Dict[str, Any]:
        """Get instant unit status (< 1 second response)"""
        
        start_time = time.time()
        
        with self.lock:
            # Check if analytics are cached
            analytics_key = f"unit_analytics_{unit}"
            if analytics_key in self.cache:
                analytics = self.cache[analytics_key]
                metadata = self.cache_metadata.get(f"unit_data_{unit}", {})
                
                response_time = time.time() - start_time
                
                return {
                    "success": True,
                    "unit": unit,
                    "response_time_ms": response_time * 1000,
                    "data_source": "memory_cache",
                    "analytics": analytics,
                    "cache_metadata": metadata,
                    "is_fresh": self._is_cache_fresh(analytics_key)
                }
            else:
                response_time = time.time() - start_time
                return {
                    "success": False,
                    "unit": unit,
                    "response_time_ms": response_time * 1000,
                    "error": "Unit not in cache - run preload_critical_data() first",
                    "data_source": "none"
                }
    
    def get_instant_multi_unit_status(self, units: List[str]) -> Dict[str, Any]:
        """Get status for multiple units instantly"""
        
        start_time = time.time()
        results = {}
        
        for unit in units:
            unit_status = self.get_instant_unit_status(unit)
            results[unit] = unit_status
        
        total_time = time.time() - start_time
        successful_units = [u for u, r in results.items() if r.get("success", False)]
        
        return {
            "success": True,
            "total_response_time_ms": total_time * 1000,
            "units_requested": len(units),
            "units_successful": len(successful_units),
            "successful_units": successful_units,
            "results": results,
            "average_response_time_ms": (total_time * 1000) / len(units) if units else 0
        }
    
    def _is_cache_fresh(self, cache_key: str) -> bool:
        """Check if cached data is still fresh"""
        if cache_key not in self.cache_metadata:
            return False
        
        loaded_at = self.cache_metadata[cache_key].get("loaded_at")
        if not loaded_at:
            return False
        
        return datetime.now() - loaded_at < self.cache_ttl
    
    def start_background_refresh(self, parquet_files: List[Path], units: List[str], 
                                refresh_interval_minutes: int = 15):
        """Start background refresh to keep cache current"""
        
        def refresh_worker():
            while self._background_refresh_active:
                try:
                    logger.info("Starting background cache refresh...")
                    self.preload_critical_data(parquet_files, units)
                    logger.info("Background cache refresh completed")
                except Exception as e:
                    logger.error(f"Background refresh failed: {e}")
                
                # Wait for next refresh cycle
                time.sleep(refresh_interval_minutes * 60)
        
        if not self._background_refresh_active:
            self._background_refresh_active = True
            refresh_thread = threading.Thread(target=refresh_worker, daemon=True)
            refresh_thread.start()
            logger.info(f"Started background cache refresh every {refresh_interval_minutes} minutes")
    
    def stop_background_refresh(self):
        """Stop background refresh process"""
        self._background_refresh_active = False
        logger.info("Stopped background cache refresh")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get detailed cache statistics"""
        
        with self.lock:
            total_memory = sum(meta.get("memory_usage_mb", 0) for meta in self.cache_metadata.values())
            fresh_items = sum(1 for key in self.cache.keys() if self._is_cache_fresh(key))
            
            return {
                "total_cached_items": len(self.cache),
                "fresh_items": fresh_items,
                "stale_items": len(self.cache) - fresh_items,
                "total_memory_usage_mb": total_memory,
                "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60,
                "background_refresh_active": self._background_refresh_active,
                "cached_units": [key.replace("unit_data_", "") for key in self.cache.keys() 
                               if key.startswith("unit_data_")],
                "metadata": self.cache_metadata
            }
    
    def clear_cache(self):
        """Clear all cached data"""
        with self.lock:
            self.cache.clear()
            self.cache_metadata.clear()
            logger.info("Cache cleared")


# Global cache instance for the application
_global_cache: Optional[InstantCache] = None

def get_instant_cache() -> InstantCache:
    """Get or create global cache instance"""
    global _global_cache
    if _global_cache is None:
        _global_cache = InstantCache()
    return _global_cache

def setup_instant_diagnostics(parquet_files: List[Path], units: List[str]) -> Dict[str, Any]:
    """Set up instant diagnostics for critical equipment"""
    cache = get_instant_cache()
    
    # Preload critical data
    result = cache.preload_critical_data(parquet_files, units)
    
    # Start background refresh
    cache.start_background_refresh(parquet_files, units, refresh_interval_minutes=10)
    
    return result