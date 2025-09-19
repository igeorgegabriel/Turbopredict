"""
Polars integration for high-performance data processing in TURBOPREDICT X PROTEAN
Provides optional Polars support for faster operations on large datasets
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)

# Try to import Polars
try:
    import polars as pl
    POLARS_AVAILABLE = True
    logger.info("Polars available - high-performance mode enabled")
except ImportError:
    POLARS_AVAILABLE = False
    pl = None
    logger.info("Polars not available - using pandas fallback")


class PolarsOptimizer:
    """Polars-optimized data operations for large PI datasets"""
    
    def __init__(self):
        """Initialize Polars optimizer"""
        self.available = POLARS_AVAILABLE
        
    def read_parquet_fast(self, file_path: Path) -> Union[pl.DataFrame, pd.DataFrame]:
        """Read Parquet file with optimal performance.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            Polars DataFrame if available, otherwise pandas DataFrame
        """
        if self.available:
            try:
                return pl.read_parquet(str(file_path))
            except Exception as e:
                logger.warning(f"Polars read failed, falling back to pandas: {e}")
                return pd.read_parquet(file_path)
        else:
            return pd.read_parquet(file_path)
    
    def get_data_info_fast(self, file_path: Path) -> Dict[str, Any]:
        """Get data info with optimal performance.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            Dictionary with data information
        """
        if self.available:
            try:
                # Use lazy evaluation for maximum performance
                df_lazy = pl.scan_parquet(str(file_path))
                
                # Get schema info efficiently
                schema = df_lazy.schema
                columns = list(schema.keys())
                
                # Get basic stats without reading full data
                stats = (df_lazy
                        .select([
                            pl.count().alias('total_records'),
                            pl.col('time').min().alias('earliest_time') if 'time' in columns else pl.lit(None).alias('earliest_time'),
                            pl.col('time').max().alias('latest_time') if 'time' in columns else pl.lit(None).alias('latest_time'),
                            pl.col('tag').n_unique().alias('unique_tags') if 'tag' in columns else pl.lit(0).alias('unique_tags')
                        ])
                        .collect()
                        .row(0, named=True))
                
                return {
                    'total_records': stats['total_records'],
                    'earliest_time': stats['earliest_time'],
                    'latest_time': stats['latest_time'],
                    'unique_tags': stats['unique_tags'],
                    'columns': columns,
                    'file_path': str(file_path),
                    'engine': 'polars'
                }
                
            except Exception as e:
                logger.warning(f"Polars analysis failed, falling back to pandas: {e}")
                return self._get_data_info_pandas(file_path)
        else:
            return self._get_data_info_pandas(file_path)
    
    def _get_data_info_pandas(self, file_path: Path) -> Dict[str, Any]:
        """Fallback pandas implementation"""
        try:
            df = pd.read_parquet(file_path)
            
            return {
                'total_records': len(df),
                'earliest_time': df['time'].min() if 'time' in df.columns else None,
                'latest_time': df['time'].max() if 'time' in df.columns else None,
                'unique_tags': df['tag'].nunique() if 'tag' in df.columns else 0,
                'columns': list(df.columns),
                'file_path': str(file_path),
                'engine': 'pandas'
            }
        except Exception as e:
            logger.error(f"Failed to analyze file {file_path}: {e}")
            return {
                'total_records': 0,
                'earliest_time': None,
                'latest_time': None,
                'unique_tags': 0,
                'columns': [],
                'file_path': str(file_path),
                'engine': 'error'
            }
    
    def query_unit_data_fast(self, file_paths: List[Path], unit: str) -> Union[pl.DataFrame, pd.DataFrame]:
        """Query unit data with optimal performance.
        
        Args:
            file_paths: List of Parquet file paths for the unit
            unit: Unit identifier
            
        Returns:
            Combined DataFrame with unit data
        """
        if not file_paths:
            return pl.DataFrame() if self.available else pd.DataFrame()
        
        if self.available:
            try:
                # Use lazy evaluation and combine multiple files efficiently
                dfs = []
                for file_path in file_paths:
                    df_lazy = pl.scan_parquet(str(file_path))
                    dfs.append(df_lazy)
                
                # Combine and optimize
                if len(dfs) == 1:
                    combined = dfs[0]
                else:
                    combined = pl.concat(dfs)
                
                # Collect only when needed
                return combined.collect()
                
            except Exception as e:
                logger.warning(f"Polars query failed, falling back to pandas: {e}")
                return self._query_unit_data_pandas(file_paths, unit)
        else:
            return self._query_unit_data_pandas(file_paths, unit)
    
    def _query_unit_data_pandas(self, file_paths: List[Path], unit: str) -> pd.DataFrame:
        """Fallback pandas implementation"""
        try:
            dfs = []
            for file_path in file_paths:
                df = pd.read_parquet(file_path)
                dfs.append(df)
            
            if not dfs:
                return pd.DataFrame()
            elif len(dfs) == 1:
                return dfs[0]
            else:
                return pd.concat(dfs, ignore_index=True)
                
        except Exception as e:
            logger.error(f"Failed to query unit data for {unit}: {e}")
            return pd.DataFrame()
    
    def get_tag_summary_fast(self, df: Union[pl.DataFrame, pd.DataFrame]) -> Union[pl.DataFrame, pd.DataFrame]:
        """Get tag summary statistics with optimal performance.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Summary DataFrame with tag statistics
        """
        if self.available and isinstance(df, pl.DataFrame):
            try:
                if 'tag' not in df.columns or 'value' not in df.columns:
                    return pl.DataFrame()
                
                summary = (df
                          .group_by('tag')
                          .agg([
                              pl.col('value').count().alias('count'),
                              pl.col('value').mean().alias('mean'),
                              pl.col('value').std().alias('std'),
                              pl.col('value').min().alias('min'),
                              pl.col('value').max().alias('max'),
                              pl.col('time').min().alias('time_min'),
                              pl.col('time').max().alias('time_max')
                          ])
                          .sort('tag'))
                
                return summary
                
            except Exception as e:
                logger.warning(f"Polars tag summary failed: {e}")
                # Convert to pandas and try again
                df_pd = df.to_pandas() if hasattr(df, 'to_pandas') else df
                return self._get_tag_summary_pandas(df_pd)
        else:
            return self._get_tag_summary_pandas(df)
    
    def _get_tag_summary_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fallback pandas implementation"""
        try:
            if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
                return pd.DataFrame()
            
            summary = df.groupby('tag').agg({
                'value': ['count', 'mean', 'std', 'min', 'max'],
                'time': ['min', 'max']
            }).round(3)
            
            # Flatten column names
            summary.columns = ['_'.join(col).strip() for col in summary.columns]
            summary = summary.reset_index()
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to create tag summary: {e}")
            return pd.DataFrame()
    
    def to_pandas(self, df: Union[pl.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        """Convert to pandas DataFrame if needed.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pandas DataFrame
        """
        if self.available and isinstance(df, pl.DataFrame):
            return df.to_pandas()
        else:
            return df