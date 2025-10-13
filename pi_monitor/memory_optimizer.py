"""Memory optimization utilities for PI DataLink system handling large datasets.

This module provides memory-efficient DataFrame operations, chunked processing,
and memory monitoring to prevent Out of Memory errors during parquet operations.
"""

from __future__ import annotations

import gc
import logging
import psutil
import warnings
from pathlib import Path
from typing import Iterator, List, Optional, Union, Dict, Any, Callable
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Memory usage monitoring and management."""

    def __init__(self, memory_threshold_gb: float = 2.0):
        """Initialize memory monitor.

        Args:
            memory_threshold_gb: Minimum free memory in GB before warnings
        """
        self.memory_threshold_bytes = memory_threshold_gb * 1024**3
        self.initial_memory = self.get_memory_usage()

    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics."""
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'available_gb': memory.available / (1024**3),
            'used_gb': memory.used / (1024**3),
            'percent': memory.percent,
            'free_gb': memory.free / (1024**3)
        }

    def check_memory_pressure(self) -> bool:
        """Check if system is under memory pressure."""
        memory_info = self.get_memory_usage()
        return memory_info['available_gb'] < (self.memory_threshold_bytes / (1024**3))

    def log_memory_status(self, context: str = "") -> None:
        """Log current memory status."""
        memory_info = self.get_memory_usage()
        logger.info(f"Memory status {context}: "
                   f"{memory_info['available_gb']:.1f}GB available "
                   f"({memory_info['percent']:.1f}% used)")

        if self.check_memory_pressure():
            logger.warning(f"LOW MEMORY: Only {memory_info['available_gb']:.1f}GB available!")

    def force_garbage_collection(self) -> None:
        """Force garbage collection and log memory recovered."""
        before = self.get_memory_usage()
        gc.collect()
        after = self.get_memory_usage()
        recovered_mb = (before['used_gb'] - after['used_gb']) * 1024
        if recovered_mb > 10:  # Only log if significant recovery
            logger.info(f"Garbage collection recovered {recovered_mb:.1f}MB")


class ChunkedProcessor:
    """Chunked DataFrame processing for memory efficiency."""

    def __init__(self, chunk_size: int = 1_000_000, memory_monitor: MemoryMonitor = None):
        """Initialize chunked processor.

        Args:
            chunk_size: Maximum rows per chunk
            memory_monitor: Optional memory monitor instance
        """
        self.chunk_size = chunk_size
        self.memory_monitor = memory_monitor or MemoryMonitor()

    def read_parquet_chunked(self, file_path: Path,
                           chunk_processor: Callable[[pd.DataFrame], pd.DataFrame] = None) -> Iterator[pd.DataFrame]:
        """Read parquet file in chunks to avoid memory issues.

        Args:
            file_path: Path to parquet file
            chunk_processor: Optional function to process each chunk

        Yields:
            DataFrame chunks
        """
        try:
            # Try to get row count without loading full file
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            total_rows = parquet_file.metadata.num_rows

            logger.info(f"Reading {total_rows:,} rows from {file_path.name} in chunks of {self.chunk_size:,}")

            # Read in batches
            for batch_start in range(0, total_rows, self.chunk_size):
                batch_end = min(batch_start + self.chunk_size, total_rows)

                # Read chunk using row indices
                chunk = pd.read_parquet(file_path,
                                      engine='pyarrow',
                                      use_pandas_metadata=True)

                # Slice to get specific rows (approximate due to parquet structure)
                if batch_start > 0 or batch_end < total_rows:
                    chunk = chunk.iloc[batch_start:batch_end]

                if chunk_processor:
                    chunk = chunk_processor(chunk)

                # Memory check and cleanup
                self.memory_monitor.log_memory_status(f"after chunk {batch_start//self.chunk_size + 1}")
                if self.memory_monitor.check_memory_pressure():
                    self.memory_monitor.force_garbage_collection()

                yield chunk

                # Clear chunk from memory
                del chunk

        except Exception as e:
            logger.error(f"Error reading parquet in chunks: {e}")
            # Fallback: read entire file if chunking fails
            logger.info("Falling back to full file read")
            df = pd.read_parquet(file_path)
            if chunk_processor:
                df = chunk_processor(df)
            yield df

    def concat_chunked(self, chunks: Iterator[pd.DataFrame],
                      output_path: Path = None) -> pd.DataFrame:
        """Concatenate chunks while managing memory.

        Args:
            chunks: Iterator of DataFrame chunks
            output_path: Optional path to write result directly

        Returns:
            Concatenated DataFrame or empty DataFrame if written to file
        """
        result_chunks = []
        chunk_count = 0

        for chunk in chunks:
            if chunk.empty:
                continue

            result_chunks.append(chunk)
            chunk_count += 1

            # Check memory pressure every 10 chunks
            if chunk_count % 10 == 0:
                self.memory_monitor.log_memory_status(f"after {chunk_count} chunks")
                if self.memory_monitor.check_memory_pressure():
                    logger.warning("Memory pressure detected during concatenation")
                    break

        if not result_chunks:
            return pd.DataFrame()

        # Concatenate all chunks
        logger.info(f"Concatenating {len(result_chunks)} chunks...")
        result = pd.concat(result_chunks, ignore_index=True)

        # Clear intermediate chunks
        del result_chunks
        self.memory_monitor.force_garbage_collection()

        # Write directly to file if requested
        if output_path:
            logger.info(f"Writing result to {output_path}")
            result.to_parquet(output_path, index=False, engine='pyarrow', compression='zstd')
            return pd.DataFrame()  # Return empty to save memory

        return result


class StreamingParquetHandler:
    """Streaming operations for large parquet files."""

    def __init__(self, temp_dir: Path = None, memory_monitor: MemoryMonitor = None):
        """Initialize streaming handler.

        Args:
            temp_dir: Directory for temporary files
            memory_monitor: Memory monitor instance
        """
        self.temp_dir = temp_dir or Path("temp_parquet")
        self.temp_dir.mkdir(exist_ok=True)
        self.memory_monitor = memory_monitor or MemoryMonitor()
        self.chunked_processor = ChunkedProcessor(memory_monitor=memory_monitor)

    def merge_large_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame,
                             merge_keys: List[str] = None) -> pd.DataFrame:
        """Memory-efficient merge of large DataFrames.

        Args:
            df1: First DataFrame
            df2: Second DataFrame
            merge_keys: Keys to merge on (default: ['time', 'tag'])

        Returns:
            Merged DataFrame
        """
        if merge_keys is None:
            merge_keys = ['time', 'tag']

        # Check memory usage of both DataFrames
        df1_mb = df1.memory_usage(deep=True).sum() / (1024**2)
        df2_mb = df2.memory_usage(deep=True).sum() / (1024**2)
        total_mb = df1_mb + df2_mb

        logger.info(f"Merging DataFrames: {len(df1):,} + {len(df2):,} rows, "
                   f"{df1_mb:.1f}MB + {df2_mb:.1f}MB = {total_mb:.1f}MB total")

        # If combined size is too large, use disk-based merge
        if total_mb > 1000 or self.memory_monitor.check_memory_pressure():
            return self._disk_based_merge(df1, df2, merge_keys)

        # Standard merge for smaller datasets
        try:
            return pd.concat([df1, df2], ignore_index=True)
        except MemoryError:
            logger.warning("MemoryError during concat, falling back to disk-based merge")
            return self._disk_based_merge(df1, df2, merge_keys)

    def _disk_based_merge(self, df1: pd.DataFrame, df2: pd.DataFrame,
                         merge_keys: List[str]) -> pd.DataFrame:
        """Disk-based merge for very large DataFrames."""
        logger.info("Using disk-based merge strategy")

        # Write DataFrames to temporary files
        temp_file1 = self.temp_dir / f"merge_temp1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        temp_file2 = self.temp_dir / f"merge_temp2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

        try:
            df1.to_parquet(temp_file1, index=False, engine='pyarrow', compression='zstd')
            df2.to_parquet(temp_file2, index=False, engine='pyarrow', compression='zstd')

            # Clear from memory
            del df1, df2
            self.memory_monitor.force_garbage_collection()

            # Use DuckDB for efficient merge if available
            try:
                import duckdb
                conn = duckdb.connect()

                merge_sql = f"""
                SELECT * FROM read_parquet('{temp_file1.as_posix()}')
                UNION ALL
                SELECT * FROM read_parquet('{temp_file2.as_posix()}')
                ORDER BY {', '.join(merge_keys)}
                """

                result = conn.execute(merge_sql).df()
                conn.close()

                return result

            except ImportError:
                logger.info("DuckDB not available, using pandas chunked merge")
                return self._pandas_chunked_merge(temp_file1, temp_file2)

        finally:
            # Cleanup temporary files
            for temp_file in [temp_file1, temp_file2]:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {temp_file}: {e}")

    def _pandas_chunked_merge(self, file1: Path, file2: Path) -> pd.DataFrame:
        """Pandas-based chunked merge."""
        chunks = []

        # Read file1 in chunks
        for chunk in self.chunked_processor.read_parquet_chunked(file1):
            chunks.append(chunk)

        # Read file2 in chunks
        for chunk in self.chunked_processor.read_parquet_chunked(file2):
            chunks.append(chunk)

        # Concatenate all chunks
        if chunks:
            result = pd.concat(chunks, ignore_index=True)
            result = result.sort_values(['time'] if 'time' in result.columns else result.columns[0])
            return result.reset_index(drop=True)

        return pd.DataFrame()

    def cleanup_temp_files(self) -> None:
        """Clean up temporary files."""
        if self.temp_dir.exists():
            for temp_file in self.temp_dir.glob("*.parquet"):
                try:
                    temp_file.unlink()
                    logger.debug(f"Cleaned up temp file: {temp_file}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {temp_file}: {e}")


def memory_efficient_dedup(df: pd.DataFrame,
                         subset: List[str] = None,
                         chunk_size: int = 1_000_000) -> pd.DataFrame:
    """Memory-efficient deduplication for large DataFrames.

    Args:
        df: DataFrame to deduplicate
        subset: Columns to use for deduplication
        chunk_size: Size of chunks for processing

    Returns:
        Deduplicated DataFrame
    """
    if subset is None:
        subset = ['plant', 'unit', 'tag', 'time']

    # Filter subset to only include existing columns
    subset = [col for col in subset if col in df.columns]
    if not subset:
        logger.warning("No valid columns for deduplication")
        return df

    df_mb = df.memory_usage(deep=True).sum() / (1024**2)
    logger.info(f"Deduplicating DataFrame: {len(df):,} rows, {df_mb:.1f}MB")

    # For smaller DataFrames, use standard dedup
    if df_mb < 500:
        return df.drop_duplicates(subset=subset, keep='last').reset_index(drop=True)

    # For large DataFrames, use chunked processing
    logger.info("Using chunked deduplication for large DataFrame")

    # Sort by subset columns first to help with chunked dedup
    try:
        df_sorted = df.sort_values(subset)
    except Exception:
        df_sorted = df  # Use original if sorting fails

    chunks = []
    monitor = MemoryMonitor()

    for i in range(0, len(df_sorted), chunk_size):
        chunk = df_sorted.iloc[i:i+chunk_size]

        # Deduplicate within chunk
        chunk_dedup = chunk.drop_duplicates(subset=subset, keep='last')
        chunks.append(chunk_dedup)

        # Memory management
        if i % (chunk_size * 5) == 0:  # Every 5 chunks
            monitor.log_memory_status(f"dedup chunk {i//chunk_size + 1}")
            if monitor.check_memory_pressure():
                monitor.force_garbage_collection()

    # Final concatenation and deduplication
    if chunks:
        result = pd.concat(chunks, ignore_index=True)
        # Final dedup pass to handle duplicates across chunk boundaries
        result = result.drop_duplicates(subset=subset, keep='last')
        return result.reset_index(drop=True)

    return df


def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by downcasting numeric types.

    Args:
        df: DataFrame to optimize

    Returns:
        Memory-optimized DataFrame
    """
    original_mb = df.memory_usage(deep=True).sum() / (1024**2)

    # Optimize numeric columns
    for col in df.columns:
        if df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')

    # Optimize string columns to category if beneficial
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
            df[col] = df[col].astype('category')

    optimized_mb = df.memory_usage(deep=True).sum() / (1024**2)
    savings_mb = original_mb - optimized_mb

    if savings_mb > 1:  # Only log if significant savings
        logger.info(f"Memory optimization: {original_mb:.1f}MB → {optimized_mb:.1f}MB "
                   f"(saved {savings_mb:.1f}MB, {savings_mb/original_mb*100:.1f}%)")

    return df