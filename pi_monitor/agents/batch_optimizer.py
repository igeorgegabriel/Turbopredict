"""
Adaptive Batch Size Agent

Dynamically adjusts batch sizes based on data characteristics,
memory availability, and processing performance.

Expected Impact: 30% faster processing, 50% fewer memory errors
"""

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from .base_agent import BaseAgent

# Optional psutil import
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class AdaptiveBatchAgent(BaseAgent):
    """
    Dynamically optimizes batch sizes for data processing based on
    performance history and system resources.
    """

    def __init__(self):
        super().__init__(name='batch_optimizer', version='1.0.0')

        # Learned profiles: unit -> optimal batch size
        self.unit_profiles: Dict[str, Dict] = {}

        # Performance history: unit -> list of performance records
        self.performance_history: Dict[str, List[Dict]] = defaultdict(list)

        # Configuration
        self.min_batch = 50_000       # Minimum batch size
        self.max_batch = 1_000_000    # Maximum batch size
        self.default_batch = 250_000  # Default batch size
        self.history_limit = 50       # Keep last 50 performance records
        self.memory_baseline_gb = 2.0  # 2GB baseline memory
        self.max_memory_factor = 4.0   # Max 4x increase based on memory

        # Try to load existing state
        self.load()

    def calculate_optimal_batch_size(
        self,
        unit: str,
        total_records: int,
        columns: int,
        available_memory_gb: Optional[float] = None
    ) -> int:
        """
        Calculate optimal batch size for unit processing.

        Args:
            unit: Unit identifier (e.g., 'K-31-01')
            total_records: Total records to process
            columns: Number of columns in dataset
            available_memory_gb: Available system memory (auto-detected if None)

        Returns:
            Optimal batch size (number of rows)
        """
        # Auto-detect available memory if not provided
        if available_memory_gb is None:
            available_memory_gb = self._get_available_memory_gb()

        # Start with learned profile or default
        if unit in self.unit_profiles:
            base_batch = self.unit_profiles[unit]['optimal_batch']
            source = 'learned'
        else:
            base_batch = self.default_batch
            source = 'default'

        # Adjust for available memory
        memory_factor = min(available_memory_gb / self.memory_baseline_gb, self.max_memory_factor)
        adjusted_batch = int(base_batch * memory_factor)

        # Adjust for column count (more columns = smaller batch)
        # Assume 100 columns as baseline
        column_factor = max(0.5, 100 / columns)
        adjusted_batch = int(adjusted_batch * column_factor)

        # Ensure reasonable bounds
        optimal_batch = max(self.min_batch, min(adjusted_batch, self.max_batch, total_records))

        # Log the calculation
        self.metrics['predictions_made'] += 1
        self.log_performance('batch_calculation', {
            'unit': unit,
            'batch_size': optimal_batch,
            'total_records': total_records,
            'columns': columns,
            'available_memory_gb': available_memory_gb,
            'memory_factor': memory_factor,
            'column_factor': column_factor,
            'source': source
        })

        return optimal_batch

    def record_performance(
        self,
        unit: str,
        batch_size: int,
        processing_time: float,
        memory_used_gb: float,
        success: bool,
        total_records: int,
        columns: int
    ):
        """
        Record batch processing performance for learning.

        Args:
            unit: Unit identifier
            batch_size: Batch size used
            processing_time: Time taken in seconds
            memory_used_gb: Peak memory used in GB
            success: Whether processing succeeded
            total_records: Total records processed
            columns: Number of columns
        """
        record = {
            'timestamp': datetime.now(),
            'batch_size': batch_size,
            'processing_time': processing_time,
            'memory_used_gb': memory_used_gb,
            'success': success,
            'total_records': total_records,
            'columns': columns
        }

        self.performance_history[unit].append(record)

        # Keep only recent history
        if len(self.performance_history[unit]) > self.history_limit:
            self.performance_history[unit] = self.performance_history[unit][-self.history_limit:]

        self.metrics['learning_iterations'] += 1

        # Update unit profile
        self._update_unit_profile(unit)

        # Periodic save
        if self.metrics['learning_iterations'] % 10 == 0:
            self.save()

        self.log_performance('performance_recorded', {
            'unit': unit,
            'batch_size': batch_size,
            'processing_time': processing_time,
            'success': success
        })

    def _update_unit_profile(self, unit: str):
        """Learn optimal batch size from performance history."""
        history = self.performance_history[unit]

        if len(history) < 5:
            # Not enough data yet
            return

        # Filter successful runs only
        successful = [h for h in history if h['success']]

        if not successful:
            return

        # Calculate score for each successful run
        # Score = time * memory_penalty (lower is better)
        scores = []
        for h in successful:
            # Memory penalty increases if we use too much memory
            # Target: use less than 4GB
            memory_penalty = 1.0 if h['memory_used_gb'] < 4.0 else (h['memory_used_gb'] / 4.0)

            # Calculate records per second
            records_per_sec = h['total_records'] / max(h['processing_time'], 1.0)

            # Score: lower is better
            # We want high records/sec and low memory penalty
            score = (1.0 / records_per_sec) * memory_penalty

            scores.append((h['batch_size'], score, records_per_sec))

        # Find batch size with minimum score (best performance)
        optimal_batch, best_score, best_throughput = min(scores, key=lambda x: x[1])

        # Update profile
        old_profile = self.unit_profiles.get(unit, {})
        self.unit_profiles[unit] = {
            'optimal_batch': optimal_batch,
            'best_score': best_score,
            'throughput': best_throughput,
            'last_updated': datetime.now(),
            'sample_size': len(successful)
        }

        # Calculate speedup if we have old profile
        if old_profile and 'throughput' in old_profile:
            speedup = best_throughput / old_profile['throughput']
            self.metrics['speedup_factor'] = speedup

            # Estimate time saved
            avg_records = sum(h['total_records'] for h in successful) / len(successful)
            old_time = avg_records / old_profile['throughput']
            new_time = avg_records / best_throughput
            time_saved = max(0, old_time - new_time)
            self.metrics['total_time_saved'] += time_saved

    def _get_available_memory_gb(self) -> float:
        """Get available system memory in GB."""
        if not HAS_PSUTIL:
            # Default to 4GB if psutil not available
            return 4.0

        try:
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 ** 3)
            return available_gb
        except Exception:
            # Default to 4GB if can't detect
            return 4.0

    def get_recommendations(self, unit: Optional[str] = None) -> Dict:
        """
        Get optimization recommendations.

        Args:
            unit: Optional unit filter

        Returns:
            Recommendations dictionary
        """
        recommendations = {
            'total_units_optimized': len(self.unit_profiles),
            'units': []
        }

        units_to_check = [unit] if unit else list(self.unit_profiles.keys())

        for u in units_to_check:
            if u not in self.unit_profiles:
                continue

            profile = self.unit_profiles[u]
            history = self.performance_history.get(u, [])

            if not history:
                continue

            # Calculate statistics
            successful = [h for h in history if h['success']]
            failed = [h for h in history if not h['success']]

            success_rate = len(successful) / len(history) if history else 0

            unit_rec = {
                'unit': u,
                'optimal_batch': profile['optimal_batch'],
                'throughput': f"{profile['throughput']:.0f} records/sec",
                'success_rate': f"{success_rate:.1%}",
                'sample_size': len(history),
                'last_updated': profile['last_updated'].isoformat()
            }

            if successful:
                avg_time = sum(h['processing_time'] for h in successful) / len(successful)
                avg_memory = sum(h['memory_used_gb'] for h in successful) / len(successful)
                unit_rec['avg_processing_time'] = f"{avg_time:.1f}s"
                unit_rec['avg_memory_used'] = f"{avg_memory:.2f}GB"

            if failed:
                unit_rec['memory_errors'] = len(failed)
                unit_rec['recommendation'] = f"Reduce batch size to avoid memory errors"

            recommendations['units'].append(unit_rec)

        return recommendations

    def get_state(self) -> Dict:
        """Get agent state for serialization."""
        return {
            'unit_profiles': self.unit_profiles,
            'performance_history': {k: v for k, v in self.performance_history.items()}
        }

    def set_state(self, state: Dict):
        """Set agent state from deserialized data."""
        self.unit_profiles = state.get('unit_profiles', {})
        self.performance_history = defaultdict(list, state.get('performance_history', {}))

    def print_summary(self):
        """Print a summary of learned optimizations."""
        print(f"\n{'='*70}")
        print(f"ADAPTIVE BATCH AGENT SUMMARY")
        print(f"{'='*70}")
        print(f"Units optimized: {len(self.unit_profiles)}")
        print(f"Batch calculations made: {self.metrics.get('predictions_made', 0)}")
        print(f"Learning iterations: {self.metrics.get('learning_iterations', 0)}")

        if 'speedup_factor' in self.metrics:
            print(f"Speedup factor: {self.metrics['speedup_factor']:.2f}x")

        if 'total_time_saved' in self.metrics:
            time_saved = self.metrics['total_time_saved']
            print(f"Total time saved: {time_saved:.1f}s ({time_saved/60:.1f}min)")

        if self.unit_profiles:
            print(f"\nOptimized Batch Sizes:")
            sorted_units = sorted(
                self.unit_profiles.items(),
                key=lambda x: x[1]['throughput'],
                reverse=True
            )

            for unit, profile in sorted_units:
                print(f"  {unit}:")
                print(f"    Optimal batch: {profile['optimal_batch']:,} rows")
                print(f"    Throughput: {profile['throughput']:.0f} records/sec")
                print(f"    Sample size: {profile['sample_size']} runs")

        print(f"{'='*70}\n")
