"""
Integration Module for AI Agents

This module provides helper functions to integrate the AI agents
with the existing TURBOPREDICT codebase.

Usage Examples:

# Example 1: Integrate with fetch operations in batch.py
from pi_monitor.agents.integration import AgentManager

agent_manager = AgentManager()

# Before fetching
timeout = agent_manager.predict_timeout(tag, plant)

# Fetch data with predicted timeout
start_time = time.time()
try:
    result = fetch_pi_data(tag, timeout)
    success = True
except TimeoutError:
    success = False

# Learn from result
agent_manager.learn_from_fetch(tag, plant, timeout, success, time.time() - start_time)


# Example 2: Integrate with parquet_auto_scan.py
from pi_monitor.agents.integration import AgentManager

agent_manager = AgentManager()

# Filter tags before fetching
filtered_tags = agent_manager.filter_tags(tags, unit)

# Calculate optimal batch size
batch_size = agent_manager.calculate_batch_size(unit, total_records, columns)

# Process with optimal batch
process_in_batches(df, batch_size)
"""

import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .timeout_predictor import TimeoutPredictionAgent
from .tag_health import TagHealthAgent
from .batch_optimizer import AdaptiveBatchAgent


class AgentManager:
    """
    Unified manager for all AI agents.
    Provides a simple interface for integrating agents with existing code.
    """

    def __init__(self, enable_agents: bool = True):
        """
        Initialize agent manager.

        Args:
            enable_agents: If False, agents are disabled (pass-through mode)
        """
        self.enabled = enable_agents

        if self.enabled:
            print("[AGENT_MANAGER] Initializing AI agents...")
            self.timeout_agent = TimeoutPredictionAgent()
            self.health_agent = TagHealthAgent()
            self.batch_agent = AdaptiveBatchAgent()
            print("[AGENT_MANAGER] All agents initialized successfully")
        else:
            print("[AGENT_MANAGER] Agents disabled (pass-through mode)")
            self.timeout_agent = None
            self.health_agent = None
            self.batch_agent = None

    # ============================================================================
    # TIMEOUT PREDICTION INTEGRATION
    # ============================================================================

    def predict_timeout(
        self,
        tag: str,
        plant: str,
        fetch_params: Optional[Dict] = None
    ) -> float:
        """
        Predict optimal timeout for a tag fetch.

        Args:
            tag: PI tag name
            plant: Plant identifier
            fetch_params: Optional dict with fetch_hours, data_age_hours, etc.

        Returns:
            Predicted timeout in seconds
        """
        if not self.enabled or self.timeout_agent is None:
            # Default behavior when agents disabled
            plant_defaults = {
                'ABF': 600.0,
                'PCMSB': 360.0,
                'PCFS': 60.0
            }
            for p, timeout in plant_defaults.items():
                if plant.upper().startswith(p):
                    return timeout
            return 60.0

        return self.timeout_agent.predict_timeout(tag, plant, fetch_params)

    def learn_from_fetch(
        self,
        tag: str,
        plant: str,
        timeout_used: float,
        success: bool,
        actual_time: float,
        fetch_params: Optional[Dict] = None
    ):
        """
        Learn from fetch result (updates both timeout and health agents).

        Args:
            tag: PI tag name
            plant: Plant identifier
            timeout_used: Timeout that was used
            success: Whether fetch succeeded
            actual_time: Actual time taken
            fetch_params: Optional fetch parameters
        """
        if not self.enabled:
            return

        # Update timeout agent
        if self.timeout_agent:
            self.timeout_agent.learn_from_result(
                tag, plant, timeout_used, success, actual_time, fetch_params
            )

        # Update health agent
        if self.health_agent:
            data_quality = {'completeness': 1.0} if success else {'completeness': 0.0}
            self.health_agent.update_tag_status(tag, success, data_quality)

    # ============================================================================
    # TAG HEALTH INTEGRATION
    # ============================================================================

    def should_skip_tag(self, tag: str) -> Tuple[bool, str]:
        """
        Check if a tag should be skipped.

        Args:
            tag: Tag to check

        Returns:
            (should_skip, reason)
        """
        if not self.enabled or self.health_agent is None:
            return False, ""

        return self.health_agent.should_skip_tag(tag)

    def filter_tags(self, tags: List[str], unit: Optional[str] = None) -> Tuple[List[str], List[Dict]]:
        """
        Filter tags, removing dead ones.

        Args:
            tags: List of tags to filter
            unit: Optional unit identifier for logging

        Returns:
            (filtered_tags, skipped_tags_info)
        """
        if not self.enabled or self.health_agent is None:
            return tags, []

        filtered = []
        skipped = []

        for tag in tags:
            should_skip, reason = self.should_skip_tag(tag)
            if should_skip:
                skipped.append({'tag': tag, 'reason': reason})
            else:
                filtered.append(tag)

        if skipped:
            print(f"\n[AGENT_MANAGER] Skipping {len(skipped)} dead tags for {unit or 'unit'}:")
            for info in skipped[:10]:  # Show first 10
                print(f"   - {info['tag']}: {info['reason']}")
            if len(skipped) > 10:
                print(f"   ... and {len(skipped) - 10} more")
            print(f"[AGENT_MANAGER] Fetching {len(filtered)}/{len(tags)} tags")

        return filtered, skipped

    def get_health_report(self, unit: Optional[str] = None) -> Dict:
        """
        Get tag health report.

        Args:
            unit: Optional unit filter

        Returns:
            Health report dictionary
        """
        if not self.enabled or self.health_agent is None:
            return {}

        return self.health_agent.get_health_report(unit)

    def auto_heal_tags(self, config_file: Path) -> int:
        """
        Automatically clean dead tags from config file.

        Args:
            config_file: Path to tag list file

        Returns:
            Number of tags removed
        """
        if not self.enabled or self.health_agent is None:
            return 0

        return self.health_agent.auto_heal_tag_list(config_file)

    # ============================================================================
    # BATCH OPTIMIZATION INTEGRATION
    # ============================================================================

    def calculate_batch_size(
        self,
        unit: str,
        total_records: int,
        columns: int,
        available_memory_gb: Optional[float] = None
    ) -> int:
        """
        Calculate optimal batch size.

        Args:
            unit: Unit identifier
            total_records: Total records to process
            columns: Number of columns
            available_memory_gb: Available memory (auto-detected if None)

        Returns:
            Optimal batch size
        """
        if not self.enabled or self.batch_agent is None:
            # Default behavior
            return min(250_000, total_records)

        return self.batch_agent.calculate_optimal_batch_size(
            unit, total_records, columns, available_memory_gb
        )

    def record_batch_performance(
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
        Record batch processing performance.

        Args:
            unit: Unit identifier
            batch_size: Batch size used
            processing_time: Time taken in seconds
            memory_used_gb: Peak memory used
            success: Whether processing succeeded
            total_records: Total records processed
            columns: Number of columns
        """
        if not self.enabled or self.batch_agent is None:
            return

        self.batch_agent.record_performance(
            unit, batch_size, processing_time, memory_used_gb,
            success, total_records, columns
        )

    # ============================================================================
    # UNIFIED REPORTING
    # ============================================================================

    def print_all_summaries(self):
        """Print summaries for all agents."""
        if not self.enabled:
            print("[AGENT_MANAGER] Agents disabled")
            return

        print("\n" + "="*80)
        print("AI AGENTS PERFORMANCE SUMMARY")
        print("="*80 + "\n")

        if self.timeout_agent:
            self.timeout_agent.print_summary()

        if self.health_agent:
            self.health_agent.print_summary()

        if self.batch_agent:
            self.batch_agent.print_summary()

    def save_all_agents(self):
        """Save state of all agents."""
        if not self.enabled:
            return

        print("\n[AGENT_MANAGER] Saving all agents...")

        if self.timeout_agent:
            self.timeout_agent.save()

        if self.health_agent:
            self.health_agent.save()

        if self.batch_agent:
            self.batch_agent.save()

        print("[AGENT_MANAGER] All agents saved successfully")

    def get_combined_metrics(self) -> Dict:
        """Get metrics from all agents."""
        if not self.enabled:
            return {'enabled': False}

        metrics = {'enabled': True}

        if self.timeout_agent:
            metrics['timeout_predictor'] = self.timeout_agent.get_metrics()

        if self.health_agent:
            metrics['tag_health'] = self.health_agent.get_metrics()

        if self.batch_agent:
            metrics['batch_optimizer'] = self.batch_agent.get_metrics()

        return metrics


# ============================================================================
# CONVENIENCE FUNCTIONS FOR INTEGRATION
# ============================================================================

# Global agent manager instance (singleton pattern)
_global_agent_manager: Optional[AgentManager] = None


def get_agent_manager(enable_agents: bool = True) -> AgentManager:
    """
    Get or create global agent manager instance.

    Args:
        enable_agents: Whether to enable agents

    Returns:
        AgentManager instance
    """
    global _global_agent_manager

    if _global_agent_manager is None:
        _global_agent_manager = AgentManager(enable_agents=enable_agents)

    return _global_agent_manager


def reset_agent_manager():
    """Reset global agent manager (useful for testing)."""
    global _global_agent_manager
    _global_agent_manager = None


# ============================================================================
# INTEGRATION HELPERS FOR EXISTING CODE
# ============================================================================

class FetchTimer:
    """Context manager for timing fetches and learning from results."""

    def __init__(self, agent_manager: AgentManager, tag: str, plant: str,
                 timeout: float, fetch_params: Optional[Dict] = None):
        self.agent_manager = agent_manager
        self.tag = tag
        self.plant = plant
        self.timeout = timeout
        self.fetch_params = fetch_params
        self.start_time = None
        self.success = False

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time

        # Success if no exception
        self.success = exc_type is None

        # Learn from result
        self.agent_manager.learn_from_fetch(
            self.tag,
            self.plant,
            self.timeout,
            self.success,
            elapsed,
            self.fetch_params
        )

        # Don't suppress exceptions
        return False


class BatchTimer:
    """Context manager for timing batch processing and recording performance."""

    def __init__(self, agent_manager: AgentManager, unit: str, batch_size: int,
                 total_records: int, columns: int):
        self.agent_manager = agent_manager
        self.unit = unit
        self.batch_size = batch_size
        self.total_records = total_records
        self.columns = columns
        self.start_time = None
        self.success = False

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        self.success = exc_type is None

        # Estimate memory used (simplified)
        try:
            import psutil
            process = psutil.Process()
            memory_gb = process.memory_info().rss / (1024 ** 3)
        except (ImportError, Exception):
            memory_gb = 0.0

        # Record performance
        self.agent_manager.record_batch_performance(
            self.unit,
            self.batch_size,
            elapsed,
            memory_gb,
            self.success,
            self.total_records,
            self.columns
        )

        return False
