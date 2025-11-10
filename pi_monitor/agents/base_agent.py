"""
Base Agent Class for TURBOPREDICT AI Agents

All agents inherit from this base class to ensure consistent
behavior, persistence, and monitoring capabilities.
"""

import json
import pickle
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


class BaseAgent(ABC):
    """Base class for all AI agents in the system."""

    def __init__(self, name: str, version: str = "1.0.0"):
        """
        Initialize the base agent.

        Args:
            name: Agent identifier (e.g., 'timeout_predictor')
            version: Agent version for model compatibility
        """
        self.name = name
        self.version = version
        self.created_at = datetime.now()
        self.last_updated = datetime.now()

        # Paths
        self.model_dir = Path("models/agents")
        self.model_dir.mkdir(parents=True, exist_ok=True)

        self.data_dir = Path("data/agent_training")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.log_dir = Path("logs/agent_performance")
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Performance tracking
        self.metrics = {
            'predictions_made': 0,
            'learning_iterations': 0,
            'accuracy': 0.0,
            'total_time_saved': 0.0
        }

    def save(self, suffix: str = "") -> Path:
        """
        Save agent state to disk.

        Args:
            suffix: Optional suffix for the filename (e.g., 'backup')

        Returns:
            Path to saved file
        """
        filename = f"{self.name}_v{self.version}"
        if suffix:
            filename += f"_{suffix}"
        filename += ".pkl"

        filepath = self.model_dir / filename

        save_data = {
            'name': self.name,
            'version': self.version,
            'created_at': self.created_at,
            'last_updated': datetime.now(),
            'metrics': self.metrics,
            'state': self.get_state()
        }

        with open(filepath, 'wb') as f:
            pickle.dump(save_data, f)

        print(f"[AGENT] Saved {self.name} to {filepath}")
        return filepath

    def load(self, filepath: Optional[Path] = None) -> bool:
        """
        Load agent state from disk.

        Args:
            filepath: Path to saved agent file. If None, uses default path.

        Returns:
            True if loaded successfully, False otherwise
        """
        if filepath is None:
            filepath = self.model_dir / f"{self.name}_v{self.version}.pkl"

        if not filepath.exists():
            print(f"[AGENT] No saved state found at {filepath}")
            return False

        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)

            self.created_at = data['created_at']
            self.last_updated = data['last_updated']
            self.metrics = data['metrics']
            self.set_state(data['state'])

            print(f"[AGENT] Loaded {self.name} from {filepath}")
            print(f"[AGENT] Last updated: {self.last_updated}")
            print(f"[AGENT] Predictions made: {self.metrics['predictions_made']}")
            return True

        except Exception as e:
            print(f"[AGENT] Error loading {filepath}: {e}")
            return False

    @abstractmethod
    def get_state(self) -> Dict[str, Any]:
        """
        Get current agent state for serialization.

        Returns:
            Dictionary containing agent state
        """
        pass

    @abstractmethod
    def set_state(self, state: Dict[str, Any]):
        """
        Set agent state from deserialized data.

        Args:
            state: Dictionary containing agent state
        """
        pass

    def log_performance(self, event_type: str, data: Dict[str, Any]):
        """
        Log performance metrics for monitoring.

        Args:
            event_type: Type of event (e.g., 'prediction', 'learning')
            data: Event data to log
        """
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'agent': self.name,
            'event_type': event_type,
            'data': data
        }

        log_file = self.log_dir / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.jsonl"

        with open(log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current performance metrics.

        Returns:
            Dictionary of metrics
        """
        return {
            'agent': self.name,
            'version': self.version,
            'created_at': self.created_at.isoformat(),
            'last_updated': self.last_updated.isoformat(),
            **self.metrics
        }

    def print_stats(self):
        """Print agent statistics in a nice format."""
        print(f"\n{'='*60}")
        print(f"Agent: {self.name} v{self.version}")
        print(f"{'='*60}")
        print(f"Created: {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Updated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nMetrics:")
        for key, value in self.metrics.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")
        print(f"{'='*60}\n")
