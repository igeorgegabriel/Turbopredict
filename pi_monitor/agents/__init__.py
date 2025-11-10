"""
AI Agents for TURBOPREDICT System Enhancement

This module contains intelligent agents that learn from system behavior
and automatically optimize performance.

Priority 1 Agents (Quick Wins):
- TimeoutPredictionAgent: Predicts optimal timeouts to reduce fetch time
- TagHealthAgent: Monitors tag health and manages skip lists
- AdaptiveBatchAgent: Optimizes batch sizes based on data characteristics

Integration:
- AgentManager: Unified manager for all agents
- get_agent_manager(): Get global agent manager instance
"""

from .base_agent import BaseAgent
from .timeout_predictor import TimeoutPredictionAgent
from .tag_health import TagHealthAgent
from .batch_optimizer import AdaptiveBatchAgent
from .integration import AgentManager, get_agent_manager, FetchTimer, BatchTimer

__all__ = [
    'BaseAgent',
    'TimeoutPredictionAgent',
    'TagHealthAgent',
    'AdaptiveBatchAgent',
    'AgentManager',
    'get_agent_manager',
    'FetchTimer',
    'BatchTimer',
]

__version__ = '1.0.0'
