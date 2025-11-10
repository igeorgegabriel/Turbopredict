"""
Intelligent Timeout Prediction Agent

Machine learning agent that predicts optimal timeouts for each tag/plant combination.
Learns from historical success/failure patterns to minimize wasted wait time.

Expected Impact: 40-60% reduction in total fetch time
"""

import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .base_agent import BaseAgent


def percentile(data: List[float], pct: float) -> float:
    """Calculate percentile without numpy dependency."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    d0 = sorted_data[f] * (c - k)
    d1 = sorted_data[c] * (k - f)
    return d0 + d1


class TimeoutPredictionAgent(BaseAgent):
    """
    Predicts optimal timeout values for PI data fetches based on historical performance.
    """

    def __init__(self):
        super().__init__(name='timeout_predictor', version='1.0.0')

        # History database: tag -> list of fetch attempts
        self.history_db: Dict[str, List[Dict]] = defaultdict(list)

        # Learned timeout profiles: tag -> optimal timeout
        self.timeout_profiles: Dict[str, float] = {}

        # Known dead tags: tags that consistently fail
        self.dead_tags: set = set()

        # Plant-specific base timeouts (from domain knowledge)
        self.plant_base_timeouts = {
            'ABF': 600.0,    # ABF is slow
            'PCMSB': 360.0,  # PCMSB is medium
            'PCFS': 60.0,    # PCFS is fast
        }

        # Configuration
        self.min_timeout = 10.0    # Minimum timeout in seconds
        self.max_timeout = 600.0   # Maximum timeout in seconds
        self.safety_margin = 1.2   # Add 20% safety margin
        self.history_limit = 50    # Keep last 50 attempts per tag
        self.dead_tag_threshold = 0.9  # 90% failure rate = dead

        # Try to load existing state
        self.load()

    def predict_timeout(self, tag: str, plant: str, fetch_params: Optional[Dict] = None) -> float:
        """
        Predict optimal timeout for a specific fetch operation.

        Args:
            tag: PI tag name
            plant: Plant identifier (ABF, PCMSB, PCFS, etc.)
            fetch_params: Optional dict with fetch_hours, data_age_hours, etc.

        Returns:
            Predicted timeout in seconds
        """
        # Check if tag is known to be dead
        if tag in self.dead_tags:
            # Return minimum timeout for dead tags
            return self.min_timeout

        # If we have learned profile, use it
        if tag in self.timeout_profiles:
            learned_timeout = self.timeout_profiles[tag]
            self.metrics['predictions_made'] += 1
            self.log_performance('prediction', {
                'tag': tag,
                'plant': plant,
                'timeout': learned_timeout,
                'source': 'learned_profile'
            })
            return learned_timeout

        # Otherwise, use plant-based heuristic
        base_timeout = self._get_plant_base_timeout(plant)

        # Adjust based on fetch parameters if provided
        if fetch_params:
            adjusted_timeout = self._adjust_for_fetch_params(base_timeout, fetch_params)
        else:
            adjusted_timeout = base_timeout

        self.metrics['predictions_made'] += 1
        self.log_performance('prediction', {
            'tag': tag,
            'plant': plant,
            'timeout': adjusted_timeout,
            'source': 'heuristic'
        })

        return adjusted_timeout

    def learn_from_result(
        self,
        tag: str,
        plant: str,
        timeout_used: float,
        success: bool,
        actual_time: float,
        fetch_params: Optional[Dict] = None
    ):
        """
        Update model with fetch result.

        Args:
            tag: PI tag name
            plant: Plant identifier
            timeout_used: Timeout value that was used
            success: Whether fetch succeeded
            actual_time: Actual time taken (or timeout if failed)
            fetch_params: Optional fetch parameters
        """
        # Record attempt in history
        attempt = {
            'timestamp': datetime.now(),
            'timeout': timeout_used,
            'success': success,
            'actual_time': actual_time,
            'plant': plant,
            'fetch_params': fetch_params or {}
        }

        self.history_db[tag].append(attempt)

        # Keep only recent history
        if len(self.history_db[tag]) > self.history_limit:
            self.history_db[tag] = self.history_db[tag][-self.history_limit:]

        self.metrics['learning_iterations'] += 1

        # Update timeout profile and dead tag detection
        self._update_tag_profile(tag)

        # Periodic save (every 10 learning iterations)
        if self.metrics['learning_iterations'] % 10 == 0:
            self.save()

        self.log_performance('learning', {
            'tag': tag,
            'success': success,
            'timeout_used': timeout_used,
            'actual_time': actual_time
        })

    def _update_tag_profile(self, tag: str):
        """Update learned timeout profile and dead tag detection for a tag."""
        history = self.history_db[tag]

        if len(history) < 5:
            # Not enough data yet
            return

        # Calculate success rate
        recent_attempts = history[-20:]  # Last 20 attempts
        successes = [a for a in recent_attempts if a['success']]
        success_rate = len(successes) / len(recent_attempts)

        # Update accuracy metric
        self.metrics['accuracy'] = success_rate

        # Check if tag is dead
        if success_rate < (1 - self.dead_tag_threshold):
            # Less than 10% success rate = dead tag
            if tag not in self.dead_tags:
                print(f"[TIMEOUT_AGENT] Marking tag as DEAD: {tag} (success rate: {success_rate:.1%})")
                self.dead_tags.add(tag)
                # Use minimum timeout for dead tags
                self.timeout_profiles[tag] = self.min_timeout
            return

        # Tag is alive, calculate optimal timeout
        if not successes:
            return

        # Calculate optimal timeout from successful attempts
        successful_times = [a['actual_time'] for a in successes]

        # Use 90th percentile of successful times
        optimal_time = percentile(successful_times, 90)

        # Add safety margin
        optimal_timeout = optimal_time * self.safety_margin

        # Clamp to reasonable bounds
        optimal_timeout = max(self.min_timeout, min(optimal_timeout, self.max_timeout))

        # Update profile
        old_timeout = self.timeout_profiles.get(tag, None)
        self.timeout_profiles[tag] = optimal_timeout

        if old_timeout is not None:
            time_saved = old_timeout - optimal_timeout
            self.metrics['total_time_saved'] += max(0, time_saved)

    def _get_plant_base_timeout(self, plant: str) -> float:
        """Get base timeout for a plant."""
        plant_upper = plant.upper()

        for known_plant, timeout in self.plant_base_timeouts.items():
            if plant_upper.startswith(known_plant):
                return timeout

        # Default timeout for unknown plants
        return 60.0

    def _adjust_for_fetch_params(self, base_timeout: float, fetch_params: Dict) -> float:
        """Adjust timeout based on fetch parameters."""
        adjusted = base_timeout

        # Larger time windows need more time
        fetch_hours = fetch_params.get('fetch_hours', 1.0)
        if fetch_hours > 24:
            adjusted *= 1.5
        elif fetch_hours > 168:  # 1 week
            adjusted *= 2.0

        # Very stale data might need more time
        data_age_hours = fetch_params.get('data_age_hours', 0)
        if data_age_hours > 168:  # More than a week old
            adjusted *= 1.3

        return min(adjusted, self.max_timeout)

    def get_recommendations(self) -> Dict:
        """Provide optimization recommendations based on learned patterns."""
        recommendations = {
            'tags_to_skip': [],
            'timeout_adjustments': [],
            'dead_tags': list(self.dead_tags),
            'total_tags_analyzed': len(self.history_db)
        }

        # Identify tags to skip (dead tags)
        for tag in self.dead_tags:
            history = self.history_db.get(tag, [])
            if history:
                last_attempt = history[-1]
                days_since = (datetime.now() - last_attempt['timestamp']).days
                recommendations['tags_to_skip'].append({
                    'tag': tag,
                    'reason': f'Dead tag (no successful fetches in {days_since} days)',
                    'success_rate': self._get_success_rate(tag)
                })

        # Suggest timeout adjustments
        for tag, timeout in self.timeout_profiles.items():
            if tag not in self.dead_tags:
                success_rate = self._get_success_rate(tag)
                if success_rate > 0.8:  # Good success rate
                    recommendations['timeout_adjustments'].append({
                        'tag': tag,
                        'recommended_timeout': timeout,
                        'success_rate': success_rate
                    })

        return recommendations

    def _get_success_rate(self, tag: str) -> float:
        """Calculate success rate for a tag."""
        history = self.history_db.get(tag, [])
        if not history:
            return 0.0

        recent = history[-20:]  # Last 20 attempts
        successes = sum(1 for a in recent if a['success'])
        return successes / len(recent)

    def get_state(self) -> Dict:
        """Get agent state for serialization."""
        return {
            'history_db': dict(self.history_db),
            'timeout_profiles': self.timeout_profiles,
            'dead_tags': list(self.dead_tags)
        }

    def set_state(self, state: Dict):
        """Set agent state from deserialized data."""
        self.history_db = defaultdict(list, state.get('history_db', {}))
        self.timeout_profiles = state.get('timeout_profiles', {})
        self.dead_tags = set(state.get('dead_tags', []))

    def print_summary(self):
        """Print a summary of learned patterns."""
        print(f"\n{'='*70}")
        print(f"TIMEOUT PREDICTION AGENT SUMMARY")
        print(f"{'='*70}")
        print(f"Total tags analyzed: {len(self.history_db)}")
        print(f"Dead tags identified: {len(self.dead_tags)}")
        print(f"Learned timeout profiles: {len(self.timeout_profiles)}")
        print(f"Predictions made: {self.metrics['predictions_made']}")
        print(f"Learning iterations: {self.metrics['learning_iterations']}")
        print(f"Total time saved: {self.metrics['total_time_saved']:.1f}s ({self.metrics['total_time_saved']/60:.1f}min)")

        if self.dead_tags:
            print(f"\nDead Tags ({len(self.dead_tags)}):")
            for tag in list(self.dead_tags)[:10]:  # Show first 10
                success_rate = self._get_success_rate(tag)
                print(f"  - {tag} (success rate: {success_rate:.1%})")

        if self.timeout_profiles:
            print(f"\nTop Optimized Tags:")
            sorted_profiles = sorted(
                self.timeout_profiles.items(),
                key=lambda x: x[1]
            )[:10]
            for tag, timeout in sorted_profiles:
                if tag not in self.dead_tags:
                    success_rate = self._get_success_rate(tag)
                    print(f"  - {tag}: {timeout:.1f}s (success: {success_rate:.1%})")

        print(f"{'='*70}\n")
