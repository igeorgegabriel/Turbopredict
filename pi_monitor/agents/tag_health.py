"""
Smart Tag Health Monitoring Agent

Monitors tag health and automatically manages skip lists.
Detects patterns: dead tags, intermittent tags, healthy tags.

Expected Impact: 30-50% faster refresh by skipping dead tags
"""

from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .base_agent import BaseAgent


class TagHealthAgent(BaseAgent):
    """
    Monitors tag health and automatically manages skip lists.
    """

    def __init__(self):
        super().__init__(name='tag_health', version='1.0.0')

        # Tag statistics database
        self.tag_stats: Dict[str, Dict] = {}

        # Classification sets
        self.skip_list: set = set()   # Dead tags to skip
        self.watch_list: set = set()  # Sick tags to monitor
        self.healthy_tags: set = set()

        # Thresholds
        self.dead_threshold = 0.1       # <10% success = dead
        self.sick_threshold = 0.5       # <50% success = sick
        self.min_attempts = 5           # Minimum attempts before classification
        self.consecutive_failures_dead = 20  # 20 straight failures = dead

        # Try to load existing state
        self.load()

    def update_tag_status(self, tag: str, success: bool, data_quality: Optional[Dict] = None):
        """
        Record tag fetch result and update classification.

        Args:
            tag: PI tag name
            success: Whether fetch was successful
            data_quality: Optional dict with quality metrics (completeness, etc.)
        """
        # Initialize stats if new tag
        if tag not in self.tag_stats:
            self.tag_stats[tag] = {
                'total_attempts': 0,
                'successes': 0,
                'failures': 0,
                'last_success': None,
                'last_failure': None,
                'consecutive_failures': 0,
                'consecutive_successes': 0,
                'data_quality_avg': 0.0,
                'first_seen': datetime.now(),
                'health_status': 'UNKNOWN'
            }

        stats = self.tag_stats[tag]
        stats['total_attempts'] += 1

        if success:
            stats['successes'] += 1
            stats['consecutive_failures'] = 0
            stats['consecutive_successes'] += 1
            stats['last_success'] = datetime.now()

            # Update data quality
            if data_quality:
                completeness = data_quality.get('completeness', 1.0)
                # Exponential moving average
                alpha = 0.1
                stats['data_quality_avg'] = (
                    stats['data_quality_avg'] * (1 - alpha) + completeness * alpha
                )
        else:
            stats['failures'] += 1
            stats['consecutive_failures'] += 1
            stats['consecutive_successes'] = 0
            stats['last_failure'] = datetime.now()

        # Classify tag health
        self._classify_tag_health(tag)

        # Update metrics
        self.metrics['total_tags'] = len(self.tag_stats)
        self.metrics['dead_tags'] = len(self.skip_list)
        self.metrics['sick_tags'] = len(self.watch_list)
        self.metrics['healthy_tags'] = len(self.healthy_tags)

        # Log the update
        self.log_performance('tag_update', {
            'tag': tag,
            'success': success,
            'health_status': stats['health_status']
        })

        # Periodic save
        if stats['total_attempts'] % 10 == 0:
            self.save()

    def _classify_tag_health(self, tag: str) -> str:
        """
        Classify tag as DEAD, SICK, HEALTHY, or UNKNOWN.

        Args:
            tag: Tag to classify

        Returns:
            Health status string
        """
        stats = self.tag_stats[tag]

        if stats['total_attempts'] < self.min_attempts:
            stats['health_status'] = 'UNKNOWN'
            return 'UNKNOWN'

        success_rate = stats['successes'] / stats['total_attempts']

        # Remove from old classifications
        self.skip_list.discard(tag)
        self.watch_list.discard(tag)
        self.healthy_tags.discard(tag)

        # DEAD: <10% success rate OR 20+ consecutive failures
        if success_rate < self.dead_threshold or stats['consecutive_failures'] >= self.consecutive_failures_dead:
            self.skip_list.add(tag)
            stats['health_status'] = 'DEAD'
            return 'DEAD'

        # SICK: 10-50% success rate
        if success_rate < self.sick_threshold:
            self.watch_list.add(tag)
            stats['health_status'] = 'SICK'
            return 'SICK'

        # HEALTHY: >50% success rate
        self.healthy_tags.add(tag)
        stats['health_status'] = 'HEALTHY'
        return 'HEALTHY'

    def should_skip_tag(self, tag: str) -> Tuple[bool, str]:
        """
        Determine if tag should be skipped.

        Args:
            tag: Tag to check

        Returns:
            (skip: bool, reason: str)
        """
        if tag in self.skip_list:
            stats = self.tag_stats.get(tag, {})

            # Calculate how long it's been dead
            last_success = stats.get('last_success')
            if last_success:
                days_dead = (datetime.now() - last_success).days
                reason = f"Dead tag (no data for {days_dead} days)"
            else:
                reason = f"Dead tag (never successful)"

            success_rate = self._get_success_rate(tag)
            reason += f" - success rate: {success_rate:.1%}"

            return True, reason

        return False, ""

    def _get_success_rate(self, tag: str) -> float:
        """Get success rate for a tag."""
        stats = self.tag_stats.get(tag, {})
        total = stats.get('total_attempts', 0)
        if total == 0:
            return 0.0
        return stats.get('successes', 0) / total

    def get_health_report(self, unit: Optional[str] = None) -> Dict:
        """
        Generate tag health report.

        Args:
            unit: Optional unit filter

        Returns:
            Report dictionary with statistics and recommendations
        """
        report = {
            'total_tags': len(self.tag_stats),
            'dead_tags': len(self.skip_list),
            'sick_tags': len(self.watch_list),
            'healthy_tags': len(self.healthy_tags),
            'unknown_tags': 0,
            'dead_tag_list': [],
            'sick_tag_list': [],
            'recommendations': []
        }

        for tag, stats in self.tag_stats.items():
            # Filter by unit if specified
            if unit and unit not in tag:
                continue

            if stats['health_status'] == 'UNKNOWN':
                report['unknown_tags'] += 1

            if stats['health_status'] == 'DEAD':
                last_success = stats.get('last_success')
                days_since = (datetime.now() - last_success).days if last_success else 999

                report['dead_tag_list'].append({
                    'tag': tag,
                    'last_success': last_success.isoformat() if last_success else None,
                    'consecutive_failures': stats['consecutive_failures'],
                    'success_rate': self._get_success_rate(tag),
                    'days_dead': days_since
                })

            elif stats['health_status'] == 'SICK':
                report['sick_tag_list'].append({
                    'tag': tag,
                    'success_rate': self._get_success_rate(tag),
                    'consecutive_failures': stats['consecutive_failures'],
                    'last_success': stats.get('last_success').isoformat() if stats.get('last_success') else None
                })

        # Generate recommendations
        if len(report['dead_tag_list']) > 10:
            report['recommendations'].append(
                f"Remove {len(report['dead_tag_list'])} dead tags from config files to save fetch time"
            )

        if len(report['sick_tag_list']) > 5:
            report['recommendations'].append(
                f"Investigate {len(report['sick_tag_list'])} intermittent tags for potential PI server issues"
            )

        # Calculate potential time savings
        time_saved_per_refresh = len(report['dead_tag_list']) * 30  # Assume 30s avg per dead tag
        if time_saved_per_refresh > 60:
            report['recommendations'].append(
                f"Skipping dead tags saves ~{time_saved_per_refresh/60:.1f} minutes per refresh cycle"
            )

        return report

    def auto_heal_tag_list(self, config_file: Path) -> int:
        """
        Automatically update tag list by removing dead tags.

        Args:
            config_file: Path to tag list file (e.g., tags_pcfs_k1201.txt)

        Returns:
            Number of tags removed
        """
        if not config_file.exists():
            print(f"[TAG_HEALTH] Config file not found: {config_file}")
            return 0

        # Read current tags
        try:
            tags = config_file.read_text(encoding='utf-8').splitlines()
        except Exception as e:
            print(f"[TAG_HEALTH] Error reading {config_file}: {e}")
            return 0

        original_count = len(tags)

        # Keep only non-dead tags
        healthy_tags = []
        removed_tags = []

        for line in tags:
            tag = line.strip()

            # Keep comments and empty lines
            if not tag or tag.startswith('#'):
                healthy_tags.append(line)
                continue

            # Check if tag is dead
            if tag in self.skip_list:
                removed_tags.append(tag)
                # Add as comment showing it was removed
                healthy_tags.append(f"# REMOVED (dead tag): {tag}")
            else:
                healthy_tags.append(line)

        # Write back
        try:
            config_file.write_text('\n'.join(healthy_tags), encoding='utf-8')
            print(f"[TAG_HEALTH] Auto-healed {config_file}")
            print(f"[TAG_HEALTH] Removed {len(removed_tags)} dead tags")
            for tag in removed_tags:
                success_rate = self._get_success_rate(tag)
                print(f"  - {tag} (success: {success_rate:.1%})")
        except Exception as e:
            print(f"[TAG_HEALTH] Error writing {config_file}: {e}")
            return 0

        return len(removed_tags)

    def get_state(self) -> Dict:
        """Get agent state for serialization."""
        return {
            'tag_stats': self.tag_stats,
            'skip_list': list(self.skip_list),
            'watch_list': list(self.watch_list),
            'healthy_tags': list(self.healthy_tags)
        }

    def set_state(self, state: Dict):
        """Set agent state from deserialized data."""
        self.tag_stats = state.get('tag_stats', {})
        self.skip_list = set(state.get('skip_list', []))
        self.watch_list = set(state.get('watch_list', []))
        self.healthy_tags = set(state.get('healthy_tags', []))

        # Update metrics
        self.metrics['total_tags'] = len(self.tag_stats)
        self.metrics['dead_tags'] = len(self.skip_list)
        self.metrics['sick_tags'] = len(self.watch_list)
        self.metrics['healthy_tags'] = len(self.healthy_tags)

    def print_summary(self):
        """Print a summary of tag health."""
        print(f"\n{'='*70}")
        print(f"TAG HEALTH AGENT SUMMARY")
        print(f"{'='*70}")
        print(f"Total tags monitored: {len(self.tag_stats)}")
        print(f"Healthy tags: {len(self.healthy_tags)} ({len(self.healthy_tags)/max(1,len(self.tag_stats))*100:.1f}%)")
        print(f"Sick tags: {len(self.watch_list)} ({len(self.watch_list)/max(1,len(self.tag_stats))*100:.1f}%)")
        print(f"Dead tags: {len(self.skip_list)} ({len(self.skip_list)/max(1,len(self.tag_stats))*100:.1f}%)")

        if self.skip_list:
            print(f"\nDead Tags (Top 10):")
            dead_list = sorted(
                [(tag, self.tag_stats[tag]) for tag in self.skip_list if tag in self.tag_stats],
                key=lambda x: x[1]['total_attempts'],
                reverse=True
            )[:10]

            for tag, stats in dead_list:
                success_rate = self._get_success_rate(tag)
                print(f"  - {tag}")
                print(f"    Success: {success_rate:.1%} ({stats['successes']}/{stats['total_attempts']})")
                print(f"    Consecutive failures: {stats['consecutive_failures']}")

        if self.watch_list:
            print(f"\nSick Tags (Top 10):")
            sick_list = sorted(
                [(tag, self.tag_stats[tag]) for tag in self.watch_list if tag in self.tag_stats],
                key=lambda x: self._get_success_rate(x[0])
            )[:10]

            for tag, stats in sick_list:
                success_rate = self._get_success_rate(tag)
                print(f"  - {tag}: {success_rate:.1%} success rate")

        # Calculate time savings
        time_saved = len(self.skip_list) * 30  # Assume 30s per dead tag
        print(f"\nEstimated time saved per refresh: {time_saved:.0f}s ({time_saved/60:.1f}min)")
        print(f"{'='*70}\n")
