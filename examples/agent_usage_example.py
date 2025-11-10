"""
AI Agents Usage Examples

This script demonstrates how to use the AI agents in TURBOPREDICT.

Run this script to see the agents in action with simulated data.
"""

import time
import random
from pathlib import Path

# Add parent directory to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from pi_monitor.agents import (
    TimeoutPredictionAgent,
    TagHealthAgent,
    AdaptiveBatchAgent,
    AgentManager,
    get_agent_manager
)


def example_1_timeout_prediction():
    """Example 1: Using the Timeout Prediction Agent"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Timeout Prediction Agent")
    print("="*80 + "\n")

    agent = TimeoutPredictionAgent()

    # Simulate tag fetch attempts with different plants
    test_tags = [
        ("PCFS.K-31-01.31FI-001.PV", "PCFS"),
        ("PCFS.K-31-01.31TI-002.PV", "PCFS"),
        ("ABF.07-MT01-1234.PV", "ABF"),
        ("PCMSB.C-02001.C-FI-003.PV", "PCMSB"),
    ]

    print("Training the timeout prediction agent...\n")

    for tag, plant in test_tags:
        print(f"Tag: {tag} (Plant: {plant})")

        # Predict timeout
        timeout = agent.predict_timeout(tag, plant)
        print(f"  Predicted timeout: {timeout:.1f}s")

        # Simulate fetch attempts (5 attempts per tag)
        for attempt in range(5):
            # Simulate fetch with some randomness
            if plant == "ABF":
                # ABF is slow
                actual_time = random.uniform(200, 400)
                success = actual_time < timeout * 0.9
            elif plant == "PCMSB":
                # PCMSB is medium
                actual_time = random.uniform(50, 150)
                success = actual_time < timeout * 0.9
            else:
                # PCFS is fast
                actual_time = random.uniform(5, 30)
                success = actual_time < timeout * 0.9

            # Learn from result
            agent.learn_from_result(tag, plant, timeout, success, actual_time)

            # Get new prediction
            timeout = agent.predict_timeout(tag, plant)

            print(f"  Attempt {attempt + 1}: {actual_time:.1f}s "
                  f"({'SUCCESS' if success else 'FAILED'}), "
                  f"New timeout: {timeout:.1f}s")

        print()

    # Show summary
    agent.print_summary()

    # Get recommendations
    recommendations = agent.get_recommendations()
    print("Recommendations:")
    print(f"  Dead tags: {len(recommendations['dead_tags'])}")
    print(f"  Total tags analyzed: {recommendations['total_tags_analyzed']}")

    # Save agent
    agent.save()


def example_2_tag_health():
    """Example 2: Using the Tag Health Agent"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Tag Health Monitoring Agent")
    print("="*80 + "\n")

    agent = TagHealthAgent()

    # Simulate tag monitoring
    test_tags = {
        "PCFS.K-31-01.31FI-001.PV": 0.95,  # Healthy tag (95% success)
        "PCFS.K-31-01.31FI-002.PV": 0.45,  # Sick tag (45% success)
        "PCFS.K-31-01.31FI-003.PV": 0.05,  # Dead tag (5% success)
        "PCFS.K-31-01.31TI-001.PV": 0.85,  # Healthy tag
        "PCFS.K-31-01.31TI-002.PV": 0.03,  # Dead tag
    }

    print("Monitoring tag health...\n")

    for tag, success_rate in test_tags.items():
        print(f"Tag: {tag} (Expected success rate: {success_rate:.0%})")

        # Simulate 20 fetch attempts
        for _ in range(20):
            success = random.random() < success_rate
            agent.update_tag_status(tag, success)

        # Check classification
        should_skip, reason = agent.should_skip_tag(tag)
        print(f"  Classification: {agent.tag_stats[tag]['health_status']}")
        if should_skip:
            print(f"  SKIP: {reason}")
        print()

    # Show summary
    agent.print_summary()

    # Get health report
    report = agent.get_health_report(unit="K-31-01")
    print("Health Report:")
    print(f"  Total tags: {report['total_tags']}")
    print(f"  Healthy: {report['healthy_tags']}")
    print(f"  Sick: {report['sick_tags']}")
    print(f"  Dead: {report['dead_tags']}")
    print(f"\nRecommendations:")
    for rec in report['recommendations']:
        print(f"  - {rec}")

    # Save agent
    agent.save()


def example_3_batch_optimization():
    """Example 3: Using the Adaptive Batch Agent"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Adaptive Batch Size Agent")
    print("="*80 + "\n")

    agent = AdaptiveBatchAgent()

    # Simulate batch processing for different units
    test_units = [
        ("K-31-01", 15_000_000, 120),  # Large unit with many columns
        ("K-16-01", 5_000_000, 80),    # Medium unit
        ("K-12-01", 1_000_000, 50),    # Small unit
    ]

    print("Optimizing batch sizes...\n")

    for unit, total_records, columns in test_units:
        print(f"Unit: {unit} ({total_records:,} records, {columns} columns)")

        # Calculate initial batch size
        batch_size = agent.calculate_optimal_batch_size(
            unit, total_records, columns, available_memory_gb=8.0
        )
        print(f"  Initial batch size: {batch_size:,} rows")

        # Simulate 5 processing runs with different batch sizes
        for run in range(5):
            # Vary batch size a bit
            test_batch = int(batch_size * random.uniform(0.8, 1.2))
            test_batch = max(50_000, min(test_batch, 1_000_000))

            # Simulate processing time (larger batches = more time, but not linear)
            processing_time = (test_batch / 100_000) * random.uniform(5, 15)

            # Simulate memory usage
            memory_used = (test_batch * columns * 8) / (1024 ** 3) * random.uniform(0.9, 1.1)

            # Success if memory not too high
            success = memory_used < 6.0

            print(f"  Run {run + 1}: batch={test_batch:,}, "
                  f"time={processing_time:.1f}s, "
                  f"memory={memory_used:.2f}GB, "
                  f"{'SUCCESS' if success else 'FAILED'}")

            # Record performance
            agent.record_performance(
                unit, test_batch, processing_time, memory_used,
                success, total_records, columns
            )

            # Get new optimal batch size
            batch_size = agent.calculate_optimal_batch_size(
                unit, total_records, columns, available_memory_gb=8.0
            )

        print(f"  Final optimal batch size: {batch_size:,} rows\n")

    # Show summary
    agent.print_summary()

    # Get recommendations
    recommendations = agent.get_recommendations()
    print("Recommendations:")
    for unit_rec in recommendations['units']:
        print(f"  {unit_rec['unit']}:")
        print(f"    Optimal batch: {unit_rec['optimal_batch']:,} rows")
        print(f"    Throughput: {unit_rec['throughput']}")
        print(f"    Success rate: {unit_rec['success_rate']}")

    # Save agent
    agent.save()


def example_4_integrated_manager():
    """Example 4: Using the AgentManager for integrated operations"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Integrated Agent Manager")
    print("="*80 + "\n")

    # Get global agent manager
    manager = get_agent_manager(enable_agents=True)

    # Example: Fetch operation with timeout prediction and health monitoring
    tags_to_fetch = [
        "PCFS.K-31-01.31FI-001.PV",
        "PCFS.K-31-01.31FI-002.PV",
        "PCFS.K-31-01.31FI-003.PV",
    ]

    plant = "PCFS"
    unit = "K-31-01"

    print(f"Fetching {len(tags_to_fetch)} tags for {unit}...\n")

    # First, train the agents with some data
    for tag in tags_to_fetch:
        for _ in range(10):
            timeout = manager.predict_timeout(tag, plant)
            success = random.random() < 0.7
            actual_time = random.uniform(5, 30) if success else timeout
            manager.learn_from_fetch(tag, plant, timeout, success, actual_time)

    # Filter tags (remove dead ones)
    filtered_tags, skipped = manager.filter_tags(tags_to_fetch, unit)

    print(f"Active tags: {len(filtered_tags)}/{len(tags_to_fetch)}")
    print(f"Skipped tags: {len(skipped)}\n")

    # Batch processing example
    total_records = 5_000_000
    columns = 80

    batch_size = manager.calculate_batch_size(unit, total_records, columns)
    print(f"Optimal batch size for {unit}: {batch_size:,} rows\n")

    # Simulate batch processing
    processing_time = random.uniform(10, 30)
    memory_used = random.uniform(2.0, 4.0)
    manager.record_batch_performance(
        unit, batch_size, processing_time, memory_used,
        True, total_records, columns
    )

    # Show all summaries
    manager.print_all_summaries()

    # Save all agents
    manager.save_all_agents()

    # Get combined metrics
    metrics = manager.get_combined_metrics()
    print("\nCombined Metrics:")
    for agent_name, agent_metrics in metrics.items():
        if agent_name != 'enabled':
            print(f"\n{agent_name}:")
            for key, value in agent_metrics.items():
                print(f"  {key}: {value}")


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("TURBOPREDICT AI AGENTS - USAGE EXAMPLES")
    print("="*80)

    examples = [
        ("Timeout Prediction", example_1_timeout_prediction),
        ("Tag Health Monitoring", example_2_tag_health),
        ("Batch Optimization", example_3_batch_optimization),
        ("Integrated Manager", example_4_integrated_manager),
    ]

    for i, (name, func) in enumerate(examples, 1):
        print(f"\n{'='*80}")
        print(f"Running Example {i}: {name}")
        print(f"{'='*80}")

        try:
            func()
        except Exception as e:
            print(f"\n[ERROR] Example failed: {e}")
            import traceback
            traceback.print_exc()

        if i < len(examples):
            input("\nPress Enter to continue to next example...")

    print("\n" + "="*80)
    print("ALL EXAMPLES COMPLETED!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
