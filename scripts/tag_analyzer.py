#!/usr/bin/env python3
"""
Industrial Tag Analyzer for TurboPredict

Analyzes all available tags across units and identifies potential speed/flow/control tags
based on naming patterns and data characteristics.
"""

import json
import re
from pathlib import Path
import glob
from collections import defaultdict


def analyze_all_tags():
    """Analyze all available tags across all units"""

    # Find all tag directories using Windows-compatible paths
    import os

    unit_tags = defaultdict(list)
    all_tags = set()

    # Walk through the dataset directory
    dataset_dir = Path("data/processed/dataset")
    if not dataset_dir.exists():
        print(f"Dataset directory not found: {dataset_dir}")
        return unit_tags, all_tags

    for plant_dir in dataset_dir.iterdir():
        if plant_dir.is_dir() and plant_dir.name.startswith('plant='):
            for unit_dir in plant_dir.iterdir():
                if unit_dir.is_dir() and unit_dir.name.startswith('unit='):
                    unit_name = unit_dir.name.replace('unit=', '')

                    for tag_dir in unit_dir.iterdir():
                        if tag_dir.is_dir() and tag_dir.name.startswith('tag='):
                            tag_name = tag_dir.name.replace('tag=', '')
                            unit_tags[unit_name].append(tag_name)
                            all_tags.add(tag_name)

    print(f"Found {len(unit_tags)} units with {len(all_tags)} unique tags")
    return unit_tags, all_tags


def categorize_tag(tag_name):
    """Categorize tag based on naming patterns"""
    tag_lower = tag_name.lower()

    categories = []

    # Flow indicators (FI, FV, etc.)
    if re.search(r'fi[-_]?\d+', tag_lower):
        categories.append('flow_indicator')

    # Temperature indicators (TI, TT, etc.)
    if re.search(r'ti[ac]?[-_]?\d+', tag_lower):
        categories.append('temperature')

    # Pressure indicators (PI, PT, etc.)
    if re.search(r'pi[-_]?\d+', tag_lower):
        categories.append('pressure')

    # Level indicators (LI, LT, etc.)
    if re.search(r'li[ac]?[-_]?\d+', tag_lower):
        categories.append('level')

    # Control valves (CV, FV, LV, etc.)
    if re.search(r'[fl]v[-_]?\d+', tag_lower) or 'mv' in tag_lower:
        categories.append('control_valve')

    # Motor/Drive related
    if any(keyword in tag_lower for keyword in ['motor', 'drive', 'speed', 'rpm']):
        categories.append('motor_drive')

    # Flow control (FHC, FIC, etc.)
    if re.search(r'f[hic]+[-_]?\d+', tag_lower):
        categories.append('flow_control')

    # Analyzer (XIA, etc.)
    if re.search(r'xia[-_]?\d+', tag_lower):
        categories.append('analyzer')

    # Differential pressure
    if 'pdia' in tag_lower or 'dpia' in tag_lower:
        categories.append('differential_pressure')

    # Special performance indicators
    if 'margin' in tag_lower or 'performance' in tag_lower:
        categories.append('performance')

    return categories if categories else ['unknown']


def generate_speed_config():
    """Generate comprehensive tag configuration"""

    unit_tags, all_tags = analyze_all_tags()

    config = {
        "generated_at": "2025-09-28T07:35:00",
        "description": "Industrial tag configuration for TurboPredict units",
        "scan_summary": {
            "total_units": len(unit_tags),
            "total_unique_tags": len(all_tags),
            "units_scanned": list(unit_tags.keys())
        },
        "units": {},
        "tag_categories": {},
        "potential_speed_tags": {},
        "recommended_monitoring": {}
    }

    # Analyze each unit
    for unit, tags in unit_tags.items():

        # Categorize tags for this unit
        unit_categories = defaultdict(list)
        potential_speed = []

        for tag in tags:
            categories = categorize_tag(tag)

            for category in categories:
                unit_categories[category].append(tag)

            # Identify potential speed/flow control tags
            if any(cat in ['flow_control', 'motor_drive', 'control_valve'] for cat in categories):
                potential_speed.append(tag)

        # Build unit configuration
        config["units"][unit] = {
            "total_tags": len(tags),
            "tag_categories": dict(unit_categories),
            "potential_speed_tags": potential_speed,
            "flow_indicators": unit_categories.get('flow_indicator', []),
            "control_valves": unit_categories.get('control_valve', []),
            "temperature_sensors": unit_categories.get('temperature', []),
            "pressure_sensors": unit_categories.get('pressure', []),
            "level_sensors": unit_categories.get('level', []),
            "analyzers": unit_categories.get('analyzer', []),
            "performance_metrics": unit_categories.get('performance', [])
        }

        # Recommended monitoring tags
        monitoring_tags = []
        monitoring_tags.extend(unit_categories.get('flow_control', [])[:3])  # Top 3 flow control
        monitoring_tags.extend(unit_categories.get('control_valve', [])[:2])  # Top 2 valves
        monitoring_tags.extend(unit_categories.get('performance', []))  # All performance

        config["recommended_monitoring"][unit] = {
            "primary_tags": monitoring_tags[:5],  # Top 5 for monitoring
            "description": f"Key operational tags for {unit} monitoring"
        }

    # Global tag categories summary
    all_categories = defaultdict(list)
    for unit, tags in unit_tags.items():
        for tag in tags:
            categories = categorize_tag(tag)
            for category in categories:
                all_categories[category].append(f"{unit}:{tag}")

    config["tag_categories"] = {
        category: {
            "count": len(tags),
            "sample_tags": tags[:5]  # First 5 examples
        }
        for category, tags in all_categories.items()
    }

    # Potential speed tags across all units
    speed_related = []
    for unit, unit_config in config["units"].items():
        for tag in unit_config["potential_speed_tags"]:
            speed_related.append(f"{unit}:{tag}")

    config["potential_speed_tags"] = {
        "count": len(speed_related),
        "tags": speed_related,
        "description": "Tags that may indicate speed, flow rate, or control parameters"
    }

    return config


def main():
    config = generate_speed_config()

    # Save to config directory
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)

    output_file = config_dir / "industrial_tags.json"

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"[CONFIG] Industrial tag configuration saved to: {output_file}")
    print(f"[SUMMARY] Found {config['scan_summary']['total_units']} units with {config['scan_summary']['total_unique_tags']} unique tags")
    print(f"[SPEED] Identified {config['potential_speed_tags']['count']} potential speed/control tags")

    # Print summary by unit
    print(f"\n[UNITS] Tag Summary by Unit:")
    for unit, unit_config in config["units"].items():
        print(f"  {unit}: {unit_config['total_tags']} tags")
        if unit_config['potential_speed_tags']:
            print(f"    Speed/Control: {len(unit_config['potential_speed_tags'])} tags")
            for tag in unit_config['potential_speed_tags'][:3]:
                print(f"      - {tag}")
        if unit_config['performance_metrics']:
            print(f"    Performance: {len(unit_config['performance_metrics'])} metrics")

    print(f"\n[MONITORING] Recommended monitoring tags saved to config file")

    return output_file


if __name__ == "__main__":
    main()