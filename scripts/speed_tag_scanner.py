#!/usr/bin/env python3
"""
Speed Tag Scanner for TurboPredict

Scans parquet files to identify speed-related tags for each unit and creates
a configuration file mapping units to their speed tags.

Usage:
    python scripts/speed_tag_scanner.py --scan-all
    python scripts/speed_tag_scanner.py --unit K-12-01
    python scripts/speed_tag_scanner.py --output config/speed_tags.json
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import json
import re
from typing import List, Dict, Set, Optional
import logging
import glob
from collections import defaultdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpeedTagScanner:
    """Scanner to identify speed-related tags in parquet files"""

    def __init__(self):
        self.speed_keywords = [
            'speed', 'rpm', 'velocity', 'rate', 'flow', 'freq', 'frequency',
            'hz', 'rps', 'rotation', 'spin', 'motor', 'drive', 'pump',
            'compressor', 'turbine', 'fan', 'agitator', 'mixer', 'stirrer'
        ]

        self.unit_speed_tags = defaultdict(set)
        self.all_units = set()
        self.processed_files = 0

    def find_parquet_files(self, unit_filter: Optional[str] = None) -> List[Path]:
        """Find parquet files for scanning"""
        patterns = []

        if unit_filter:
            patterns = [
                f"data/processed/dataset/plant=PCFS/unit={unit_filter}/**/*.parquet"
            ]
        else:
            # Scan all units
            patterns = [
                "data/processed/dataset/plant=PCFS/unit=*/**/*.parquet"
            ]

        files = []
        for pattern in patterns:
            found = glob.glob(pattern, recursive=True)
            files.extend(found)

        parquet_files = [Path(f) for f in files]
        logger.info(f"Found {len(parquet_files)} parquet files to scan")
        return parquet_files

    def extract_unit_from_path(self, file_path: Path) -> Optional[str]:
        """Extract unit name from file path"""
        # Look for unit= pattern in path
        match = re.search(r'unit=([^/\\]+)', str(file_path))
        if match:
            return match.group(1)
        return None

    def extract_tag_from_path(self, file_path: Path) -> Optional[str]:
        """Extract tag name from file path"""
        # Look for tag= pattern in path
        match = re.search(r'tag=([^/\\]+)', str(file_path))
        if match:
            return match.group(1)
        return None

    def is_speed_related_tag(self, tag_name: str) -> bool:
        """Check if a tag name is likely speed-related"""
        if not tag_name:
            return False

        tag_lower = tag_name.lower()

        # Check for speed keywords
        for keyword in self.speed_keywords:
            if keyword in tag_lower:
                return True

        # Check for common speed tag patterns
        speed_patterns = [
            r'.*speed.*',
            r'.*rpm.*',
            r'.*freq.*',
            r'.*hz.*',
            r'.*motor.*speed.*',
            r'.*pump.*speed.*',
            r'.*fan.*speed.*',
            r'.*drive.*speed.*',
            r'.*rotation.*',
            r'.*velocity.*'
        ]

        for pattern in speed_patterns:
            if re.match(pattern, tag_lower):
                return True

        return False

    def scan_file_for_speed_data(self, file_path: Path) -> Dict:
        """Scan individual parquet file for speed-related data"""
        try:
            # Extract metadata from path
            unit = self.extract_unit_from_path(file_path)
            tag = self.extract_tag_from_path(file_path)

            if not unit or not tag:
                return {}

            self.all_units.add(unit)

            # Check if tag name suggests speed data
            is_speed_tag = self.is_speed_related_tag(tag)

            result = {
                'unit': unit,
                'tag': tag,
                'file': str(file_path),
                'is_speed_related': is_speed_tag
            }

            if is_speed_tag:
                # Try to read a sample of the data to confirm
                try:
                    df = pd.read_parquet(file_path)
                    if not df.empty and 'value' in df.columns:
                        values = df['value'].dropna()
                        if len(values) > 0:
                            result.update({
                                'sample_count': len(values),
                                'min_value': float(values.min()),
                                'max_value': float(values.max()),
                                'mean_value': float(values.mean()),
                                'has_data': True
                            })

                            # Add to unit speed tags
                            self.unit_speed_tags[unit].add(tag)
                        else:
                            result['has_data'] = False
                    else:
                        result['has_data'] = False

                except Exception as e:
                    logger.warning(f"Could not read data from {file_path}: {e}")
                    result['read_error'] = str(e)
                    result['has_data'] = False

            self.processed_files += 1

            if self.processed_files % 100 == 0:
                logger.info(f"Processed {self.processed_files} files...")

            return result

        except Exception as e:
            logger.error(f"Error scanning {file_path}: {e}")
            return {}

    def scan_all_files(self, unit_filter: Optional[str] = None) -> Dict:
        """Scan all parquet files for speed tags"""
        files = self.find_parquet_files(unit_filter)

        results = {
            'total_files': len(files),
            'speed_tags_found': [],
            'units_scanned': set(),
            'summary_by_unit': {}
        }

        logger.info(f"Scanning {len(files)} files for speed tags...")

        for file_path in files:
            scan_result = self.scan_file_for_speed_data(file_path)

            if scan_result and scan_result.get('is_speed_related'):
                results['speed_tags_found'].append(scan_result)
                results['units_scanned'].add(scan_result['unit'])

        # Create summary by unit
        for unit in self.all_units:
            unit_tags = list(self.unit_speed_tags.get(unit, set()))
            results['summary_by_unit'][unit] = {
                'speed_tag_count': len(unit_tags),
                'speed_tags': sorted(unit_tags)
            }

        results['units_scanned'] = list(results['units_scanned'])

        logger.info(f"Scan complete: {len(results['speed_tags_found'])} speed tags found across {len(results['units_scanned'])} units")

        return results

    def generate_speed_config(self, scan_results: Dict) -> Dict:
        """Generate speed tag configuration"""
        config = {
            'generated_at': pd.Timestamp.now().isoformat(),
            'scan_summary': {
                'total_files_scanned': scan_results['total_files'],
                'speed_tags_found': len(scan_results['speed_tags_found']),
                'units_with_speed_tags': len(scan_results['units_scanned'])
            },
            'units': {}
        }

        # Process each unit
        for unit, unit_data in scan_results['summary_by_unit'].items():
            if unit_data['speed_tag_count'] > 0:
                config['units'][unit] = {
                    'speed_tags': unit_data['speed_tags'],
                    'primary_speed_tag': unit_data['speed_tags'][0] if unit_data['speed_tags'] else None,
                    'tag_count': unit_data['speed_tag_count']
                }

        # Add detailed tag information
        config['tag_details'] = {}
        for tag_info in scan_results['speed_tags_found']:
            if tag_info.get('has_data'):
                tag_name = tag_info['tag']
                config['tag_details'][tag_name] = {
                    'unit': tag_info['unit'],
                    'sample_count': tag_info.get('sample_count', 0),
                    'value_range': {
                        'min': tag_info.get('min_value'),
                        'max': tag_info.get('max_value'),
                        'mean': tag_info.get('mean_value')
                    }
                }

        return config

    def save_config(self, config: Dict, output_path: str = "config/speed_tags.json"):
        """Save speed tag configuration to file"""
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        logger.info(f"Speed tag configuration saved to {output_file}")
        return output_file


def print_scan_summary(results: Dict):
    """Print formatted scan summary"""
    print(f"\n[SPEED TAGS] SCAN SUMMARY")
    print(f"{'='*50}")
    print(f"Total files scanned: {results['total_files']}")
    print(f"Speed tags found: {len(results['speed_tags_found'])}")
    print(f"Units with speed tags: {len(results['units_scanned'])}")

    if results['summary_by_unit']:
        print(f"\n[UNITS] Speed Tags by Unit:")
        for unit, data in results['summary_by_unit'].items():
            if data['speed_tag_count'] > 0:
                print(f"  {unit}: {data['speed_tag_count']} speed tags")
                for tag in data['speed_tags'][:3]:  # Show first 3
                    print(f"    - {tag}")
                if len(data['speed_tags']) > 3:
                    print(f"    ... and {len(data['speed_tags']) - 3} more")

    print(f"\n[INFO] Run with --output to save configuration file")


def main():
    parser = argparse.ArgumentParser(description="TurboPredict Speed Tag Scanner")
    parser.add_argument('--scan-all', action='store_true', help='Scan all units for speed tags')
    parser.add_argument('--unit', type=str, help='Scan specific unit (e.g., K-12-01)')
    parser.add_argument('--output', type=str, default='config/speed_tags.json',
                       help='Output configuration file path')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create scanner
    scanner = SpeedTagScanner()

    # Determine what to scan
    if args.unit:
        unit_filter = args.unit
        print(f"[SCAN] Scanning unit {unit_filter} for speed tags...")
    elif args.scan_all:
        unit_filter = None
        print("[SCAN] Scanning all units for speed tags...")
    else:
        # Default: quick scan of first few files per unit
        unit_filter = None
        print("[SCAN] Quick scan for speed tags (use --scan-all for complete scan)...")

    # Perform scan
    results = scanner.scan_all_files(unit_filter)

    # Print results
    print_scan_summary(results)

    # Generate and save configuration
    if args.output or args.scan_all:
        config = scanner.generate_speed_config(results)
        output_file = scanner.save_config(config, args.output)

        print(f"\n[CONFIG] Speed tag configuration saved to: {output_file}")
        print(f"[CONFIG] Found {len(config['units'])} units with speed tags")


if __name__ == "__main__":
    main()