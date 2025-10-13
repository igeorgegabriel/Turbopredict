#!/usr/bin/env python3
"""
Validation script for TurboPredict X Protean containerized setup
Tests the configuration and deployment readiness
"""

import os
import sys
import yaml
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_file_structure():
    """Validate required files and directories exist"""
    logger.info("Validating file structure...")

    project_root = Path(__file__).parent
    required_files = [
        'docker-compose.yml',
        'containers/unit-base/Dockerfile',
        'containers/unit-base/entrypoint.sh',
        'containers/unit-base/api_server.py',
        'containers/orchestrator/Dockerfile',
        'containers/orchestrator/orchestrator.py',
        'monitoring/prometheus/prometheus.yml'
    ]

    required_dirs = [
        'data/units',
        'data/shared',
        'logs/units',
        'config/units',
        'containers/unit-base',
        'containers/orchestrator',
        'monitoring/prometheus',
        'monitoring/grafana'
    ]

    missing_files = []
    missing_dirs = []

    for file_path in required_files:
        full_path = project_root / file_path
        if not full_path.exists():
            missing_files.append(str(file_path))

    for dir_path in required_dirs:
        full_path = project_root / dir_path
        if not full_path.exists():
            missing_dirs.append(str(dir_path))

    if missing_files:
        logger.error("Missing required files:")
        for file_path in missing_files:
            logger.error(f"  - {file_path}")

    if missing_dirs:
        logger.error("Missing required directories:")
        for dir_path in missing_dirs:
            logger.error(f"  - {dir_path}")

    return len(missing_files) == 0 and len(missing_dirs) == 0

def validate_docker_compose():
    """Validate docker-compose.yml structure"""
    logger.info("Validating docker-compose.yml...")

    compose_file = Path(__file__).parent / 'docker-compose.yml'
    if not compose_file.exists():
        logger.error("docker-compose.yml not found")
        return False

    try:
        with open(compose_file, 'r') as f:
            compose_data = yaml.safe_load(f)

        # Check required services
        required_services = [
            'k-12-01', 'k-16-01', 'k-19-01', 'k-31-01', 'xt-07002',
            'c-02001', 'c-104', 'c-13001', 'c-1301', 'c-1302', 'c-201', 'c-202',
            '07-mt01-k001', 'orchestrator', 'prometheus', 'grafana'
        ]

        services = compose_data.get('services', {})
        missing_services = []

        for service in required_services:
            if service not in services:
                missing_services.append(service)

        if missing_services:
            logger.error("Missing services in docker-compose.yml:")
            for service in missing_services:
                logger.error(f"  - {service}")
            return False

        logger.info(f"All {len(required_services)} services defined in docker-compose.yml")
        return True

    except Exception as e:
        logger.error(f"Error parsing docker-compose.yml: {e}")
        return False

def validate_unit_configs():
    """Validate unit configuration files"""
    logger.info("Validating unit configurations...")

    config_dir = Path(__file__).parent / 'config' / 'units'
    if not config_dir.exists():
        logger.error("Unit config directory not found")
        return False

    units = [
        'K-12-01', 'K-16-01', 'K-19-01', 'K-31-01', 'XT-07002',
        'C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202',
        '07-MT01-K001'
    ]

    valid_configs = 0
    for unit in units:
        config_file = config_dir / f"{unit}.yml"
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)

                # Validate required keys
                required_keys = ['unit_id', 'plant', 'data', 'processing', 'monitoring']
                missing_keys = [key for key in required_keys if key not in config]

                if missing_keys:
                    logger.warning(f"Unit {unit} config missing keys: {missing_keys}")
                else:
                    valid_configs += 1
                    logger.debug(f"Unit {unit} config valid")

            except Exception as e:
                logger.error(f"Error parsing config for {unit}: {e}")
        else:
            logger.warning(f"Config file not found for unit: {unit}")

    logger.info(f"Valid unit configurations: {valid_configs}/{len(units)}")
    return valid_configs > 0

def validate_data_migration():
    """Validate data has been properly migrated"""
    logger.info("Validating data migration...")

    units_data_dir = Path(__file__).parent / 'data' / 'units'
    if not units_data_dir.exists():
        logger.error("Units data directory not found")
        return False

    units_with_data = 0
    total_units = 13

    for unit_dir in units_data_dir.iterdir():
        if unit_dir.is_dir():
            parquet_files = list(unit_dir.glob('*.parquet'))
            if parquet_files:
                units_with_data += 1
                logger.debug(f"Unit {unit_dir.name} has {len(parquet_files)} parquet files")

    logger.info(f"Units with data: {units_with_data}/{total_units}")
    return units_with_data > 0

def validate_container_architecture():
    """Validate the overall container architecture"""
    logger.info("=" * 60)
    logger.info("TURBOPREDICT X PROTEAN - CONTAINER VALIDATION")
    logger.info("=" * 60)

    validation_results = {}

    validation_results['file_structure'] = validate_file_structure()
    validation_results['docker_compose'] = validate_docker_compose()
    validation_results['unit_configs'] = validate_unit_configs()
    validation_results['data_migration'] = validate_data_migration()

    # Summary
    passed = sum(validation_results.values())
    total = len(validation_results)

    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)

    for test_name, result in validation_results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{test_name.replace('_', ' ').title()}: {status}")

    logger.info("=" * 60)
    logger.info(f"Overall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 CONTAINERIZED ARCHITECTURE READY FOR DEPLOYMENT!")
        logger.info("Run 'python deploy_containers.py' to deploy")
    else:
        logger.warning("⚠️  Some validations failed. Please fix issues before deployment.")

    logger.info("=" * 60)

    return passed == total

if __name__ == "__main__":
    success = validate_container_architecture()
    sys.exit(0 if success else 1)