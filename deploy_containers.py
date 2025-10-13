#!/usr/bin/env python3
"""
Deployment script for TurboPredict X Protean containerized architecture
Migrates existing data and sets up the container environment
"""

import os
import sys
import shutil
from pathlib import Path
import yaml
import subprocess
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContainerDeployer:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.existing_data = self.project_root / "data" / "processed"
        self.container_data = self.project_root / "data" / "units"
        self.shared_data = self.project_root / "data" / "shared"

        # Units from the current system
        self.units = [
            'K-12-01', 'K-16-01', 'K-19-01', 'K-31-01', 'XT-07002',
            'C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202',
            '07-MT01-K001'
        ]

    def migrate_existing_data(self):
        """Migrate existing Parquet data to unit-specific directories"""
        logger.info("Migrating existing data to containerized structure...")

        if not self.existing_data.exists():
            logger.warning(f"Existing data directory not found: {self.existing_data}")
            return

        for unit in self.units:
            unit_dir = self.container_data / unit
            unit_dir.mkdir(parents=True, exist_ok=True)

            # Find and copy unit-specific files
            pattern = f"{unit}_*.parquet"
            for file_path in self.existing_data.glob(pattern):
                dest_path = unit_dir / file_path.name
                if not dest_path.exists():
                    logger.info(f"Copying {file_path.name} to {unit} container")
                    shutil.copy2(file_path, dest_path)
                else:
                    logger.info(f"File already exists: {dest_path}")

    def create_shared_excel_directory(self):
        """Set up shared Excel directory"""
        logger.info("Setting up shared Excel directory...")

        excel_shared = self.shared_data / "excel"
        excel_shared.mkdir(parents=True, exist_ok=True)

        # Copy existing Excel files to shared directory
        excel_source = self.project_root / "excel"
        if excel_source.exists():
            for excel_file in excel_source.glob("*.xlsx"):
                dest_path = excel_shared / excel_file.name
                if not dest_path.exists():
                    logger.info(f"Copying {excel_file.name} to shared Excel directory")
                    shutil.copy2(excel_file, dest_path)

    def generate_unit_configs(self):
        """Generate configuration files for all units"""
        logger.info("Generating unit configuration files...")

        config_dir = self.project_root / "config" / "units"
        config_dir.mkdir(parents=True, exist_ok=True)

        for unit in self.units:
            config_file = config_dir / f"{unit}.yml"

            if config_file.exists():
                logger.info(f"Configuration already exists: {config_file}")
                continue

            # Determine plant based on unit name
            if unit.startswith('XT-'):
                plant = "PCMSB"
                excel_file = "PCMSB_Automation.xlsx"
            elif unit.startswith(('K-', 'C-', '07-')):
                plant = "PCFS"
                excel_file = "PCFS_Automation.xlsx"
            else:
                plant = "PCFS"  # Default
                excel_file = "PCFS_Automation.xlsx"

            config = {
                'unit_id': unit,
                'plant': plant,
                'description': f"Process Control Unit {unit}",
                'data': {
                    'parquet_file': f"{unit}_1y_0p1h.parquet",
                    'excel_file': excel_file,
                    'sheet_name': 'DL_WORK',
                    'max_age_hours': 1.0
                },
                'processing': {
                    'memory_limit_gb': 2.0,
                    'chunk_size': 250000,
                    'enable_deduplication': True,
                    'enable_anomaly_detection': True
                },
                'monitoring': {
                    'health_check_interval': 30,
                    'metrics_port': 9000,
                    'log_level': 'INFO'
                },
                'container': {
                    'restart_policy': 'unless-stopped',
                    'cpu_limit': 1.0,
                    'memory_limit': '2GB'
                },
                'specific_tags': [],
                'excel': {
                    'timeout_seconds': 300,
                    'calculation_mode': 'full',
                    'save_strategy': 'working_copy'
                }
            }

            logger.info(f"Creating configuration: {config_file}")
            with open(config_file, 'w') as f:
                yaml.safe_dump(config, f, default_flow_style=False)

    def create_docker_requirements(self):
        """Create requirements.txt for containers"""
        logger.info("Creating Docker requirements file...")

        # Copy existing requirements if available
        existing_requirements = self.project_root / "requirements.txt"
        container_requirements = self.project_root / "containers" / "unit-base" / "requirements.txt"

        if existing_requirements.exists():
            shutil.copy2(existing_requirements, container_requirements)
            logger.info(f"Copied requirements to container: {container_requirements}")
        else:
            # Create basic requirements
            basic_requirements = [
                "pandas>=2.1.0",
                "pyarrow>=15.0.0",
                "matplotlib>=3.8.0",
                "scikit-learn>=1.4.0",
                "polars>=1.6.0",
                "flask>=3.0.0",
                "pyyaml>=6.0.1",
                "requests>=2.31.0",
                "schedule>=1.2.0"
            ]

            with open(container_requirements, 'w') as f:
                f.write('\n'.join(basic_requirements))
            logger.info(f"Created basic requirements file: {container_requirements}")

    def validate_docker_environment(self):
        """Validate Docker is available"""
        try:
            result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"Docker available: {result.stdout.strip()}")
                return True
            else:
                logger.error("Docker command failed")
                return False
        except FileNotFoundError:
            logger.error("Docker not found. Please install Docker first.")
            return False

    def build_containers(self):
        """Build Docker containers"""
        if not self.validate_docker_environment():
            return False

        logger.info("Building Docker containers...")

        try:
            # Build containers using docker-compose
            result = subprocess.run([
                'docker-compose', 'build'
            ], cwd=self.project_root, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("Container build successful")
                return True
            else:
                logger.error(f"Container build failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Error building containers: {e}")
            return False

    def start_containers(self, detached=True):
        """Start the containerized environment"""
        if not self.validate_docker_environment():
            return False

        logger.info("Starting containerized environment...")

        try:
            cmd = ['docker-compose', 'up']
            if detached:
                cmd.append('-d')

            result = subprocess.run(cmd, cwd=self.project_root, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("Containers started successfully")
                if detached:
                    logger.info("Access the orchestrator dashboard at: http://localhost:8080")
                    logger.info("Access Grafana at: http://localhost:3000 (admin/admin)")
                    logger.info("Access Prometheus at: http://localhost:9090")
                return True
            else:
                logger.error(f"Failed to start containers: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Error starting containers: {e}")
            return False

    def deploy(self, build_only=False):
        """Run full deployment"""
        logger.info("=" * 60)
        logger.info("TURBOPREDICT X PROTEAN - CONTAINERIZED DEPLOYMENT")
        logger.info("=" * 60)

        # Step 1: Migrate data
        self.migrate_existing_data()

        # Step 2: Set up shared directories
        self.create_shared_excel_directory()

        # Step 3: Generate configurations
        self.generate_unit_configs()

        # Step 4: Set up Docker requirements
        self.create_docker_requirements()

        # Step 5: Build containers
        if not self.build_containers():
            logger.error("Deployment failed at build stage")
            return False

        if build_only:
            logger.info("Build-only mode complete")
            return True

        # Step 6: Start containers
        if not self.start_containers():
            logger.error("Deployment failed at startup stage")
            return False

        logger.info("=" * 60)
        logger.info("DEPLOYMENT COMPLETE!")
        logger.info("=" * 60)
        logger.info("Services available:")
        logger.info("  - Orchestrator Dashboard: http://localhost:8080")
        logger.info("  - Grafana Monitoring: http://localhost:3000")
        logger.info("  - Prometheus Metrics: http://localhost:9090")
        logger.info("=" * 60)

        return True

def main():
    deployer = ContainerDeployer()

    build_only = "--build-only" in sys.argv
    success = deployer.deploy(build_only=build_only)

    if success:
        logger.info("Deployment successful!")
        sys.exit(0)
    else:
        logger.error("Deployment failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()