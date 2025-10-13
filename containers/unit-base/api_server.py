#!/usr/bin/env python3
"""
Unit API Server for TurboPredict X Protean
Provides REST API endpoints for each unit container
"""

import os
import sys
from pathlib import Path
from flask import Flask, jsonify, request
from datetime import datetime
import logging
import traceback

# Add project root to path
sys.path.insert(0, '/app')

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
from pi_monitor.parquet_database import ParquetDatabase

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Get unit configuration from environment
UNIT_ID = os.getenv('UNIT_ID', 'UNKNOWN')
PLANT = os.getenv('PLANT', 'UNKNOWN')
DATA_PATH = os.getenv('DATA_PATH', '/app/data')

# Initialize components
config = Config()
scanner = ParquetAutoScanner(config, Path(DATA_PATH))
db = ParquetDatabase(Path(DATA_PATH))

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Quick health check
        return jsonify({
            'status': 'healthy',
            'unit_id': UNIT_ID,
            'plant': PLANT,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'unit_id': UNIT_ID,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def unit_status():
    """Get unit status and data freshness"""
    try:
        # Scan unit data
        unit_data = scanner.scan_all_units()

        unit_info = None
        if UNIT_ID in unit_data:
            unit_info = unit_data[UNIT_ID]

        return jsonify({
            'unit_id': UNIT_ID,
            'plant': PLANT,
            'status': 'fresh' if (unit_info and unit_info.get('fresh', False)) else 'stale',
            'data': unit_info,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error getting unit status: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'unit_id': UNIT_ID,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/refresh', methods=['POST'])
def trigger_refresh():
    """Trigger manual refresh for this unit"""
    try:
        # Force refresh for this unit
        result = scanner.refresh_stale_units(force_units=[UNIT_ID])

        return jsonify({
            'unit_id': UNIT_ID,
            'refresh_triggered': True,
            'result': result,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error triggering refresh: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'unit_id': UNIT_ID,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/analyze', methods=['POST'])
def run_analysis():
    """Run analysis for this unit"""
    try:
        analysis_type = request.json.get('type', 'basic') if request.json else 'basic'

        # Run unit analysis
        unit_data = scanner.scan_all_units()
        unit_info = unit_data.get(UNIT_ID, {})

        return jsonify({
            'unit_id': UNIT_ID,
            'analysis_type': analysis_type,
            'analysis_complete': True,
            'unit_info': unit_info,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error running analysis: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'unit_id': UNIT_ID,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get Prometheus-style metrics"""
    try:
        unit_data = scanner.scan_all_units()
        unit_info = unit_data.get(UNIT_ID, {})

        metrics = []
        metrics.append(f'# HELP turbopredict_unit_status Unit operational status')
        metrics.append(f'# TYPE turbopredict_unit_status gauge')
        status_value = 1 if unit_info.get('fresh', False) else 0
        metrics.append(f'turbopredict_unit_status{{unit="{UNIT_ID}",plant="{PLANT}"}} {status_value}')

        if 'records' in unit_info:
            metrics.append(f'# HELP turbopredict_unit_records Number of records in unit')
            metrics.append(f'# TYPE turbopredict_unit_records gauge')
            metrics.append(f'turbopredict_unit_records{{unit="{UNIT_ID}",plant="{PLANT}"}} {unit_info["records"]}')

        if 'age_hours' in unit_info:
            metrics.append(f'# HELP turbopredict_unit_age_hours Age of unit data in hours')
            metrics.append(f'# TYPE turbopredict_unit_age_hours gauge')
            metrics.append(f'turbopredict_unit_age_hours{{unit="{UNIT_ID}",plant="{PLANT}"}} {unit_info["age_hours"]}')

        return '\n'.join(metrics), 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return f'# ERROR: {str(e)}', 500, {'Content-Type': 'text/plain'}

@app.route('/info', methods=['GET'])
def unit_info():
    """Get detailed unit information"""
    return jsonify({
        'unit_id': UNIT_ID,
        'plant': PLANT,
        'data_path': DATA_PATH,
        'config': {
            'max_age_hours': getattr(config, 'MAX_AGE_HOURS', 1.0),
        },
        'container_info': {
            'hostname': os.getenv('HOSTNAME', 'unknown'),
            'python_version': sys.version,
        },
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    logger.info(f"Starting API server for unit {UNIT_ID} on port 8000")
    app.run(host='0.0.0.0', port=8000, debug=False)