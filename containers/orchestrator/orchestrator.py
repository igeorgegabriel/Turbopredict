#!/usr/bin/env python3
"""
TurboPredict X Protean Orchestrator
Central management and coordination for all unit containers
"""

import os
import time
import logging
import requests
import yaml
from flask import Flask, jsonify, render_template, request
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Unit container configuration
UNITS = [
    'k-12-01', 'k-16-01', 'k-19-01', 'k-31-01', 'xt-07002',
    'c-02001', 'c-104', 'c-13001', 'c-1301', 'c-1302', 'c-201', 'c-202',
    '07-mt01-k001'
]

class UnitManager:
    """Manages communication with unit containers"""

    def __init__(self):
        self.units = UNITS
        self.base_url = "http://{unit}:8000"

    def get_unit_url(self, unit_id):
        """Get the URL for a unit container"""
        return self.base_url.format(unit=unit_id.lower().replace('_', '-'))

    def check_unit_health(self, unit_id, timeout=5):
        """Check health of a specific unit"""
        try:
            url = f"{self.get_unit_url(unit_id)}/health"
            response = requests.get(url, timeout=timeout)
            return response.status_code == 200, response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Health check failed for {unit_id}: {e}")
            return False, None

    def get_unit_status(self, unit_id, timeout=10):
        """Get status of a specific unit"""
        try:
            url = f"{self.get_unit_url(unit_id)}/status"
            response = requests.get(url, timeout=timeout)
            return response.status_code == 200, response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Status check failed for {unit_id}: {e}")
            return False, None

    def trigger_unit_refresh(self, unit_id, timeout=30):
        """Trigger refresh for a specific unit"""
        try:
            url = f"{self.get_unit_url(unit_id)}/refresh"
            response = requests.post(url, timeout=timeout)
            return response.status_code == 200, response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Refresh failed for {unit_id}: {e}")
            return False, None

    def get_all_unit_status(self):
        """Get status of all units in parallel"""
        results = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all health checks
            future_to_unit = {
                executor.submit(self.check_unit_health, unit): unit
                for unit in self.units
            }

            # Collect results
            for future in as_completed(future_to_unit):
                unit = future_to_unit[future]
                try:
                    is_healthy, health_data = future.result()
                    results[unit] = {
                        'healthy': is_healthy,
                        'health_data': health_data,
                        'checked_at': datetime.now().isoformat()
                    }
                except Exception as e:
                    logger.error(f"Error checking {unit}: {e}")
                    results[unit] = {
                        'healthy': False,
                        'error': str(e),
                        'checked_at': datetime.now().isoformat()
                    }

        return results

# Initialize unit manager
unit_manager = UnitManager()

@app.route('/')
def dashboard():
    """Main orchestrator dashboard"""
    return render_template('dashboard.html', units=UNITS)

@app.route('/health')
def health():
    """Orchestrator health check"""
    return jsonify({
        'status': 'healthy',
        'service': 'turbopredict-orchestrator',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/units')
def list_units():
    """List all managed units"""
    return jsonify({
        'units': UNITS,
        'count': len(UNITS),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/units/status')
def all_units_status():
    """Get status of all units"""
    status_data = unit_manager.get_all_unit_status()

    # Summary statistics
    total_units = len(UNITS)
    healthy_units = sum(1 for unit_data in status_data.values() if unit_data.get('healthy', False))

    return jsonify({
        'summary': {
            'total_units': total_units,
            'healthy_units': healthy_units,
            'unhealthy_units': total_units - healthy_units,
            'health_percentage': (healthy_units / total_units) * 100 if total_units > 0 else 0
        },
        'units': status_data,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/units/<unit_id>/health')
def unit_health(unit_id):
    """Check specific unit health"""
    is_healthy, health_data = unit_manager.check_unit_health(unit_id)

    return jsonify({
        'unit_id': unit_id,
        'healthy': is_healthy,
        'health_data': health_data,
        'timestamp': datetime.now().isoformat()
    }), 200 if is_healthy else 503

@app.route('/api/units/<unit_id>/status')
def unit_status(unit_id):
    """Get specific unit status"""
    is_ok, status_data = unit_manager.get_unit_status(unit_id)

    return jsonify({
        'unit_id': unit_id,
        'status_ok': is_ok,
        'status_data': status_data,
        'timestamp': datetime.now().isoformat()
    }), 200 if is_ok else 503

@app.route('/api/units/<unit_id>/refresh', methods=['POST'])
def trigger_unit_refresh(unit_id):
    """Trigger refresh for specific unit"""
    is_ok, refresh_data = unit_manager.trigger_unit_refresh(unit_id)

    return jsonify({
        'unit_id': unit_id,
        'refresh_triggered': is_ok,
        'refresh_data': refresh_data,
        'timestamp': datetime.now().isoformat()
    }), 200 if is_ok else 500

@app.route('/api/system/refresh-all', methods=['POST'])
def refresh_all_units():
    """Trigger refresh for all units"""
    results = {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_unit = {
            executor.submit(unit_manager.trigger_unit_refresh, unit): unit
            for unit in UNITS
        }

        for future in as_completed(future_to_unit):
            unit = future_to_unit[future]
            try:
                is_ok, refresh_data = future.result()
                results[unit] = {
                    'success': is_ok,
                    'data': refresh_data
                }
            except Exception as e:
                results[unit] = {
                    'success': False,
                    'error': str(e)
                }

    successful_refreshes = sum(1 for result in results.values() if result.get('success', False))

    return jsonify({
        'total_units': len(UNITS),
        'successful_refreshes': successful_refreshes,
        'failed_refreshes': len(UNITS) - successful_refreshes,
        'results': results,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/metrics')
def prometheus_metrics():
    """Prometheus metrics endpoint"""
    try:
        status_data = unit_manager.get_all_unit_status()

        metrics = []
        metrics.append('# HELP turbopredict_orchestrator_units_total Total number of managed units')
        metrics.append('# TYPE turbopredict_orchestrator_units_total gauge')
        metrics.append(f'turbopredict_orchestrator_units_total {len(UNITS)}')

        healthy_count = sum(1 for unit_data in status_data.values() if unit_data.get('healthy', False))
        metrics.append('# HELP turbopredict_orchestrator_units_healthy Number of healthy units')
        metrics.append('# TYPE turbopredict_orchestrator_units_healthy gauge')
        metrics.append(f'turbopredict_orchestrator_units_healthy {healthy_count}')

        for unit, unit_data in status_data.items():
            health_value = 1 if unit_data.get('healthy', False) else 0
            metrics.append(f'turbopredict_unit_health{{unit="{unit}"}} {health_value}')

        return '\n'.join(metrics), 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        return f'# ERROR: {str(e)}', 500, {'Content-Type': 'text/plain'}

def background_monitoring():
    """Background task to monitor units and log status"""
    while True:
        try:
            logger.info("Running background unit health monitoring...")
            status_data = unit_manager.get_all_unit_status()

            healthy_units = [unit for unit, data in status_data.items() if data.get('healthy', False)]
            unhealthy_units = [unit for unit, data in status_data.items() if not data.get('healthy', False)]

            logger.info(f"Unit health summary: {len(healthy_units)} healthy, {len(unhealthy_units)} unhealthy")

            if unhealthy_units:
                logger.warning(f"Unhealthy units: {', '.join(unhealthy_units)}")

            time.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logger.error(f"Background monitoring error: {e}")
            time.sleep(60)  # Wait 1 minute on error

if __name__ == '__main__':
    logger.info("Starting TurboPredict X Protean Orchestrator")

    # Start background monitoring thread
    monitor_thread = Thread(target=background_monitoring, daemon=True)
    monitor_thread.start()

    # Start Flask app
    app.run(host='0.0.0.0', port=8080, debug=False)