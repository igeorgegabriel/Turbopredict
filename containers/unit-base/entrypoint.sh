#!/bin/bash

# TurboPredict Unit Container Entrypoint
set -e

echo "======================================================"
echo "TURBOPREDICT X PROTEAN - UNIT CONTAINER STARTING"
echo "Unit ID: ${UNIT_ID:-UNKNOWN}"
echo "======================================================"

# Validate required environment variables
if [ -z "$UNIT_ID" ]; then
    echo "ERROR: UNIT_ID environment variable is required"
    exit 1
fi

# Create unit-specific directories
mkdir -p "/app/data/$UNIT_ID"
mkdir -p "/app/logs/$UNIT_ID"
mkdir -p "/app/reports/$UNIT_ID"

# Set unit-specific environment variables
export PLANT=${PLANT:-$(echo $UNIT_ID | cut -d'-' -f1)}
export UNIT=$UNIT_ID
export DATA_PATH="/app/data/$UNIT_ID"
export LOGS_PATH="/app/logs/$UNIT_ID"
export REPORTS_PATH="/app/reports/$UNIT_ID"

echo "Configuration:"
echo "  Plant: $PLANT"
echo "  Unit: $UNIT"
echo "  Data Path: $DATA_PATH"
echo "  Logs Path: $LOGS_PATH"
echo "  Reports Path: $REPORTS_PATH"

# Load unit-specific configuration if available
if [ -f "/app/config/units/$UNIT_ID.yml" ]; then
    echo "Loading unit configuration: /app/config/units/$UNIT_ID.yml"
    export CONFIG_FILE="/app/config/units/$UNIT_ID.yml"
fi

# Start the unit service based on the command
case "$1" in
    "hourly-refresh")
        echo "Starting hourly refresh service for $UNIT_ID"
        exec python scripts/hourly_refresh.py
        ;;
    "api-server")
        echo "Starting API server for $UNIT_ID"
        exec python containers/unit-base/api_server.py
        ;;
    "analysis")
        echo "Running analysis for $UNIT_ID"
        exec python -m pi_monitor.cli auto-scan --unit $UNIT_ID
        ;;
    "dashboard")
        echo "Starting cyberpunk dashboard for $UNIT_ID"
        exec python turbopredict.py
        ;;
    *)
        echo "Starting default service (hourly refresh + API)"
        # Start both services
        python containers/unit-base/api_server.py &
        exec python scripts/hourly_refresh.py
        ;;
esac