# Base container for TurboPredict unit processing
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Excel automation
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pi_monitor/ ./pi_monitor/
COPY scripts/ ./scripts/
COPY config/ ./config/

# Copy unit-specific entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/reports

# Set environment variables
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from pi_monitor.parquet_auto_scan import ParquetAutoScanner; ParquetAutoScanner().scan_all_units()" || exit 1

# Expose ports
EXPOSE 8000 9000

# Use entrypoint script
ENTRYPOINT ["/entrypoint.sh"]