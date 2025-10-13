# Orchestrator container for TurboPredict X Protean
FROM python:3.11-slim

WORKDIR /app

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy orchestrator application
COPY orchestrator.py .
COPY templates/ ./templates/
COPY static/ ./static/

# Create directories
RUN mkdir -p /app/logs

# Environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose port
EXPOSE 8080

# Start orchestrator
CMD ["python", "orchestrator.py"]