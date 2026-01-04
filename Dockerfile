# Multi-stage build for MultiPlex Stats
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy application code
COPY multiplex_stats/ ./multiplex_stats/
COPY flask_app/ ./flask_app/
COPY run_multiplex_stats.py .

# Create necessary directories and set ownership
RUN mkdir -p instance/cache && \
    chown -R appuser:appuser /app

# Set environment variables
ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=run_multiplex_stats.py
ENV PORT=8487

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8487

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8487')" || exit 1

# Run the application
CMD ["python3", "run_multiplex_stats.py"]
