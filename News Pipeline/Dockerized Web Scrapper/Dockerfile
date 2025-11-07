FROM selenium/standalone-chrome:latest

# Switch to root to install Python
USER root

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set up Python environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DOCKER_MODE=true

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY docker_version_monthly_api_scrapper.py .

# Create data directory
RUN mkdir -p /home/seluser/data && chown seluser:seluser /home/seluser/data

# Switch back to selenium user
USER seluser

# Default command
CMD ["python3", "docker_version_monthly_api_scrapper.py"]