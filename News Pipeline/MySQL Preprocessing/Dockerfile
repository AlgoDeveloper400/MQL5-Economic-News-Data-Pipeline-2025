FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies for mysql-connector and pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip first
RUN pip install --upgrade pip

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy main script
COPY main.py .

# Default command
CMD ["python", "main.py"]