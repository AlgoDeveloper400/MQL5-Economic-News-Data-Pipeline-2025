FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including for better memory management
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY data_script_merge_batch_plus_monthly.py .

# Create the directory structure that will be mounted
RUN mkdir -p /app/data/main_batch /app/data/monthly_batch /app/data/merged_batch

# Set Python memory optimization flags
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the entry point
CMD ["python", "data_script_merge_batch_plus_monthly.py"]