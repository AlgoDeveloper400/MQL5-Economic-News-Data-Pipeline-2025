FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Python and Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    openjdk-17-jdk \
    procps \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY spark_data_sorted_chrono_order.py .
RUN mkdir -p /app/input /app/output
CMD ["python", "spark_data_sorted_chrono_order.py"]