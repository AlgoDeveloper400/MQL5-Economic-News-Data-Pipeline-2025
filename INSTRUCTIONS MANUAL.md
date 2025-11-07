# 🧠 MQL5 News Data ML Automated Pipeline

This repository provides a **modular, Dockerized data engineering and preprocessing pipeline** for collecting, processing, and preparing **MQL5 News data** for downstream machine learning workflows.

Designed for **flexibility, reproducibility, and ease of use**, this pipeline allows you to run individual modules independently or execute the entire workflow end-to-end.

---

## 🧩 Included Components

All of the following components are **fully containerized** for isolated, reproducible execution:

| Component | Description |
|-----------|-------------|
| 🕸️ **Web Scraper** | Python-based scraper that automatically collects and stores MQL5 news data in the configured volume. Fully configurable via environment variables. |
| 🔥 **PySpark Processor** | Distributed preprocessing engine using Spark. Handles filtering, transformation, and data preparation for database ingestion or ML training. |
| 🧮 **Python Data Merger Scripts** | Lightweight scripts to merge “main” and “monthly” batches into unified datasets. Can run independently or inside the container. |
| 🗄️ **MySQL Server** | Containerized MySQL instance for storing cleaned and structured data. Supports downstream dashboards or ML pipelines. |

> 💡 Each component has its own **Dockerfile** and/or **docker-compose YAML**, ensuring modularity and reproducibility.

---

## 🧱 Components *Not Included* (To Be Recreated)

These components are **intentionally omitted** to maintain flexibility and security:

| Missing Component | Reason |
|------------------|--------|
| 🧭 **MLflow UI & Tracking Server** | Users should configure their own MLflow backend to track experiments, models, and parameters. |
| ⚡ **FastAPI Model Serving (ML Model)** | API endpoints for serving the trained ML model are excluded so users can implement custom model-serving logic and authentication. |
| 📊 **Analytics Dashboard** | Dashboard tools (Streamlit, Grafana, etc.) are left for the user to build according to specific analytics needs. |
| 🔐 **`.env` File** | Fully user-defined. Users can create this file with any credentials, paths, or environment variables they like. |

---

## ⚙️ Quick Start Guide

### 1️⃣ Create Your `.env` File
Before running any container, create a `.env` file with your desired environment variables. This file can contain credentials, volume paths, API keys, or any configuration you need.

### 2️⃣ Start Containers
Use Docker Compose for each module:

```bash
# Start MySQL container
docker-compose -f mysql/docker-compose.yml up -d

# Start Web Scraper container
docker-compose -f web-scraper/docker-compose.yml up -d

# Start PySpark Processor container
docker-compose -f pyspark/docker-compose.yml up -d

# Run Python Data Merger (can be executed inside container)
docker-compose -f data-merger/docker-compose.yml up -d
