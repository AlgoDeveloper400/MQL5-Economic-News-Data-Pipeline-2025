# Problem Statement

Trading strategies that react to economic events need a reliable feed of that news, but economic calendar data isn't something you can just pull from a clean API. Sites like MQL5 publish it as web content that changes layout, has inconsistent formatting, and produces broken or misaligned schemas when scraped naively. Without a proper pipeline, that raw data is unusable for anything downstream, and any model trained on it inherits those errors silently.

This project was built to solve that problem directly. It's an automated pipeline that collects economic news and event data from MQL5, repairs and normalizes it into a consistent schema, and turns it into a structured dataset that can support model training and live prediction. Specifically, it needed to:

- Scrape economic event data from MQL5 on a repeatable, scheduled basis rather than as a one-off pull
- Detect and fix broken or misaligned schemas so downstream processing doesn't fail or silently corrupt data
- Process and normalize the data at scale using distributed tooling, then store it in a queryable form
- Feed the cleaned dataset into an ML lifecycle (training, validation, testing) with full experiment tracking
- Serve live predictions through an API and dashboard, and keep the whole pipeline running unattended in production

Everything below, from scraping through schema repair, Spark processing, the ML lifecycle, and deployment, is the system built to solve that problem end to end.

---

# 🧠 MQL5 Economic News Data Pipeline 2025

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-lightblue?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Deployed-brightgreen?logo=kubernetes)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Infra-yellow?logo=googlecloud)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend%20API-teal?logo=fastapi)
![MLflow](https://img.shields.io/badge/MLflow-Model%20Tracking-lightgrey?logo=mlflow)
![MySQL](https://img.shields.io/badge/MySQL-Database-orange?logo=mysql)
![JavaScript](https://img.shields.io/badge/JavaScript-Dashboard-yellow?logo=javascript)
[![YouTube](https://img.shields.io/badge/YouTube-%40bdb5905-red?logo=youtube)](https://www.youtube.com/@bdb5905)

---

A **production-grade data and machine learning pipeline** designed to collect, process, and make predictions using **economic news data from the MQL5 website**.  
The architecture integrates **Python, PySpark, Airflow, Docker, Kubernetes, GCP, FastAPI, MLflow, MySQL, and JavaScript** into a scalable and modular system for automated data workflows.

> ⚠️ **Note:**  
> Certain components of this pipeline are intentionally left out in this repository for privacy and environment-specific reasons.  
> The provided modules represent the core production logic and structure.

---

## 📊 Pipeline Architecture

![Pipeline Diagram](./News%20ML%20Pipeline%20WorkFlow.png)

This diagram outlines the end-to-end flow — from data ingestion to transformation, modeling, tracking, and real-time prediction delivery.

---

## ⚙️ Tech Stack Overview

| Layer | Technology | Purpose |
|-------|-------------|----------|
| **Data Ingestion** | Python | Scrape MQL5 economic news data |
| **Schema Handling** | Python (pandas) | Repair and normalize broken schemas |
| **Processing Engine** | PySpark | Distributed data processing and structuring |
| **Database** | MySQL | Store cleaned and transformed data |
| **Orchestration** | Apache Airflow | Automate and schedule pipeline tasks |
| **API Layer** | FastAPI | Model serving and inference endpoints |
| **Experiment Tracking** | MLflow | Track, compare, and register models |
| **Visualization** | JavaScript | Real-time prediction dashboard |
| **Deployment** | Docker & Kubernetes (GCP) | Scalable, containerized production deployment |

---

## 🧩 Step-by-Step Breakdown

### **1. Data Ingestion and Collection**
- Scrapes the **MQL5 website** for economic event and news data.  
- Uses **Python** scripts to extract, format, and store raw data as **CSV** files.  
- Establishes a consistent and traceable data input pipeline.

---

### **2. Schema Fix**
- Runs schema validation and correction using a dedicated **Python script**.  
- Fixes missing or misaligned columns, enforces consistent data types, and standardizes field naming.  
- Ensures clean and structured data for distributed processing.

---

### **3. Spark Processing**
- Utilizes **PySpark** for distributed data transformation and normalization.  
- Processes large datasets efficiently, preparing them for storage and downstream tasks.  
- Outputs structured, uniform datasets for transformation and analysis.

---

### **4. Data Transformation**
- Conducts final data cleanup and feature selection within **MySQL**.  
- Removes redundant fields, applies filters, and stores the refined dataset.  
- Produces a high-quality feature set ready for machine learning.

---

### **5. FastAPI ML Model UI**
Represents the **training, validation, and testing** stages of the ML lifecycle.

- **Training:** Trains models using the prepared MySQL dataset.  
- **Validation:** Assesses model accuracy and performance metrics (**MSE**, **R²**, etc.).  
- **Testing:** Evaluates the model on unseen data to confirm reliability.  

A **FastAPI** service exposes REST endpoints to trigger training, validation, and prediction.

---

### **6. MLflow Tracking and Model Registry**
- Uses **MLflow** to record metrics, parameters, and artifacts for every experiment.  
- Manages all model versions through the **MLflow Model Registry**.  
- Enables experiment reproducibility and controlled production rollout.

---

### **7. Dashboard and Live Predictions**
- A **JavaScript dashboard** visualizes live predictions and key metrics in real time.  
- Communicates with FastAPI endpoints for streaming results and monitoring performance.  
- Provides actionable insights for economic data and event analysis.

---

### **8. Deployment**
- Each stage of the pipeline is **Dockerized** for environment consistency.  
- Deployed on **Kubernetes (GCP)** for scaling, load balancing, and reliability.  
- **Airflow** orchestrates retraining, monitoring, and periodic updates.  
- Designed for modular scaling — each component operates independently in production.

---

## 🧭 Summary

The **MQL5 Economic News Data Pipeline 2025** delivers a scalable, modular, and automated production pipeline for financial and economic data.  
It unifies the full ML lifecycle — ingestion, schema repair, distributed processing, model training, versioning, and deployment — in a robust, cloud-native environment.

This repository serves as a reference architecture and implementation baseline for enterprise-grade ML systems focused on automation, reproducibility, and performance.

---

## 🎥 Video Explanation

A full playlist walkthrough explaining this pipeline — including architecture, components, and workflow execution — will be uploaded to **[Big Data Brain (@bdb5905)](https://www.youtube.com/watch?v=TEO_aC3QD90&list=PLnMdLu-_m6HKM7gM4BDHAxfmcQVkAS2Nh)** on YouTube.

Subscribe to the channel to get notified when it goes live and for more content on **Big Data, Machine Learning Pipelines, and Production Systems**.

---
