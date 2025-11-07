# News ML Pipeline Documentation

## Overview
This document describes an automated machine learning pipeline for processing, analyzing, and serving news data. The pipeline is designed as a series of modular steps, orchestrated by Apache Airflow, with key components containerized for deployment on Google Cloud Platform (GCP) Kubernetes Engine.

![Pipeline Overview](News%20ML%20Pipeline%20WorkFlow.png)
 
## Pipeline Components

### 1. Data Scraping and Collection
**Tools: Selenium, WebDriver, CSV Export**

This initial stage gathers raw data from **MQL5 news sources** using Selenium WebDriver to automate browser interactions for websites that require dynamic content rendering. The scraper extracts relevant information including economic forecasts, dates and previous values. Collected data is exported to CSV files, providing a portable format for subsequent processing. This service runs as a scheduled Docker container triggered monthly.

**Key Functions:**
- Scraping news data specifically from **MQL5**
- Browser automation using Selenium WebDriver
- Targeted website crawling
- Structured data extraction
- CSV format standardization
- Error handling and retry mechanisms

### 2. Data Preprocessing
**Tools: Python (Pandas, NumPy)**

Raw scraped data undergoes comprehensive cleaning and preparation to ensure quality for analysis. This critical step addresses data inconsistencies and noise through multiple processing layers.

**Processing Steps:**
- **Data Cleaning:** Removal of invalid entries, duplicates, or corrupt records
- **Normalization:** Standardization of date formats, numeric values, and text fields
- **Data Validation:** Quality checks and integrity verification

### 3. Data Transformation & Storage
**Tools: PySpark, MySQL**

Preprocessed data undergoes final transformation using PySpark for efficient distributed processing of large datasets. The structured output is loaded into a MySQL database serving as the central data repository.

**Database Architecture:**
- Optimized schema for news data analytics
- Indexed tables for efficient querying
- Relationship management between articles and metadata
- The MySQL instance is deployed on GCP Kubernetes for high availability

### 4. Model Training & Evaluation
**Tools: Scikit-learn, PyTorch, MLflow**

The machine learning core where models are developed for news analysis tasks such as classification, trend prediction, or other structured-data-based predictions. The models intended for deployment with FastAPI use **Scikit-learn and PyTorch**.

**Training Workflow:**
1. **Feature Engineering:** Transformation of structured and textual features into model-ready format
2. **Model Training:** Algorithm training using Scikit-learn or PyTorch on historical news data
3. **Validation:** Hyperparameter tuning and performance assessment
4. **Testing:** Final evaluation on separate test datasets
5. **Versioning:** Comprehensive tracking using MLflow for reproducibility

### 5. Model Serving & Inference
**Tools: FastAPI, Scikit-learn, PyTorch**

The best-performing model is deployed as a REST API using FastAPI, providing real-time prediction capabilities. The service uses models trained in **Scikit-learn and PyTorch**, and accepts new news forecasts, returning structured predictions.

**Deployment Note:** This FastAPI service leverages online notebook platforms with GPU acceleration for high-performance inference and is not containerized in the current deployment cycle.

### 6. Dashboard & Visualization
**Tools: Streamlit/Dash, Docker, Kubernetes**

An interactive dashboard enables end-users to explore analyzed news data through visualizations and analytics. The application connects directly to the MySQL database to display processed articles with model predictions, trends, and insights.

**Dashboard Features:**
- Real-time data visualization
- Interactive filtering and search
- Trend analysis over time
- Model performance metrics
- The application is containerized and deployed on GCP Kubernetes

## Orchestration & Deployment

### Apache Airflow Automation
The entire pipeline is automated and scheduled using Apache Airflow, which manages workflow dependencies and execution timing.

**Orchestration Strategy:**
- Monthly pipeline execution schedule
- Dependency management between sequential steps
- Error handling and alert mechanisms
- Resource optimization through containerized task execution

### Infrastructure Architecture
**GCP Kubernetes Deployment:**
- **Persistent Services:** Dashboard and MySQL database run as stable, always-on services
- **Scheduled Tasks:** Data scraping, preprocessing, transformation, and model training execute as ephemeral Docker containers
- **Scalable Design:** Kubernetes-native architecture ensures resource efficiency and horizontal scaling capabilities

### Monitoring & Maintenance
- Pipeline health monitoring through Airflow dashboard
- Model performance tracking via MLflow
- Database performance optimization
- Regular updates to data sources and model retraining schedules


