# ⚡ Automated MLOps & Data Pipeline (Medallion Architecture)

## Overview
This repository contains an end-to-end, containerized Data Engineering and Machine Learning pipeline. It is designed to autonomously ingest raw external API data, process it through a strict Medallion Architecture (Bronze, Silver, Gold), store it in a cloud data warehouse, and utilize it to train a serialized Machine Learning classification model.

## 🏗️ Architecture & Tech Stack
* **Orchestration:** Apache Airflow (Containerized via Docker)
* **Data Warehouse / Feature Store:** Google Cloud BigQuery
* **Data Processing:** Python, Pandas, PyArrow
* **Machine Learning:** Scikit-Learn (Random Forest Classifier)
* **Security:** IAM Service Accounts, `.env` isolated credentials

## ⚙️ Pipeline Flow
1. **Extraction (Bronze):** Airflow DAG triggers a Python script to pull raw JSON data from external REST APIs and land it locally.
2. **Transformation (Silver):** Data is cleaned, typed, and normalized. Class imbalances are addressed via dynamic thresholding to prevent zero-variance ML failures.
3. **Serving (Gold):** Fully structured, ML-ready features are pushed to Google Cloud BigQuery using high-speed Storage APIs.
4. **Machine Learning:** A local Python environment pulls the Gold data, performs Time-Series Imputation (Forward/Backward Fill) to handle missing values, and trains a dynamically-weighted Random Forest model. 
5. **Serialization:** The trained model is serialized into a `.joblib` artifact for future inference.

## 🚀 Local Setup Instructions

### Prerequisites
* Docker & Docker Compose
* Python 3.10+
* Google Cloud Platform Account (with BigQuery Admin/Read Session User IAM roles)

### Installation
1. Clone the repository:
   ```bash
   git clone [https://github.com/savvas-pehli/weather-ml-pipeline.git](https://github.com/savvas-pehli/weather-ml-pipeline.git)
   cd weather-ml-pipeline