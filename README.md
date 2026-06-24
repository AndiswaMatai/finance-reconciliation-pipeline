# 🏦 Finance Reconciliation Engine (Spark + Databricks + Azure)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-green)
![Status](https://img.shields.io/badge/Status-Production%20Simulation-success)

---

## 🚀 Overview

A production-style financial reconciliation engine built using **Apache Spark**, simulating how banks reconcile transactions between **Cash and RADA systems**.

The system is designed using **Medallion Architecture (Bronze → Silver → Gold)** and demonstrates enterprise-grade data engineering practices including reconciliation logic, data quality validation, and cloud deployment design.

---

## 🧠 Business Problem

Financial institutions often face:

- Mismatched transaction records across systems  
- Delayed reconciliation cycles  
- Manual exception handling  
- Audit and compliance risks  

This system solves these by automating reconciliation using scalable Spark pipelines.

---

## 🎯 Solution Overview

This platform addresses these challenges by implementing an automated, scalable reconciliation engine using Apache Spark.

It ensures:

- End-to-end transaction traceability across systems
- Automated detection of mismatches and reconciliation breaks
- Standardised data quality validation across all processing layers
- Structured audit-ready outputs suitable for regulatory review
- Scalable processing design aligned with modern cloud data platforms

---

## 🏗️ Architecture

Bronze Layer → Raw ingestion (Cash + RADA)
Silver Layer → Standardisation + Data Quality
Reconciliation Engine → Matching logic (Spark)
Gold Layer → KPIs + Reporting datasets

---

## ⚙️ Key Features

### 🔹 Data Engineering
- Apache Spark distributed processing
- SHA-256 deterministic business keys
- Broadcast joins for performance optimisation
- Incremental snapshot processing

### 🔹 Reconciliation Engine
- Matched records
- Unmatched records
- New records detection
- Field-level mismatch detection

### 🔹 Data Quality
- Null checks
- Duplicate removal
- Negative value detection
- Schema enforcement

### 🔹 Platform Engineering
- Docker containerisation
- CI/CD with GitHub Actions
- Terraform infrastructure design (Azure-ready)
- Databricks job orchestration

---

## 📊 Outputs

- Reconciled transaction dataset
- Exception report (unmatched records)
- KPI summary table
- Gold-layer reporting dataset (Power BI ready)

---

## 🧱 Tech Stack

- Apache Spark (PySpark)
- Python
- Delta Lake (design pattern)
- Azure Data Lake (architecture design)
- Databricks (deployment design)
- Docker
- GitHub Actions
- Terraform

---

## 📂 Project Structure

finance-reconciliation-engine/
│
├── src/
├── config/
├── data/
├── tests/
├── scripts/
├── databricks/
├── infrastructure/
├── Dockerfile
└── README.md


---

## ▶️ How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt

