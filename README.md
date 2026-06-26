# 🏦 Finance Reconciliation Engine (Spark + Databricks + Azure)

![Azure](https://img.shields.io/badge/Cloud-Azure-blue?logo=microsoftazure)
![Databricks](https://img.shields.io/badge/Platform-Databricks-orange?logo=databricks)
![Apache Spark](https://img.shields.io/badge/Framework-Apache%20Spark-red?logo=apachespark)
![Python](https://img.shields.io/badge/Language-Python-yellow?logo=python)
![Terraform](https://img.shields.io/badge/DevOps-Terraform-purple?logo=terraform)
![CI/CD](https://img.shields.io/badge/DevOps-CI%2FCD-green?logo=gitlab)

---

## 🚀 Overview
A production-style financial reconciliation engine built using Apache Spark, simulating how banks reconcile transactions between Cash and RADA systems.  
Designed with **Medallion Architecture (Bronze → Silver → Gold)**, it demonstrates enterprise-grade practices: reconciliation logic, data quality validation, and cloud deployment design.

---

## 🧠 Business Problem
Financial institutions often face:
- Mismatched transaction records across systems  
- Delayed reconciliation cycles  
- Manual exception handling  
- Audit and compliance risks  

This system solves these by automating reconciliation with scalable Spark pipelines.

---

## 🎯 Solution Overview
- End-to-end transaction traceability across systems  
- Automated detection of mismatches and reconciliation breaks  
- Standardised data quality validation across all layers  
- Audit-ready outputs suitable for regulatory review  
- Scalable processing aligned with modern cloud platforms  

---

## 🏗️ Architecture
- **Bronze Layer** → Raw ingestion (Cash + RADA)  
- **Silver Layer** → Standardisation + Data Quality  
- **Reconciliation Engine** → Matching logic (Spark)  
- **Gold Layer** → KPIs + Reporting datasets  

---

## ⚙️ Key Features
**🔹 Data Engineering**  
- Apache Spark distributed processing  
- SHA-256 deterministic business keys  
- Broadcast joins for performance optimisation  
- Incremental snapshot processing  

**🔹 Reconciliation Engine**  
- Matched records  
- Unmatched records  
- New records detection  
- Field-level mismatch detection  

**🔹 Data Quality**  
- Null checks  
- Duplicate removal  
- Negative value detection  
- Schema enforcement  

**🔹 Platform Engineering**  
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
```
finance-reconciliation-engine/
├── src/
├── config/
├── data/
├── tests/
├── scripts/
├── databricks/
├── infrastructure/
├── Dockerfile
└── README.md
```
---

## ▶️ How to Run
1. Install dependencies  
   ```bash
   pip install -r requirements.txt


