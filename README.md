finance-reconciliation-pipeline/
│
├── README.md
├── requirements.txt
├── run_pipeline.py   # (or src/run_pipeline.py if already exists)
│
├── src/
│   ├── ingestion/
│   │   ├── ingest.py                 # aligns with README
│   │
│   ├── transform/
│   │   ├── transform.py              # bronze → silver → gold logic
│   │
│   ├── reconciliation/
│   │   ├── reconcile.py             # source → subledger → GL logic
│   │
│   ├── data_quality/
│   │   ├── data_quality.py          # DQ checks
│   │
│   ├── models/
│   │   ├── scd2_dim_account.py      # THIS aligns with your README claim
│   │
│   ├── utils/
│   │   ├── logging.py
│   │   ├── config.py
│
├── sql/
│   ├── scd2_dim_account.sql
│   ├── reconciliation_queries.sql
│
├── tests/
│   ├── test_pipeline.py
│   ├── test_reconciliation.py
│   ├── test_data_quality.py
│
├── data/
│   ├── raw/
│   ├── staging/
│   ├── output/
│
├── cost_optimization/
│   ├── cost_calculator.py
│
├── monitoring/
│   ├── alert_rules.tf
│   ├── kql_queries.md
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│
├── docs/
│   ├── architecture.md
│   ├── production_design.md
│   ├── tradeoffs.md
│
├── .github/
│   ├── workflows/
│       ├── ci.yml
│       ├── cd.yml
│
└── assets/
    ├── system_architecture.png
    ├── data_flow_diagram.png



# 🏦 Finance Reconciliation Engine (Production-Style Data System)

A production-inspired data engineering system designed to reconcile financial datasets between Cash and RADA systems in a simulated banking environment.

The system demonstrates how enterprise financial institutions detect mismatches, maintain data integrity, and produce reconciled reporting layers for downstream consumption.

---

## 🎯 Business Problem

In financial institutions, transaction data is sourced from multiple systems that often:

- Arrive at different times
- Contain inconsistent formatting
- Include duplicates or missing references
- Require regulatory-level accuracy

This leads to:
- reconciliation breaks
- manual investigation effort
- delayed financial reporting
- operational risk

---

## 🏗️ Architecture Overview

The system is built using a layered data engineering approach:

1. **Ingestion Layer**
   - Loads raw Cash and RADA datasets

2. **Standardisation Layer**
   - Cleans and normalizes data
   - Ensures consistent formats across systems

3. **Key Construction Layer**
   - Builds composite business keys
   - Prepares data for deterministic matching

4. **Reconciliation Engine**
   - Performs record matching logic
   - Classifies records into:
     - Matched
     - Unmatched
     - New
     - Cleared

5. **Output Layer**
   - Produces reporting-ready datasets
   - Feeds Power BI dashboards or downstream systems

---

## 🧠 Core Engineering Logic

The system uses deterministic matching based on:

- Account identifiers
- Trade references
- Settlement dates
- Value attributes

Reconciliation is performed by comparing:

- Current dataset vs previous snapshot
- Business key alignment
- Field-level comparisons

---

## ⚙️ Production Considerations

This system is designed with enterprise patterns in mind:

- Idempotent pipeline execution
- Modular transformation layers
- Separation of ingestion and processing logic
- Extensible matching rules
- Audit-friendly outputs

---

## 📊 Outputs

- Matched transactions dataset
- Unmatched exceptions dataset
- Cleared records dataset
- Power BI-ready reconciliation layer

---

## 🛠️ Tech Stack

- Python
- SQL
- Data Engineering principles
- Optional: Spark / Databricks concepts
- GitHub Actions (CI simulation)

---

## 🚀 How to Run

```bash
python pipelines/orchestration_flow.py
