# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Standardisation + Data Quality Gate
# MAGIC
# MAGIC Reads from Bronze, applies standardisation rules (type casting,
# MAGIC normalisation, deduplication), runs the full DQ suite, and writes
# MAGIC clean Silver tables. Halts the Job if any DQ check fails — the
# MAGIC reconciliation engine never runs on bad input data.

# COMMAND ----------

import sys
sys.path.insert(0, "/Repos/production/finance-reconciliation-engine")

from src.silver.standardise_cash import standardise_cash
from src.silver.standardise_rada import standardise_rada
from src.data_quality.dq_checks import run_dq_suite

# COMMAND ----------

dbutils.widgets.text("catalog", "finance_recon", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# COMMAND ----------

# MAGIC %md ## Standardise Cash

# COMMAND ----------

cash_result = standardise_cash(
    spark,
    bronze_table=f"{catalog}.bronze.cash_transactions",
    silver_table=f"{catalog}.silver.cash_transactions",
    quarantine_table=f"{catalog}.silver.cash_quarantine",
)
print(f"Cash — clean: {cash_result['clean_count']:,} | quarantine: {cash_result['quarantine_count']:,}")

# COMMAND ----------

# MAGIC %md ## Standardise RADA

# COMMAND ----------

rada_result = standardise_rada(
    spark,
    bronze_table=f"{catalog}.bronze.rada_transactions",
    silver_table=f"{catalog}.silver.rada_transactions",
    quarantine_table=f"{catalog}.silver.rada_quarantine",
)
print(f"RADA  — clean: {rada_result['clean_count']:,} | quarantine: {rada_result['quarantine_count']:,}")

# COMMAND ----------

# MAGIC %md ## Data Quality Gate
# MAGIC
# MAGIC Runs 8 checks across both Silver tables. Fails the notebook (and the
# MAGIC Databricks Job task) if any check drops below its threshold.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

dq_results = run_dq_suite(
    spark,
    cash_table=f"{catalog}.silver.cash_transactions",
    rada_table=f"{catalog}.silver.rada_transactions",
    dq_results_table=f"{catalog}.silver.dq_results",
    fail_on_error=True,
)

passed = sum(1 for r in dq_results if r.status == "PASS")
print(f"\nDQ Gate: {passed}/{len(dq_results)} checks passed")

# COMMAND ----------

dbutils.notebook.exit(
    f"Silver complete. Cash: {cash_result['clean_count']:,} | RADA: {rada_result['clean_count']:,} | DQ: {passed}/{len(dq_results)} passed"
)
