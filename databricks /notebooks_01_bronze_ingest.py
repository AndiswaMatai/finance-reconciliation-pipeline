# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Cash & RADA Ingestion
# MAGIC
# MAGIC Reads daily Cash and RADA extracts from the ADLS Gen2 landing zone
# MAGIC using Auto Loader and writes append-only Delta tables in the Bronze schema.
# MAGIC
# MAGIC **Trigger:** ADF pipeline `pl_finance_reconciliation` → Copy activity drops
# MAGIC files into `abfss://landing@{storage}.dfs.core.windows.net/` then invokes
# MAGIC this notebook as a Databricks Job task.

# COMMAND ----------

import sys
sys.path.insert(0, "/Repos/production/finance-reconciliation-engine")

from src.bronze.ingest_cash import ingest_cash, validate_cash_schema
from src.bronze.ingest_rada import ingest_rada

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "ADLS Storage Account Name")
dbutils.widgets.text("catalog",         "finance_recon", "Unity Catalog name")
dbutils.widgets.text("batch_date",      "", "Batch date (YYYY-MM-DD), blank = today")

storage = dbutils.widgets.get("storage_account")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

LANDING = f"abfss://landing@{storage}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md ## Ingest Cash

# COMMAND ----------

cash_count = ingest_cash(
    spark,
    source_path=f"{LANDING}/cash/",
    bronze_table=f"{catalog}.bronze.cash_transactions",
    checkpoint_path=f"abfss://checkpoints@{storage}.dfs.core.windows.net/cash",
)
print(f"bronze.cash_transactions: {cash_count:,} total rows")

cash_validation = validate_cash_schema(spark, f"{catalog}.bronze.cash_transactions")
assert cash_validation["schema_valid"], f"Cash schema invalid: {cash_validation['missing_mandatory_columns']}"

# COMMAND ----------

# MAGIC %md ## Ingest RADA

# COMMAND ----------

rada_count = ingest_rada(
    spark,
    source_path=f"{LANDING}/rada/",
    bronze_table=f"{catalog}.bronze.rada_transactions",
    checkpoint_path=f"abfss://checkpoints@{storage}.dfs.core.windows.net/rada",
)
print(f"bronze.rada_transactions: {rada_count:,} total rows")

# COMMAND ----------

dbutils.notebook.exit(f"Bronze complete. Cash: {cash_count:,} | RADA: {rada_count:,}")
