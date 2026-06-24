# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Layer: KPI Summary and Reconciliation Report
# MAGIC
# MAGIC Queries the Gold reconciliation results and produces the daily KPI
# MAGIC summary consumed by the Power BI reconciliation dashboard via DirectLake.
# MAGIC
# MAGIC This notebook is the last task in the Databricks Job — successful
# MAGIC completion here means Finance can rely on today's numbers.

# COMMAND ----------

import sys
sys.path.insert(0, "/Repos/production/finance-reconciliation-engine")

# COMMAND ----------

dbutils.widgets.text("catalog", "finance_recon", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md ## Daily KPI Summary

# COMMAND ----------

from pyspark.sql import functions as F

results = spark.table(f"{catalog}.gold.reconciliation_results") \
               .filter(F.col("_run_date") == F.current_date())

total = results.count()
by_status = results.groupBy("recon_status").count().collect()
status_map = {r["recon_status"]: r["count"] for r in by_status}

matched    = status_map.get("MATCHED",         0)
unmatched  = status_map.get("UNMATCHED",       0)
new        = status_map.get("NEW",             0)
cleared    = status_map.get("CLEARED",         0)
mismatch   = status_map.get("AMOUNT_MISMATCH", 0)
match_rate = round(matched / total * 100, 2) if total else 0

print("=" * 55)
print("DAILY RECONCILIATION SUMMARY")
print("=" * 55)
print(f"Total records:     {total:,}")
print(f"Matched:           {matched:,}")
print(f"Unmatched:         {unmatched:,}")
print(f"New (RADA only):   {new:,}")
print(f"Cleared:           {cleared:,}")
print(f"Amount mismatch:   {mismatch:,}")
print(f"Match rate:        {match_rate}%")

# COMMAND ----------

# MAGIC %md ## Unmatched Exposure by Currency

# COMMAND ----------

exposure = (
    results
    .filter(F.col("recon_status") == "UNMATCHED")
    .groupBy("currency")
    .agg(F.sum("amount").alias("exposure"), F.count("*").alias("records"))
    .orderBy(F.desc("exposure"))
)
display(exposure)

# COMMAND ----------

# MAGIC %md ## Amount Mismatch Detail

# COMMAND ----------

display(
    results
    .filter(F.col("recon_status") == "AMOUNT_MISMATCH")
    .select("cash_id", "rada_id", "account_number", "trade_reference",
            "settlement_date", "currency", "amount", "rada_amount", "amount_variance")
    .orderBy(F.desc(F.abs("amount_variance")))
    .limit(50)
)

# COMMAND ----------

# MAGIC %md ## Optimise Gold Table

# COMMAND ----------

# OPTIMIZE + ZORDER on the most common filter columns to keep Power BI DirectLake queries fast
spark.sql(f"OPTIMIZE {catalog}.gold.reconciliation_results ZORDER BY (recon_status, settlement_date, currency)")
spark.sql(f"OPTIMIZE {catalog}.gold.reconciliation_kpis")

# COMMAND ----------

dbutils.notebook.exit(f"Gold complete. Match rate: {match_rate}% | Total: {total:,}")
