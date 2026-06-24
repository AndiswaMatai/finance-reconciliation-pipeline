# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Reconciliation Engine: Cash vs RADA Matching
# MAGIC
# MAGIC Builds SHA-256 business keys, performs broadcast-join matching between
# MAGIC Cash and RADA, classifies every record into MATCHED / UNMATCHED / NEW /
# MAGIC CLEARED / AMOUNT_MISMATCH, and writes results to the Gold reconciliation
# MAGIC table via Delta MERGE (idempotent — safe to re-run).

# COMMAND ----------

import sys
sys.path.insert(0, "/Repos/production/finance-reconciliation-engine")

from src.reconciliation.key_builder import prepare_cash_for_matching, prepare_rada_for_matching
from src.reconciliation.engine import reconcile, detect_cleared

# COMMAND ----------

dbutils.widgets.text("catalog",          "finance_recon", "Unity Catalog name")
dbutils.widgets.text("amount_tolerance", "1.00",          "Amount match tolerance (ZAR)")

catalog          = dbutils.widgets.get("catalog")
amount_tolerance = float(dbutils.widgets.get("amount_tolerance"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md ## Build Business Keys

# COMMAND ----------

cash_keyed = prepare_cash_for_matching(spark, silver_table=f"{catalog}.silver.cash_transactions")
rada_keyed = prepare_rada_for_matching(spark, silver_table=f"{catalog}.silver.rada_transactions")

print(f"Cash records keyed: {cash_keyed.count():,}")
print(f"RADA records keyed: {rada_keyed.count():,}")

# COMMAND ----------

# MAGIC %md ## Run Reconciliation Engine
# MAGIC
# MAGIC RADA is broadcast — smaller dataset, avoids full shuffle on the Cash side.

# COMMAND ----------

matched, unmatched, new, amount_mismatched = reconcile(
    spark, cash_keyed, rada_keyed, amount_tolerance=amount_tolerance
)

print(f"MATCHED:          {matched.count():,}")
print(f"UNMATCHED (Cash): {unmatched.count():,}")
print(f"NEW (RADA only):  {new.count():,}")
print(f"AMOUNT MISMATCH:  {amount_mismatched.count():,}")

# COMMAND ----------

# MAGIC %md ## Detect CLEARED Records
# MAGIC
# MAGIC Compares today's Unmatched against the previous snapshot using Delta
# MAGIC Time Travel to surface records that settled in a prior cycle.

# COMMAND ----------

cleared, still_unmatched = detect_cleared(
    spark,
    current_unmatched=unmatched,
    gold_table=f"{catalog}.gold.reconciliation_results",
    lookback_days=5,
)
print(f"CLEARED:          {cleared.count():,}")
print(f"STILL UNMATCHED:  {still_unmatched.count():,}")

# COMMAND ----------

# MAGIC %md ## Write to Gold (MERGE — idempotent)

# COMMAND ----------

from src.gold.kpi_layer import merge_to_gold, compute_kpis

merge_to_gold(
    spark, matched, still_unmatched, new, cleared, amount_mismatched,
    gold_table=f"{catalog}.gold.reconciliation_results",
)

compute_kpis(
    spark,
    gold_table=f"{catalog}.gold.reconciliation_results",
    kpi_table=f"{catalog}.gold.reconciliation_kpis",
)

# COMMAND ----------

total = matched.count() + still_unmatched.count() + new.count() + cleared.count() + amount_mismatched.count()
match_rate = round(matched.count() / total * 100, 2) if total else 0

dbutils.notebook.exit(
    f"Reconciliation complete. Match rate: {match_rate}% | "
    f"Matched: {matched.count():,} | Unmatched: {still_unmatched.count():,} | "
    f"New: {new.count():,} | Cleared: {cleared.count():,} | Mismatch: {amount_mismatched.count():,}"
)
