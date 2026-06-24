"""
Silver Layer — Cash Standardisation

Reads from bronze.cash_transactions, applies data quality rules, normalises
field formats, and writes a clean Silver table ready for the Key Construction
and Reconciliation Engine layers.

Standardisation rules:
  - Parse settlement_date / value_date / transaction_date to DateType
  - Trim and upper-case account_number, trade_reference, currency
  - Standardise debit_credit to 'D' / 'C' only
  - Drop records that fail mandatory field checks (written to quarantine)
  - Remove duplicates on cash_id (idempotent — safe to re-run)
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def standardise_cash(
    spark: SparkSession,
    bronze_table: str = "bronze.cash_transactions",
    silver_table: str = "silver.cash_transactions",
    quarantine_table: str = "silver.cash_quarantine",
) -> dict:
    """
    Standardises the Cash Bronze table and writes to Silver.
    Returns a summary dict with clean and quarantine counts.
    """
    df = spark.table(bronze_table)

    # ── Type casting and normalisation ───────────────────────────────────────
    df = (
        df
        .withColumn("settlement_date",  F.to_date("settlement_date",  "yyyy-MM-dd"))
        .withColumn("value_date",       F.to_date("value_date",       "yyyy-MM-dd"))
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .withColumn("account_number",   F.upper(F.trim("account_number")))
        .withColumn("trade_reference",  F.upper(F.trim("trade_reference")))
        .withColumn("currency",         F.upper(F.trim("currency")))
        .withColumn("swift_bic",        F.upper(F.trim("swift_bic")))
        .withColumn("debit_credit",     F.upper(F.trim("debit_credit")))
        .withColumn("amount",           F.col("amount").cast("double"))
    )

    # ── Data quality filters ─────────────────────────────────────────────────
    mandatory_ok = (
        F.col("cash_id").isNotNull() &
        F.col("account_number").isNotNull() &
        F.col("trade_reference").isNotNull() &
        F.col("settlement_date").isNotNull() &
        F.col("currency").isNotNull() &
        (F.col("amount").isNotNull()) &
        (F.col("amount") >= 0) &
        F.col("debit_credit").isin("D", "C")
    )

    clean = df.filter(mandatory_ok).dropDuplicates(["cash_id"])
    quarantine = df.filter(~mandatory_ok).withColumn("_rejection_reason", F.lit("mandatory_field_or_value_check_failed"))

    # ── Write to Silver (MERGE to avoid duplicates on re-run) ────────────────
    clean.withColumn("_standardised_ts", F.current_timestamp()) \
         .write.format("delta") \
         .mode("overwrite") \
         .option("overwriteSchema", "true") \
         .saveAsTable(silver_table)

    if quarantine.count() > 0:
        quarantine.write.format("delta").mode("append").saveAsTable(quarantine_table)

    return {
        "silver_table": silver_table,
        "clean_count": clean.count(),
        "quarantine_count": quarantine.count(),
    }
