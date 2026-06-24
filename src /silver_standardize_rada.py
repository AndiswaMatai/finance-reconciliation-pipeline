"""
Silver Layer — RADA Standardisation

Mirrors standardise_cash.py exactly — same rules, same output schema,
different source table. Keeping the two standardisation modules separate
(rather than a shared generic function) makes it easy to accommodate
RADA-specific edge cases (e.g. RADA sends amounts in minor currency units
that need dividing by 100) without polluting the Cash logic.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def standardise_rada(
    spark: SparkSession,
    bronze_table: str = "bronze.rada_transactions",
    silver_table: str = "silver.rada_transactions",
    quarantine_table: str = "silver.rada_quarantine",
) -> dict:
    """
    Standardises the RADA Bronze table and writes to Silver.
    """
    df = spark.table(bronze_table)

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

    mandatory_ok = (
        F.col("rada_id").isNotNull() &
        F.col("account_number").isNotNull() &
        F.col("trade_reference").isNotNull() &
        F.col("settlement_date").isNotNull() &
        F.col("currency").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") >= 0) &
        F.col("debit_credit").isin("D", "C")
    )

    clean = df.filter(mandatory_ok).dropDuplicates(["rada_id"])
    quarantine = df.filter(~mandatory_ok).withColumn(
        "_rejection_reason", F.lit("mandatory_field_or_value_check_failed")
    )

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
