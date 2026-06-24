"""
Reconciliation Engine — Cash vs RADA Matching

Performs deterministic record matching between the standardised, keyed
Cash and RADA Silver datasets and classifies every record into one of
four states:

  MATCHED    — same business key, amount within tolerance
  UNMATCHED  — exists in Cash but not in RADA (or vice versa)
  NEW        — exists in RADA but has no Cash counterpart
  CLEARED    — was Unmatched in a previous snapshot, now has a match
               (detected by comparing today's result against the
               previous day's Gold snapshot)

Architecture decisions:
  - Broadcast join: RADA is the smaller dataset (custody system vs cash
    ledger), so broadcasting it avoids a full shuffle on the Cash side.
  - Amount tolerance: configurable in config/config.yaml — FX rounding
    and minor currency differences should not produce false Unmatch flags.
  - Snapshot comparison: the previous Gold snapshot is read via Delta
    Time Travel (VERSION AS OF or TIMESTAMP AS OF) so the Cleared
    detection logic is fully reproducible for audit.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

AMOUNT_TOLERANCE = 1.00   # ZAR equivalent — from config/config.yaml


def reconcile(
    spark: SparkSession,
    cash_df: DataFrame,
    rada_df: DataFrame,
    amount_tolerance: float = AMOUNT_TOLERANCE,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Core matching function. Takes keyed Cash and RADA DataFrames,
    returns four DataFrames: matched, unmatched, new, mismatched_amount.

    Args:
        spark:            Active SparkSession
        cash_df:          Cash DataFrame with business_key column
        rada_df:          RADA DataFrame with business_key column
        amount_tolerance: Maximum amount difference for a match

    Returns:
        (matched, unmatched_cash, new_rada, amount_mismatched)
    """

    # Broadcast RADA — smaller dataset, avoids full shuffle on Cash side
    rada_broadcast = F.broadcast(
        rada_df.select(
            "business_key",
            F.col("amount").alias("rada_amount"),
            F.col("rada_id"),
            F.col("status").alias("rada_status"),
        )
    )

    # Full outer join on business_key to surface all four states
    joined = cash_df.alias("c").join(
        rada_broadcast.alias("r"),
        on="business_key",
        how="full_outer",
    )

    # ── MATCHED: key exists in both, amount within tolerance ─────────────────
    matched = (
        joined
        .filter(F.col("c.cash_id").isNotNull() & F.col("r.rada_id").isNotNull())
        .filter(F.abs(F.col("c.amount") - F.col("r.rada_amount")) <= amount_tolerance)
        .withColumn("recon_status", F.lit("MATCHED"))
        .withColumn("amount_variance", F.col("c.amount") - F.col("r.rada_amount"))
        .select(
            "business_key", "c.cash_id", "r.rada_id",
            "c.account_number", "c.trade_reference", "c.settlement_date",
            "c.currency", "c.amount", "r.rada_amount", "amount_variance",
            "c.swift_bic", "c.debit_credit", "recon_status",
        )
    )

    # ── UNMATCHED: exists in Cash, missing from RADA ──────────────────────────
    unmatched_cash = (
        joined
        .filter(F.col("c.cash_id").isNotNull() & F.col("r.rada_id").isNull())
        .withColumn("recon_status", F.lit("UNMATCHED"))
        .select(
            "business_key", "c.cash_id",
            "c.account_number", "c.trade_reference", "c.settlement_date",
            "c.currency", "c.amount", "c.swift_bic", "c.debit_credit", "recon_status",
        )
    )

    # ── NEW: exists in RADA, missing from Cash ────────────────────────────────
    new_rada = (
        joined
        .filter(F.col("c.cash_id").isNull() & F.col("r.rada_id").isNotNull())
        .withColumn("recon_status", F.lit("NEW"))
        .select(
            "business_key", "r.rada_id",
            F.col("r.rada_amount").alias("amount"),
            "recon_status",
        )
    )

    # ── AMOUNT MISMATCH: key matches but amount exceeds tolerance ─────────────
    amount_mismatched = (
        joined
        .filter(F.col("c.cash_id").isNotNull() & F.col("r.rada_id").isNotNull())
        .filter(F.abs(F.col("c.amount") - F.col("r.rada_amount")) > amount_tolerance)
        .withColumn("recon_status", F.lit("AMOUNT_MISMATCH"))
        .withColumn("amount_variance", F.col("c.amount") - F.col("r.rada_amount"))
        .select(
            "business_key", "c.cash_id", "r.rada_id",
            "c.account_number", "c.trade_reference", "c.settlement_date",
            "c.currency", "c.amount", "r.rada_amount", "amount_variance",
            "c.debit_credit", "recon_status",
        )
    )

    return matched, unmatched_cash, new_rada, amount_mismatched


def detect_cleared(
    spark: SparkSession,
    current_unmatched: DataFrame,
    gold_table: str = "gold.reconciliation_results",
    lookback_days: int = 5,
) -> tuple[DataFrame, DataFrame]:
    """
    Compares today's Unmatched records against the previous snapshot to
    identify CLEARED records — items that were Unmatched in a prior run
    but are now absent (meaning they matched in a subsequent settlement cycle).

    Uses Delta Time Travel (timestampAsOf) for reproducible snapshot comparison.

    Returns:
        (cleared_df, still_unmatched_df)
    """
    try:
        previous = (
            spark.read.format("delta")
            .option("timestampAsOf", F.date_sub(F.current_date(), lookback_days).cast("string"))
            .table(gold_table)
            .filter(F.col("recon_status") == "UNMATCHED")
            .select("business_key")
        )

        # Keys that were unmatched before but are NOT in today's unmatched set = CLEARED
        cleared_keys = previous.join(
            current_unmatched.select("business_key"),
            on="business_key",
            how="left_anti",
        )

        cleared = previous.join(cleared_keys, on="business_key", how="inner") \
                          .withColumn("recon_status", F.lit("CLEARED"))

        still_unmatched = current_unmatched.join(
            cleared.select("business_key"), on="business_key", how="left_anti"
        )

        return cleared, still_unmatched

    except Exception:
        # First run — no previous snapshot exists yet
        return spark.createDataFrame([], current_unmatched.schema), current_unmatched
