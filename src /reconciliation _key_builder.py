"""
Key Construction Layer — SHA-256 Deterministic Business Keys

Builds composite matching keys from the standardised Silver tables. Using
SHA-256 over a concatenation of the matching fields guarantees:

  1. Determinism — the same record always produces the same key, regardless
     of which system it came from (Cash or RADA), so the reconciliation
     engine can join on a single column.

  2. Sensitivity — a single character difference (e.g. ZAR vs zar, or a
     trailing space) produces a completely different hash, so no false
     matches slip through.

  3. Auditability — the key_input column records exactly what went into
     the hash, so a Finance analyst can reconstruct and verify any key.

Matching fields (from config/config.yaml):
  account_number | trade_reference | settlement_date | currency | debit_credit
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


MATCHING_FIELDS = [
    "account_number",
    "trade_reference",
    "settlement_date",
    "currency",
    "debit_credit",
]

SEPARATOR = "|"


def build_key_expression() -> F.Column:
    """
    Builds the SHA-256 expression used for both Cash and RADA.
    Concatenates matching fields with a pipe separator then hashes.
    Casting settlement_date to string first ensures consistent format
    regardless of how the DateType is serialised in the Spark plan.
    """
    parts = [F.coalesce(F.col(f).cast("string"), F.lit("NULL")) for f in MATCHING_FIELDS]
    concat_expr = F.concat_ws(SEPARATOR, *parts)
    return F.sha2(concat_expr, 256).alias("business_key"), concat_expr.alias("key_input")


def add_business_keys(df: DataFrame) -> DataFrame:
    """
    Adds business_key (SHA-256 hash) and key_input (raw concatenation)
    columns to a Silver DataFrame.
    """
    business_key, key_input = build_key_expression()
    return df.withColumn("business_key", business_key) \
             .withColumn("key_input",    key_input)


def prepare_cash_for_matching(
    spark: SparkSession,
    silver_table: str = "silver.cash_transactions",
) -> DataFrame:
    df = spark.table(silver_table)
    return add_business_keys(df).select(
        "business_key", "key_input",
        "cash_id", "account_number", "trade_reference", "swift_bic",
        "settlement_date", "value_date", "currency", "amount", "debit_credit",
        "narrative", "status",
    )


def prepare_rada_for_matching(
    spark: SparkSession,
    silver_table: str = "silver.rada_transactions",
) -> DataFrame:
    df = spark.table(silver_table)
    return add_business_keys(df).select(
        "business_key", "key_input",
        "rada_id", "account_number", "trade_reference", "swift_bic",
        "settlement_date", "value_date", "currency", "amount", "debit_credit",
        "narrative", "status",
    )
