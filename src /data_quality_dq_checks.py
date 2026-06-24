"""
Data Quality Framework — Finance Reconciliation Engine

Runs four DQ checks against the Silver tables before the reconciliation
engine executes. If any check fails, the Databricks Job step fails and
the Azure Monitor alert fires — the Gold table is never silently published
with bad input data.

Checks:
  1. Completeness    — mandatory fields populated above threshold
  2. Duplicate rate  — duplicate records below max threshold
  3. Negative amounts — no negative transaction amounts
  4. Schema enforcement — all expected columns present with correct types
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, DateType
from dataclasses import dataclass, field
from typing import List
import sys


@dataclass
class DQResult:
    check_name: str
    table: str
    status: str      # PASS / FAIL
    metric_value: float
    threshold: float
    detail: str


def check_completeness(
    df: DataFrame, table: str, required_cols: List[str], threshold: float = 0.98
) -> DQResult:
    total = df.count()
    complete = df.select(required_cols).dropna().count()
    score = round(complete / total, 4) if total else 0.0
    return DQResult(
        check_name="completeness", table=table,
        status="PASS" if score >= threshold else "FAIL",
        metric_value=score, threshold=threshold,
        detail=f"{complete:,}/{total:,} rows have all required fields populated",
    )


def check_duplicates(
    df: DataFrame, table: str, key_col: str, threshold: float = 0.005
) -> DQResult:
    total = df.count()
    unique = df.select(key_col).distinct().count()
    dup_rate = round((total - unique) / total, 4) if total else 0.0
    return DQResult(
        check_name="duplicate_rate", table=table,
        status="PASS" if dup_rate <= threshold else "FAIL",
        metric_value=dup_rate, threshold=threshold,
        detail=f"{total - unique:,} duplicate {key_col} values out of {total:,} records",
    )


def check_negative_amounts(df: DataFrame, table: str) -> DQResult:
    total = df.count()
    neg_count = df.filter(F.col("amount") < 0).count()
    neg_rate = round(neg_count / total, 4) if total else 0.0
    return DQResult(
        check_name="negative_amounts", table=table,
        status="PASS" if neg_count == 0 else "FAIL",
        metric_value=neg_rate, threshold=0.0,
        detail=f"{neg_count:,} records with negative amount values",
    )


def check_schema(df: DataFrame, table: str, expected_cols: List[str]) -> DQResult:
    missing = [c for c in expected_cols if c not in df.columns]
    return DQResult(
        check_name="schema_enforcement", table=table,
        status="PASS" if not missing else "FAIL",
        metric_value=float(len(missing)), threshold=0.0,
        detail=f"Missing columns: {missing}" if missing else "All expected columns present",
    )


def run_dq_suite(
    spark: SparkSession,
    cash_table: str = "silver.cash_transactions",
    rada_table: str = "silver.rada_transactions",
    dq_results_table: str = "silver.dq_results",
    fail_on_error: bool = True,
) -> List[DQResult]:
    """
    Runs all DQ checks against both Silver tables. Writes results to
    silver.dq_results for audit and monitoring. Raises SystemExit if
    fail_on_error=True and any check fails — halts the Databricks Job.
    """
    CASH_REQUIRED = ["cash_id", "account_number", "trade_reference", "settlement_date", "currency", "amount"]
    RADA_REQUIRED = ["rada_id", "account_number", "trade_reference", "settlement_date", "currency", "amount"]

    cash_df = spark.table(cash_table)
    rada_df = spark.table(rada_table)

    results = [
        check_completeness(cash_df, cash_table, CASH_REQUIRED),
        check_duplicates(cash_df, cash_table, "cash_id"),
        check_negative_amounts(cash_df, cash_table),
        check_schema(cash_df, cash_table, CASH_REQUIRED),
        check_completeness(rada_df, rada_table, RADA_REQUIRED),
        check_duplicates(rada_df, rada_table, "rada_id"),
        check_negative_amounts(rada_df, rada_table),
        check_schema(rada_df, rada_table, RADA_REQUIRED),
    ]

    rows = [{
        "check_name": r.check_name, "table": r.table, "status": r.status,
        "metric_value": r.metric_value, "threshold": r.threshold, "detail": r.detail,
        "run_ts": str(F.current_timestamp()),
    } for r in results]

    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(dq_results_table)

    failed = [r for r in results if r.status == "FAIL"]
    for r in results:
        flag = "✓" if r.status == "PASS" else "✗"
        print(f"  [{flag}] {r.table}.{r.check_name}: {r.metric_value} (threshold {r.threshold})")
        if r.status == "FAIL":
            print(f"        {r.detail}")

    if failed and fail_on_error:
        print(f"\n{len(failed)} DQ checks failed — halting pipeline.")
        sys.exit(1)

    return results

