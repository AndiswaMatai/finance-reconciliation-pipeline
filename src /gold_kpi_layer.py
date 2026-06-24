"""
Gold Layer — Reconciliation KPIs and Reporting Dataset

Merges today's reconciliation results into the Gold Delta table using
MERGE INTO (upsert) so the table always reflects the latest status of
every business key — never appends duplicates on re-run.

KPIs produced:
  - Match rate (%) — the headline operational metric
  - Total matched, unmatched, new, cleared, amount_mismatch counts
  - Total exposure on unmatched records (ZAR equivalent)
  - Average amount variance on matched records
  - Currency-level breakdown

The Gold table is the DirectLake source for the Power BI reconciliation
dashboard, so a successful write here is what "publishes" numbers to Finance.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def merge_to_gold(
    spark: SparkSession,
    matched: DataFrame,
    unmatched: DataFrame,
    new: DataFrame,
    cleared: DataFrame,
    amount_mismatched: DataFrame,
    gold_table: str = "gold.reconciliation_results",
) -> None:
    """
    Unions all four classification DataFrames and MERGEs into the Gold table.
    MERGE ensures idempotent writes — re-running produces the same result.
    """
    run_ts = F.current_timestamp()

    all_results = (
        matched
        .unionByName(unmatched, allowMissingColumns=True)
        .unionByName(new,       allowMissingColumns=True)
        .unionByName(cleared,   allowMissingColumns=True)
        .unionByName(amount_mismatched, allowMissingColumns=True)
        .withColumn("_run_ts", run_ts)
        .withColumn("_run_date", F.current_date())
    )

    if spark.catalog.tableExists(gold_table):
        target = DeltaTable.forName(spark, gold_table)
        (
            target.alias("t")
            .merge(
                all_results.alias("s"),
                "t.business_key = s.business_key AND t._run_date = s._run_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        all_results.write.format("delta") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(gold_table)


def compute_kpis(
    spark: SparkSession,
    gold_table: str = "gold.reconciliation_results",
    kpi_table: str = "gold.reconciliation_kpis",
) -> None:
    """
    Aggregates the Gold reconciliation table into a daily KPI summary.
    This is the table Power BI queries — small, fast, pre-aggregated.
    """
    df = spark.table(gold_table).filter(F.col("_run_date") == F.current_date())

    total = df.count()
    status_counts = df.groupBy("recon_status").count().collect()
    status_map = {row["recon_status"]: row["count"] for row in status_counts}

    matched_count = status_map.get("MATCHED", 0)
    match_rate = round(matched_count / total * 100, 2) if total else 0

    # Unmatched exposure — total ZAR value sitting in breaks today
    unmatched_exposure = (
        df.filter(F.col("recon_status") == "UNMATCHED")
          .agg(F.sum("amount").alias("total_exposure"))
          .collect()[0]["total_exposure"] or 0.0
    )

    # Average amount variance on matched records
    avg_variance = (
        df.filter(F.col("recon_status").isin("MATCHED", "AMOUNT_MISMATCH"))
          .agg(F.avg(F.abs("amount_variance")).alias("avg_variance"))
          .collect()[0]["avg_variance"] or 0.0
    )

    # Currency breakdown
    currency_breakdown = (
        df.groupBy("currency", "recon_status")
          .agg(F.count("*").alias("count"), F.sum("amount").alias("total_amount"))
    )

    kpis = spark.createDataFrame([{
        "run_date": str(F.current_date()),
        "total_records": total,
        "matched": matched_count,
        "unmatched": status_map.get("UNMATCHED", 0),
        "new": status_map.get("NEW", 0),
        "cleared": status_map.get("CLEARED", 0),
        "amount_mismatch": status_map.get("AMOUNT_MISMATCH", 0),
        "match_rate_pct": match_rate,
        "unmatched_exposure_zar": round(float(unmatched_exposure), 2),
        "avg_amount_variance": round(float(avg_variance), 4),
    }])

    kpis.write.format("delta").mode("append").saveAsTable(kpi_table)
    currency_breakdown.withColumn("_run_date", F.current_date()) \
                       .write.format("delta").mode("append") \
                       .saveAsTable("gold.recon_currency_breakdown")

    print(f"Match rate: {match_rate}% | Unmatched exposure: ZAR {unmatched_exposure:,.2f}")
