"""
Bronze Layer — RADA System Ingestion

Reads raw RADA (Reference and Data Archive) transaction exports and writes
them as append-only Delta tables in the Bronze schema. RADA is the
counterpart to Cash — reconciling between them is the core of this engine.

RADA extracts typically arrive as fixed-width or pipe-delimited files from
the custody/settlement system. This module normalises to CSV-on-Delta for
consistency with the Cash Bronze layer.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

RADA_SCHEMA = StructType([
    StructField("rada_id",          StringType(), False),
    StructField("account_number",   StringType(), False),
    StructField("trade_reference",  StringType(), False),
    StructField("swift_bic",        StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("value_date",       StringType(), True),
    StructField("settlement_date",  StringType(), True),
    StructField("currency",         StringType(), True),
    StructField("amount",           DoubleType(), True),
    StructField("debit_credit",     StringType(), True),
    StructField("narrative",        StringType(), True),
    StructField("status",           StringType(), True),
])


def ingest_rada(
    spark: SparkSession,
    source_path: str,
    bronze_table: str = "bronze.rada_transactions",
    checkpoint_path: str = None,
) -> int:
    """
    Reads RADA extracts via Auto Loader and appends to the bronze Delta table.
    Mirrors ingest_cash.py — same pattern, different source schema.
    """
    checkpoint_path = checkpoint_path or source_path.replace("landing", "checkpoints") + "/rada"

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("header", "true")
        .schema(RADA_SCHEMA)
        .load(source_path)
        .withColumn("_ingested_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_date", F.current_date())
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )

    query.awaitTermination()

    return spark.table(bronze_table).count()
