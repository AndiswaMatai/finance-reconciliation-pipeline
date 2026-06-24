"""
Bronze Layer — Cash System Ingestion

Reads raw Cash transaction extracts from the ADLS Gen2 landing zone and
writes them as append-only Delta tables in the Bronze schema on Unity Catalog.

Production trigger: ADF Copy activity drops the daily Cash extract into
  abfss://landing@{storage}.dfs.core.windows.net/cash/
Databricks Auto Loader (cloudFiles) picks it up — this module contains the
same logic as databricks/notebooks/01_bronze_ingest.py in function form so
it can be unit-tested and imported by the orchestration notebook.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, TimestampType
)

CASH_SCHEMA = StructType([
    StructField("cash_id",           StringType(),    False),
    StructField("account_number",    StringType(),    False),
    StructField("trade_reference",   StringType(),    False),
    StructField("swift_bic",         StringType(),    True),
    StructField("transaction_date",  StringType(),    True),
    StructField("value_date",        StringType(),    True),
    StructField("settlement_date",   StringType(),    True),
    StructField("currency",          StringType(),    True),
    StructField("amount",            DoubleType(),    True),
    StructField("debit_credit",      StringType(),    True),
    StructField("narrative",         StringType(),    True),
    StructField("status",            StringType(),    True),
])


def ingest_cash(
    spark: SparkSession,
    source_path: str,
    bronze_table: str = "bronze.cash_transactions",
    checkpoint_path: str = None,
) -> int:
    """
    Reads Cash extracts via Auto Loader (cloudFiles) and appends to the
    bronze Delta table. Returns the number of new records ingested.

    Args:
        spark:          Active SparkSession
        source_path:    ADLS Gen2 path to the Cash landing folder
        bronze_table:   Unity Catalog table name (catalog.schema.table)
        checkpoint_path: Auto Loader checkpoint location on ADLS

    Returns:
        Row count of newly ingested records
    """
    checkpoint_path = checkpoint_path or source_path.replace("landing", "checkpoints") + "/cash"

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("header", "true")
        .schema(CASH_SCHEMA)
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


def validate_cash_schema(spark: SparkSession, bronze_table: str) -> dict:
    """
    Quick schema validation after ingest — confirms mandatory fields are
    present before Silver standardisation runs.
    """
    df = spark.table(bronze_table)
    missing_cols = [c for c in ["cash_id", "account_number", "trade_reference",
                                "settlement_date", "currency", "amount"]
                    if c not in df.columns]
    return {
        "table": bronze_table,
        "row_count": df.count(),
        "missing_mandatory_columns": missing_cols,
        "schema_valid": len(missing_cols) == 0,
    
    
