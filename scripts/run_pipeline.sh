# scripts/run_pipeline.sh
spark-submit src/bronze_ingestion.py
spark-submit src/silver_standardisation.py
spark-submit src/reconciliation_engine.py
spark-submit src/gold_reporting.py
