from src.reconciliation.reconciliation_engine import ReconciliationEngine


def test_reconciliation(spark, logger):

    df1 = spark.createDataFrame([
        ("A1", "T1", "2024-01-01", 100, "ZAR", "K1")
    ], ["Account","TradeReference","SettlementDate","Amount","Currency","BusinessKey"])

    df2 = df1

    engine = ReconciliationEngine(logger)

    result = engine.reconcile(df1, df2)

    assert result["matched"].count() == 1
