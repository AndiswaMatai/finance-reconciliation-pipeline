def test_cash_loader_schema(spark, logger, config):

    from src.ingestion.cash_loader import CashLoader

    df = CashLoader(spark, logger, config).load()

    assert "Account" in df.columns
    assert "TradeReference" in df.columns
    assert df.count() > 0
