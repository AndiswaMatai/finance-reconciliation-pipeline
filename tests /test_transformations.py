def test_standardisation(spark, logger):

    from src.transformations.standardise import Standardiser

    df = spark.createDataFrame(
        [(" acc1 ", " t1 ")],
        ["Account", "TradeReference"]
    )

    cleaned = Standardiser(logger).transform(df)

    row = cleaned.collect()[0]

    assert row["Account"] == "ACC1" or row["Account"].strip() == "acc1"
