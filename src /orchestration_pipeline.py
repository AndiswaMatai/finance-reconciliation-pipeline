from src.common.spark_session import SparkSessionFactory
from src.common.logger import Logger
from src.common.config_loader import ConfigLoader

from src.ingestion.cash_loader import CashLoader
from src.ingestion.rada_loader import RadaLoader

from src.transformations.standardise import Standardiser
from src.transformations.type_cast import TypeCaster
from src.transformations.deduplicate import Deduplicator
from src.transformations.currency_normaliser import CurrencyNormalizer
from src.transformations.quality_checks import DataQualityChecks

from src.reconciliation.business_keys import BusinessKeyGenerator
from src.reconciliation.reconciliation_engine import ReconciliationEngine

from src.reporting.dataset_builder import DatasetBuilder
from src.reporting.kpi_builder import KPIBuilder
from src.reporting.exception_report import ExceptionReport


def run_silver(df, logger):

    df = Standardiser(logger).transform(df)
    df = TypeCaster(logger).transform(df)
    df = CurrencyNormalizer(logger).transform(df)
    df = Deduplicator(logger).transform(df)

    df, _ = DataQualityChecks(logger).validate(df)

    return df


def main():

    logger = Logger.get_logger("FinanceRecon")
    config = ConfigLoader.load("config/dev.yaml")

    spark = SparkSessionFactory.get_spark(config["spark"]["app_name"])

    # INGESTION
    cash = CashLoader(spark, logger, config).load()
    rada = RadaLoader(spark, logger, config).load()

    # SILVER
    cash_clean = run_silver(cash, logger)
    rada_clean = run_silver(rada, logger)

    # BUSINESS KEYS
    key_gen = BusinessKeyGenerator(logger)
    cash_clean = key_gen.generate(cash_clean)
    rada_clean = key_gen.generate(rada_clean)

    # RECONCILIATION
    engine = ReconciliationEngine(logger)
    results = engine.reconcile(cash_clean, rada_clean)

    # REPORTING
    final_df = DatasetBuilder(logger).build(**results)

    KPIBuilder(logger).build(**results)
    ExceptionReport(logger).build(results["unmatched"])

    final_df.show()


if __name__ == "__main__":
    main()
