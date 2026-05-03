import pytest
from pyspark.sql import SparkSession
from src.bronze.bronze import read_csv
from config.schemas import SCHEMA
from config.bronze_config import INGESTION_CONFIG


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.mark.parametrize("item", INGESTION_CONFIG)
def test_source_has_data(spark, item):
    df = read_csv(spark, item["path"], SCHEMA[item["table"]])
    assert df.count() > 0, f"{item['table']} source is empty or unreadable"