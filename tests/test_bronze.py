import pytest
from unittest.mock import MagicMock
from src.bronze.bronze_logic import read_csv, write_delta_table, ingest_table, run_bronze_ingestion
from config.bronze_config import INGESTION_CONFIG


@pytest.fixture
def spark():
    mock = MagicMock()
    mock.read.option.return_value.schema.return_value.csv.return_value = MagicMock(name="df")
    return mock


# --- read_csv ---
def test_read_csv_returns_dataframe(spark):
    result = read_csv(spark, "/some/path.csv", "col1 string")
    assert result is not None


# --- write_delta_table ---
def test_write_uses_correct_table_name():
    df = MagicMock()
    write_delta_table(df, "databricks-project", "bronze", "crm_cust_info")
    df.write.mode.return_value.format.return_value.saveAsTable.assert_called_once_with(
        "`databricks-project`.`bronze`.`crm_cust_info`"
    )


def test_write_default_mode_is_overwrite():
    df = MagicMock()
    write_delta_table(df, "cat", "schema", "table")
    df.write.mode.assert_called_once_with("overwrite")


# --- ingest_table ---
def test_ingest_returns_dataframe(spark):
    item = {"source": "crm", "path": "/p.csv", "table": "crm_cust_info", "catalog": "cat", "schema": "bronze"}
    result = ingest_table(spark, item)
    assert result is not None


# --- run_bronze_ingestion ---
def test_run_processes_all_items(spark):
    results = run_bronze_ingestion(spark, INGESTION_CONFIG)
    assert len(results) == len(INGESTION_CONFIG)


# --- config ---
def test_config_has_required_keys():
    required = {"source", "path", "table", "catalog", "schema"}
    for item in INGESTION_CONFIG:
        assert required.issubset(item.keys())


def test_config_paths_end_in_csv():
    for item in INGESTION_CONFIG:
        assert item["path"].endswith(".csv")