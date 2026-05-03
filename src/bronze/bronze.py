from typing import Any
from pyspark.sql import DataFrame
from config.schemas import SCHEMA

def read_csv(spark, path: str, schema: str) -> DataFrame:
    """Read a CSV file into a DataFrame."""
    return (
        spark.read.option("header", "true").schema(schema).csv(path)
    )


def write_delta_table(spark,df: DataFrame, catalog: str, schema: str, table: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to a Delta table."""
    spark.sql(f"USE CATALOG `{catalog}`")
    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
    (
        df.write.mode(mode).format("delta").saveAsTable(full_table_name)
    )


def ingest_table(spark, item: dict[str, Any]) -> DataFrame:
    """
    Ingest a single table config entry.
    Returns the DataFrame so that the callers can inspect/test it.
    """
    print(f"Ingesting {item['source']} → {item['catalog']}.{item['schema']}.{item['table']}")
    df = read_csv(spark, item["path"], SCHEMA[item["table"]])
    write_delta_table(spark,df, item["catalog"], item["schema"], item["table"])
    return df


def run_bronze_ingestion(spark, config: list[dict]) -> list[DataFrame]:
    """Run full bronze ingestion. Returns list of DataFrames for observability."""

    return [ingest_table(spark, item) for item in config]