from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Extraction
def extract_bronze_erp_px_cat_g1v2(spark, table: str = "`databricks-project`.bronze.erp_px_cat_g1v2") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)

# Transformation
def transform_erp_px_cat_g1v2(df: DataFrame) -> DataFrame:
    """
    Select required columns to produce
    the silver-layer version of erp_px_cat_g1v2.
    No cleaning or normalization required for this table.
    """
    df_clean = df.select(
        F.col("id").alias("category_id"),
        F.col("cat").alias("category"),
        F.col("subcat").alias("subcategory"),
        F.col("maintenance").alias("maintenance_flag"),
    )

    return df_clean

# Loading
def load_silver_erp_px_cat_g1v2(df: DataFrame, table: str = "`databricks-project`.silver.erp_px_cat_g1v2") -> None:
    """
    Overwrite the silver target table.
    Uses 'overwrite' mode to replicate TRUNCATE + INSERT behaviour.
    """
    print(f">> Truncating & inserting into: {table}")
    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable(table)
    )
    print(f">> Load complete: {table}")

# Pipeline Entry Point
def run_pipeline_erp_px_cat_g1v2(
    spark,
    source_table: str = "`databricks-project`.bronze.erp_px_cat_g1v2",
    target_table: str = "`databricks-project`.silver.erp_px_cat_g1v2",
) -> None:
    """End-to-end ETL pipeline for silver.erp_px_cat_g1v2."""

    bronze_df = extract_bronze_erp_px_cat_g1v2(spark, source_table)
    silver_df = transform_erp_px_cat_g1v2(bronze_df)
    load_silver_erp_px_cat_g1v2(silver_df, target_table)

    print(">> Pipeline finished successfully.")