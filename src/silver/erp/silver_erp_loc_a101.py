from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Extraction
def extract_bronze_erp_loc_a101(spark, table: str = "`databricks-project`.bronze.erp_loc_a101") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)

# Transformation Helpers
def clean_cid_erp_loc_a101(col_name: str) -> F.Column:
    """
    Remove all '-' characters from cid.
    Mirrors:
        REPLACE(cid, '-', '')
    """
    return F.regexp_replace(F.col(col_name), "-", "").alias(col_name)
    
def normalize_country_erp_loc_a101(col_name: str) -> F.Column:
    """
    Normalize country codes to full country names.
    Blank or NULL values are set to 'n/a', unrecognized values are kept as-is (trimmed).
    Mirrors:
        CASE WHEN TRIM(cntry) = 'DE'            THEN 'Germany'
             WHEN TRIM(cntry) IN ('US', 'USA')  THEN 'United States'
             WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
             ELSE TRIM(cntry)
        END
    """
    trimmed = F.trim(F.col(col_name))
    return (
        F.when(trimmed == "DE",                    "Germany")
         .when(trimmed.isin("US", "USA"),           "United States")
         .when(trimmed.isNull() | (trimmed == ""), "n/a")
         .otherwise(trimmed)
    ).alias(col_name)

# Transformation
def transform_erp_loc_a101(df: DataFrame) -> DataFrame:
    """
    Apply all cleaning and normalization rules to produce
    the silver-layer version of erp_loc_a101.
    """
    df_clean = df.select(
        clean_cid_erp_loc_a101("cid").alias("customer_number"),
        normalize_country_erp_loc_a101("cntry").alias("country"),
    )

    return df_clean

# Loading
def load_silver_erp_loc_a101(df: DataFrame, table: str = "`databricks-project`.silver.erp_loc_a101") -> None:
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
def run_pipeline_erp_loc_a101(
    spark,
    source_table: str = "`databricks-project`.bronze.erp_loc_a101",
    target_table: str = "`databricks-project`.silver.erp_loc_a101",
) -> None:
    """End-to-end ETL pipeline for silver.erp_loc_a101."""

    bronze_df = extract_bronze_erp_loc_a101(spark, source_table)
    silver_df = transform_erp_loc_a101(bronze_df)
    load_silver_erp_loc_a101(silver_df, target_table)

    print(">> Pipeline finished successfully.")