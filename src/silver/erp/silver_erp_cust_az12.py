from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import functions as F

# Extraction
def extract_bronze_erp_cust_az12(spark: SparkSession, table: str = "`databricks-project`.bronze.erp_cust_az12") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)
# Transformation Helpers
def clean_cid_erp_cust_az12(col_name: str) -> F.Column:
    """
    Remove 'NAS' prefix from cid if present.
    """
    col = F.col(col_name)
    return (
        F.when(col.startswith("NAS"), F.substring(col, 4, F.length(col)))
         .otherwise(col)
    ).alias(col_name)


def clean_bdate_erp_cust_az12(col_name: str) -> F.Column:
    """
    Set future birthdates to NULL.
    """
    col = F.col(col_name)
    return (
        F.when(col > F.current_date(), F.lit(None))
         .otherwise(col)
    ).alias(col_name)


def normalize_gender_erp_cust_az12(col_name: str) -> F.Column:
    """
    Normalize gender values to 'Female', 'Male', or 'n/a'.
    """
    cleaned = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(cleaned.isin("F", "FEMALE"), "Female")
         .when(cleaned.isin("M", "MALE"),   "Male")
         .otherwise("n/a")
    ).alias(col_name)
# Transformation
def transform_erp_cust_az12(df: DataFrame) -> DataFrame:
    """
    Apply all cleaning and normalization rules to produce
    the silver-layer version of erp_cust_az12.
    """
    df_clean = df.select(
        clean_cid_erp_cust_az12("cid").alias("customer_number"),
        clean_bdate_erp_cust_az12("bdate").alias("birth_date"),
        normalize_gender_erp_cust_az12("gen").alias("gender"),
    )

    return df_clean
# Loading
def load_silver_erp_cust_az12(df: DataFrame, table: str = "`databricks-project`.silver.erp_cust_az12") -> None:
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
def run_pipeline_erp_cust_az12(spark: SparkSession,source_table: str = "`databricks-project`.bronze.erp_cust_az12",target_table: str = "`databricks-project`.silver.erp_cust_az12",) -> None:
    bronze_df = extract_bronze_erp_cust_az12(spark, source_table)
    silver_df = transform_erp_cust_az12(bronze_df)
    load_silver_erp_cust_az12(silver_df, target_table)

    print(">> Pipeline finished successfully.")