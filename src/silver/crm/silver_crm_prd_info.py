from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Extraction
def extract_bronze_crm_prd_info(spark, table: str = "`databricks-project`.bronze.crm_prd_info") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)

# Transformation Helpers
def extract_crm_prd_info_cat_id(col_name: str) -> F.Column:
    """
    Extract category ID from the first 5 chars of prd_key,
    replacing '-' with '_'.
    """
    return F.regexp_replace(F.substring(F.col(col_name), 1, 5), "-", "_").alias("cat_id")
    
def extract_crm_prd_info_prd_key(col_name: str) -> F.Column:
    """
    Extract the actual product key starting from position 7 onwards.
    """
    return F.substring(F.col(col_name), 7, F.length(F.col(col_name))).alias("prd_key")


def normalize_crm_prd_info_prd_line(col_name: str) -> F.Column:
    """Map abbreviated product line codes to human-readable labels."""
    cleaned = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(cleaned == "M", "Mountain")
         .when(cleaned == "R", "Road")
         .when(cleaned == "S", "Other Sales")
         .when(cleaned == "T", "Touring")
         .otherwise("n/a")
    ).alias("prd_line")


def calculate_end_date_crm_prd_info(partition_col: str, order_col: str) -> F.Column:
    """
    Derive prd_end_dt as one day before the next product's start date,
    partitioned by prd_key and ordered by prd_start_dt.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(order_col)
    next_start = F.lead(F.col(order_col)).over(window_spec)
    return (next_start - F.expr("INTERVAL 1 DAY")).cast("date").alias("prd_end_dt")

# Transformation
def transform_crm_prd_info(df: DataFrame) -> DataFrame:
    """
    Apply all cleaning and normalization rules to produce
    the silver-layer version of crm_prd_info.
    """
    df_clean = df.select(
        F.col("prd_id").alias("product_id"),
        extract_crm_prd_info_cat_id("prd_key").alias("category_id"),
        extract_crm_prd_info_prd_key("prd_key").alias("product_number"),
        F.col("prd_nm").alias("product_name"),
        F.coalesce(F.col("prd_cost"), F.lit(0)).alias("prd_cost").alias("product_cost"),  # ISNULL → coalesce
        normalize_crm_prd_info_prd_line("prd_line").alias("product_line"),
        F.col("prd_start_dt").cast("date").alias("start_date"),
        calculate_end_date_crm_prd_info(partition_col="prd_key", order_col="prd_start_dt").alias("end_date"),
    )

    return df_clean

# Loading
def load_silver_crm_prd_info(df: DataFrame, table: str = "`databricks-project`.silver.crm_prd_info") -> None:
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
def run_pipeline_silver_crm_prd_info(spark,source_table: str = "`databricks-project`.bronze.crm_prd_info",target_table: str = "`databricks-project`.silver.crm_prd_info",) -> None:
    bronze_df = extract_bronze_crm_prd_info(spark, source_table)
    silver_df = transform_crm_prd_info(bronze_df)
    load_silver_crm_prd_info(silver_df, target_table)

    print(">> Pipeline finished successfully.")