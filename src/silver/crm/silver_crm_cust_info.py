from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Extraction
def extract_bronze_crm_cust_info(spark, table: str = "`databricks-project`.bronze.crm_cust_info") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)

#Removing Duplicates
def deduplicate_crm_cust_info(df: DataFrame, partition_col: str, order_col: str) -> DataFrame:
    """
    Keep only the latest record per partition_col,
    ordered descending by order_col.
    Rows where partition_col IS NULL are dropped.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
 
    return (
        df.filter(F.col(partition_col).isNotNull())
          .withColumn("flag_last", F.row_number().over(window_spec))
          .filter(F.col("flag_last") == 1)
          .drop("flag_last")
    )

#Normalization 
def normalize_marital_status_crm_cust_info(col_name: str) -> F.Column:
    """Map abbreviated marital status codes to human-readable labels."""
    cleaned = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(cleaned == "S", "Single")
         .when(cleaned == "M", "Married")
         .otherwise("n/a")
    ).alias(col_name)
 
 
def normalize_gender_crm_cust_info(col_name: str) -> F.Column:
    """Map abbreviated gender codes to human-readable labels."""
    cleaned = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(cleaned == "F", "Female")
         .when(cleaned == "M", "Male")
         .otherwise("n/a")
    ).alias(col_name)

#Transforming
def transform_crm_cust_info(df: DataFrame) -> DataFrame:
    """
    Apply all cleaning and normalization rules to produce
    the silver-layer version of crm_cust_info.
    """
    # Step 1 – deduplicate: keep most recent record per customer
    df_deduped = deduplicate_crm_cust_info(df, partition_col="cst_id", order_col="cst_create_date")
 
    # Step 2 – select & clean columns
    df_clean = df_deduped.select(
        F.col("cst_id").alias("customer_id"),
        F.col("cst_key").alias("customer_number"),
        F.trim(F.col("cst_firstname")).alias("first_name"),
        F.trim(F.col("cst_lastname")).alias("last_name"),
        normalize_marital_status_crm_cust_info("cst_martial_status").alias("marital_status"),
        normalize_gender_crm_cust_info("cst_gndr").alias("gender"),
        F.col("cst_create_date").alias("created_date")
    )
 
    return df_clean

#Loading

def load_silver_crm_cust_info(df: DataFrame, table: str = "`databricks-project`.silver.crm_cust_info") -> None:
    """
    Overwrite the silver target table.
    Uses 'overwrite' mode to replicate TRUNCATE + INSERT behaviour.
    """
    print(f">> Truncating & inserting into: {table}")
    (
        df.write
          .mode("overwrite").format("delta")
          .saveAsTable(table)
    )
    print(f">> Load complete: {table}")

#Pipeline Entry Point
def run_pipeline(
    spark,
    source_table: str = "`databricks-project`.bronze.crm_cust_info",
    target_table: str = "`databricks-project`.silver.crm_cust_info",
) -> None:
    """End-to-end ETL pipeline for silver.crm_cust_info."""
 
    bronze_df   = extract_bronze_crm_cust_info(spark, source_table)
    silver_df   = transform_crm_cust_info(bronze_df)
    load_silver_crm_cust_info(silver_df, target_table)
 
    print(">> Pipeline finished successfully.")