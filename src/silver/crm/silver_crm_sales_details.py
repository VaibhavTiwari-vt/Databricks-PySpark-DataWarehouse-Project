from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Extraction
def extract_bronze_crm_sales_details(spark, table: str = "`databricks-project`.bronze.crm_sales_details") -> DataFrame:
    """Load source data from the bronze layer."""
    print(f">> Reading from: {table}")
    return spark.table(table)

# Transformation Helpers
def parse_int_to_date_crm_sales_details(col_name: str) -> F.Column:
    """
    Convert integer date columns (YYYYMMDD) to proper date type.
    Returns NULL if value is 0 or does not have exactly 8 digits.
    """
    col = F.col(col_name)
    return (
        F.when(
            (col == 0) | (F.length(col.cast("string")) != 8),
            F.lit(None)
        )
        .otherwise(F.to_date(col.cast("string"), "yyyyMMdd"))
    ).alias(col_name)


def fix_sales_crm_sales_details() -> F.Column:
    """
    Recalculate sls_sales if the original value is NULL, <= 0,
    or doesn't match sls_quantity * ABS(sls_price).
    Mirrors:
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END
    """
    expected = F.col("sls_quantity") * F.abs(F.col("sls_price"))
    return (
        F.when(
            F.col("sls_sales").isNull() |
            (F.col("sls_sales") <= 0) |
            (F.col("sls_sales") != expected),
            expected
        )
        .otherwise(F.col("sls_sales"))
    ).alias("sls_sales")


def fix_price_crm_sales_details() -> F.Column:
    """
    Derive sls_price from sls_sales / sls_quantity if original price
    is NULL or <= 0. Uses nullif-equivalent to avoid division by zero.
    Mirrors:
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END
    """
    derived = F.col("sls_sales") / F.nullif(F.col("sls_quantity"), F.lit(0))
    return (
        F.when(
            F.col("sls_price").isNull() | (F.col("sls_price") <= 0),
            derived
        )
        .otherwise(F.col("sls_price"))
    ).alias("sls_price")

# Transformation
def transform_crm_sales_details(df: DataFrame) -> DataFrame:
    """
    Apply all cleaning and normalization rules to produce
    the silver-layer version of crm_sales_details.
    """
    df_clean = df.select(
        F.col("sls_ord_num").alias("order_number"),
        F.col("sls_prd_key").alias("product_number"),
        F.col("sls_cust_id").alias("customer_id"),
        parse_int_to_date_crm_sales_details("sls_order_dt").alias("order_date"),
        parse_int_to_date_crm_sales_details("sls_ship_dt").alias("ship_date"),
        parse_int_to_date_crm_sales_details("sls_due_dt").alias("due_date"),
        fix_sales_crm_sales_details().alias("sales_amount"),
        F.col("sls_quantity").alias("quantity"),
        fix_price_crm_sales_details().alias("price"),
    )

    return df_clean

# Loading
def load_silver_crm_sales_details(df: DataFrame, table: str = "`databricks-project`.silver.crm_sales_details") -> None:
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
def run_pipeline_crm_sales_details(spark,source_table: str = "`databricks-project`.bronze.crm_sales_details",target_table: str = "`databricks-project`.silver.crm_sales_details",) -> None:

    bronze_df = extract_bronze_crm_sales_details(spark, source_table)
    silver_df = transform_crm_sales_details(bronze_df)
    load_silver_crm_sales_details(silver_df, target_table)

    print(">> Pipeline finished successfully.")