def get_fact_sales_query() -> str:
    """
    Builds the SELECT query for gold.fact_sales.
    Business Rules
    --------------
    - product_key  : Resolved from gold.dim_products via product_number.
    - customer_key : Resolved from gold.dim_customers via customer_id.
    - No date filter: All sales records are included (historical + current).
    """
    return """
        SELECT
            sd.order_number,
            pr.product_key,
            cu.customer_key,
            sd.order_date,
            sd.ship_date,
            sd.due_date,
            sd.sales_amount,
            sd.quantity,
            sd.price
        FROM       `databricks-project`.silver.crm_sales_details   sd
        LEFT JOIN  `databricks-project`.gold.dim_products          pr  ON sd.product_number = pr.product_number
        LEFT JOIN  `databricks-project`.gold.dim_customers        cu  ON sd.customer_id    = cu.customer_id
    """

def build_fact_sales(spark):
    """Executes the query and returns a DataFrame."""
    query = get_fact_sales_query()
    return spark.sql(query)

def write_fact_sales(df) -> None:
    """Persists the DataFrame as a Delta table."""
    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable("`databricks-project`.gold.fact_sales")
    )
    
def run_facts_sales(spark) -> None:
    """End-to-end pipeline: build → write."""
    df = build_fact_sales(spark)
    write_fact_sales(df)
    print("[fact_sales] Written to databricks-project.gold.fact_sales (overwrite/delta)")