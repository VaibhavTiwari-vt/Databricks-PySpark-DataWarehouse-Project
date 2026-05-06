def get_dim_products_query() -> str:
    """
    Builds the SELECT query for gold.dim_products.
    Business Rules
    --------------
    - product_key : Surrogate key via ROW_NUMBER() ordered by start date and product key.
    - Active only : Historical records (prd_end_dt IS NOT NULL) are excluded.
    - category    : Enriched from ERP category table joined on cat_id.
    """
    return """
        SELECT
            ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number)   AS product_key,
            pn.product_id,
            pn.product_number,
            pn.product_name,
            pn.category_id,
            pc.category,
            pc.subcategory,
            pc.maintenance_flag,
            pn.product_cost,
            pn.product_line,
            pn.start_date
        FROM `databricks-project`.silver.crm_prd_info pn
        LEFT JOIN  `databricks-project`.silver.erp_px_cat_g1v2 pc  ON pn.category_id = pc.category_id
        WHERE pn.end_date IS NULL
    """

def build_dim_products(spark):
    """Executes the query and returns a DataFrame."""
    query = get_dim_products_query()
    return spark.sql(query)

def write_dim_products(df) -> None:
    """Persists the DataFrame as a Delta table."""
    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable("`databricks-project`.gold.dim_products")
    )
    
def run_dim_products(spark) -> None:
    df = build_dim_products(spark)
    write_dim_products(df)
    print(f"[dim_products] Written to databricks-project.gold.dim_products (overwrite/delta)")