
def get_dim_customers_query() -> str:
    """
    Builds the SELECT query for gold.dim_customers.
    Business Rules
    --------------
    - customer_key  : Surrogate key via ROW_NUMBER().
    - gender        : CRM is the primary source; falls back to ERP when CRM value is 'n/a'.
    """
    return f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY ci.customer_id)   AS customer_key,
            ci.customer_id,
            ci.customer_number,
            ci.first_name,
            ci.last_name,
            la.country,
            ci.marital_status,
            CASE
                WHEN ci.gender <> 'n/a' THEN ci.gender        -- CRM is primary source
                ELSE COALESCE(ca.gender, 'n/a')               -- Fallback to ERP
            END                                               AS gender,
            ca.birth_date                                     AS birthdate,
            ci.created_date                                   AS create_date
        FROM       `databricks-project`.silver.crm_cust_info         ci
        LEFT JOIN  `databricks-project`.silver.erp_cust_az12         ca  ON ci.customer_number = ca.customer_number
        LEFT JOIN  `databricks-project`.silver.erp_loc_a101 la  ON ci.customer_number = la.customer_number
    """

def build_dim_customers(spark):
    """Executes the query and returns a DataFrame."""
    query = get_dim_customers_query()
    return spark.sql(query)

def write_dim_customers(df) -> None:
    """Persists the DataFrame as a Delta table."""
    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable("`databricks-project`.gold.dim_customers")
    )

def run_dim_customers(spark) -> None:
    """End-to-end pipeline: build → write."""
    df = build_dim_customers(spark)
    write_dim_customers(df)
    print(f"[dim_customers] Written to databricks-project.gold.dim_customers (overwrite/delta)")