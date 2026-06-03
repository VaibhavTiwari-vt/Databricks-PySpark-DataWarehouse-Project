import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.gold.gold_facts_sales import get_fact_sales_query, build_fact_sales


#Creating Spark Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()

#Schema Definition
CrmSalesSchema = (
    "order_number string, product_number string, customer_id int, "
    "order_date date, ship_date date, due_date date, "
    "sales_amount double, quantity int, price double"
)

DimProductsSchema = "product_key int, product_number string"
DimCustomersSchema = "customer_key int, customer_id int"


#Test Data
@pytest.fixture(scope="session")
def crm_sales_df(spark):
    rows = [
        # Fully resolvable — both product and customer match
        ("SO-001", "P001", 1, date(2023, 1, 10), date(2023, 1, 15), date(2023, 1, 20), 299.99, 2, 149.99),
        ("SO-002", "P002", 2, date(2022, 6, 5),  date(2022, 6, 10), date(2022, 6, 15), 49.99,  1, 49.99),
        # product_number has no dim_products match → product_key should be NULL
        ("SO-003", "PXXX", 1, date(2023, 3, 1),  date(2023, 3, 5),  date(2023, 3, 10), 15.00,  3, 5.00),
        # customer_id has no dim_customers match → customer_key should be NULL
        ("SO-004", "P001", 999, date(2023, 4, 1), date(2023, 4, 3),  date(2023, 4, 5),  75.00,  1, 75.00),
        # Both FK unresolvable → both keys NULL, row still present
        ("SO-005", "PYYY", 888, date(2021, 12, 1),date(2021, 12, 3), date(2021, 12, 7), 10.00,  1, 10.00),
        # Historical record — included without any date filter
        ("SO-006", "P002", 2, date(2018, 7, 4),  date(2018, 7, 9),  date(2018, 7, 14), 99.99,  2, 49.99),
    ]
    return spark.createDataFrame(rows, schema=CrmSalesSchema)


@pytest.fixture(scope="session")
def dim_products_df(spark):
    rows = [
        (101, "P001"),
        (102, "P002"),
        # P003 intentionally absent to test NULL FK handling
    ]
    return spark.createDataFrame(rows, schema=DimProductsSchema)


@pytest.fixture(scope="session")
def dim_customers_df(spark):
    rows = [
        (201, 1),
        (202, 2),
        # customer_id 999 and 888 intentionally absent
    ]
    return spark.createDataFrame(rows, schema=DimCustomersSchema)


#Registering Views
def register_views(crm_sales, dim_products, dim_customers):
    crm_sales.createOrReplaceTempView("crm_sales_test")
    dim_products.createOrReplaceTempView("dim_products_test")
    dim_customers.createOrReplaceTempView("dim_customers_test")



#Product key resolved correctly from dim_products via product_number
def test_product_key_resolved_correctly(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so1 = result.filter(F.col("order_number") == "SO-001").first()
    assert so1["product_key"] == 101, "product_key should resolve to 101 for product_number 'P001'"


#Customer key resolved correctly from dim_customers via customer_id
def test_customer_key_resolved_correctly(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so1 = result.filter(F.col("order_number") == "SO-001").first()
    assert so1["customer_key"] == 201, "customer_key should resolve to 201 for customer_id=1"


#Unresolvable product_number -> product_key is NULL, row is retained (LEFT JOIN)
def test_null_product_key_when_product_not_in_dim(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so3 = result.filter(F.col("order_number") == "SO-003").first()
    assert so3 is not None,               "SO-003 must be retained even with unresolvable product_number"
    assert so3["product_key"] is None,    "product_key must be NULL when product_number has no dim match"


#Unresolvable customer_id -> customer_key is NULL, row is retained (LEFT JOIN)
def test_null_customer_key_when_customer_not_in_dim(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so4 = result.filter(F.col("order_number") == "SO-004").first()
    assert so4 is not None,               "SO-004 must be retained even with unresolvable customer_id"
    assert so4["customer_key"] is None,   "customer_key must be NULL when customer_id has no dim match"


#Both FKs unresolvable -> row still present, both keys NULL
def test_row_retained_when_both_fks_unresolvable(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so5 = result.filter(F.col("order_number") == "SO-005").first()
    assert so5 is not None,               "SO-005 must appear even when both FKs are unresolvable"
    assert so5["product_key"]  is None,   "product_key should be NULL"
    assert so5["customer_key"] is None,   "customer_key should be NULL"


#LEFT JOIN on dim_products preserves all CRM sales rows
def test_left_join_dim_products_preserves_all_sales(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    result_orders = {r["order_number"] for r in result.collect()}
    source_orders = {r["order_number"] for r in crm_sales_df.collect()}
    assert source_orders == result_orders, \
        "All CRM sales orders must appear in fact_sales regardless of dim_products match"


#LEFT JOIN on dim_customers preserves all CRM sales rows
def test_left_join_dim_customers_preserves_all_sales(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    assert result.count() == crm_sales_df.count(), \
        "Row count must equal CRM source — no sales dropped by LEFT JOINs"


#No duplicate order_numbers after joins (guards against fan-out on dim tables)
def test_no_duplicate_order_numbers_after_joins(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    orders = [r["order_number"] for r in result.collect()]
    assert len(orders) == len(set(orders)), \
        "Each order_number must appear exactly once — joins must not fan-out rows"


#Historical records are included — no implicit date filter applied
def test_historical_records_are_included(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    orders = {r["order_number"] for r in result.collect()}
    assert "SO-006" in orders, \
        "Historical order SO-006 (2018) must be included — no date filter should be applied"


#Measure columns carry through unchanged from source
def test_measure_values_are_preserved(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so2 = result.filter(F.col("order_number") == "SO-002").first()
    assert so2["sales_amount"] == pytest.approx(49.99),  "sales_amount must match source value"
    assert so2["quantity"]     == 1,                     "quantity must match source value"
    assert so2["price"]        == pytest.approx(49.99),  "price must match source value"


#Date columns carry through unchanged from source
def test_date_columns_are_preserved(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    so1 = result.filter(F.col("order_number") == "SO-001").first()
    assert so1["order_date"] == date(2023, 1, 10), "order_date must match source"
    assert so1["ship_date"]  == date(2023, 1, 15), "ship_date must match source"
    assert so1["due_date"]   == date(2023, 1, 20), "due_date must match source"


#Output schema must contain exactly the expected columns
def test_fact_sales_schema(spark, crm_sales_df, dim_products_df, dim_customers_df):
    register_views(crm_sales_df, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    assert set(result.columns) == {
        "order_number", "product_key", "customer_key",
        "order_date", "ship_date", "due_date",
        "sales_amount", "quantity", "price",
    }, f"Unexpected schema: {result.columns}"


#Empty CRM sales input -> zero output rows, no crash
def test_empty_crm_sales_returns_empty_result(spark, dim_products_df, dim_customers_df):
    empty_sales = spark.createDataFrame([], schema=CrmSalesSchema)
    register_views(empty_sales, dim_products_df, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    assert result.count() == 0, "Empty CRM sales input should produce zero output rows"


#Empty dim_products -> all sales rows present, product_key all NULL
def test_empty_dim_products_preserves_all_sales_with_null_keys(spark, crm_sales_df, dim_customers_df):
    empty_products = spark.createDataFrame([], schema=DimProductsSchema)
    register_views(crm_sales_df, empty_products, dim_customers_df)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    assert result.count() == crm_sales_df.count(), \
        "All sales rows must be retained when dim_products is empty"
    null_count = result.filter(F.col("product_key").isNull()).count()
    assert null_count == crm_sales_df.count(), \
        "All product_key values must be NULL when dim_products is empty"


#Empty dim_customers -> all sales rows present, customer_key all NULL
def test_empty_dim_customers_preserves_all_sales_with_null_keys(spark, crm_sales_df, dim_products_df):
    empty_customers = spark.createDataFrame([], schema=DimCustomersSchema)
    register_views(crm_sales_df, dim_products_df, empty_customers)
    result = spark.sql(get_fact_sales_query("crm_sales_test", "dim_products_test", "dim_customers_test"))
    assert result.count() == crm_sales_df.count(), \
        "All sales rows must be retained when dim_customers is empty"
    null_count = result.filter(F.col("customer_key").isNull()).count()
    assert null_count == crm_sales_df.count(), \
        "All customer_key values must be NULL when dim_customers is empty"