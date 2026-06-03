import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.gold.gold_dim_products import get_dim_products_query, build_dim_products


#Getting Active Spark Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


#Defining Schema for testing
CrmSchema = "product_id int, product_number string, product_name string, category_id string, product_cost double, product_line string, start_date date, end_date date"
ErpSchema = "category_id string, category string, subcategory string, maintenance_flag boolean"


#Global test data shared across all tests
@pytest.fixture(scope="session")
def crm_df(spark):
    rows = [
        (1, "P001", "Widget A", "CAT1", 9.99,  "Road",     date(2022, 1, 1),  None),   #Active product
        (2, "P002", "Widget B", "CAT1", 19.99, "Road",     date(2021, 6, 1),  date(2023, 12, 31)), #Inactive (historical) product
        (3, "P003", "Widget C", "CAT2", 4.50,  "Mountain", date(2023, 3, 15), None),  #Active product, different category
        (4, "P004", "Widget D", "NONE", 2.00,  "Road",     date(2024, 1, 1),  None),  #Active product, unmatched category
    ]
    return spark.createDataFrame(rows, schema=CrmSchema)


@pytest.fixture(scope="session")
def erp_df(spark):
    rows = [
        ("CAT1", "Bikes",  "Road Bikes",  True),   # Matched category
        ("CAT2", "Frames", "Road Frames", False),   # Matched category
    ]
    return spark.createDataFrame(rows, schema=ErpSchema)


#Helper to register both DataFrames as temp views
def register_views(crm, erp):
    crm.createOrReplaceTempView("crm_test")
    erp.createOrReplaceTempView("erp_test")


#Testing that only active products (end_date IS NULL) are included
def test_only_active_products_included(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    ids = [r["product_id"] for r in result.collect()]
    assert 1 in ids   #Active should be there
    assert 2 not in ids  #Inactive must be excluded
    assert 3 in ids   #Active should be there


#Testing that product_key is assigned sequentially starting from 1
def test_product_key_generated_sequentially(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    keys = sorted(r["product_key"] for r in result.collect())
    assert keys == list(range(1, len(keys) + 1))  # Gapless sequence


#Testing that LEFT JOIN keeps all active products even with no category match
def test_left_join_retains_all_products(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    ids = [r["product_id"] for r in result.collect()]
    assert 4 in ids  #product_id=4 has category_id="NONE" with no ERP match — must still appear


#Testing that category fields are enriched correctly when a match exists
def test_category_fields_enriched_correctly(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    p1 = result.filter(F.col("product_id") == 1).first()
    assert p1["category"]         == "Bikes"       #Category enriched from ERP
    assert p1["subcategory"]      == "Road Bikes"  #Subcategory enriched from ERP
    assert p1["maintenance_flag"] == True           #maintenance_flag enriched from ERP


# Testing that the output schema has all expected columns
def test_dim_products_schema(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    assert set(result.columns) == {
        "product_key", "product_id", "product_number", "product_name",
        "category_id", "category", "subcategory", "maintenance_flag",
        "product_cost", "product_line", "start_date",
    }


#Testing that row count equals only the active (end_date IS NULL) source rows
def test_row_count_matches_active_products(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    assert result.count() == 3  #product_id 1, 3, 4 are active; product_id 2 is historical


#Testing that product_key has no duplicates across the output
def test_product_key_is_unique(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    keys = [r["product_key"] for r in result.collect()]
    assert len(keys) == len(set(keys))  #Every surrogate key must be unique


#Testing that an unmatched category_id yields NULL enrichment columns, not a dropped row
def test_unmatched_category_returns_nulls(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    p4 = result.filter(F.col("product_id") == 4).first()
    assert p4 is not None                    #Row must be present
    assert p4["category"]         is None    #No ERP match -> NULL
    assert p4["subcategory"]      is None    #No ERP match -> NULL
    assert p4["maintenance_flag"] is None    #No ERP match -> NULL


#Testing that no product_id appears more than once (guards against join fan-out)
def test_no_duplicate_product_ids(spark, crm_df, erp_df):
    register_views(crm_df, erp_df)
    result = spark.sql(get_dim_products_query("crm_test","erp_test"))
    product_ids = [r["product_id"] for r in result.collect()]
    assert len(product_ids) == len(set(product_ids))  #Each product appears exactly once