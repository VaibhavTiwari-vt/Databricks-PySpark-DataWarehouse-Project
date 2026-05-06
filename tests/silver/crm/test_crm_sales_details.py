import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.silver.crm.silver_crm_sales_details import (
    parse_int_to_date_crm_sales_details,
    fix_sales_crm_sales_details,
    fix_price_crm_sales_details,
    transform_crm_sales_details,
)

# Schema definition
Schema = """sls_ord_num string,sls_prd_key string,sls_cust_id int,sls_order_dt int,sls_ship_dt int,sls_due_dt int,
sls_sales double,sls_quantity int,sls_price double"""

# Getting Active Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        ("SO001", "PRD-001", 1, 20210101, 20210110, 20210115, 500.0,  5,  100.0),  #Valid Row
        ("SO002", "PRD-002", 2, 0,        20210210, 20210215, 200.0,  2,  100.0),  #Order Date Zero to Null
        ("SO003", "PRD-003", 3, 2021010,  20210310, 20210315, 300.0,  3,  100.0),  #Order Date 7 digits to Null
        ("SO004", "PRD-004", 4, 20210401, 20210410, 20210415, None,   4,  50.0),   #Null -> Recalculate
        ("SO005", "PRD-005", 5, 20210501, 20210510, 20210515, -100.0, 5,  50.0),   #Negative Sales -> Recalculate
        ("SO006", "PRD-006", 6, 20210601, 20210610, 20210615, 999.0,  3,  100.0),  #Sales not matching
        ("SO007", "PRD-007", 7, 20210701, 20210710, 20210715, 400.0,  4,  None),   #Null Price
        ("SO008", "PRD-008", 8, 20210801, 20210810, 20210815, 400.0,  4,  -50.0),  #Negative price
    ]
    return spark.createDataFrame(rows, schema=Schema)


# Testing Dates
def test_parse_int_to_date_crm_sales_details(bronze_df):
    result = (
        bronze_df
        .select(parse_int_to_date_crm_sales_details("sls_order_dt"))
        .collect()
    )
    values = [r[0] for r in result]

    assert values[0] == date(2021, 1, 1) 
    assert values[1] is None              
    assert values[2] is None               


# Testing Sales
def test_fix_sales_crm_sales_details(bronze_df):
    result = (
        bronze_df
        .select(fix_sales_crm_sales_details())
        .collect()
    )
    values = [r[0] for r in result]

    assert values[0] == 500.0   
    assert values[3] == 200.0   
    assert values[4] == 250.0   
    assert values[5] == 300.0   


# Testing Price
def test_fix_price_crm_sales_details(bronze_df):
    result = (
        bronze_df
        .select(fix_price_crm_sales_details())
        .collect()
    )
    values = [r[0] for r in result]

    assert values[0] == 100.0 
    assert values[6] == 100.0 
    assert values[7] == 100.0


# Complete testing
def test_transform_crm_sales_details(bronze_df):
    result = transform_crm_sales_details(bronze_df)

    # Column rename checking
    assert set(result.columns) == {
        "order_number", "product_number", "customer_id",
        "order_date", "ship_date", "due_date",
        "sales_amount", "quantity", "price",
    }

    # Valid date checking
    valid = result.filter(F.col("order_number") == "SO001").first()
    assert valid["order_date"] == date(2021, 1, 1)

    #0 to Null Date Checking
    zero_dt = result.filter(F.col("order_number") == "SO002").first()
    assert zero_dt["order_date"] is None

    #Null Sales recalculation
    null_sales = result.filter(F.col("order_number") == "SO004").first()
    assert null_sales["sales_amount"] == 200.0

    #Null price derivation
    null_price = result.filter(F.col("order_number") == "SO007").first()
    assert null_price["price"] == 100.0