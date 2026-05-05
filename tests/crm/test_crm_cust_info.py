import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.silver.crm.silver_crm_cust_info import (deduplicate_crm_cust_info,normalize_gender_crm_cust_info,normalize_marital_status_crm_cust_info,transform_crm_cust_info,)


#Getting Active Spark Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()

#Defining Schema for testing
Schema = """cst_id int,cst_key string,cst_firstname string,cst_lastname string,cst_martial_status string,cst_gndr string,cst_create_date date"""

#Testing deduplicate function for keeping the latest values only
def test_deduplicate_keeps_latest(spark):
    #Creating test data for testing
    rows = [
        (1, "K001", "Alice", "Smith", "S", "F", date(2023, 1, 1)),  #old-data
        (1, "K001", "Alice", "Smith", "S", "F", date(2024, 6, 1)),  #latest-data
        (2, "K002", "Bob",   "Jones", "M", "M", date(2024, 3, 1)),
    ]
    df = spark.createDataFrame(rows, schema=Schema)
    result = deduplicate_crm_cust_info(df, "cst_id", "cst_create_date")
    assert result.count() == 2
    cst1 = result.filter(F.col("cst_id") == 1).first()
    assert cst1["cst_create_date"] == date(2024, 6, 1)

#Testing deduplicate function for dropping null keys
def test_deduplicate_drops_null_keys(spark):
    #Creating test data
    rows = [
        (None, "K999", "Ghost", "Row",   "S", "F", date(2024, 1, 1)),
        (1,    "K001", "Alice", "Smith", "S", "F", date(2024, 1, 1)),
    ]
    df = spark.createDataFrame(rows, schema=Schema)
    result = deduplicate_crm_cust_info(df, "cst_id", "cst_create_date")
    assert result.count() == 1
    assert result.first()["cst_id"] == 1

#Testing normalization function for gender
def test_normalize_gender(spark):
    #Creating test data
    rows = [("F",), ("M",), ("m",), ("X",), (None,)]
    df = spark.createDataFrame(rows, ["cst_gndr"])
    result = df.select(normalize_gender_crm_cust_info("cst_gndr")).collect()
    assert result[0][0] == "Female"
    assert result[1][0] == "Male"
    assert result[2][0] == "Male"   #Lowercase
    assert result[3][0] == "n/a"    #Random Value
    assert result[4][0] == "n/a"    #Null

#Testing normalization function for marital status
def test_normalize_marital_status(spark):
    #Creating test data
    rows = [("S",), ("M",), ("s",), ("D",), (None,)]
    df = spark.createDataFrame(rows, ["cst_martial_status"])
    result = df.select(normalize_marital_status_crm_cust_info("cst_martial_status")).collect()
    assert result[0][0] == "Single"
    assert result[1][0] == "Married"
    assert result[2][0] == "Single"  #Lowercase
    assert result[3][0] == "n/a"     #Random Value
    assert result[4][0] == "n/a"     #Null

#Testing all at once
def test_transform_end_to_end(spark):
    rows = [
        (1, "C-001", "  Alice  ", "  Smith  ", "S", "F", date(2023, 1, 1)),  #Old-Data
        (1, "C-001", "  Alice  ", "  Smith  ", "S", "F", date(2024, 6, 1)),  #Latest-Data
        (2, "C-002", "Bob",       "Jones",     "M", "M", date(2024, 3, 15)),
        (None, "C-999", "Ghost",  "Row",       "S", "F", date(2024, 1, 1)),  #Dropping Nulls
    ]
    df = spark.createDataFrame(rows, schema=Schema)
    result = transform_crm_cust_info(df)
    #Testing row count after deduplication and null dropping
    assert result.count() == 2
    #Checking for column names
    assert set(result.columns) == {
        "customer_id", "customer_number", "first_name",
        "last_name", "marital_status", "gender", "created_date",
    }
    cst1 = result.filter(F.col("customer_id") == 1).first()
    assert cst1["first_name"]      == "Alice"           #Trimming Check
    assert cst1["gender"]          == "Female"          #Normalization Check for Gender
    assert cst1["marital_status"]  == "Single"          #Normalization Check for Marital Status
    assert cst1["created_date"]    == date(2024, 6, 1)  #Latest Data