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

#Global test data shared across all tests
@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        (1, "C-001", "  Alice  ", "  Smith  ", "S",   "F",  date(2023, 1, 1)),  #Old-Data
        (1, "C-001", "  Alice  ", "  Smith  ", "S",   "F",  date(2024, 6, 1)),  #Latest-Data
        (2, "C-002", "Bob",       "Jones",     "M",   "M",  date(2024, 3, 15)),
        (None, "C-999", "Ghost",  "Row",       "S",   "F",  date(2024, 1, 1)),  #Null Key
        (3, "C-003", "Eve",       "Brown",     None,  None, date(2024, 4, 1)),  #Null gender and marital status to n/a
    ]
    return spark.createDataFrame(rows, schema=Schema)


#Testing deduplicate function for keeping the latest values only
def test_deduplicate_keeps_latest(bronze_df):
    result = deduplicate_crm_cust_info(bronze_df, "cst_id", "cst_create_date")
    assert result.count() == 3
    cst1 = result.filter(F.col("cst_id") == 1).first()
    assert cst1["cst_create_date"] == date(2024, 6, 1)

#Testing deduplicate function for dropping null keys
def test_deduplicate_drops_null_keys(bronze_df):
    result = deduplicate_crm_cust_info(bronze_df, "cst_id", "cst_create_date")
    ids = [r["cst_id"] for r in result.collect()]
    assert None not in ids

#Testing normalization function for gender
def test_normalize_gender(bronze_df):
    result = bronze_df.select(normalize_gender_crm_cust_info("cst_gndr")).collect()
    values = [r[0] for r in result]
    assert "Female" in values   #F to Female
    assert "Male"   in values   #M to Male
    assert "n/a"    in values   #Null to n/a

#Testing normalization function for marital status
def test_normalize_marital_status(bronze_df):
    result = bronze_df.select(normalize_marital_status_crm_cust_info("cst_martial_status")).collect()
    values = [r[0] for r in result]
    assert "Single"  in values  #S to Single
    assert "Married" in values  #M to Married
    assert "n/a"     in values  #Null to n/a

#Testing all at once
def test_transform_end_to_end(bronze_df):
    result = transform_crm_cust_info(bronze_df)
    #Testing row count after deduplication and null dropping
    assert result.count() == 3
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