import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.silver.erp.silver_erp_cust_az12 import (
    normalize_gender_erp_cust_az12,
    clean_bdate_erp_cust_az12,
    transform_erp_cust_az12,
)

# Schema definition
Schema = """cid string,bdate date,gen string"""

# Getting Active Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        ("NAS001",  date(1990, 5, 15),  "F"),       
        ("002",     date(1985, 8, 20),  "M"),     
        ("NAS003",  date(2099, 1, 1),   "FEMALE"),  
        ("004",     date(1995, 3, 10),  "MALE"),    
        ("005",     date(2000, 7, 25),  "X"),      
        ("006",     date(2001, 11, 5),  None),    
    ]
    return spark.createDataFrame(rows, schema=Schema)


# Testing gender normalization
def test_normalize_gender_erp_cust_az12(bronze_df):
    result = bronze_df.select(normalize_gender_erp_cust_az12("gen")).collect()
    values = [r[0] for r in result]

    assert values[0] == "Female"  # F -> Female
    assert values[1] == "Male"    # M -> Male
    assert values[2] == "Female"  # FEMALE -> Female
    assert values[3] == "Male"    # MALE -> Male
    assert values[4] == "n/a"     # Unknown Code
    assert values[5] == "n/a"     # Null


# Testing birthdate cleaning
def test_clean_bdate_erp_cust_az12(bronze_df):
    result = bronze_df.select(clean_bdate_erp_cust_az12("bdate")).collect()
    values = [r[0] for r in result]

    assert values[0] == date(1990, 5, 15)  #Don't Change Past Date
    assert values[2] is None               #Future Date must be Null


#Complete testing
def test_transform_erp_cust_az12(bronze_df):
    result = transform_erp_cust_az12(bronze_df)

    #Column rename checking
    assert set(result.columns) == {"customer_number", "birth_date", "gender"}

    rows = result.collect()

    #NAS prefix stripped
    assert rows[0]["customer_number"] == "001"

    #No prefix row unchanged
    assert rows[1]["customer_number"] == "002"

    # Future birthdate -> NULL
    assert rows[2]["birth_date"] is None

    # Valid birthdate -> unchanged
    assert rows[1]["birth_date"] == date(1985, 8, 20)

    #Gender normalized check
    assert rows[0]["gender"] == "Female"
    assert rows[1]["gender"] == "Male"
    assert rows[4]["gender"] == "n/a"