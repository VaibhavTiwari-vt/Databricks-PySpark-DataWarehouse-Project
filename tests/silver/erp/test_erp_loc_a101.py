import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.silver.erp.silver_erp_loc_a101 import (
    normalize_country_erp_loc_a101,
    transform_erp_loc_a101,
)

# Schema definition
Schema = """cid string,cntry string"""

# Getting Active Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        ("1-2-3",   "DE"),  
        ("4-5-6",   "US"),  
        ("789",     "USA"), 
        ("101",     "FR"),   
        ("102",     "  "), 
        ("103",     None), 
    ]
    return spark.createDataFrame(rows, schema=Schema)


# Testing country normalization
def test_normalize_country_erp_loc_a101(bronze_df):
    result = bronze_df.select(normalize_country_erp_loc_a101("cntry")).collect()
    values = [r[0] for r in result]

    assert values[0] == "Germany"        #DE
    assert values[1] == "United States"  #US
    assert values[2] == "United States"  #USA
    assert values[3] == "FR"             #Unrecognized
    assert values[4] == "n/a"            #Blank
    assert values[5] == "n/a"            #Null


#Complete testing
def test_transform_erp_loc_a101(bronze_df):
    result = transform_erp_loc_a101(bronze_df)

    #Column rename checking
    assert set(result.columns) == {"customer_number", "country"}

    rows = result.collect()

    #Hyphens stripped from cid
    assert rows[0]["customer_number"] == "123"
    assert rows[1]["customer_number"] == "456"

    #No hyphen to unchanged
    assert rows[2]["customer_number"] == "789"

    #Country normalized check
    assert rows[0]["country"] == "Germany"
    assert rows[1]["country"] == "United States"
    assert rows[4]["country"] == "n/a"