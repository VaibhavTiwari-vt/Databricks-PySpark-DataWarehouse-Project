import pytest
from pyspark.sql import SparkSession
from src.silver.erp.silver_erp_px_cat_g1v2 import transform_erp_px_cat_g1v2

# Schema definition
Schema = """id string,cat string,subcat string,maintenance string"""

# Getting Active Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        ("CAT-001", "Bikes","Mountain Bikes","Yes"),
        ("CAT-002", "Bikes","Road Bikes","No"),
        ("CAT-003", "Accessories", "Helmets","Yes"),
    ]
    return spark.createDataFrame(rows, schema=Schema)


#Complete testing
def test_transform_erp_px_cat_g1v2(bronze_df):
    result = transform_erp_px_cat_g1v2(bronze_df)

    #Column rename checking
    assert set(result.columns) == {
        "category_id", "category", "subcategory", "maintenance_flag"
    }
    rows = result.collect()

    #Values passed through correctly
    assert rows[0]["category_id"]      == "CAT-001"
    assert rows[0]["category"]         == "Bikes"
    assert rows[0]["subcategory"]      == "Mountain Bikes"
    assert rows[0]["maintenance_flag"] == "Yes"

    #Row count unchanged — No rows dropped
    assert result.count() == 3