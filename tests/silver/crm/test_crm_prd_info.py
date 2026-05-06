import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.silver.crm.silver_crm_prd_info import (extract_crm_prd_info_cat_id,extract_crm_prd_info_prd_key,normalize_crm_prd_info_prd_line,calculate_end_date_crm_prd_info,transform_crm_prd_info,)

#Schema definition
Schema = """prd_id int,prd_key string,prd_nm string,prd_cost float,prd_line string,prd_start_dt date"""

#Getting Active Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def bronze_df(spark):
    rows = [
        (1, "AC-He-001", "Helmet",       50.0,   "M", date(2021, 1,  1)), 
        (2, "AC-He-001", "Helmet Pro",   60.0,   "M", date(2022, 6,  1)),
        (3, "RO-Ad-002", "Road Bike",    1200.0, "R", date(2021, 3,  1)),
        (4, "TO-Tr-003", "Touring Bike", None,   "T", date(2021, 5,  1)), #Null Cost
        (5, "OT-Xx-004", "Misc Item",    10.0,   "S", date(2021, 7,  1)),
        (6, "OT-Xx-005", "Unknown Item", 10.0,   "Z", date(2021, 7,  1)), #Unknow item
    ]
    return spark.createDataFrame(rows, schema=Schema)


#Testing Normalization
def test_normalize_crm_prd_info_prd_line(bronze_df):
    result = bronze_df.select(normalize_crm_prd_info_prd_line("prd_line")).collect()
    values = [r[0] for r in result]

    assert "Mountain"    in values  
    assert "Road"        in values  
    assert "Touring"     in values  
    assert "Other Sales" in values  
    assert "n/a"         in values 


#Testing end date calculation
def test_calculate_end_date_crm_prd_info(bronze_df):
    df_with_end = bronze_df.withColumn(
        "prd_end_dt",
        calculate_end_date_crm_prd_info(partition_col="prd_key", order_col="prd_start_dt")
    )

    rows = (
        df_with_end
        .filter(F.col("prd_key") == "AC-He-001")
        .orderBy("prd_start_dt")
        .select("prd_start_dt", "prd_end_dt")
        .collect()
    )

    assert rows[0]["prd_end_dt"] == date(2022, 5, 31)
    assert rows[1]["prd_end_dt"] is None  


#Testing Category id extraction
def test_extract_cat_id(bronze_df):
    """First 5 chars of prd_key with '-' replaced by '_'."""
    result = (
        bronze_df
        .filter(F.col("prd_key") == "AC-He-001")
        .select(extract_crm_prd_info_cat_id("prd_key"))
        .first()[0]
    )
    assert result == "AC_He"

#Testing Product Key Extration
def test_extract_prd_key(bronze_df):
    """Characters from position 7 onward become the product_number."""
    result = (
        bronze_df
        .filter(F.col("prd_key") == "AC-He-001")
        .select(extract_crm_prd_info_prd_key("prd_key"))
        .first()[0]
    )
    assert result == "001"


#Complete testing
def test_transform_crm_prd_info(bronze_df):
    result = transform_crm_prd_info(bronze_df)

    #Rename Checking
    assert set(result.columns) == {
        "product_id", "category_id", "product_number",
        "product_name", "product_cost", "product_line",
        "start_date", "end_date",
    }

    #Null to 0 checking
    touring = result.filter(F.col("product_id") == 4).first()
    assert touring["product_cost"] == 0

    #End_date checking
    first_version = result.filter(F.col("product_id") == 1).first()
    assert first_version["end_date"] == date(2022, 5, 31)