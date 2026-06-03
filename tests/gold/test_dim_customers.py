import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from src.gold.gold_dim_customers import get_dim_customers_query, build_dim_customers


#Creating Spark Session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


#Schema Definition
CrmCustSchema = (
    "customer_id int, customer_number string, first_name string, last_name string, "
    "gender string, marital_status string, created_date date"
)
ErpCustSchema = "customer_number string, gender string, birth_date date"
ErpLocSchema  = "customer_number string, country string"


#Test Data
@pytest.fixture(scope="session")
def crm_df(spark):
    rows = [
        (1, "C001", "Alice", "Smith",  "Female", "Married",  date(2020, 1, 15)),   # CRM gender set
        (2, "C002", "Bob",   "Jones",  "Male",   "Single",   date(2021, 3, 10)),   # CRM gender set
        (3, "C003", "Carol", "White",  "n/a",    "Married",  date(2019, 7, 22)),   # CRM gender n/a → use ERP
        (4, "C004", "Dave",  "Brown",  "n/a",    "Single",   date(2022, 5, 5)),    # CRM gender n/a, ERP also absent
        (5, "C005", "Eve",   "Taylor", "Female", "Married",  date(2023, 2, 28)),   # No ERP match at all
    ]
    return spark.createDataFrame(rows, schema=CrmCustSchema)


@pytest.fixture(scope="session")
def erp_cust_df(spark):
    rows = [
        ("C001", "Female", date(1985, 6, 20)),   # Matches C001 — gender already set in CRM
        ("C002", "Male",   date(1990, 11, 3)),   # Matches C002
        ("C003", "Female", date(1992, 4, 14)),   # Matches C003 — provides fallback gender
        # C004 present but gender is NULL → COALESCE should return 'n/a'
        ("C004", None,     date(1988, 8, 8)),
        # C005 has no row here → left-join keeps C005 with NULL ERP fields
    ]
    return spark.createDataFrame(rows, schema=ErpCustSchema)


@pytest.fixture(scope="session")
def erp_loc_df(spark):
    rows = [
        ("C001", "United States"),
        ("C002", "Canada"),
        ("C003", "Germany"),
        # C004 and C005 have no location row → country should be NULL
    ]
    return spark.createDataFrame(rows, schema=ErpLocSchema)


#Registering views for testing
def register_views(crm, erp_cust, erp_loc):
    crm.createOrReplaceTempView("crm_cust_test")
    erp_cust.createOrReplaceTempView("erp_cust_test")
    erp_loc.createOrReplaceTempView("erp_loc_test")



#Customer key generation — sequential, gapless, starting at 1
def test_customer_key_generated_sequentially(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    keys = sorted(r["customer_key"] for r in result.collect())
    assert keys == list(range(1, len(keys) + 1)), \
        f"Expected gapless keys [1..{len(keys)}], got {keys}"


#Customer key must be unique across all rows
def test_customer_key_is_unique(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    keys = [r["customer_key"] for r in result.collect()]
    assert len(keys) == len(set(keys)), "Duplicate customer_key values found"


#When CRM gender is not 'n/a', it takes priority over ERP
def test_crm_gender_takes_priority(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))

    c1 = result.filter(F.col("customer_id") == 1).first()
    assert c1["gender"] == "Female", "CRM gender 'Female' should be used for customer_id=1"

    c2 = result.filter(F.col("customer_id") == 2).first()
    assert c2["gender"] == "Male", "CRM gender 'Male' should be used for customer_id=2"


#When CRM gender is 'n/a', ERP gender is the fallback
def test_erp_gender_fallback_when_crm_is_na(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    c3 = result.filter(F.col("customer_id") == 3).first()
    assert c3["gender"] == "Female", \
        "ERP gender should be used as fallback when CRM gender is 'n/a'"


#When CRM gender is 'n/a' AND ERP gender is NULL, result should be 'n/a'
def test_gender_defaults_to_na_when_both_sources_absent(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    c4 = result.filter(F.col("customer_id") == 4).first()
    assert c4["gender"] == "n/a", \
        "gender should fall back to 'n/a' when both CRM is 'n/a' and ERP is NULL"


#LEFT JOIN on erp_cust — customers with no ERP match must still appear
def test_left_join_erp_cust_retains_all_crm_rows(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    ids = [r["customer_id"] for r in result.collect()]
    assert 5 in ids, "customer_id=5 has no ERP match but must still appear (LEFT JOIN)"


#LEFT JOIN on erp_loc — customers with no location match must still appear
def test_left_join_erp_loc_retains_all_crm_rows(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    c4 = result.filter(F.col("customer_id") == 4).first()
    assert c4 is not None,         "customer_id=4 has no location row but must still appear"
    assert c4["country"] is None,  "country should be NULL when no erp_loc match exists"


#No duplicate customer_id rows (guards against join fan-out)
def test_no_duplicate_customer_ids(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    ids = [r["customer_id"] for r in result.collect()]
    assert len(ids) == len(set(ids)), "Each customer_id must appear exactly once (no join fan-out)"


#Output schema must contain exactly the expected columns
def test_dim_customers_schema(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    assert set(result.columns) == {
        "customer_key", "customer_id", "customer_number",
        "first_name", "last_name", "country",
        "marital_status", "gender", "birthdate", "create_date",
    }, f"Unexpected schema: {result.columns}"


#Row count must equal the number of CRM source rows (all customers kept)
def test_row_count_matches_crm_source(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    assert result.count() == crm_df.count(), \
        "Output row count must match CRM source count — all customers must be preserved"


#Empty CRM input -> empty output (no crash, zero rows)
def test_empty_crm_returns_empty_result(spark, erp_cust_df, erp_loc_df):
    empty_crm = spark.createDataFrame([], schema=CrmCustSchema)
    register_views(empty_crm, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    assert result.count() == 0, "Empty CRM input should produce zero output rows"


#Empty ERP tables -> all CRM rows preserved with NULL enrichment columns
def test_empty_erp_tables_preserve_all_crm_rows(spark, crm_df):
    empty_erp_cust = spark.createDataFrame([], schema=ErpCustSchema)
    empty_erp_loc  = spark.createDataFrame([], schema=ErpLocSchema)
    register_views(crm_df, empty_erp_cust, empty_erp_loc)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))

    assert result.count() == crm_df.count(), \
        "All CRM rows must be retained even when both ERP tables are empty"

    #Enrichment columns should all be NULL
    sample = result.filter(F.col("customer_id") == 1).first()
    assert sample["country"]   is None, "country should be NULL when erp_loc is empty"
    assert sample["birthdate"] is None, "birthdate should be NULL when erp_cust is empty"


#Location data enriched correctly when a match exists
def test_location_enriched_correctly(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    c1 = result.filter(F.col("customer_id") == 1).first()
    assert c1["country"] == "United States", "Country should be enriched from erp_loc"


#Birth date enriched correctly from erp_cust when a match exists
def test_birthdate_enriched_correctly(spark, crm_df, erp_cust_df, erp_loc_df):
    register_views(crm_df, erp_cust_df, erp_loc_df)
    result = spark.sql(get_dim_customers_query("crm_cust_test", "erp_cust_test", "erp_loc_test"))
    c1 = result.filter(F.col("customer_id") == 1).first()
    assert c1["birthdate"] == date(1985, 6, 20), \
        "birthdate should be sourced from erp_cust.birth_date"