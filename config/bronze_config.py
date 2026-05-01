INGESTION_CONFIG = [
    {
        "source": "crm",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_crm/cust_info.csv",
        "table": "crm_cust_info",
        "catalog":"databricks-project",
        "schema":"bronze"
    },
    {
        "source": "crm",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_crm/prd_info.csv",
        "table": "crm_prd_info",
        "catalog":"databricks-project",
        "schema":"bronze"
    },
    {
        "source": "crm",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_crm/sales_details.csv",
        "table": "crm_sales_details",
        "catalog":"databricks-project",
        "schema":"bronze"
    },
    {
        "source": "erp",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_erp/CUST_AZ12.csv",
        "table": "erp_cust_az12",
        "catalog":"databricks-project",
        "schema":"bronze"
    },
    {
        "source": "erp",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_erp/LOC_A101.csv",
        "table": "erp_loc_a101",
        "catalog":"databricks-project",
        "schema":"bronze"
    },
    {
        "source": "erp",
        "path": "/Volumes/databricks-project/bronze/source_systems/source_erp/PX_CAT_G1V2.csv",
        "table": "erp_px_cat_g1v2",
        "catalog":"databricks-project",
        "schema":"bronze"
    }
]