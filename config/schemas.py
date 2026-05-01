SCHEMA = {
    "crm_cust_info": """cst_id int,cst_key string,cst_firstname string,cst_lastname string,cst_martial_status string,cst_gndr string,cst_create_date date""",

    "crm_prd_info": """prd_id int,prd_key string,prd_nm string,prd_cost int,prd_line string,prd_start_dt timestamp,prd_end_dt timestamp""",

    "crm_sales_details": """sls_ord_num string,sls_prd_key string,sls_cust_id int,sls_order_dt int,sls_ship_dt int,sls_due_dt int,sls_sales int,sls_quantity int,sls_price int""",

    "erp_loc_a101": """cid string,cntry string""",

    "erp_cust_az12": """cid string,bdate date,gen string""",

    "erp_px_cat_g1v2": """id string,cat string,subcat string,maintenance string"""
}