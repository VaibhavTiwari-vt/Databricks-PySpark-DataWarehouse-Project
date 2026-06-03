# Databricks PySpark Data Warehouse Project

## Overview

This project demonstrates the design and implementation of an end-to-end Data Warehouse using **Databricks**, **PySpark**, **Delta Lake**, and **Medallion Architecture** (Bronze → Silver → Gold).

The primary objective of this project was to create a **scalable, reusable, modular, and testable ETL framework** capable of transforming raw data into analytics-ready datasets for business reporting and decision-making.

The entire pipeline was developed using a layered architecture approach with extensive unit testing to ensure data quality, consistency, and reliability.

---

# Project Architecture

The project follows the **Medallion Architecture** pattern:

```text
Raw Files
   ↓
Bronze Layer (Raw Ingestion)
   ↓
Silver Layer (Data Cleansing & Standardization)
   ↓
Gold Layer (Business-Level Data Model)
   ↓
Analytics / Reporting / BI
```

---

# Tech Stack

| Technology           | Purpose                     |
| -------------------- | --------------------------- |
| Databricks           | Data Engineering Platform   |
| PySpark              | Distributed Data Processing |
| Spark SQL            | Data Transformation         |
| Delta Lake           | Reliable Storage Format     |
| Python               | ETL Development             |
| PyTest               | Unit Testing                |
| Databricks Workflows | Pipeline Orchestration      |

---

# Repository Structure

```text
Databricks-PySpark-DataWarehouse-Project/
│
├── config/
│   ├── bronze_config.py
│   └── schemas.py
│
├── notebooks/
│   ├── bronze_pipeline.ipynb
│   ├── silver_pipeline.ipynb
│   └── gold_pipeline.ipynb
│
├── src/
│   ├── bronze/
│   │   └── bronze.py
│   │
│   ├── silver/
│   │   ├── crm/
│   │   └── erp/
│   │
│   └── gold/
│       ├── gold_dim_customers.py
│       ├── gold_dim_products.py
│       └── gold_facts_sales.py
│
├── tests/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── conftest.py
├── pytest.ini
└── README.md
```

---

# Bronze Layer

## Objective

The Bronze layer acts as the **raw ingestion layer** responsible for:

* ingesting source files,
* preserving raw data,
* ensuring traceability,
* and maintaining source-level integrity.

## Key Features

* Dynamic configuration-driven ingestion
* Raw file preservation
* Schema handling
* Source-to-target mapping
* Delta table writing
* Modular ingestion logic

## Implementation

The Bronze layer was implemented inside:

```text
src/bronze/bronze.py
```

Configuration management was separated into:

```text
config/bronze_config.py
config/schemas.py
```

## Processing Flow

1. Read source files
2. Apply schema
3. Iterate through configured datasets
4. Write data into Bronze schema
5. Store data in Delta format

---

# Silver Layer

## Objective

The Silver layer focuses on:

* data cleansing,
* standardization,
* validation,
* and transformation.

This layer converts inconsistent raw data into trusted and standardized datasets.

---

# Silver Layer Transformations

The following validations and transformations were implemented:

## Data Quality Checks

* Duplicate removal
* Primary key validation
* Foreign key separation
* Null handling
* Invalid date handling
* Standardization of categorical values
* String trimming
* Whitespace removal
* Sales amount correction
* Price correction
* Data type correction

---

# CRM Tables Processed

## Customer Information

```text
silver_crm_cust_info.py
```

### Transformations

* Customer ID validation
* Gender standardization
* Null handling
* Duplicate removal
* String cleanup

---

## Product Information

```text
silver_crm_prd_info.py
```

### Transformations

* Product category extraction
* Product maintenance handling
* Product cost standardization
* Date validation

---

## Sales Details

```text
silver_crm_sales_details.py
```

### Transformations

* Invalid sales amount correction
* Quantity validation
* Price correction
* Date consistency checks

---

# ERP Tables Processed

## ERP Customer Data

```text
silver_erp_cust_az12.py
```

### Transformations

* Customer number cleanup
* Gender normalization
* Birthdate validation

---

## ERP Location Data

```text
silver_erp_loc_a101.py
```

### Transformations

* Country normalization
* Location standardization

---

## ERP Product Category Data

```text
silver_erp_px_cat_g1v2.py
```

### Transformations

* Product category standardization
* Maintenance field cleanup

---

# Gold Layer

## Objective

The Gold layer represents the **business-level analytical model**.

This layer was designed using a **Star Schema** consisting of:

* Dimension tables
* Fact table

The transformations in this layer were implemented using **Spark SQL** to make the logic easier to understand for:

* analysts,
* power users,
* and non-technical stakeholders.

---

# Data Model

## Dimension Tables

### dim_customers

Contains:

* customer information
* demographic details
* country
* marital status
* gender
* birthdate

### Business Rules

* CRM gender has priority
* ERP used as fallback
* Surrogate key generation using ROW_NUMBER()

---

### dim_products

Contains:

* product details
* category information
* maintenance status
* pricing details

### Business Rules

* Active products only
* Historical filtering
* Surrogate key generation

---

# Fact Table

## fact_sales

Contains:

* sales transactions
* quantity
* pricing
* sales amount
* customer-product relationships

### Business Rules

* Product surrogate key resolution
* Customer surrogate key resolution
* Historical + current sales inclusion

---

# Unit Testing

A major focus of this project was creating a **fully testable ETL framework**.

Unit tests were implemented for:

* Bronze Layer
* Silver Layer
* Gold Layer

The project uses:

```text
PyTest
```

---

# Bronze Layer Tests

## Validations

* Data availability
* File ingestion validation
* Schema validation

---

# Silver Layer Tests

## Validations

* Duplicate removal
* Invalid date handling
* Null handling
* Standardization checks
* Transformation validation
* Sales correction logic

---

# Gold Layer Tests

## Validations

* Surrogate key generation
* Left join preservation
* Business rule validation
* Row count validation
* Fact-to-dimension mapping
* Join correctness

---

# Orchestration

After developing individual modules, all layers were orchestrated using:

* Databricks notebooks
* Databricks Workflows
* Databricks Pipelines

Execution Flow:

```text
Bronze Notebook
      ↓
Silver Notebook
      ↓
Gold Notebook
```

---

# Key Engineering Concepts Demonstrated

* Medallion Architecture
* ETL Framework Design
* Modular Data Engineering
* Data Cleansing Pipelines
* Delta Lake Implementation
* Distributed Data Processing
* Star Schema Modeling
* Unit Testing for Data Pipelines
* Spark SQL Transformations
* Databricks Workflow Orchestration

---

# How to Run the Project

## 1. Clone Repository

```bash
git clone <repository-url>
```

---

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Run Unit Tests

```bash
pytest
```

---

## 4. Execute Databricks Pipelines

Run notebooks in the following order:

```text
1. bronze_pipeline.ipynb
2. silver_pipeline.ipynb
3. gold_pipeline.ipynb
```

---

# Future Improvements

* Incremental loading
* Slowly Changing Dimensions (SCD)
* Data Quality Monitoring
* CI/CD Integration
* Automated Deployment
* Data Lineage Tracking
* Logging Framework
* Parameterized Pipeline Execution

---

# Project Highlights

* Fully modular ETL framework
* End-to-end Medallion Architecture
* Production-style transformation logic
* Extensive unit testing
* Spark SQL + PySpark implementation
* Business-ready dimensional model
* Databricks orchestration

---

# Author

## Vaibhav Tiwari

Data Engineering | PySpark | Databricks | SQL | ETL | Data Warehousing
