# SQL Data Warehouse & Analytics

A SQL Server portfolio project that builds a layered data warehouse from CRM and ERP source files, applies data cleansing and standardization, and exposes a dimensional model for analytics.

## Project architecture

```text
CRM CSV files                 ERP CSV files
     |                             |
     +-------------+---------------+
                   |
                   v
          BRONZE (raw landing)
                   |
                   v
          SILVER (cleaned data)
                   |
                   v
          GOLD (analytical views)
          - dim_customers
          - dim_products
          - fact_sales
                   |
                   v
           SQL analysis / reporting
```

The warehouse uses three schemas: `bronze`, `silver`, and `gold`.

## What is implemented

### 1. Database and schemas
- `init_database.sql` creates the `DataWarehouse` database and the three schemas.
- The initialization script is non-destructive: it creates the database and schemas only when they do not already exist.

### 2. Bronze layer
- Defines six raw landing tables for CRM and ERP source data.
- `scripts/bronze/proc_load_bronze.sql` uses `BULK INSERT` to load the CSV files.
- The load procedure records per-table elapsed time and includes TRY/CATCH handling.

### 3. Silver layer
- Standardizes text values and categorical codes.
- Removes duplicate customer records using `ROW_NUMBER()`.
- Handles invalid or missing dates and normalizes customer, product, and sales fields.
- Derives product validity end dates with `LEAD()`.
- Includes SQL quality-check queries for nulls, duplicates, invalid dates, inconsistent values, and referential consistency.

### 4. Gold layer
Creates analytical views:
- `gold.dim_customers` — customer attributes integrated from CRM and ERP sources.
- `gold.dim_products` — current product attributes and category details.
- `gold.fact_sales` — sales measures joined to customer and product dimension keys.

The resulting model follows a fact-and-dimension structure suitable for BI tools and SQL analysis.

## Repository structure

```text
.
├── init_database.sql
├── EDA.sql
├── AdvancedSQL.sql
├── datasets/
├── scripts/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── tests/
    ├── quality_checks_silver.sql
    └── quality_checks_gold.sql
```

## Prerequisites

- Microsoft SQL Server (T-SQL)
- SQL Server Management Studio or Azure Data Studio
- Permission to create databases, schemas, tables, views, and stored procedures
- Access to the CSV source files in `datasets/`

## Run locally

1. Review `init_database.sql`. It is destructive to an existing `DataWarehouse` database.
2. Run the database initialization script in a development instance.
3. Run the DDL scripts in `scripts/bronze/` and `scripts/silver/` to create the landing and cleaned tables.
4. In `scripts/bronze/proc_load_bronze.sql`, set `@DataDirectory` to the absolute folder path containing the six CSV files in this repository's `datasets/` directory. The SQL Server service account must have read access to that folder.
5. Execute the Bronze load procedure.
6. Execute `silver.load_silver` from `scripts/silver/data_transformation.sql`.
7. Create the Gold views using `scripts/gold/ddl_gold.sql`.
8. Run the Silver and Gold quality-check scripts and investigate any returned rows.
9. Explore the warehouse with `EDA.sql` and `AdvancedSQL.sql`.

## Validation

The repository contains validation queries, but their presence does not by itself prove that every check has been executed successfully. Run the scripts against your own SQL Server instance and record the results before treating the warehouse as production-ready.

## Current limitations and next improvements

- Configure `@DataDirectory` for the SQL Server host before loading.
- Add repeatable orchestration and environment-specific configuration.
- The Bronze loader now rethrows failures after logging the error; add execution logging and monitoring around the procedure.
- Add documented execution evidence, sample query outputs, and a warehouse diagram.
- Add automated checks for row counts, referential integrity, and incremental-load behavior.

## Technologies

Microsoft SQL Server · T-SQL · ETL · Data Warehousing · Dimensional Modelling · Data Quality
