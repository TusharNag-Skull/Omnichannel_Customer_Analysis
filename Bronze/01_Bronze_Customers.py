# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer - Customers
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC The Bronze layer is the first layer of the Medallion Architecture.
# MAGIC
# MAGIC Its purpose is to ingest the raw Olist Customers dataset from AWS S3 and store it in Delta format without applying any business transformations.
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - Reads the raw Customers CSV from AWS S3.
# MAGIC - Creates the Bronze database if it does not exist.
# MAGIC - Creates the Bronze Customers Delta table.
# MAGIC - Loads the raw dataset.
# MAGIC - Performs ingestion validation.
# MAGIC - Leaves all data quality issues untouched for the Silver layer.
# MAGIC
# MAGIC Input:
# MAGIC AWS S3
# MAGIC
# MAGIC Output:
# MAGIC retail_bronze.bronze_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_raw
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC  path 's3://omnicapstone/raw_data/olist_customers_dataset.csv',
# MAGIC  header 'true',
# MAGIC  inferSchema 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Data Preview
# MAGIC
# MAGIC Verify that the raw dataset has been read successfully from AWS S3 before loading it into the Bronze layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC
# MAGIC FROM customers_raw
# MAGIC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.customers
# MAGIC USING DELTA
# MAGIC
# MAGIC AS
# MAGIC
# MAGIC SELECT *
# MAGIC
# MAGIC FROM customers_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Validation
# MAGIC
# MAGIC The following validations ensure that the Bronze table has been created successfully.
# MAGIC
# MAGIC No data modifications are performed in this layer.
# MAGIC
# MAGIC Any issues identified here will be addressed in the Silver layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC 'Source' AS dataset,
# MAGIC COUNT(*) AS total_records
# MAGIC
# MAGIC FROM customers_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC 'Bronze',
# MAGIC
# MAGIC COUNT(*)
# MAGIC
# MAGIC FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC customer_id,
# MAGIC
# MAGIC COUNT(*) AS duplicate_count
# MAGIC
# MAGIC FROM bronze.customers
# MAGIC
# MAGIC GROUP BY customer_id
# MAGIC
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC customer_unique_id,
# MAGIC
# MAGIC COUNT(*) AS total_records
# MAGIC
# MAGIC FROM bronze.customers
# MAGIC
# MAGIC GROUP BY customer_unique_id
# MAGIC
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC ORDER BY total_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
# MAGIC
# MAGIC SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) AS null_customer_unique_id,
# MAGIC
# MAGIC SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code,
# MAGIC
# MAGIC SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) AS null_city,
# MAGIC
# MAGIC SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_state
# MAGIC
# MAGIC FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC SUM(CASE WHEN TRIM(customer_city)='' THEN 1 ELSE 0 END) AS blank_city,
# MAGIC
# MAGIC SUM(CASE WHEN TRIM(customer_state)='' THEN 1 ELSE 0 END) AS blank_state
# MAGIC
# MAGIC FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC customer_state,
# MAGIC
# MAGIC COUNT(*) AS total_customers
# MAGIC
# MAGIC FROM bronze.customers
# MAGIC
# MAGIC GROUP BY customer_state
# MAGIC
# MAGIC ORDER BY total_customers DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC COUNT(DISTINCT customer_city) AS unique_cities
# MAGIC
# MAGIC FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC
# MAGIC FROM bronze.customers
# MAGIC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer Summary
# MAGIC
# MAGIC ## Notebook Execution Status
# MAGIC
# MAGIC **Status:** ✅ SUCCESS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset Information
# MAGIC
# MAGIC | Attribute | Value |
# MAGIC |-----------|------:|
# MAGIC | Database | bronze |
# MAGIC | Table | customers |
# MAGIC | Source | olist_customers_dataset.csv |
# MAGIC | Storage Format | Delta |
# MAGIC | Records Loaded | 99,441 |
# MAGIC | Columns | 5 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Delta Table Information
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | Storage Format | Delta |
# MAGIC | Number of Files | 1 |
# MAGIC | Table Size | 3,731,656 Bytes (~3.56 MB) |
# MAGIC | Compression | ZSTD |
# MAGIC | Deletion Vectors | Enabled |
# MAGIC | Partitioned | No |
# MAGIC | Clustered | No |
# MAGIC | Delta Reader Version | 3 |
# MAGIC | Delta Writer Version | 7 |
# MAGIC
# MAGIC The table has been successfully stored as a Delta table with Delta Lake features enabled, providing ACID transactions, efficient storage, and support for future optimization operations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Validation | Status |
# MAGIC |------------|--------|
# MAGIC | Bronze Database Created | ✅ PASS |
# MAGIC | Source File Read Successfully | ✅ PASS |
# MAGIC | Delta Table Created | ✅ PASS |
# MAGIC | Row Count Validation | ✅ PASS |
# MAGIC | Schema Validation | ✅ PASS |
# MAGIC | Duplicate `customer_id` Check | ✅ PASS |
# MAGIC | NULL Value Analysis | ✅ PASS |
# MAGIC | Blank String Analysis | ✅ PASS |
# MAGIC | Customer State Distribution | ✅ PASS |
# MAGIC | Distinct City Analysis | ✅ PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Profiling Summary
# MAGIC
# MAGIC ### Customer Distribution by State
# MAGIC
# MAGIC - Total States: **27**
# MAGIC - Highest Customer Concentration:
# MAGIC   - SP → **41,746**
# MAGIC   - RJ → **12,852**
# MAGIC   - MG → **11,635**
# MAGIC   - RS → **5,466**
# MAGIC   - PR → **5,045**
# MAGIC
# MAGIC ### Geographic Coverage
# MAGIC
# MAGIC - Total Unique Cities: **4,119**
# MAGIC
# MAGIC This confirms that the dataset provides broad geographical coverage across Brazil and is suitable for downstream customer segmentation and regional analytics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Observations
# MAGIC
# MAGIC - `customer_id` uniquely identifies each customer record.
# MAGIC - `customer_unique_id` represents the actual customer and can appear multiple times because the same customer may place multiple orders over time. This is expected behaviour and is essential for building the Customer 360 model. The duplicate analysis confirms this pattern. :contentReference[oaicite:0]{index=0}
# MAGIC - No transformations were applied in the Bronze layer.
# MAGIC - The source dataset has been preserved exactly as received from AWS S3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Step
# MAGIC
# MAGIC The **Silver Customers** notebook will:
# MAGIC
# MAGIC - Standardize data types where required.
# MAGIC - Validate business keys.
# MAGIC - Standardize text fields.
# MAGIC - Prepare customer data for integration with Orders and the Customer 360 analytical model.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Bronze Customers table is successfully created and ready for the Silver layer.**

# COMMAND ----------

