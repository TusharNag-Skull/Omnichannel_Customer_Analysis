# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Geolocation
# MAGIC
# MAGIC ### Notebook: 09_Bronze_Geolocation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist Geolocation dataset from AWS S3 into the
# MAGIC Databricks Bronze layer while preserving the source data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `olist_geolocation_dataset.csv`
# MAGIC
# MAGIC `s3://olist-retail-project/olist_geolocation_dataset.csv`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.bronze.geolocation`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `geolocation_zip_code_prefix` | ZIP code prefix |
# MAGIC | `geolocation_lat` | Latitude |
# MAGIC | `geolocation_lng` | Longitude |
# MAGIC | `geolocation_city` | City |
# MAGIC | `geolocation_state` | State |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC The raw geolocation dataset contains multiple records for ZIP code
# MAGIC prefixes.
# MAGIC
# MAGIC The raw records are preserved in Bronze.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Policy
# MAGIC
# MAGIC The source data is preserved as received.
# MAGIC
# MAGIC No deduplication, aggregation, datatype conversion, city/state
# MAGIC standardization, filtering, or business transformation is performed
# MAGIC in Bronze.
# MAGIC
# MAGIC All source columns are stored as STRING.

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_PATH = 's3://omnicapstone/raw_data/olist_geolocation_dataset.csv'

BRONZE_TABLE = "workspace.bronze.geolocation"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Read Raw Geolocation Dataset
# MAGIC
# MAGIC Read the source CSV directly from AWS S3.
# MAGIC
# MAGIC Schema inference is disabled so that the raw source representation is
# MAGIC preserved in Bronze.

# COMMAND ----------

geolocation_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

geolocation_raw_df.printSchema()

display(
    geolocation_raw_df.limit(20)
)

source_count = geolocation_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Expected Source Columns

# COMMAND ----------

expected_columns = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
]

actual_columns = geolocation_raw_df.columns

if actual_columns != expected_columns:
    raise ValueError(
        f"Column mismatch.\n"
        f"Expected: {expected_columns}\n"
        f"Actual:   {actual_columns}"
    )

print("PASS — Source columns match the expected structure.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Bronze Schema
# MAGIC
# MAGIC All Bronze tables are stored under:
# MAGIC
# MAGIC `workspace.bronze`

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("Bronze schema is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 — Create Bronze Delta Table
# MAGIC
# MAGIC The source column names and raw representation are preserved.
# MAGIC
# MAGIC All source fields are stored as STRING.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.bronze.geolocation (
    geolocation_zip_code_prefix STRING,
    geolocation_lat STRING,
    geolocation_lng STRING,
    geolocation_city STRING,
    geolocation_state STRING
)
USING DELTA
""")

print("Bronze Geolocation table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Load Raw Data into Bronze

# COMMAND ----------

(
    geolocation_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
)

print(
    f"Bronze table created successfully: {BRONZE_TABLE}"
)

# COMMAND ----------

bronze_geolocation_df = spark.table(
    BRONZE_TABLE
)

bronze_geolocation_df.printSchema()

display(
    bronze_geolocation_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Record Count
# MAGIC
# MAGIC Compare the source record count with the Bronze record count.
# MAGIC
# MAGIC The counts must match because Bronze preserves the raw source dataset.

# COMMAND ----------

bronze_count = bronze_geolocation_df.count()

print(f"Source Records : {source_count:,}")
print(f"Bronze Records : {bronze_count:,}")

if source_count != bronze_count:
    raise ValueError(
        f"Row count mismatch: "
        f"source={source_count:,}, "
        f"bronze={bronze_count:,}"
    )

print("PASS — Source and Bronze row counts match.")

# COMMAND ----------

actual_bronze_columns = bronze_geolocation_df.columns

if actual_bronze_columns != expected_columns:
    raise ValueError(
        f"Column mismatch.\n"
        f"Expected: {expected_columns}\n"
        f"Actual:   {actual_bronze_columns}"
    )

print("PASS — Bronze columns match the source.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Verify Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.bronze.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Verify Delta History

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Bronze Table Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.bronze.geolocation"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Geolocation — Execution Summary
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `olist_geolocation_dataset.csv`
# MAGIC
# MAGIC ## Source Location
# MAGIC
# MAGIC `s3://olist-retail-project/olist_geolocation_dataset.csv`
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.geolocation`
# MAGIC
# MAGIC ## Source Records
# MAGIC
# MAGIC 1,000,163
# MAGIC
# MAGIC ## Bronze Records
# MAGIC
# MAGIC 1,000,163
# MAGIC
# MAGIC ## Columns
# MAGIC
# MAGIC 5
# MAGIC
# MAGIC ## Validation
# MAGIC
# MAGIC | Check | Result |
# MAGIC |---|---|
# MAGIC | Source ingestion | PASS |
# MAGIC | Source columns | PASS |
# MAGIC | Bronze table creation | PASS |
# MAGIC | Source-to-Bronze row count | PASS |
# MAGIC | Bronze columns | PASS |
# MAGIC | Delta table verification | PASS |
# MAGIC | Delta history verification | PASS |
# MAGIC
# MAGIC ## Bronze Data Preservation
# MAGIC
# MAGIC The raw geolocation records are preserved in Bronze.
# MAGIC
# MAGIC No:
# MAGIC
# MAGIC - Deduplication
# MAGIC - Aggregation
# MAGIC - Datatype conversion
# MAGIC - City standardization
# MAGIC - State standardization
# MAGIC - Filtering
# MAGIC - Derived columns
# MAGIC - Joins
# MAGIC - Business transformations
# MAGIC
# MAGIC were applied.
# MAGIC
# MAGIC ## Bronze Status
# MAGIC
# MAGIC **SUCCESS — Geolocation raw data has been loaded into
# MAGIC `workspace.bronze.geolocation` with the source structure preserved.**

# COMMAND ----------

