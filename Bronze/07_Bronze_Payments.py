# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Order Payments
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist Order Payments dataset from AWS S3 into the
# MAGIC Databricks Bronze layer.
# MAGIC
# MAGIC The Bronze layer preserves the source data with minimal processing.
# MAGIC
# MAGIC Detailed data-quality checks, business rules, standardization,
# MAGIC referential integrity, and business transformations will be performed
# MAGIC in the Silver layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC **File:**
# MAGIC
# MAGIC `olist_order_payments_dataset.csv`
# MAGIC
# MAGIC **Location:**
# MAGIC
# MAGIC `s3://olist-retail-project/olist_order_payments_dataset.csv`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One row represents one payment record associated with an order.
# MAGIC
# MAGIC The source contains:
# MAGIC
# MAGIC - `order_id`
# MAGIC - `payment_sequential`
# MAGIC - `payment_type`
# MAGIC - `payment_installments`
# MAGIC - `payment_value`
# MAGIC
# MAGIC `order_id` is not expected to be unique because an order can contain
# MAGIC multiple payment records.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC ## 1. Source and Target Configuration
# MAGIC
# MAGIC The source S3 location and Bronze target are defined centrally.
# MAGIC
# MAGIC All Bronze tables in this project are stored under:
# MAGIC
# MAGIC `workspace.bronze`

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_PATH = 's3://omnicapstone/raw_data/olist_order_payments_dataset.csv'

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "order_payments"

BRONZE_FULL_TABLE = (
    f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Read the Raw Order Payments Dataset
# MAGIC
# MAGIC The source CSV is read directly from AWS S3.
# MAGIC
# MAGIC Schema inference is disabled so that the Bronze layer preserves the
# MAGIC source representation consistently.
# MAGIC
# MAGIC Payment-related numeric fields remain STRING in Bronze.
# MAGIC
# MAGIC The source values are not modified during ingestion.

# COMMAND ----------

order_payments_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

order_payments_raw_df.printSchema()

display(
    order_payments_raw_df.limit(20)
)

source_count = order_payments_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Expected Source Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `order_id` | Order identifier |
# MAGIC | `payment_sequential` | Sequential payment number within an order |
# MAGIC | `payment_type` | Payment method used |
# MAGIC | `payment_installments` | Number of payment installments |
# MAGIC | `payment_value` | Payment amount |
# MAGIC
# MAGIC The source column names are preserved exactly in Bronze.

# COMMAND ----------

expected_columns = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
]

actual_columns = order_payments_raw_df.columns

missing_columns = [
    column
    for column in expected_columns
    if column not in actual_columns
]

unexpected_columns = [
    column
    for column in actual_columns
    if column not in expected_columns
]

print("Expected columns :", len(expected_columns))
print("Actual columns   :", len(actual_columns))
print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if not missing_columns and not unexpected_columns:
    print("PASS — Source column structure is correct.")
else:
    print("FAIL — Source column structure requires investigation.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Bronze Schema
# MAGIC
# MAGIC Create the Bronze schema if it does not already exist.
# MAGIC
# MAGIC All Bronze tables for the project are stored under:
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
# MAGIC ## Step 2 — Create the Bronze Delta Table
# MAGIC
# MAGIC The Bronze table uses the source column names exactly as provided by
# MAGIC the CSV.
# MAGIC
# MAGIC All source fields are stored as STRING.
# MAGIC
# MAGIC This preserves the raw source representation.
# MAGIC
# MAGIC No source values are transformed during table creation.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_FULL_TABLE} (
    order_id STRING,
    payment_sequential STRING,
    payment_type STRING,
    payment_installments STRING,
    payment_value STRING
)
USING DELTA
""")

print(f"Created table: {BRONZE_FULL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Load Raw Data
# MAGIC
# MAGIC The raw source DataFrame is written directly into the Bronze Delta
# MAGIC table.
# MAGIC
# MAGIC No filtering, casting, cleaning, renaming, deduplication, or
# MAGIC enrichment is performed.

# COMMAND ----------

(
    order_payments_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_FULL_TABLE)
)

print(f"Successfully loaded {BRONZE_FULL_TABLE}")

# COMMAND ----------

bronze_order_payments_df = spark.table(
    BRONZE_FULL_TABLE
)

display(
    bronze_order_payments_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Step 4 — Bronze Validation
# MAGIC
# MAGIC The following checks validate that the Bronze table faithfully
# MAGIC contains the source dataset.
# MAGIC
# MAGIC Validation identifies source-level issues but does not modify the
# MAGIC source data.

# COMMAND ----------

bronze_count = bronze_order_payments_df.count()

print(f"Source records : {source_count:,}")
print(f"Bronze records : {bronze_count:,}")

if source_count == bronze_count:
    print("PASS — Source and Bronze row counts match.")
else:
    print("FAIL — Source and Bronze row counts do not match.")

# COMMAND ----------

expected_columns = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
]

actual_columns = bronze_order_payments_df.columns

missing_columns = [
    column
    for column in expected_columns
    if column not in actual_columns
]

unexpected_columns = [
    column
    for column in actual_columns
    if column not in expected_columns
]

print("Expected columns :", len(expected_columns))
print("Actual columns   :", len(actual_columns))
print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if not missing_columns and not unexpected_columns:
    print("PASS — Source column structure is correct.")
else:
    print("FAIL — Column structure requires investigation.")

# COMMAND ----------

bronze_order_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Order ID NULL Check
# MAGIC
# MAGIC Check whether the raw source contains NULL `order_id` values.
# MAGIC
# MAGIC The values are preserved exactly as provided by the source.

# COMMAND ----------

order_id_nulls = bronze_order_payments_df.select(
    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("null_order_id")
)

display(order_id_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Payment Grain Check
# MAGIC
# MAGIC The payment-level grain is:
# MAGIC
# MAGIC `order_id + payment_sequential`
# MAGIC
# MAGIC Profile duplicate combinations without removing any records.

# COMMAND ----------

duplicate_payment_records = (
    bronze_order_payments_df
        .groupBy(
            "order_id",
            "payment_sequential"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
)

duplicate_payment_count = duplicate_payment_records.count()

print(
    f"Duplicate (order_id, payment_sequential) groups: "
    f"{duplicate_payment_count}"
)

display(
    duplicate_payment_records.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## NULL Analysis
# MAGIC
# MAGIC Profile NULL values across all source columns.
# MAGIC
# MAGIC NULL values are retained exactly as provided by the source.

# COMMAND ----------

null_profile = bronze_order_payments_df.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("null_order_id"),

    F.sum(
        F.when(
            F.col("payment_sequential").isNull(),
            1
        ).otherwise(0)
    ).alias("null_payment_sequential"),

    F.sum(
        F.when(
            F.col("payment_type").isNull(),
            1
        ).otherwise(0)
    ).alias("null_payment_type"),

    F.sum(
        F.when(
            F.col("payment_installments").isNull(),
            1
        ).otherwise(0)
    ).alias("null_payment_installments"),

    F.sum(
        F.when(
            F.col("payment_value").isNull(),
            1
        ).otherwise(0)
    ).alias("null_payment_value")
)

display(null_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Blank String Analysis
# MAGIC
# MAGIC Check for blank-string values across the raw source columns.
# MAGIC
# MAGIC No blank values are modified in Bronze.

# COMMAND ----------

blank_profile = bronze_order_payments_df.select(
    F.sum(
        F.when(
            F.col("order_id").isNotNull()
            & (F.trim(F.col("order_id")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_order_id"),

    F.sum(
        F.when(
            F.col("payment_sequential").isNotNull()
            & (F.trim(F.col("payment_sequential")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_payment_sequential"),

    F.sum(
        F.when(
            F.col("payment_type").isNotNull()
            & (F.trim(F.col("payment_type")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_payment_type"),

    F.sum(
        F.when(
            F.col("payment_installments").isNotNull()
            & (F.trim(F.col("payment_installments")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_payment_installments"),

    F.sum(
        F.when(
            F.col("payment_value").isNotNull()
            & (F.trim(F.col("payment_value")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_payment_value")
)

display(blank_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Payment Type Profiling
# MAGIC
# MAGIC Profile the payment types present in the raw source.
# MAGIC
# MAGIC No payment type is changed or standardized.

# COMMAND ----------

payment_type_profile = (
    bronze_order_payments_df
        .groupBy("payment_type")
        .count()
        .orderBy(F.desc("count"))
)

display(payment_type_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Distinct Payment Types
# MAGIC
# MAGIC Calculate the number of distinct payment types present in the source.

# COMMAND ----------

bronze_order_payments_df.select(
    F.countDistinct("payment_type")
        .alias("distinct_payment_types")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Payment Attribute Profiling
# MAGIC
# MAGIC Profile the raw numeric-looking payment fields.
# MAGIC
# MAGIC The Bronze columns remain STRING.
# MAGIC
# MAGIC Casting is used only for source-level profiling and does not modify
# MAGIC the Bronze table.

# COMMAND ----------

payment_numeric_profile = bronze_order_payments_df.select(
    F.min(
        F.col("payment_sequential").cast("double")
    ).alias("minimum_payment_sequential"),

    F.max(
        F.col("payment_sequential").cast("double")
    ).alias("maximum_payment_sequential"),

    F.avg(
        F.col("payment_sequential").cast("double")
    ).alias("average_payment_sequential"),

    F.min(
        F.col("payment_installments").cast("double")
    ).alias("minimum_payment_installments"),

    F.max(
        F.col("payment_installments").cast("double")
    ).alias("maximum_payment_installments"),

    F.avg(
        F.col("payment_installments").cast("double")
    ).alias("average_payment_installments"),

    F.min(
        F.col("payment_value").cast("double")
    ).alias("minimum_payment_value"),

    F.max(
        F.col("payment_value").cast("double")
    ).alias("maximum_payment_value"),

    F.avg(
        F.col("payment_value").cast("double")
    ).alias("average_payment_value")
)

display(payment_numeric_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Payment Value Analysis
# MAGIC
# MAGIC Profile negative and zero payment values present in the raw source.
# MAGIC
# MAGIC No source values are changed or removed.

# COMMAND ----------

payment_value_analysis = bronze_order_payments_df.select(
    F.sum(
        F.when(
            F.col("payment_value").cast("double") < 0,
            1
        ).otherwise(0)
    ).alias("negative_payment_value"),

    F.sum(
        F.when(
            F.col("payment_value").cast("double") == 0,
            1
        ).otherwise(0)
    ).alias("zero_payment_value")
)

display(payment_value_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Invalid Numeric Format Analysis
# MAGIC
# MAGIC Check whether numeric-looking source fields contain values that cannot
# MAGIC be interpreted as numeric.
# MAGIC
# MAGIC The raw Bronze values are not modified.

# COMMAND ----------

invalid_numeric = bronze_order_payments_df.select(
    F.sum(
        F.when(
            F.col("payment_sequential").isNotNull()
            & F.col("payment_sequential").cast("double").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_payment_sequential"),

    F.sum(
        F.when(
            F.col("payment_installments").isNotNull()
            & F.col("payment_installments").cast("double").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_payment_installments"),

    F.sum(
        F.when(
            F.col("payment_value").isNotNull()
            & F.col("payment_value").cast("double").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_payment_value")
)

display(invalid_numeric)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Payment Records by Order
# MAGIC
# MAGIC Profile the number of payment records associated with each order.
# MAGIC
# MAGIC This is used to understand the source grain.
# MAGIC
# MAGIC No aggregation is written back to Bronze.

# COMMAND ----------

payments_per_order_profile = (
    bronze_order_payments_df
        .groupBy("order_id")
        .count()
        .select(
            F.min("count")
                .alias("minimum_payments_per_order"),

            F.max("count")
                .alias("maximum_payments_per_order"),

            F.avg("count")
                .alias("average_payments_per_order")
        )
)

display(payments_per_order_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Source Records with NULL Order IDs
# MAGIC
# MAGIC Display any records where the source `order_id` is NULL.
# MAGIC
# MAGIC The records are only inspected and are not removed or modified.

# COMMAND ----------

display(
    bronze_order_payments_df
        .filter(
            F.col("order_id").isNull()
        )
        .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delta Table Metadata
# MAGIC
# MAGIC Verify the Bronze Delta table metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.bronze.order_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delta Transaction History
# MAGIC
# MAGIC Inspect the Delta transaction history for the Bronze table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.order_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Bronze Table Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.bronze.order_payments"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Order Payments — Execution Summary
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC **Source File:**
# MAGIC
# MAGIC `olist_order_payments_dataset.csv`
# MAGIC
# MAGIC **Source Location:**
# MAGIC
# MAGIC `s3://olist-retail-project/olist_order_payments_dataset.csv`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One record represents one payment associated with an order.
# MAGIC
# MAGIC An order may contain multiple payment records.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Expected Columns
# MAGIC
# MAGIC | Column | Bronze Data Type |
# MAGIC |---|---|
# MAGIC | `order_id` | STRING |
# MAGIC | `payment_sequential` | STRING |
# MAGIC | `payment_type` | STRING |
# MAGIC | `payment_installments` | STRING |
# MAGIC | `payment_value` | STRING |
# MAGIC
# MAGIC The source representation is preserved in Bronze.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Processing
# MAGIC
# MAGIC The notebook performs:
# MAGIC
# MAGIC - Raw CSV ingestion from AWS S3
# MAGIC - Source schema inspection
# MAGIC - Source row-count capture
# MAGIC - Bronze schema creation
# MAGIC - Bronze Delta table creation
# MAGIC - Raw data loading
# MAGIC - Bronze table verification
# MAGIC - Source-to-Bronze row-count reconciliation
# MAGIC - Bronze schema validation
# MAGIC - Delta metadata verification
# MAGIC - Delta history verification
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Validation Results
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |---|---|
# MAGIC | Source ingestion | PASS |
# MAGIC | Source schema | PASS |
# MAGIC | Bronze table creation | PASS |
# MAGIC | Bronze data load | PASS |
# MAGIC | Source-to-Bronze row count | PASS |
# MAGIC | Bronze schema | PASS |
# MAGIC | Delta table validation | PASS |
# MAGIC | Delta history validation | PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Data Preservation
# MAGIC
# MAGIC No transformations were applied to the source data.
# MAGIC
# MAGIC The following were NOT performed:
# MAGIC
# MAGIC - Column renaming
# MAGIC - Datatype conversion
# MAGIC - NULL handling
# MAGIC - Duplicate removal
# MAGIC - Data cleaning
# MAGIC - Standardization
# MAGIC - Business rules
# MAGIC - Joins
# MAGIC - Aggregations
# MAGIC - Derived columns
# MAGIC - Metadata-column additions
# MAGIC
# MAGIC The raw source representation is preserved in:
# MAGIC
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Status
# MAGIC
# MAGIC **SUCCESS — Bronze Order Payments ingestion completed successfully.**

# COMMAND ----------

