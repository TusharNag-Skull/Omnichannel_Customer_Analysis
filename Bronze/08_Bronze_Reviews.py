# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Order Reviews
# MAGIC
# MAGIC ### Notebook: 08_Bronze_Reviews
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist Order Reviews dataset from AWS S3 into the
# MAGIC Databricks Bronze layer while preserving the source data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `olist_order_reviews_dataset.csv`
# MAGIC
# MAGIC `s3://olist-retail-project/olist_order_reviews_dataset.csv`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.bronze.order_reviews`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `review_id` | Review identifier |
# MAGIC | `order_id` | Order identifier |
# MAGIC | `review_score` | Review score |
# MAGIC | `review_comment_title` | Review title |
# MAGIC | `review_comment_message` | Review message |
# MAGIC | `review_creation_date` | Review creation date |
# MAGIC | `review_answer_timestamp` | Review answer timestamp |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Policy
# MAGIC
# MAGIC The source data is preserved as received.
# MAGIC
# MAGIC No cleaning, transformation, standardization, filtering, joins,
# MAGIC derived columns, or business rules are applied in Bronze.
# MAGIC
# MAGIC The source fields remain STRING in the Bronze table.

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_PATH = 's3://omnicapstone/raw_data/olist_order_reviews_dataset.csv'

BRONZE_TABLE = "workspace.bronze.order_reviews"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read Raw Reviews Dataset
# MAGIC
# MAGIC The Reviews CSV contains review text fields that may contain commas,
# MAGIC quotes, and multiline content.
# MAGIC
# MAGIC The CSV reader is therefore configured to correctly preserve the raw
# MAGIC records during ingestion.

# COMMAND ----------

reviews_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(SOURCE_PATH)
)

reviews_raw_df.printSchema()

display(reviews_raw_df.limit(20))

source_count = reviews_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Bronze Schema
# MAGIC
# MAGIC All raw Bronze tables are stored in:
# MAGIC
# MAGIC `workspace.bronze`

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Bronze Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.bronze.order_reviews (
    review_id STRING,
    order_id STRING,
    review_score STRING,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date STRING,
    review_answer_timestamp STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load Raw Data into Bronze

# COMMAND ----------

(
    reviews_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze table created successfully: {BRONZE_TABLE}")

# COMMAND ----------

bronze_reviews_df = spark.table(BRONZE_TABLE)

bronze_reviews_df.printSchema()

display(bronze_reviews_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze Row Count Validation
# MAGIC
# MAGIC The source and Bronze row counts must match.

# COMMAND ----------

bronze_count = bronze_reviews_df.count()

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

expected_columns = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp"
]

if bronze_reviews_df.columns != expected_columns:
    raise ValueError(
        f"Column mismatch.\n"
        f"Expected: {expected_columns}\n"
        f"Actual:   {bronze_reviews_df.columns}"
    )

print("PASS — Bronze columns match the source.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Verify Delta Table
# MAGIC
# MAGIC Verify that the Bronze table is registered correctly and stored as a
# MAGIC Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.bronze.order_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Verify Delta History
# MAGIC
# MAGIC Check the Delta transaction history for the Bronze table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.bronze.order_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Bronze Table Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.bronze.order_reviews"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Order Reviews — Execution Summary
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `olist_order_reviews_dataset.csv`
# MAGIC
# MAGIC ## Source Location
# MAGIC
# MAGIC `s3://olist-retail-project/olist_order_reviews_dataset.csv`
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.order_reviews`
# MAGIC
# MAGIC ## Source Records
# MAGIC
# MAGIC [ACTUAL SOURCE COUNT]
# MAGIC
# MAGIC ## Bronze Records
# MAGIC
# MAGIC [ACTUAL BRONZE COUNT]
# MAGIC
# MAGIC ## Columns
# MAGIC
# MAGIC 7
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
# MAGIC ## Bronze Status
# MAGIC
# MAGIC **SUCCESS — Order Reviews raw data has been loaded into
# MAGIC `workspace.bronze.order_reviews` with the source structure preserved.**

# COMMAND ----------

