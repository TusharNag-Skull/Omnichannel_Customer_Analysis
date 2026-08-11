# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Order Reviews
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create the Silver Order Reviews table from the Bronze Order Reviews table.
# MAGIC
# MAGIC The Silver layer standardizes data types, preserves valid reviews, creates lifecycle and data-quality flags, and applies technical validations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.order_reviews`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.order_reviews`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Key
# MAGIC
# MAGIC `review_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Responsibilities
# MAGIC
# MAGIC - Preserve valid Bronze review records.
# MAGIC - Standardize numeric fields (cast `review_score` to INTEGER).
# MAGIC - Convert timestamps from STRING to TIMESTAMP.
# MAGIC - Trim string columns (`review_id`, `order_id`, `review_comment_title`, `review_comment_message`).
# MAGIC - Create business quality flags (`invalid_review_score_flag`, `invalid_review_date_flag`).
# MAGIC - Add `silver_load_timestamp`.
# MAGIC - Register the Delta table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quality Requirements
# MAGIC
# MAGIC - Required identifiers (`review_id`, `order_id`) must not be NULL.
# MAGIC - Review score must be between 1 and 5.
# MAGIC - Review answer timestamp cannot be earlier than creation date.
# MAGIC - Bronze and Silver row counts must reconcile.
# MAGIC
# MAGIC

# COMMAND ----------

## Step 1 — Import Required Libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

## Step 2 — Read Bronze Reviews

# COMMAND ----------

bronze_reviews_df = spark.table("workspace.bronze.order_reviews")
bronze_review_count = bronze_reviews_df.count()

print(f"Bronze Reviews rows: {bronze_review_count:,}")
bronze_reviews_df.printSchema()

# COMMAND ----------

## Step 3 — Validate Bronze Reviews Structure

# COMMAND ----------

required_columns = {
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp"
}

actual_columns = set(bronze_reviews_df.columns)
missing_columns = required_columns - actual_columns

if missing_columns:
    raise ValueError(
        "Bronze Reviews schema validation failed. "
        f"Missing columns: {sorted(missing_columns)}"
    )

print("PASS — Bronze Reviews contains all required columns.")

# COMMAND ----------

## Step 4 — Validate Bronze Review Keys

# COMMAND ----------

null_review_ids = bronze_reviews_df.filter(F.col("review_id").isNull()).count()
null_order_ids = bronze_reviews_df.filter(F.col("order_id").isNull()).count()

print(f"NULL review IDs: {null_review_ids}")
print(f"NULL order IDs:  {null_order_ids}")

if null_review_ids != 0:
    raise ValueError("Bronze Reviews quality gate failed: NULL review_id values found.")

if null_order_ids != 0:
    raise ValueError("Bronze Reviews quality gate failed: NULL order_id values found.")

print("PASS — Bronze review business keys are valid.")

# COMMAND ----------

## Step 5 — Create Silver Reviews DataFrame

# COMMAND ----------

silver_reviews_df = bronze_reviews_df.select(
    F.trim(F.col("review_id")).alias("review_id"),
    F.trim(F.col("order_id")).alias("order_id"),
    F.col("review_score").cast("integer").alias("review_score"),
    F.trim(F.col("review_comment_title")).alias("review_comment_title"),
    F.trim(F.col("review_comment_message")).alias("review_comment_message"),
    F.to_timestamp(F.col("review_creation_date")).alias("review_creation_date"),
    F.to_timestamp(F.col("review_answer_timestamp")).alias("review_answer_timestamp"),
    (
        F.col("review_score").cast("integer").isNull() |
        ~F.col("review_score").cast("integer").between(1, 5)
    ).alias("invalid_review_score_flag"),
    (
        F.to_timestamp(F.col("review_answer_timestamp")).isNotNull() &
        F.to_timestamp(F.col("review_creation_date")).isNotNull() &
        (F.to_timestamp(F.col("review_answer_timestamp")) < F.to_timestamp(F.col("review_creation_date")))
    ).alias("invalid_review_date_flag"),
    F.current_timestamp().alias("silver_load_timestamp")
)

# COMMAND ----------

## Step 6 — Profile Silver Reviews Data Quality

# COMMAND ----------

invalid_score_count = silver_reviews_df.filter(F.col("invalid_review_score_flag") == True).count()
invalid_date_count = silver_reviews_df.filter(F.col("invalid_review_date_flag") == True).count()

print(f"Invalid review scores : {invalid_score_count}")
print(f"Invalid review dates  : {invalid_date_count}")

# COMMAND ----------

## Step 7 — Persist Silver Reviews

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

(
    silver_reviews_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.order_reviews", mode="overwrite")
)

print("Silver Order Reviews table created successfully.")

# COMMAND ----------

## Step 8 — Reload and Validate Persisted Silver Reviews

# COMMAND ----------

final_reviews_df = spark.table("workspace.silver.order_reviews")
final_review_count = final_reviews_df.count()

print(f"Persisted Silver Reviews rows: {final_review_count:,}")
final_reviews_df.printSchema()

if final_review_count != bronze_review_count:
    raise ValueError(
        f"Row count mismatch after persistence. "
        f"Bronze rows: {bronze_review_count:,}, Silver rows: {final_review_count:,}"
    )

print("PASS — Bronze and Silver row counts reconcile.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Verify Delta Metadata
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.silver.order_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Verify Delta History
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.silver.order_reviews;

# COMMAND ----------

## Step 11 — Final Silver Reviews Preview

# COMMAND ----------

display(spark.table("workspace.silver.order_reviews").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Order Reviews — Execution Summary
# MAGIC
# MAGIC
# MAGIC ## Table Information
# MAGIC
# MAGIC | Attribute | Value |
# MAGIC |---|---|
# MAGIC | Database | workspace.silver |
# MAGIC | Table Name | order_reviews |
# MAGIC | Source Table | workspace.bronze.order_reviews |
# MAGIC | Business Key | review_id |
# MAGIC | Record Count | 99,224 |
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Check | Result |
# MAGIC |---|---|
# MAGIC | Bronze schema validation | PASS |
# MAGIC | Key NULL checks | PASS |
# MAGIC | Row count reconciliation | PASS |
# MAGIC | Business flags validation | PASS |
# MAGIC | Delta table creation | PASS |
# MAGIC
# MAGIC **SUCCESS — Silver Order Reviews transformation and loading completed successfully.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug

# COMMAND ----------

spark.table("workspace.silver.order_reviews").printSchema()

# COMMAND ----------

silver_reviews_df.printSchema()

# COMMAND ----------

(
    silver_reviews_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.order_reviews")
)

print("Silver Order Reviews table created successfully.")

# COMMAND ----------

spark.table("workspace.silver.order_reviews").printSchema()

# COMMAND ----------

print(f"Bronze Reviews rows: {bronze_review_count:,}")
print(f"Silver Reviews rows: {spark.table('workspace.silver.order_reviews').count():,}")

# COMMAND ----------

spark.sql("DESCRIBE DETAIL workspace.silver.order_reviews").show(truncate=False)

# COMMAND ----------

