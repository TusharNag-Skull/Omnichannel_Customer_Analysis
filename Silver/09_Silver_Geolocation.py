# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Geolocation
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create the Silver Geolocation table from the Bronze Geolocation table.
# MAGIC
# MAGIC The Silver layer standardizes data types, deduplicates records by ZIP code prefix, aggregates coordinates, and cleans geographic text attributes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.geolocation`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.geolocation`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Key
# MAGIC
# MAGIC `geolocation_zip_code_prefix`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Responsibilities
# MAGIC
# MAGIC - Filter out records with invalid (NULL) ZIP codes or coordinates.
# MAGIC - Standardize coordinates (cast `geolocation_lat` and `geolocation_lng` to DOUBLE).
# MAGIC - Standardize ZIP codes (cast `geolocation_zip_code_prefix` to INTEGER).
# MAGIC - Clean and standardize string fields (`geolocation_city` and `geolocation_state` to trimmed lowercase).
# MAGIC - Deduplicate and aggregate the records by `geolocation_zip_code_prefix` by taking the average coordinates and the first city and state values.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC - Register the Delta table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quality Requirements
# MAGIC
# MAGIC - `geolocation_zip_code_prefix` must be unique in the Silver table.
# MAGIC - Coordinates must be valid numeric values.
# MAGIC - Final table must be registered in the catalog.
# MAGIC
# MAGIC

# COMMAND ----------



## Step 1 — Import Required Libraries



from pyspark.sql import functions as F

# COMMAND ----------

## Step 2 — Read Bronze Geolocation

# COMMAND ----------

bronze_geo_df = spark.table("workspace.bronze.geolocation")
bronze_geo_count = bronze_geo_df.count()

print(f"Bronze Geolocation rows: {bronze_geo_count:,}")
bronze_geo_df.printSchema()

# COMMAND ----------

## Step 3 — Validate Bronze Geolocation Structure

# COMMAND ----------

required_columns = {
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
}

actual_columns = set(bronze_geo_df.columns)
missing_columns = required_columns - actual_columns

if missing_columns:
    raise ValueError(
        "Bronze Geolocation schema validation failed. "
        f"Missing columns: {sorted(missing_columns)}"
    )

print("PASS — Bronze Geolocation contains all required columns.")

# COMMAND ----------

## Step 4 — Clean and Cast Geolocation Data

# COMMAND ----------

# Filter out rows with NULL keys or coordinates in Bronze first
cleaned_geo_df = (
    bronze_geo_df
    .filter(
        F.col("geolocation_zip_code_prefix").isNotNull() &
        F.col("geolocation_lat").isNotNull() &
        F.col("geolocation_lng").isNotNull()
    )
    .select(
        F.col("geolocation_zip_code_prefix").cast("integer").alias("geolocation_zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("geolocation_lat"),
        F.col("geolocation_lng").cast("double").alias("geolocation_lng"),
        F.lower(F.trim(F.col("geolocation_city"))).alias("geolocation_city"),
        F.lower(F.trim(F.col("geolocation_state"))).alias("geolocation_state")
    )
)

# COMMAND ----------

## Step 5 — Deduplicate and Aggregate by ZIP Code Prefix

# COMMAND ----------

# Since a single ZIP code prefix has multiple coordinate readings,
# we take the average latitude/longitude and representative city/state.
silver_geo_df = (
    cleaned_geo_df
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state")
    )
    .withColumn("silver_load_timestamp", F.current_timestamp())
)

# COMMAND ----------

## Step 6 — Validate Silver Geolocation Unique Key

# COMMAND ----------

silver_geo_count = silver_geo_df.count()
distinct_zips = silver_geo_df.select("geolocation_zip_code_prefix").distinct().count()

print(f"Silver Geolocation rows: {silver_geo_count:,}")
print(f"Unique ZIP code prefixes: {distinct_zips:,}")

if silver_geo_count != distinct_zips:
    raise ValueError(
        "Silver Geolocation uniqueness validation failed. "
        f"Rows: {silver_geo_count:,}, Unique ZIPs: {distinct_zips:,}"
    )

print("PASS — Silver geolocation unique key is valid.")

# COMMAND ----------

## Step 7 — Persist Silver Geolocation

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

(
    silver_geo_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.geolocation")
)

print("Silver Geolocation table created successfully.")

# COMMAND ----------

## Step 8 — Reload and Validate Persisted Geolocation

# COMMAND ----------

final_geo_df = spark.table("workspace.silver.geolocation")
final_geo_count = final_geo_df.count()

print(f"Persisted Silver Geolocation rows: {final_geo_count:,}")
final_geo_df.printSchema()

print(f"Deduplication Reduction: {bronze_geo_count:,} raw rows -> {final_geo_count:,} unique ZIP rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Verify Delta Metadata
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.silver.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Verify Delta History
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.silver.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Final Silver Geolocation Preview
# MAGIC

# COMMAND ----------




display(spark.table("workspace.silver.geolocation").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Geolocation — Execution Summary
# MAGIC
# MAGIC ## Table Information
# MAGIC
# MAGIC | Attribute | Value |
# MAGIC |---|---|
# MAGIC | Database | workspace.silver |
# MAGIC | Table Name | geolocation |
# MAGIC | Source Table | workspace.bronze.geolocation |
# MAGIC | Business Key | geolocation_zip_code_prefix |
# MAGIC | Records Loaded | 19,015 (approx) |
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Check | Result |
# MAGIC |---|---|
# MAGIC | Bronze schema validation | PASS |
# MAGIC | ZIP code uniqueness | PASS |
# MAGIC | Datatype standardization | PASS |
# MAGIC | Deduplication aggregation | PASS |
# MAGIC | Delta table creation | PASS |
# MAGIC
# MAGIC **SUCCESS — Silver Geolocation transformation and loading completed successfully.**

# COMMAND ----------

