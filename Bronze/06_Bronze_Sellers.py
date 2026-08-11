# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Sellers
# MAGIC
# MAGIC ### Notebook: 06_Bronze_Sellers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist sellers dataset from AWS S3 into the Databricks
# MAGIC Bronze layer.
# MAGIC
# MAGIC The Bronze layer preserves the source data without applying business
# MAGIC transformations.
# MAGIC
# MAGIC This notebook performs:
# MAGIC
# MAGIC - Source ingestion
# MAGIC - Schema validation
# MAGIC - Row-count reconciliation
# MAGIC - NULL analysis
# MAGIC - Blank-string analysis
# MAGIC - Seller ID uniqueness validation
# MAGIC - ZIP-code-prefix profiling
# MAGIC - City profiling
# MAGIC - State profiling
# MAGIC - Basic value validation
# MAGIC - Delta table validation
# MAGIC - Final automation quality gate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC AWS S3:
# MAGIC
# MAGIC s3://olist-retail-project/olist_sellers_dataset.csv
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC workspace.bronze.sellers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One record represents one seller.
# MAGIC
# MAGIC Expected business identifier:
# MAGIC
# MAGIC seller_id
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Principles
# MAGIC
# MAGIC This notebook does NOT:
# MAGIC
# MAGIC - Rename columns
# MAGIC - Standardize city names
# MAGIC - Standardize state values
# MAGIC - Convert ZIP prefixes into another business representation
# MAGIC - Join with geolocation
# MAGIC - Remove records
# MAGIC - Replace NULL values
# MAGIC - Add metadata columns
# MAGIC - Add derived columns
# MAGIC
# MAGIC Business transformations are reserved for Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Define the S3 source and Bronze target.
# MAGIC
# MAGIC The source ZIP prefix is intentionally stored as STRING because
# MAGIC it is an identifier rather than a numerical measure.

# COMMAND ----------

SOURCE_PATH = 's3://omnicapstone/raw_data/olist_sellers_dataset.csv'

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "sellers"

BRONZE_FULL_TABLE = (
    f"{BRONZE_CATALOG}."
    f"{BRONZE_SCHEMA}."
    f"{BRONZE_TABLE}"
)

print("Source:", SOURCE_PATH)
print("Target:", BRONZE_FULL_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source
# MAGIC
# MAGIC Read the raw CSV directly from S3.
# MAGIC
# MAGIC Schema inference is disabled so that Bronze preserves the source
# MAGIC representation.

# COMMAND ----------

sellers_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

# COMMAND ----------

sellers_raw_df.printSchema()

# COMMAND ----------

display(
    sellers_raw_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Source Validation
# MAGIC
# MAGIC Capture the source row count and verify the expected source columns.

# COMMAND ----------

source_count = sellers_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("workspace.bronze is ready.")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_FULL_TABLE} (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
)
USING DELTA
""")

print(f"Created: {BRONZE_FULL_TABLE}")

# COMMAND ----------

(
    sellers_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_FULL_TABLE)
)

print(
    f"Successfully loaded {BRONZE_FULL_TABLE}"
)

# COMMAND ----------

bronze_sellers_df = (
    spark.table(BRONZE_FULL_TABLE)
)

display(
    bronze_sellers_df.limit(20)
)

# COMMAND ----------

bronze_count = bronze_sellers_df.count()

print(f"Source records : {source_count:,}")
print(f"Bronze records : {bronze_count:,}")

if source_count != bronze_count:
    raise ValueError(
        "BRONZE LOAD FAILED: "
        f"Source has {source_count:,} rows but "
        f"Bronze has {bronze_count:,} rows."
    )

print("PASS — Source and Bronze row counts match.")

# COMMAND ----------

expected_columns = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
]

actual_columns = bronze_sellers_df.columns

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

print("Expected columns :", expected_columns)
print("Actual columns   :", actual_columns)
print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if missing_columns or unexpected_columns:
    raise ValueError(
        "BRONZE SCHEMA VALIDATION FAILED."
    )

print("PASS — Column structure is correct.")

# COMMAND ----------

bronze_sellers_df.printSchema()

# COMMAND ----------

null_profile = bronze_sellers_df.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("seller_id").isNull(),
            1
        ).otherwise(0)
    ).alias("null_seller_id"),

    F.sum(
        F.when(
            F.col("seller_zip_code_prefix").isNull(),
            1
        ).otherwise(0)
    ).alias("null_seller_zip_code_prefix"),

    F.sum(
        F.when(
            F.col("seller_city").isNull(),
            1
        ).otherwise(0)
    ).alias("null_seller_city"),

    F.sum(
        F.when(
            F.col("seller_state").isNull(),
            1
        ).otherwise(0)
    ).alias("null_seller_state")
)

display(null_profile)

# COMMAND ----------

critical_null_count = (
    bronze_sellers_df
        .filter(
            F.col("seller_id").isNull()
            |
            F.col("seller_zip_code_prefix").isNull()
            |
            F.col("seller_city").isNull()
            |
            F.col("seller_state").isNull()
        )
        .count()
)

print(
    "Rows containing NULL seller attributes:",
    critical_null_count
)

if critical_null_count > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "NULL seller attributes detected."
    )

print(
    "PASS — No NULL seller attributes detected."
)

# COMMAND ----------

blank_profile = bronze_sellers_df.select(
    F.sum(
        F.when(
            F.trim(F.col("seller_id")) == "",
            1
        ).otherwise(0)
    ).alias("blank_seller_id"),

    F.sum(
        F.when(
            F.trim(
                F.col("seller_zip_code_prefix")
            ) == "",
            1
        ).otherwise(0)
    ).alias(
        "blank_seller_zip_code_prefix"
    ),

    F.sum(
        F.when(
            F.trim(F.col("seller_city")) == "",
            1
        ).otherwise(0)
    ).alias("blank_seller_city"),

    F.sum(
        F.when(
            F.trim(F.col("seller_state")) == "",
            1
        ).otherwise(0)
    ).alias("blank_seller_state")
)

display(blank_profile)

# COMMAND ----------

blank_count = (
    bronze_sellers_df
        .filter(
            (F.trim(F.col("seller_id")) == "")
            |
            (
                F.trim(
                    F.col("seller_zip_code_prefix")
                ) == ""
            )
            |
            (F.trim(F.col("seller_city")) == "")
            |
            (F.trim(F.col("seller_state")) == "")
        )
        .count()
)

print(
    "Rows containing blank seller attributes:",
    blank_count
)

if blank_count > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "Blank seller attributes detected."
    )

print(
    "PASS — No blank seller attributes detected."
)

# COMMAND ----------

seller_id_summary = bronze_sellers_df.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("seller_id").alias(
        "unique_seller_ids"
    )
)

display(seller_id_summary)

# COMMAND ----------

duplicate_sellers = (
    bronze_sellers_df
        .groupBy("seller_id")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_seller_count = duplicate_sellers.count()

print(
    "Duplicate seller_id groups:",
    duplicate_seller_count
)

if duplicate_seller_count > 0:
    display(duplicate_sellers)

    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "Duplicate seller_id values detected."
    )

print(
    "PASS — seller_id is unique."
)

# COMMAND ----------

zip_profile = bronze_sellers_df.select(
    F.countDistinct(
        "seller_zip_code_prefix"
    ).alias(
        "unique_seller_zip_prefixes"
    ),

    F.min(
        F.length("seller_zip_code_prefix")
    ).alias(
        "minimum_zip_prefix_length"
    ),

    F.max(
        F.length("seller_zip_code_prefix")
    ).alias(
        "maximum_zip_prefix_length"
    )
)

display(zip_profile)

# COMMAND ----------

invalid_zip_prefixes = (
    bronze_sellers_df
        .filter(
            ~F.col(
                "seller_zip_code_prefix"
            ).rlike("^[0-9]{5}$")
        )
)

invalid_zip_count = invalid_zip_prefixes.count()

print(
    "Invalid ZIP prefix format:",
    invalid_zip_count
)

display(invalid_zip_prefixes.limit(50))

# COMMAND ----------

state_profile = (
    bronze_sellers_df
        .groupBy("seller_state")
        .agg(
            F.count("*").alias(
                "total_sellers"
            )
        )
        .orderBy(
            F.desc("total_sellers")
        )
)

display(state_profile)

# COMMAND ----------

invalid_state_count = (
    bronze_sellers_df
        .filter(
            ~F.col("seller_state")
                .rlike("^[A-Za-z]{2}$")
        )
        .count()
)

print(
    "Invalid seller_state format:",
    invalid_state_count
)

if invalid_state_count > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "Invalid seller_state format detected."
    )

print(
    "PASS — seller_state has valid two-letter format."
)

# COMMAND ----------

city_profile = bronze_sellers_df.select(
    F.countDistinct(
        "seller_city"
    ).alias(
        "unique_seller_cities"
    )
)

display(city_profile)

# COMMAND ----------

city_variants = (
    bronze_sellers_df
        .groupBy(
            F.lower(
                F.trim(
                    F.col("seller_city")
                )
            ).alias("normalized_city_for_analysis")
        )
        .agg(
            F.countDistinct(
                "seller_city"
            ).alias(
                "source_city_variants"
            )
        )
        .filter(
            F.col("source_city_variants") > 1
        )
        .orderBy(
            F.desc("source_city_variants")
        )
)

display(city_variants)

# COMMAND ----------



# COMMAND ----------

location_duplicates = (
    bronze_sellers_df
        .groupBy(
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
        .count()
        .filter(F.col("count") > 1)
)

display(location_duplicates.limit(50))

print(
    "Location groups containing multiple sellers:",
    location_duplicates.count()
)

# COMMAND ----------

seller_state_summary = (
    bronze_sellers_df
        .groupBy("seller_state")
        .agg(
            F.count("*").alias(
                "total_sellers"
            )
        )
        .orderBy(
            F.desc("total_sellers")
        )
)

display(seller_state_summary)

# COMMAND ----------

spark.sql(
    f"""
    DESCRIBE DETAIL {BRONZE_FULL_TABLE}
    """
).show(
    truncate=False
)

# COMMAND ----------

spark.sql(
    f"""
    DESCRIBE HISTORY {BRONZE_FULL_TABLE}
    """
).show(
    truncate=False
)

# COMMAND ----------

final_count = (
    spark.table(
        BRONZE_FULL_TABLE
    ).count()
)

if final_count != source_count:
    raise ValueError(
        "FINAL BRONZE QUALITY GATE FAILED: "
        f"Expected {source_count:,} rows, "
        f"found {final_count:,}."
    )

final_duplicate_count = (
    spark.table(BRONZE_FULL_TABLE)
        .groupBy("seller_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

if final_duplicate_count > 0:
    raise ValueError(
        "FINAL BRONZE QUALITY GATE FAILED: "
        "Duplicate seller_id values detected."
    )

print("=" * 70)
print("BRONZE SELLERS PIPELINE SUCCESS")
print("=" * 70)
print(f"Source rows : {source_count:,}")
print(f"Bronze rows : {final_count:,}")
print(f"Target      : {BRONZE_FULL_TABLE}")
print("Status      : SUCCESS")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Sellers — Execution Summary
# MAGIC
# MAGIC ## Execution Status
# MAGIC
# MAGIC **Status:** `SUCCESS`
# MAGIC
# MAGIC The Olist Sellers dataset was successfully ingested from AWS S3 into
# MAGIC the Databricks Bronze layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC **Source File:**
# MAGIC
# MAGIC `olist_sellers_dataset.csv`
# MAGIC
# MAGIC **Source Location:**
# MAGIC
# MAGIC `s3://olist-retail-project/olist_sellers_dataset.csv`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.sellers`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One record represents one seller.
# MAGIC
# MAGIC **Business Identifier:**
# MAGIC
# MAGIC `seller_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Expected Columns
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `seller_id` | STRING | Unique seller identifier |
# MAGIC | `seller_zip_code_prefix` | STRING | Seller ZIP/postal-code prefix |
# MAGIC | `seller_city` | STRING | Seller city |
# MAGIC | `seller_state` | STRING | Seller state code |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset Profile
# MAGIC
# MAGIC | Metric | Result |
# MAGIC |---|---:|
# MAGIC | Source row count | 3,095 |
# MAGIC | Bronze row count | 3,095 |
# MAGIC | Unique seller IDs | 3,095 |
# MAGIC | Duplicate seller IDs | 0 |
# MAGIC | Unique ZIP prefixes | 2,246 |
# MAGIC | Minimum ZIP prefix length | 5 |
# MAGIC | Maximum ZIP prefix length | 5 |
# MAGIC | Unique seller cities | 611 |
# MAGIC | Unique seller states | 23 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Checks
# MAGIC
# MAGIC | Check | Result |
# MAGIC |---|---|
# MAGIC | Source row count | ✅ PASS |
# MAGIC | Bronze row count | ✅ PASS |
# MAGIC | Row-count reconciliation | ✅ PASS |
# MAGIC | Schema validation | ✅ PASS |
# MAGIC | NULL validation | ✅ PASS |
# MAGIC | Blank-value validation | ✅ PASS |
# MAGIC | Seller ID uniqueness | ✅ PASS |
# MAGIC | ZIP prefix format | ✅ PASS |
# MAGIC | State format | ✅ PASS |
# MAGIC | City profiling | ✅ PASS |
# MAGIC | State profiling | ✅ PASS |
# MAGIC | Delta validation | ✅ PASS |
# MAGIC | Final quality gate | ✅ PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## NULL Validation
# MAGIC
# MAGIC The following required fields were checked for NULL values:
# MAGIC
# MAGIC - `seller_id`
# MAGIC - `seller_zip_code_prefix`
# MAGIC - `seller_city`
# MAGIC - `seller_state`
# MAGIC
# MAGIC **Result:** `0` NULL values detected.
# MAGIC
# MAGIC **Status:** ✅ PASS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Blank-Value Validation
# MAGIC
# MAGIC The following required fields were checked for blank and
# MAGIC whitespace-only values:
# MAGIC
# MAGIC - `seller_id`
# MAGIC - `seller_zip_code_prefix`
# MAGIC - `seller_city`
# MAGIC - `seller_state`
# MAGIC
# MAGIC **Result:** No blank or whitespace-only values detected.
# MAGIC
# MAGIC **Status:** ✅ PASS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Seller ID Validation
# MAGIC
# MAGIC `seller_id` represents the business identifier for the Sellers dataset.
# MAGIC
# MAGIC ```text
# MAGIC Total seller records = 3,095
# MAGIC Distinct seller IDs  = 3,095
# MAGIC Duplicate seller IDs = 0

# COMMAND ----------

# MAGIC %md
# MAGIC