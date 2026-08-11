# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Sellers
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create the Silver Sellers dimension from
# MAGIC `workspace.bronze.sellers`.
# MAGIC
# MAGIC The Silver layer standardizes seller attributes while preserving
# MAGIC the Bronze seller records.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.sellers`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.sellers`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Grain
# MAGIC
# MAGIC One row represents one seller.
# MAGIC
# MAGIC Business key:
# MAGIC
# MAGIC `seller_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Transformations
# MAGIC
# MAGIC 1. Preserve every Bronze seller.
# MAGIC 2. Standardize `seller_city`.
# MAGIC 3. Standardize `seller_state`.
# MAGIC 4. Preserve the seller ZIP prefix.
# MAGIC 5. Add `silver_load_timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Rules
# MAGIC
# MAGIC - `seller_id` must not be NULL.
# MAGIC - `seller_id` must be unique.
# MAGIC - Seller ZIP prefix must be represented consistently.
# MAGIC - Seller state must be standardized.
# MAGIC - Seller city must be standardized.
# MAGIC - Bronze and Silver row counts must match.
# MAGIC - Silver load timestamp must be populated.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important
# MAGIC
# MAGIC No seller records are intentionally filtered out.
# MAGIC
# MAGIC Silver performs standardization and quality enforcement.
# MAGIC Bronze remains the raw source layer.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Import Required Libraries
# MAGIC
# MAGIC Import PySpark functions required for the Silver transformation
# MAGIC and validation steps.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 — Read Bronze Sellers
# MAGIC
# MAGIC Read the existing Bronze Sellers Delta table.
# MAGIC
# MAGIC No direct read from S3 is required in Silver because Bronze is the
# MAGIC established source for the Silver layer.

# COMMAND ----------

bronze_sellers_df = spark.table(
    "workspace.bronze.sellers"
)

print(
    f"Bronze Sellers rows: {bronze_sellers_df.count():,}"
)

bronze_sellers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Validate Bronze Seller Structure
# MAGIC
# MAGIC Verify that the Bronze table contains the columns required to build
# MAGIC the Silver Sellers dimension.

# COMMAND ----------

required_seller_columns = {
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
}

actual_seller_columns = set(
    bronze_sellers_df.columns
)

missing_seller_columns = (
    required_seller_columns
    - actual_seller_columns
)

if missing_seller_columns:
    raise ValueError(
        "Bronze Sellers schema validation failed. "
        f"Missing columns: {sorted(missing_seller_columns)}"
    )

print(
    "PASS — Bronze Sellers contains all required columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Seller Business Key
# MAGIC
# MAGIC `seller_id` is the business identifier for the Sellers dimension.
# MAGIC
# MAGIC Validate NULL and duplicate seller IDs before creating Silver.

# COMMAND ----------

bronze_null_seller_ids = (
    bronze_sellers_df
        .filter(
            F.col("seller_id").isNull()
        )
        .count()
)

bronze_duplicate_seller_ids = (
    bronze_sellers_df
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

print(
    f"NULL seller IDs       : {bronze_null_seller_ids}"
)

print(
    f"Duplicate seller IDs  : {bronze_duplicate_seller_ids}"
)

if bronze_null_seller_ids != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: "
        "NULL seller_id values found."
    )

if bronze_duplicate_seller_ids != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: "
        "Duplicate seller_id values found."
    )

print(
    "PASS — Bronze seller business key is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Create Silver Sellers DataFrame
# MAGIC
# MAGIC Apply only the transformations required for the Silver layer.
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC - Trim seller city.
# MAGIC - Convert seller city to title case.
# MAGIC - Trim seller state.
# MAGIC - Convert seller state to uppercase.
# MAGIC - Preserve seller ZIP prefix.
# MAGIC - Add Silver load timestamp.
# MAGIC
# MAGIC No seller records are filtered.

# COMMAND ----------

silver_sellers_df = (
    bronze_sellers_df
        .select(
            F.col("seller_id"),

            F.col("seller_zip_code_prefix"),

            F.initcap(
                F.trim(
                    F.col("seller_city")
                )
            ).alias("seller_city"),

            F.upper(
                F.trim(
                    F.col("seller_state")
                )
            ).alias("seller_state"),

            F.current_timestamp().alias(
                "silver_load_timestamp"
            )
        )
)

silver_sellers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Preview Silver Sellers
# MAGIC
# MAGIC Review the transformed seller records before writing the Silver table.

# COMMAND ----------

display(
    silver_sellers_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Create Silver Schema
# MAGIC
# MAGIC Ensure that the `workspace.silver` schema exists before creating the
# MAGIC Silver Sellers table.

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

print(
    "Silver schema is ready."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 8 — Write Silver Sellers
# MAGIC
# MAGIC Create or replace the managed Delta table in
# MAGIC `workspace.silver.sellers`.
# MAGIC
# MAGIC The table contains the standardized seller dimension.

# COMMAND ----------

# ============================================================================
# STEP 8 : Create Silver Sellers Table
# ============================================================================
#
# The existing Silver Sellers table may contain an older schema.
# Since this notebook is responsible for rebuilding the Silver dimension,
# replace the existing table with the current validated Silver DataFrame.
# ============================================================================

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.sellers
""")

(
    silver_sellers_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "workspace.silver.sellers"
        )
)

print(
    "Silver Sellers table created successfully."
)

# COMMAND ----------

silver_sellers_df = spark.table(
    "workspace.silver.sellers"
)

print(
    f"Silver Sellers rows: {silver_sellers_df.count():,}"
)

silver_sellers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 10 — Validate Bronze-to-Silver Row Count
# MAGIC
# MAGIC Every Bronze seller must be retained in Silver.
# MAGIC
# MAGIC No filtering is performed in the Silver Sellers transformation.

# COMMAND ----------

bronze_seller_count = (
    bronze_sellers_df.count()
)

silver_seller_count = (
    silver_sellers_df.count()
)

print(
    f"Bronze Sellers rows : {bronze_seller_count:,}"
)

print(
    f"Silver Sellers rows : {silver_seller_count:,}"
)

if bronze_seller_count != silver_seller_count:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "Bronze and Silver row counts do not match."
    )

print(
    "PASS — Bronze and Silver seller counts match."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 11 — Validate Silver Seller IDs
# MAGIC
# MAGIC Validate the business key again after the Silver transformation.
# MAGIC
# MAGIC `seller_id` must remain non-NULL and unique.

# COMMAND ----------

silver_null_seller_ids = (
    silver_sellers_df
        .filter(
            F.col("seller_id").isNull()
        )
        .count()
)

silver_duplicate_seller_ids = (
    silver_sellers_df
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

print(
    f"NULL seller IDs      : {silver_null_seller_ids}"
)

print(
    f"Duplicate seller IDs : {silver_duplicate_seller_ids}"
)

if silver_null_seller_ids != 0:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "NULL seller_id values found."
    )

if silver_duplicate_seller_ids != 0:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "Duplicate seller_id values found."
    )

print(
    "PASS — Silver seller identifier validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 12 — Validate Seller Geography
# MAGIC
# MAGIC Validate the geographic attributes in the persisted Silver Sellers
# MAGIC table.
# MAGIC
# MAGIC NULL values are reported and are not automatically removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN seller_zip_code_prefix IS NULL
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS null_zip_code,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN seller_city IS NULL
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS null_city,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN seller_state IS NULL
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS null_state
# MAGIC
# MAGIC FROM workspace.silver.sellers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 13 — Validate Seller State Format
# MAGIC
# MAGIC Seller states should be represented as uppercase two-character
# MAGIC Brazilian state codes.

# COMMAND ----------

invalid_state_count = (
    silver_sellers_df
        .filter(
            F.col("seller_state").isNotNull()
            & (
                ~F.col("seller_state").rlike(
                    "^[A-Z]{2}$"
                )
            )
        )
        .count()
)

print(
    f"Invalid seller state values: {invalid_state_count}"
)

if invalid_state_count != 0:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "Invalid seller_state values found."
    )

print(
    "PASS — Seller state format is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 14 — Validate ZIP Prefix Format
# MAGIC
# MAGIC Seller ZIP prefixes are expected to contain five numeric digits.
# MAGIC
# MAGIC The validation is performed against the persisted Silver value.

# COMMAND ----------

invalid_zip_count = (
    silver_sellers_df
        .filter(
            F.col("seller_zip_code_prefix").isNotNull()
            & (
                ~F.col(
                    "seller_zip_code_prefix"
                ).rlike(
                    "^[0-9]{5}$"
                )
            )
        )
        .count()
)

print(
    f"Invalid ZIP prefix values: {invalid_zip_count}"
)

if invalid_zip_count != 0:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "Invalid seller_zip_code_prefix values found."
    )

print(
    "PASS — Seller ZIP prefix format is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 15 — Validate Silver Load Timestamp
# MAGIC
# MAGIC Every Silver seller record must contain a Silver load timestamp.

# COMMAND ----------

null_silver_timestamps = silver_sellers_df.filter(
    F.col("silver_load_timestamp").isNull()
).count()

print(
    f"NULL Silver load timestamps: {null_silver_timestamps}"
)

if null_silver_timestamps != 0:
    raise ValueError(
        "Silver Sellers quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Silver load timestamp is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 16 — Validate Silver Datatypes
# MAGIC
# MAGIC Verify that the persisted Silver Sellers table contains the expected
# MAGIC datatypes.

# COMMAND ----------

expected_seller_types = {
    "seller_id": "string",
    "seller_zip_code_prefix": "string",
    "seller_city": "string",
    "seller_state": "string",
    "silver_load_timestamp": "timestamp"
}

actual_seller_types = dict(
    silver_sellers_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_seller_types.get(column)
    }
    for column, expected_type in expected_seller_types.items()
    if actual_seller_types.get(column) != expected_type
}

if datatype_errors:
    raise ValueError(
        "Silver Sellers datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Silver Sellers datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 17 — Final Silver Sellers Quality Gate
# MAGIC
# MAGIC All critical Silver Sellers validations are checked against the
# MAGIC persisted Silver table.
# MAGIC
# MAGIC The notebook fails if any critical validation fails.

# COMMAND ----------

final_silver_sellers_df = spark.table(
    "workspace.silver.sellers"
)

final_silver_seller_count = final_silver_sellers_df.count()

final_null_seller_ids = (
    final_silver_sellers_df
        .filter(
            F.col("seller_id").isNull()
        )
        .count()
)

final_duplicate_seller_ids = (
    final_silver_sellers_df
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

final_invalid_states = final_silver_sellers_df.filter(
    F.col("seller_state").isNotNull()
    & (
        ~F.col("seller_state").rlike("^[A-Z]{2}$")
    )
).count()

final_invalid_zip_prefixes = final_silver_sellers_df.filter(
    F.col("seller_zip_code_prefix").isNotNull()
    & (
        ~F.col("seller_zip_code_prefix").rlike("^[0-9]{5}$")
    )
).count()

final_null_timestamps = final_silver_sellers_df.filter(
    F.col("silver_load_timestamp").isNull()
).count()

print("=" * 70)
print("SILVER SELLERS QUALITY GATE")
print("=" * 70)

print(
    f"Bronze rows             : "
    f"{bronze_seller_count:,}"
)

print(
    f"Silver rows             : "
    f"{final_silver_seller_count:,}"
)

print(
    f"NULL seller IDs         : "
    f"{final_null_seller_ids}"
)

print(
    f"Duplicate seller IDs    : "
    f"{final_duplicate_seller_ids}"
)

print(
    f"Invalid state values    : "
    f"{final_invalid_states}"
)

print(
    f"Invalid ZIP prefixes    : "
    f"{final_invalid_zip_prefixes}"
)

print(
    f"NULL load timestamps    : "
    f"{final_null_timestamps}"
)

print("=" * 70)

if (
    final_silver_seller_count == bronze_seller_count
    and final_null_seller_ids == 0
    and final_duplicate_seller_ids == 0
    and final_invalid_states == 0
    and final_invalid_zip_prefixes == 0
    and final_null_timestamps == 0
):
    print(
        "STATUS: ALL SILVER SELLERS CHECKS PASSED"
    )
else:
    raise ValueError(
        "SILVER SELLERS QUALITY GATE FAILED."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 18 — Seller State Distribution
# MAGIC
# MAGIC Review the standardized seller state distribution for downstream
# MAGIC seller and geographic analytics.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     seller_state,
# MAGIC     COUNT(*) AS total_sellers
# MAGIC FROM workspace.silver.sellers
# MAGIC GROUP BY seller_state
# MAGIC ORDER BY total_sellers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 19 — Seller City Profile
# MAGIC
# MAGIC Review the standardized seller cities.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     seller_city,
# MAGIC     COUNT(*) AS total_sellers
# MAGIC FROM workspace.silver.sellers
# MAGIC GROUP BY seller_city
# MAGIC ORDER BY total_sellers DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 20 — Verify Silver Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.silver.sellers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 21 — Verify Silver Delta History

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.silver.sellers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 22 — Verify Silver Table Registration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Silver Sellers Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.silver.sellers"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver Sellers — Execution Summary
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.sellers`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.sellers`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One row represents one seller.
# MAGIC
# MAGIC Business key:
# MAGIC
# MAGIC `seller_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Transformations
# MAGIC
# MAGIC - Preserved all Bronze seller records.
# MAGIC - Trimmed seller city values.
# MAGIC - Standardized seller city using title case.
# MAGIC - Standardized seller state using uppercase.
# MAGIC - Preserved seller ZIP prefix.
# MAGIC - Added `silver_load_timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |---|---|
# MAGIC | Bronze → Silver row count | PASS |
# MAGIC | Seller ID NULL validation | PASS |
# MAGIC | Seller ID uniqueness | PASS |
# MAGIC | Seller state validation | PASS |
# MAGIC | Seller ZIP prefix validation | PASS |
# MAGIC | Silver timestamp validation | PASS |
# MAGIC | Silver datatype validation | PASS |
# MAGIC | Final Silver quality gate | PASS |
# MAGIC | Delta metadata validation | PASS |
# MAGIC | Delta history validation | PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Status
# MAGIC
# MAGIC **SUCCESS — Silver Sellers transformation and quality validation
# MAGIC completed successfully.**

# COMMAND ----------

