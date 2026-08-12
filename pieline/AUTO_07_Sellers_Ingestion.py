# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC ## Production Bronze Ingestion — Sellers
# MAGIC
# MAGIC **Notebook:** `AUTO_07_Sellers_Ingestion`
# MAGIC
# MAGIC **Source:** `s3://olist-retail-project/raw/sellers/`
# MAGIC
# MAGIC **Checkpoint:** `s3://olist-retail-project/_checkpoints/sellers_ingestion/`
# MAGIC
# MAGIC **Target:** `workspace.bronze.sellers`
# MAGIC
# MAGIC Bronze preserves the raw seller representation. Silver is responsible
# MAGIC for seller city/state standardization and business-key quality enforcement.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# ================================================================
# CONFIGURATION
# ================================================================

SOURCE_PATH = "s3://olist-retail-project/raw/sellers/"

CHECKPOINT_PATH = (
    "s3://olist-retail-project/_checkpoints/"
    "sellers_ingestion/"
)

SCHEMA_LOCATION = (
    "s3://olist-retail-project/_schemas/"
    "sellers_ingestion/"
)

BRONZE_TABLE = "workspace.bronze.sellers"

EXPECTED_COLUMNS = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]

EXPECTED_TYPES = {
    "seller_id": "string",
    "seller_zip_code_prefix": "string",
    "seller_city": "string",
    "seller_state": "string",
}

print("Source     :", SOURCE_PATH)
print("Checkpoint :", CHECKPOINT_PATH)
print("Target     :", BRONZE_TABLE)

# COMMAND ----------

# ================================================================
# STEP 1 — VERIFY EXISTING BRONZE TARGET
# ================================================================

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Bronze target does not exist: {BRONZE_TABLE}. "
        "Load the existing Sellers baseline before enabling incremental ingestion."
    )

bronze_before_df = spark.table(BRONZE_TABLE)
before_count = bronze_before_df.count()

print(f"Current Bronze Sellers rows : {before_count:,}")
print(f"Bronze target               : {BRONZE_TABLE}")

# COMMAND ----------

# ================================================================
# STEP 2 — BRONZE SCHEMA CONTRACT
# ================================================================

actual_columns = bronze_before_df.columns

if actual_columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Bronze Sellers schema contract failed.\n"
        f"Expected columns: {EXPECTED_COLUMNS}\n"
        f"Actual columns  : {actual_columns}"
    )

actual_types = dict(bronze_before_df.dtypes)

type_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_types.get(column),
    }
    for column, expected_type in EXPECTED_TYPES.items()
    if actual_types.get(column) != expected_type
}

if type_errors:
    raise ValueError(
        f"Bronze Sellers datatype contract failed: {type_errors}"
    )

print("PASS — Existing Bronze Sellers schema matches the contract.")
bronze_before_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 3 — AUTO LOADER STREAM
# ================================================================
#
# Seller ZIP prefixes are identifiers, not measures, so Bronze keeps
# all four source columns as STRING.
#
# Existing files are intentionally excluded from the incremental stream.
# The existing baseline is already represented in workspace.bronze.sellers.
#
# Future files placed under raw/sellers/ are ingested incrementally.

# COMMAND ----------

sellers_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "false")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .load(SOURCE_PATH)
        .select(
            F.col("seller_id").cast("string"),
            F.col("seller_zip_code_prefix").cast("string"),
            F.col("seller_city").cast("string"),
            F.col("seller_state").cast("string"),
        )
)

if sellers_stream_df.columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Incoming Sellers schema does not match the Bronze contract.\n"
        f"Expected: {EXPECTED_COLUMNS}\n"
        f"Actual: {sellers_stream_df.columns}"
    )

print("PASS — Auto Loader stream configured for new Sellers files.")
print("PASS — Incoming stream schema matches the Bronze contract.")

# COMMAND ----------

# ================================================================
# STEP 4 — INCREMENTAL BRONZE APPEND
# ================================================================

query = (
    sellers_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

query.awaitTermination()

print("PASS — Sellers incremental ingestion completed successfully.")

# COMMAND ----------

# ================================================================
# STEP 5 — POPULATION VALIDATION
# ================================================================

bronze_after_df = spark.table(BRONZE_TABLE)
after_count = bronze_after_df.count()

print(f"Current Bronze Sellers rows : {after_count:,}")
print(f"Bronze target               : {BRONZE_TABLE}")

if after_count < before_count:
    raise ValueError(
        "Bronze Sellers population decreased during incremental ingestion."
    )

print("PASS — Bronze Sellers population is non-decreasing.")

# COMMAND ----------

# ================================================================
# STEP 6 — SCHEMA VALIDATION AFTER INGESTION
# ================================================================

after_columns = bronze_after_df.columns

if after_columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Bronze Sellers schema changed after ingestion.\n"
        f"Expected: {EXPECTED_COLUMNS}\n"
        f"Actual: {after_columns}"
    )

after_types = dict(bronze_after_df.dtypes)

after_type_errors = {
    column: {
        "expected": expected_type,
        "actual": after_types.get(column),
    }
    for column, expected_type in EXPECTED_TYPES.items()
    if after_types.get(column) != expected_type
}

if after_type_errors:
    raise ValueError(
        f"Bronze Sellers datatype contract failed after ingestion: "
        f"{after_type_errors}"
    )

print("PASS — Bronze Sellers schema remains unchanged.")
bronze_after_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 7 — SELLER ID VALIDATION
# ================================================================
#
# seller_id is the business identifier for Sellers.
# Bronze does not deduplicate records; however, because Silver also
# requires seller_id uniqueness, a duplicate seller identity is treated
# as a quality failure for this dataset.

null_seller_ids = bronze_after_df.filter(
    F.col("seller_id").isNull()
).count()

blank_seller_ids = bronze_after_df.filter(
    F.col("seller_id").isNotNull()
    & (F.trim(F.col("seller_id")) == "")
).count()

duplicate_seller_id_groups = (
    bronze_after_df
        .filter(F.col("seller_id").isNotNull())
        .groupBy("seller_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

print(f"NULL seller_id values       : {null_seller_ids}")
print(f"Blank seller_id values      : {blank_seller_ids}")
print(f"Duplicate seller_id groups  : {duplicate_seller_id_groups}")

if null_seller_ids != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: NULL seller_id values found."
    )

if blank_seller_ids != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: blank seller_id values found."
    )

if duplicate_seller_id_groups != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: duplicate seller_id values found."
    )

print("PASS — Bronze seller business key is valid.")

# COMMAND ----------

# ================================================================
# STEP 8 — REQUIRED FIELD PROFILING
# ================================================================
#
# Bronze preserves the raw values. These checks profile the source and
# do not standardize city/state or replace NULLs.

profile_df = bronze_after_df.select(
    F.sum(
        F.when(F.col("seller_zip_code_prefix").isNull(), 1).otherwise(0)
    ).alias("null_seller_zip_code_prefix"),
    F.sum(
        F.when(F.col("seller_city").isNull(), 1).otherwise(0)
    ).alias("null_seller_city"),
    F.sum(
        F.when(F.col("seller_state").isNull(), 1).otherwise(0)
    ).alias("null_seller_state"),
    F.sum(
        F.when(
            F.col("seller_zip_code_prefix").isNotNull()
            & (F.trim(F.col("seller_zip_code_prefix")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_seller_zip_code_prefix"),
    F.sum(
        F.when(
            F.col("seller_city").isNotNull()
            & (F.trim(F.col("seller_city")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_seller_city"),
    F.sum(
        F.when(
            F.col("seller_state").isNotNull()
            & (F.trim(F.col("seller_state")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_seller_state"),
)

display(profile_df)

print("PASS — Bronze Sellers raw-preservation profiling completed.")

# COMMAND ----------

# ================================================================
# STEP 9 — ZIP PREFIX PROFILE
# ================================================================

invalid_zip_format_count = bronze_after_df.filter(
    F.col("seller_zip_code_prefix").isNotNull()
    & ~F.trim(F.col("seller_zip_code_prefix")).rlike(r"^[0-9]{5}$")
).count()

print(
    f"Seller ZIP prefixes with invalid 5-digit format : "
    f"{invalid_zip_format_count}"
)

if invalid_zip_format_count != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: invalid seller ZIP prefix format found."
    )

print("PASS — Seller ZIP prefix format validation passed.")

# COMMAND ----------

# ================================================================
# STEP 10 — STATE PROFILE
# ================================================================

invalid_state_format_count = bronze_after_df.filter(
    F.col("seller_state").isNotNull()
    & ~F.upper(F.trim(F.col("seller_state"))).rlike(r"^[A-Z]{2}$")
).count()

print(
    f"Seller state values with invalid 2-character format : "
    f"{invalid_state_format_count}"
)

if invalid_state_format_count != 0:
    raise ValueError(
        "Bronze Sellers quality gate failed: invalid seller state format found."
    )

print("PASS — Seller state format validation passed.")

# COMMAND ----------

# ================================================================
# STEP 11 — TARGET AVAILABILITY
# ================================================================

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Bronze Sellers target is unavailable after ingestion: {BRONZE_TABLE}"
    )

print("PASS — Bronze Sellers table is available after ingestion.")

# COMMAND ----------

# ================================================================
# FINAL SUMMARY
# ================================================================

print("=" * 72)
print("SELLERS AUTOMATED INGESTION — SUCCESS")
print("=" * 72)
print(f"Source       : {SOURCE_PATH}")
print(f"Checkpoint   : {CHECKPOINT_PATH}")
print(f"Target       : {BRONZE_TABLE}")
print(f"Rows before  : {before_count:,}")
print(f"Rows after   : {after_count:,}")
print("Mode         : Incremental append")
print("File handling: Auto Loader")
print("Schema mode  : Strict contract validation")
print("Bronze role  : Raw source preservation")
print("=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downstream Dependency
# MAGIC
# MAGIC `INGEST_SELLERS`
# MAGIC
# MAGIC ↓
# MAGIC
# MAGIC `SILVER_SELLERS`
# MAGIC
# MAGIC The existing Silver Sellers notebook reads:
# MAGIC `workspace.bronze.sellers`
# MAGIC
# MAGIC and writes:
# MAGIC `workspace.silver.sellers`.
