# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC ## Production Bronze Ingestion — Order Reviews
# MAGIC
# MAGIC **Notebook:** `AUTO_06_Order_Reviews_Ingestion`
# MAGIC
# MAGIC Source:
# MAGIC `s3://olist-retail-project/raw/reviews/`
# MAGIC
# MAGIC Target:
# MAGIC `workspace.bronze.order_reviews`
# MAGIC
# MAGIC This notebook performs incremental Bronze ingestion with Auto Loader.
# MAGIC Bronze preserves the raw source structure. Silver remains responsible
# MAGIC for data-type standardization and business-quality rules.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# ================================================================
# CONFIGURATION
# ================================================================

SOURCE_PATH = "s3://olist-retail-project/raw/reviews/"

CHECKPOINT_PATH = (
    "s3://olist-retail-project/_checkpoints/"
    "order_reviews_ingestion/"
)

SCHEMA_LOCATION = (
    "s3://olist-retail-project/_schemas/"
    "order_reviews_ingestion/"
)

BRONZE_TABLE = "workspace.bronze.order_reviews"

EXPECTED_COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]

EXPECTED_TYPES = {
    "review_id": "string",
    "order_id": "string",
    "review_score": "string",
    "review_comment_title": "string",
    "review_comment_message": "string",
    "review_creation_date": "string",
    "review_answer_timestamp": "string",
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
        "Run the existing Reviews baseline notebook first."
    )

bronze_before_df = spark.table(BRONZE_TABLE)
before_count = bronze_before_df.count()

print(f"Current Bronze Order Reviews rows : {before_count:,}")
print(f"Bronze target                      : {BRONZE_TABLE}")

# COMMAND ----------

# ================================================================
# STEP 2 — BRONZE SCHEMA CONTRACT
# ================================================================

actual_columns = bronze_before_df.columns

if actual_columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Bronze Order Reviews schema contract failed.\n"
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
        f"Bronze Order Reviews datatype contract failed: {type_errors}"
    )

print("PASS — Existing Bronze schema matches the contract.")
bronze_before_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 3 — AUTO LOADER CONFIGURATION
# ================================================================
#
# Reviews contain free-text fields. The CSV reader therefore preserves
# quoted commas, escaped quotes, and multiline review content.
#
# includeExistingFiles=false prevents the initial production stream from
# replaying the historical file already represented in Bronze.
#
# Future files arriving under raw/reviews/ are processed incrementally.

# COMMAND ----------

reviews_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "false")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .load(SOURCE_PATH)
        .select(
            F.col("review_id").cast("string"),
            F.col("order_id").cast("string"),
            F.col("review_score").cast("string"),
            F.col("review_comment_title").cast("string"),
            F.col("review_comment_message").cast("string"),
            F.col("review_creation_date").cast("string"),
            F.col("review_answer_timestamp").cast("string"),
        )
)

if reviews_stream_df.columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Incoming Order Reviews schema does not match the Bronze contract.\n"
        f"Expected: {EXPECTED_COLUMNS}\n"
        f"Actual: {reviews_stream_df.columns}"
    )

print("PASS — Auto Loader stream configured for new Order Reviews files.")
print("PASS — Incoming stream schema matches the Bronze contract.")

# COMMAND ----------

# ================================================================
# STEP 4 — INCREMENTAL BRONZE APPEND
# ================================================================

query = (
    reviews_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

query.awaitTermination()

print("PASS — Order Reviews incremental ingestion completed successfully.")

# COMMAND ----------

# ================================================================
# STEP 5 — POST-INGESTION POPULATION VALIDATION
# ================================================================

bronze_after_df = spark.table(BRONZE_TABLE)
after_count = bronze_after_df.count()

print(f"Current Bronze Order Reviews rows : {after_count:,}")
print(f"Bronze target                      : {BRONZE_TABLE}")

if after_count < before_count:
    raise ValueError(
        "Bronze population decreased during incremental ingestion."
    )

print("PASS — Bronze population is non-decreasing.")

# COMMAND ----------

# ================================================================
# STEP 6 — POST-INGESTION SCHEMA VALIDATION
# ================================================================

after_columns = bronze_after_df.columns

if after_columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Bronze schema changed after ingestion.\n"
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
        f"Bronze datatype contract failed after ingestion: "
        f"{after_type_errors}"
    )

print("PASS — Bronze schema remains unchanged.")
bronze_after_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 7 — REQUIRED IDENTIFIER VALIDATION
# ================================================================

null_review_ids = bronze_after_df.filter(
    F.col("review_id").isNull()
).count()

null_order_ids = bronze_after_df.filter(
    F.col("order_id").isNull()
).count()

blank_review_ids = bronze_after_df.filter(
    F.col("review_id").isNotNull()
    & (F.trim(F.col("review_id")) == "")
).count()

blank_order_ids = bronze_after_df.filter(
    F.col("order_id").isNotNull()
    & (F.trim(F.col("order_id")) == "")
).count()

print(f"NULL review_id values   : {null_review_ids}")
print(f"NULL order_id values    : {null_order_ids}")
print(f"Blank review_id values  : {blank_review_ids}")
print(f"Blank order_id values   : {blank_order_ids}")

if null_review_ids != 0 or blank_review_ids != 0:
    raise ValueError(
        "Bronze Order Reviews validation failed: "
        "invalid review_id values found."
    )

if null_order_ids != 0 or blank_order_ids != 0:
    raise ValueError(
        "Bronze Order Reviews validation failed: "
        "invalid order_id values found."
    )

print("PASS — Bronze review identifiers are populated.")

# COMMAND ----------

# ================================================================
# STEP 8 — REVIEW KEY DUPLICATE PROFILING
# ================================================================
#
# Silver identifies review_id as the business key.
# Bronze remains a raw-preservation layer and does not silently
# deduplicate records.

duplicate_review_ids = (
    bronze_after_df
        .groupBy("review_id")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_review_id_count = duplicate_review_ids.count()

print(
    f"Duplicate review_id groups in Bronze : "
    f"{duplicate_review_id_count:,}"
)

if duplicate_review_id_count > 0:
    display(
        duplicate_review_ids
            .orderBy(F.desc("count"))
            .limit(20)
    )
    print(
        "INFO — Bronze preserves source records; "
        "no deduplication was applied."
    )
else:
    print("PASS — No duplicate review_id groups detected.")

# COMMAND ----------

# ================================================================
# STEP 9 — RAW REVIEW FIELD PROFILING
# ================================================================
#
# These checks profile the raw source. They do not cast, clean,
# filter, or reject Bronze records. Silver owns business validation.

raw_profile = bronze_after_df.select(
    F.sum(
        F.when(F.col("review_score").isNull(), 1).otherwise(0)
    ).alias("null_review_score"),
    F.sum(
        F.when(F.col("review_comment_title").isNull(), 1).otherwise(0)
    ).alias("null_review_comment_title"),
    F.sum(
        F.when(F.col("review_comment_message").isNull(), 1).otherwise(0)
    ).alias("null_review_comment_message"),
    F.sum(
        F.when(F.col("review_creation_date").isNull(), 1).otherwise(0)
    ).alias("null_review_creation_date"),
    F.sum(
        F.when(F.col("review_answer_timestamp").isNull(), 1).otherwise(0)
    ).alias("null_review_answer_timestamp"),
)

display(raw_profile)

print("PASS — Bronze raw review profiling completed.")

# COMMAND ----------

# ================================================================
# STEP 10 — TARGET AVAILABILITY
# ================================================================

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Bronze target is unavailable after ingestion: {BRONZE_TABLE}"
    )

print("PASS — Bronze Order Reviews table is available after ingestion.")

# COMMAND ----------

# ================================================================
# FINAL SUMMARY
# ================================================================

print("=" * 72)
print("ORDER REVIEWS AUTOMATED INGESTION — SUCCESS")
print("=" * 72)
print(f"Source       : {SOURCE_PATH}")
print(f"Checkpoint   : {CHECKPOINT_PATH}")
print(f"Target       : {BRONZE_TABLE}")
print(f"Rows before  : {before_count:,}")
print(f"Rows after   : {after_count:,}")
print("Mode         : Incremental append")
print("File handling: Auto Loader")
print("Schema mode  : Strict contract validation")
print("CSV handling : Quotes, escapes, multiline review text")
print("Bronze role  : Raw source preservation")
print("=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Dependency
# MAGIC
# MAGIC `INGEST_ORDER_REVIEWS`
# MAGIC
# MAGIC ↓
# MAGIC
# MAGIC `SILVER_ORDER_REVIEWS`
# MAGIC
# MAGIC The existing Silver notebook reads:
# MAGIC `workspace.bronze.order_reviews`
# MAGIC
# MAGIC and writes:
# MAGIC `workspace.silver.order_reviews`.
# MAGIC
# MAGIC The existing Silver transformation and quality rules remain unchanged.
