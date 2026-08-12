# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC ## Production Bronze Ingestion — Order Payments
# MAGIC
# MAGIC **Notebook:** `AUTO_05_Order_Payments_Ingestion`
# MAGIC
# MAGIC This notebook provides incremental Auto Loader ingestion for the
# MAGIC existing Order Payments Bronze table.
# MAGIC
# MAGIC Source:
# MAGIC `s3://olist-retail-project/raw/order_payments/`
# MAGIC
# MAGIC Target:
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC The existing Bronze and Silver business logic is not modified.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# ================================================================
# CONFIGURATION
# ================================================================

SOURCE_PATH = "s3://olist-retail-project/raw/order_payments/"

CHECKPOINT_PATH = (
    "s3://olist-retail-project/_checkpoints/"
    "order_payments_ingestion/"
)

SCHEMA_LOCATION = (
    "s3://olist-retail-project/_schemas/"
    "order_payments_ingestion/"
)

BRONZE_TABLE = "workspace.bronze.order_payments"

EXPECTED_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

EXPECTED_TYPES = {
    "order_id": "string",
    "payment_sequential": "string",
    "payment_type": "string",
    "payment_installments": "string",
    "payment_value": "string",
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
        "Run the existing Order Payments baseline notebook first."
    )

bronze_before_df = spark.table(BRONZE_TABLE)
before_count = bronze_before_df.count()

print(f"Current Bronze Order Payments rows : {before_count:,}")
print(f"Bronze target                       : {BRONZE_TABLE}")

# COMMAND ----------

# ================================================================
# STEP 2 — BRONZE SCHEMA CONTRACT
# ================================================================

actual_columns = bronze_before_df.columns

if actual_columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Bronze Order Payments schema contract failed.\n"
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
        f"Bronze Order Payments datatype contract failed: {type_errors}"
    )

print("PASS — Existing Bronze schema matches the contract.")
bronze_before_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 3 — AUTO LOADER CONFIGURATION
# ================================================================
#
# The historical source data is already represented in the existing
# Bronze table. Therefore the first production stream start must not
# replay the existing historical file.
#
# Newly arriving files under raw/order_payments/ are processed
# incrementally.

# COMMAND ----------

order_payments_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "false")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .load(SOURCE_PATH)
        .select(
            F.col("order_id").cast("string"),
            F.col("payment_sequential").cast("string"),
            F.col("payment_type").cast("string"),
            F.col("payment_installments").cast("string"),
            F.col("payment_value").cast("string"),
        )
)

if order_payments_stream_df.columns != EXPECTED_COLUMNS:
    raise ValueError(
        "Incoming Order Payments schema does not match the Bronze contract.\n"
        f"Expected: {EXPECTED_COLUMNS}\n"
        f"Actual: {order_payments_stream_df.columns}"
    )

print("PASS — Auto Loader stream configured for new Order Payments files.")
print("PASS — Incoming stream schema matches the Bronze contract.")

# COMMAND ----------

# ================================================================
# STEP 4 — INCREMENTAL BRONZE APPEND
# ================================================================

query = (
    order_payments_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

query.awaitTermination()

print("PASS — Order Payments incremental ingestion completed successfully.")

# COMMAND ----------

# ================================================================
# STEP 5 — POST-INGESTION POPULATION VALIDATION
# ================================================================

bronze_after_df = spark.table(BRONZE_TABLE)
after_count = bronze_after_df.count()

print(f"Current Bronze Order Payments rows : {after_count:,}")
print(f"Bronze target                       : {BRONZE_TABLE}")

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
        f"Bronze datatype contract failed after ingestion: {after_type_errors}"
    )

print("PASS — Bronze schema remains unchanged.")
bronze_after_df.printSchema()

# COMMAND ----------

# ================================================================
# STEP 7 — REQUIRED IDENTIFIER VALIDATION
# ================================================================

null_order_ids = bronze_after_df.filter(
    F.col("order_id").isNull()
).count()

null_payment_sequences = bronze_after_df.filter(
    F.col("payment_sequential").isNull()
).count()

print(f"NULL order_id values             : {null_order_ids}")
print(f"NULL payment_sequential values   : {null_payment_sequences}")

if null_order_ids != 0:
    raise ValueError(
        "Bronze Order Payments validation failed: NULL order_id values found."
    )

if null_payment_sequences != 0:
    raise ValueError(
        "Bronze Order Payments validation failed: "
        "NULL payment_sequential values found."
    )

print("PASS — Required payment identifiers are populated.")

# COMMAND ----------

# ================================================================
# STEP 8 — RAW NULL / BLANK PROFILING
# ================================================================
#
# Bronze preserves source data. These checks profile the data but do
# not filter or modify Bronze records.

null_profile = bronze_after_df.select(
    F.sum(
        F.when(F.col("order_id").isNull(), 1).otherwise(0)
    ).alias("null_order_id"),
    F.sum(
        F.when(F.col("payment_sequential").isNull(), 1).otherwise(0)
    ).alias("null_payment_sequential"),
    F.sum(
        F.when(F.col("payment_type").isNull(), 1).otherwise(0)
    ).alias("null_payment_type"),
    F.sum(
        F.when(F.col("payment_installments").isNull(), 1).otherwise(0)
    ).alias("null_payment_installments"),
    F.sum(
        F.when(F.col("payment_value").isNull(), 1).otherwise(0)
    ).alias("null_payment_value"),
)

display(null_profile)

blank_profile = bronze_after_df.select(
    F.sum(
        F.when(
            F.col("order_id").isNotNull()
            & (F.trim(F.col("order_id")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_order_id"),
    F.sum(
        F.when(
            F.col("payment_sequential").isNotNull()
            & (F.trim(F.col("payment_sequential")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_payment_sequential"),
    F.sum(
        F.when(
            F.col("payment_type").isNotNull()
            & (F.trim(F.col("payment_type")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_payment_type"),
    F.sum(
        F.when(
            F.col("payment_installments").isNotNull()
            & (F.trim(F.col("payment_installments")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_payment_installments"),
    F.sum(
        F.when(
            F.col("payment_value").isNotNull()
            & (F.trim(F.col("payment_value")) == ""),
            1,
        ).otherwise(0)
    ).alias("blank_payment_value"),
)

display(blank_profile)

print("PASS — Bronze raw-preservation profiling completed.")

# COMMAND ----------

# ================================================================
# STEP 9 — PAYMENT GRAIN PROFILING
# ================================================================
#
# The source grain is:
# order_id + payment_sequential
#
# Bronze does NOT deduplicate this key. Duplicate groups are reported
# for visibility only.

duplicate_payment_keys = (
    bronze_after_df
        .groupBy("order_id", "payment_sequential")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_payment_key_count = duplicate_payment_keys.count()

print(
    "Duplicate (order_id, payment_sequential) groups : "
    f"{duplicate_payment_key_count:,}"
)

if duplicate_payment_key_count > 0:
    display(
        duplicate_payment_keys.orderBy(F.desc("count")).limit(20)
    )
    print(
        "INFO — Bronze preserves source records; no deduplication was applied."
    )
else:
    print("PASS — No duplicate payment keys detected.")

# COMMAND ----------

# ================================================================
# STEP 10 — TARGET AVAILABILITY
# ================================================================

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Bronze target is unavailable after ingestion: {BRONZE_TABLE}"
    )

print("PASS — Bronze Order Payments table is available after ingestion.")

# COMMAND ----------

# ================================================================
# FINAL SUMMARY
# ================================================================

print("=" * 72)
print("ORDER PAYMENTS AUTOMATED INGESTION — SUCCESS")
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
# MAGIC ## Workflow Dependency
# MAGIC
# MAGIC `INGEST_ORDER_PAYMENTS`
# MAGIC
# MAGIC ↓
# MAGIC
# MAGIC `SILVER_ORDER_PAYMENTS`
# MAGIC
# MAGIC The existing Silver notebook reads:
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC and writes:
# MAGIC `workspace.silver.order_payments`.
# MAGIC
# MAGIC The existing Silver transformation remains unchanged.
