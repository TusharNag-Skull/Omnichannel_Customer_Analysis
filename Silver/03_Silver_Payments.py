# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Order Payments
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create the Silver Order Payments table from the Bronze payment data.
# MAGIC
# MAGIC The Silver layer standardizes payment attributes, preserves valid
# MAGIC payment records, handles the source timestamp/data types where required,
# MAGIC and applies data-quality validation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.order_payments`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Grain
# MAGIC
# MAGIC One row represents one payment record for an order.
# MAGIC
# MAGIC Business relationship:
# MAGIC
# MAGIC `order_id`
# MAGIC
# MAGIC Payment sequence:
# MAGIC
# MAGIC `payment_sequential`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Responsibilities
# MAGIC
# MAGIC - Preserve valid Bronze payment records.
# MAGIC - Standardize payment type values.
# MAGIC - Convert numeric payment fields to appropriate numeric types.
# MAGIC - Preserve payment values without changing their business meaning.
# MAGIC - Validate payment-related domains.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quality Requirements
# MAGIC
# MAGIC - Required identifiers must not be NULL.
# MAGIC - Payment sequence must be valid.
# MAGIC - Payment installments must be valid.
# MAGIC - Payment value must not be negative.
# MAGIC - Payment type must be valid.
# MAGIC - Bronze and Silver row counts must reconcile.
# MAGIC - Silver load timestamp must be populated.
# MAGIC - Final quality gate must pass.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important
# MAGIC
# MAGIC No Gold-layer business metrics or customer analytics are created here.
# MAGIC
# MAGIC Customer-level payment metrics will be created downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Import Required Libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 — Read Bronze Order Payments
# MAGIC
# MAGIC Read the existing Bronze payment table.
# MAGIC
# MAGIC Silver uses Bronze as its source rather than reading directly from S3.

# COMMAND ----------

bronze_payments_df = spark.table(
    "workspace.bronze.order_payments"
)

bronze_payment_count = bronze_payments_df.count()

print(
    f"Bronze Order Payments rows: "
    f"{bronze_payment_count:,}"
)

bronze_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Validate Bronze Payment Structure

# COMMAND ----------

required_payment_columns = {
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
}

actual_payment_columns = set(
    bronze_payments_df.columns
)

missing_payment_columns = (
    required_payment_columns
    - actual_payment_columns
)

if missing_payment_columns:
    raise ValueError(
        "Bronze Order Payments schema validation failed. "
        f"Missing columns: {sorted(missing_payment_columns)}"
    )

print(
    "PASS — Bronze Order Payments contains all required columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Required Payment Identifiers
# MAGIC
# MAGIC `order_id` identifies the order associated with the payment.
# MAGIC
# MAGIC `payment_sequential` identifies the payment sequence for that order.

# COMMAND ----------

null_order_ids = bronze_payments_df.filter(
    F.col("order_id").isNull()
).count()

null_payment_sequences = bronze_payments_df.filter(
    F.col("payment_sequential").isNull()
).count()

print(
    f"NULL order IDs              : {null_order_ids}"
)

print(
    f"NULL payment sequences      : {null_payment_sequences}"
)

if null_order_ids != 0:
    raise ValueError(
        "Bronze Order Payments quality gate failed: "
        "NULL order_id values found."
    )

if null_payment_sequences != 0:
    raise ValueError(
        "Bronze Order Payments quality gate failed: "
        "NULL payment_sequential values found."
    )

print(
    "PASS — Required payment identifiers are populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Create Silver Order Payments
# MAGIC
# MAGIC Apply only Silver-layer transformations.
# MAGIC
# MAGIC Payment numeric fields are explicitly cast to stable numeric types,
# MAGIC and payment type is standardized.
# MAGIC
# MAGIC No payment records are filtered at this stage.

# COMMAND ----------

silver_payments_df = (
    bronze_payments_df
        .select(
            F.trim(
                F.col("order_id")
            ).alias("order_id"),

            F.col("payment_sequential")
                .cast("integer")
                .alias("payment_sequential"),

            F.lower(
                F.trim(
                    F.col("payment_type")
                )
            ).alias("payment_type"),

            F.col("payment_installments")
                .cast("integer")
                .alias("payment_installments"),

            F.col("payment_value")
                .cast("decimal(18,2)")
                .alias("payment_value"),

            F.current_timestamp().alias(
                "silver_load_timestamp"
            )
        )
)

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Preview Silver Order Payments

# COMMAND ----------

display(
    silver_payments_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Ensure Silver Schema Exists

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
# MAGIC ## Step 8 — Write Silver Order Payments
# MAGIC
# MAGIC Replace the previous Silver payment table with the current validated
# MAGIC Silver DataFrame.
# MAGIC
# MAGIC This ensures that an obsolete table schema does not interfere with
# MAGIC pipeline execution.

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.order_payments
""")

(
    silver_payments_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "workspace.silver.order_payments"
        )
)

print(
    "Silver Order Payments table created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 9 — Read Persisted Silver Order Payments

# COMMAND ----------

silver_payments_df = spark.table(
    "workspace.silver.order_payments"
)

silver_payment_count = silver_payments_df.count()

print(
    f"Silver Order Payments rows: "
    f"{silver_payment_count:,}"
)

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 10 — Bronze-to-Silver Row Count Validation
# MAGIC
# MAGIC Every Bronze payment record must be retained in Silver.

# COMMAND ----------

print(
    f"Bronze Order Payments rows : "
    f"{bronze_payment_count:,}"
)

print(
    f"Silver Order Payments rows : "
    f"{silver_payment_count:,}"
)

if bronze_payment_count != silver_payment_count:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Bronze and Silver row counts do not match."
    )

print(
    "PASS — Bronze and Silver payment counts match."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 11 — Validate Payment Types
# MAGIC
# MAGIC Payment type values are standardized to lowercase.
# MAGIC
# MAGIC The source dataset contains a defined set of payment methods.

# COMMAND ----------

valid_payment_types = {
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined"
}

invalid_payment_types = (
    silver_payments_df
        .filter(
            F.col("payment_type").isNotNull()
            & (
                ~F.col("payment_type").isin(
                    list(valid_payment_types)
                )
            )
        )
        .count()
)

null_payment_types = (
    silver_payments_df
        .filter(
            F.col("payment_type").isNull()
        )
        .count()
)

print(
    f"NULL payment types       : {null_payment_types}"
)

print(
    f"Invalid payment types    : {invalid_payment_types}"
)

if null_payment_types != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "NULL payment_type values found."
    )

if invalid_payment_types != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Invalid payment_type values found."
    )

print(
    "PASS — Payment type validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 12 — Validate Payment Sequence
# MAGIC
# MAGIC Payment sequence must be a positive integer.

# COMMAND ----------

invalid_payment_sequences = (
    silver_payments_df
        .filter(
            F.col("payment_sequential").isNull()
            | (
                F.col("payment_sequential") < 1
            )
        )
        .count()
)

print(
    f"Invalid payment sequences: "
    f"{invalid_payment_sequences}"
)

if invalid_payment_sequences != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Invalid payment_sequential values found."
    )

print(
    "PASS — Payment sequence validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 13 — Validate Payment Installments
# MAGIC
# MAGIC Payment installments cannot be negative.

# COMMAND ----------

invalid_payment_installments = (
    silver_payments_df
        .filter(
            F.col("payment_installments").isNull()
            | (
                F.col("payment_installments") < 0
            )
        )
        .count()
)

print(
    f"Invalid payment installments: "
    f"{invalid_payment_installments}"
)

if invalid_payment_installments != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Invalid payment_installments values found."
    )

print(
    "PASS — Payment installment validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 14 — Validate Payment Value
# MAGIC
# MAGIC Payment values must not be negative.

# COMMAND ----------

invalid_payment_values = (
    silver_payments_df
        .filter(
            F.col("payment_value").isNull()
            | (
                F.col("payment_value") < 0
            )
        )
        .count()
)

print(
    f"Invalid payment values: "
    f"{invalid_payment_values}"
)

if invalid_payment_values != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Invalid payment_value values found."
    )

print(
    "PASS — Payment value validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 15 — Validate Silver Load Timestamp

# COMMAND ----------

null_silver_timestamps = silver_payments_df.filter(
    F.col("silver_load_timestamp").isNull()
).count()

print(
    f"NULL Silver load timestamps: "
    f"{null_silver_timestamps}"
)

if null_silver_timestamps != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Silver load timestamp is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 16 — Validate Silver Datatypes

# COMMAND ----------

expected_payment_types = {
    "order_id": "string",
    "payment_sequential": "int",
    "payment_type": "string",
    "payment_installments": "int",
    "payment_value": "decimal(18,2)",
    "silver_load_timestamp": "timestamp"
}

actual_payment_types = dict(
    silver_payments_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_payment_types.get(column)
    }
    for column, expected_type in expected_payment_types.items()
    if actual_payment_types.get(column) != expected_type
}

if datatype_errors:
    raise ValueError(
        "Silver Order Payments datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Silver Order Payments datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 17 — Final Silver Order Payments Quality Gate

# COMMAND ----------

final_silver_payments_df = spark.table(
    "workspace.silver.order_payments"
)

final_payment_count = final_silver_payments_df.count()

final_null_order_ids = final_silver_payments_df.filter(
    F.col("order_id").isNull()
).count()

final_null_payment_types = final_silver_payments_df.filter(
    F.col("payment_type").isNull()
).count()

final_invalid_sequences = final_silver_payments_df.filter(
    F.col("payment_sequential").isNull()
    | (
        F.col("payment_sequential") < 1
    )
).count()

final_invalid_installments = final_silver_payments_df.filter(
    F.col("payment_installments").isNull()
    | (
        F.col("payment_installments") < 0
    )
).count()

final_invalid_values = final_silver_payments_df.filter(
    F.col("payment_value").isNull()
    | (
        F.col("payment_value") < 0
    )
).count()

final_null_timestamps = final_silver_payments_df.filter(
    F.col("silver_load_timestamp").isNull()
).count()

print("=" * 70)
print("SILVER ORDER PAYMENTS QUALITY GATE")
print("=" * 70)

print(
    f"Bronze rows              : "
    f"{bronze_payment_count:,}"
)

print(
    f"Silver rows              : "
    f"{final_payment_count:,}"
)

print(
    f"NULL order IDs           : "
    f"{final_null_order_ids}"
)

print(
    f"NULL payment types       : "
    f"{final_null_payment_types}"
)

print(
    f"Invalid payment sequence: "
    f"{final_invalid_sequences}"
)

print(
    f"Invalid installments     : "
    f"{final_invalid_installments}"
)

print(
    f"Invalid payment values   : "
    f"{final_invalid_values}"
)

print(
    f"NULL load timestamps     : "
    f"{final_null_timestamps}"
)

print("=" * 70)

if (
    final_payment_count == bronze_payment_count
    and final_null_order_ids == 0
    and final_null_payment_types == 0
    and final_invalid_sequences == 0
    and final_invalid_installments == 0
    and final_invalid_values == 0
    and final_null_timestamps == 0
):
    print(
        "STATUS: ALL SILVER ORDER PAYMENTS CHECKS PASSED"
    )
else:
    raise ValueError(
        "SILVER ORDER PAYMENTS QUALITY GATE FAILED."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 18 — Payment Type Distribution
# MAGIC
# MAGIC Review payment methods present in the persisted Silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     COUNT(*) AS payment_count
# MAGIC FROM workspace.silver.order_payments
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY payment_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 19 — Verify Silver Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.silver.order_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 20 — Verify Silver Delta History

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.silver.order_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 21 — Verify Silver Table Registration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

payment_key_duplicates = (
    final_silver_payments_df
        .groupBy(
            "order_id",
            "payment_sequential"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

print(
    f"Duplicate payment keys: "
    f"{payment_key_duplicates}"
)

if payment_key_duplicates != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Duplicate (order_id, payment_sequential) keys found."
    )

print(
    "PASS — Payment grain is unique."
)

# COMMAND ----------

null_counts = final_silver_payments_df.select(
    [
        F.sum(
            F.when(
                F.col(column).isNull(),
                1
            ).otherwise(0)
        ).alias(column)
        for column in final_silver_payments_df.columns
    ]
)

display(null_counts)

# COMMAND ----------

null_count_values = null_counts.collect()[0].asDict()

null_columns = {
    column: count
    for column, count in null_count_values.items()
    if count != 0
}

if null_columns:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        f"NULL values found: {null_columns}"
    )

print(
    "PASS — No NULL values found in the persisted Silver payment table."
)

# COMMAND ----------

zero_payment_values_df = (
    final_silver_payments_df
        .filter(
            F.col("payment_value") == 0
        )
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        )
        .orderBy(
            "payment_type",
            "order_id"
        )
)

print(
    f"Zero payment value records: "
    f"{zero_payment_values_df.count()}"
)

display(
    zero_payment_values_df
)

# COMMAND ----------

zero_installments_df = (
    final_silver_payments_df
        .filter(
            F.col("payment_installments") == 0
        )
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        )
        .orderBy(
            "payment_type",
            "order_id"
        )
)

print(
    f"Zero installment records: "
    f"{zero_installments_df.count()}"
)

display(
    zero_installments_df
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.silver

# COMMAND ----------

# DBTITLE 1,Cell 49
silver_orders_df = spark.table(
    "workspace.silver.orders"
)

orphan_payment_records = (
    final_silver_payments_df
        .join(
            silver_orders_df.select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
)

print(
    f"Payment records without matching orders: "
    f"{orphan_payment_records}"
)

if orphan_payment_records != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Payment records reference orders that do not exist "
        "in workspace.silver.orders."
    )

print(
    "PASS — All Silver payment records reference valid orders."
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

payment_records_per_order = (
    final_silver_payments_df
        .groupBy("order_id")
        .count()
)

display(
    payment_records_per_order
        .groupBy("count")
        .count()
        .orderBy("count")
)

# COMMAND ----------

payment_key_duplicates = (
    final_silver_payments_df
        .groupBy(
            "order_id",
            "payment_sequential"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

print(
    f"Duplicate payment keys: "
    f"{payment_key_duplicates}"
)

if payment_key_duplicates != 0:
    raise ValueError(
        "Silver Order Payments quality gate failed: "
        "Duplicate (order_id, payment_sequential) keys found."
    )

print(
    "PASS — Payment grain is unique."
)

# COMMAND ----------

zero_payment_value_df = (
    final_silver_payments_df
        .filter(
            F.col("payment_value") == 0
        )
        .orderBy(
            "payment_type",
            "order_id",
            "payment_sequential"
        )
)

print(
    f"Zero payment value records: "
    f"{zero_payment_value_df.count()}"
)

display(
    zero_payment_value_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Silver Order Payments Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.silver.order_payments"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver Order Payments — Execution Summary
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.order_payments`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.order_payments`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One row represents one payment record associated with an order.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Transformations
# MAGIC
# MAGIC - Preserved Bronze payment records.
# MAGIC - Standardized payment type values.
# MAGIC - Explicitly cast payment sequence to integer.
# MAGIC - Explicitly cast payment installments to integer.
# MAGIC - Explicitly cast payment value to `DECIMAL(18,2)`.
# MAGIC - Added `silver_load_timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |---|---|
# MAGIC | Bronze → Silver row count | PASS |
# MAGIC | Order ID validation | PASS |
# MAGIC | Payment type validation | PASS |
# MAGIC | Payment sequence validation | PASS |
# MAGIC | Payment installments validation | PASS |
# MAGIC | Payment value validation | PASS |
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
# MAGIC **SUCCESS — Silver Order Payments transformation and quality
# MAGIC validation completed successfully.**

# COMMAND ----------

