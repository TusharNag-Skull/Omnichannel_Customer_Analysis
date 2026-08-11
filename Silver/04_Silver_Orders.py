# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Orders
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create and validate the Silver Orders table from the Bronze Orders
# MAGIC table.
# MAGIC
# MAGIC The Silver layer will standardize the order attributes, preserve the
# MAGIC source order records, create lifecycle quality flags, and enforce
# MAGIC pipeline-blocking quality checks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.orders`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Key
# MAGIC
# MAGIC `order_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Responsibilities
# MAGIC
# MAGIC - Preserve Bronze order records.
# MAGIC - Standardize order status.
# MAGIC - Convert order timestamps to timestamp datatypes.
# MAGIC - Create order lifecycle quality flags.
# MAGIC - Validate the order business key.
# MAGIC - Validate order lifecycle consistency.
# MAGIC - Reconcile Bronze and Silver row counts.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC
# MAGIC No Gold-layer metrics are created in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Import Required Libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 — Read Bronze Orders

# COMMAND ----------

bronze_orders_df = spark.table(
    "workspace.bronze.orders"
)

bronze_order_count = bronze_orders_df.count()

print(
    f"Bronze Orders rows: {bronze_order_count:,}"
)

bronze_orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Validate Bronze Orders Structure

# COMMAND ----------

required_order_columns = {
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
}

actual_order_columns = set(
    bronze_orders_df.columns
)

missing_order_columns = (
    required_order_columns
    - actual_order_columns
)

if missing_order_columns:
    raise ValueError(
        "Bronze Orders schema validation failed. "
        f"Missing columns: {sorted(missing_order_columns)}"
    )

print(
    "PASS — Bronze Orders contains all required columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Bronze Order Business Key

# COMMAND ----------

null_order_ids = bronze_orders_df.filter(
    F.col("order_id").isNull()
).count()

duplicate_order_ids = (
    bronze_orders_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"NULL order IDs      : {null_order_ids}")
print(f"Duplicate order IDs : {duplicate_order_ids}")

if null_order_ids != 0:
    raise ValueError(
        "Bronze Orders quality gate failed: "
        "NULL order_id values found."
    )

if duplicate_order_ids != 0:
    raise ValueError(
        "Bronze Orders quality gate failed: "
        "Duplicate order_id values found."
    )

print("PASS — Bronze order business key is valid.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Validate Order Lifecycle
# MAGIC
# MAGIC The order lifecycle is evaluated in chronological order:
# MAGIC
# MAGIC Purchase → Approval → Carrier Delivery → Customer Delivery.
# MAGIC
# MAGIC Legitimate NULL timestamps are preserved because an order may not have
# MAGIC reached a particular lifecycle stage.

# COMMAND ----------

approval_before_purchase = bronze_orders_df.filter(
    F.col("order_approved_at").isNotNull()
    & (
        F.col("order_approved_at")
        < F.col("order_purchase_timestamp")
    )
).count()

carrier_before_approval = bronze_orders_df.filter(
    F.col("order_delivered_carrier_date").isNotNull()
    & F.col("order_approved_at").isNotNull()
    & (
        F.col("order_delivered_carrier_date")
        < F.col("order_approved_at")
    )
).count()

customer_delivery_before_carrier = bronze_orders_df.filter(
    F.col("order_delivered_customer_date").isNotNull()
    & F.col("order_delivered_carrier_date").isNotNull()
    & (
        F.col("order_delivered_customer_date")
        < F.col("order_delivered_carrier_date")
    )
).count()

print(
    f"Approval before purchase        : {approval_before_purchase}"
)

print(
    f"Carrier before approval         : {carrier_before_approval}"
)

print(
    f"Customer delivery before carrier: "
    f"{customer_delivery_before_carrier}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Create Silver Orders DataFrame
# MAGIC
# MAGIC Apply the required Silver transformations.
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC - Trim `order_id` and `customer_id`.
# MAGIC - Standardize `order_status` to lowercase.
# MAGIC - Convert all order lifecycle fields to TIMESTAMP.
# MAGIC - Create `approval_missing_flag`.
# MAGIC - Create `timeline_issue_flag`.
# MAGIC - Preserve all Bronze order records.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC
# MAGIC Lifecycle anomalies are retained and flagged rather than removed.

# COMMAND ----------

silver_orders_df = bronze_orders_df.select(
    F.trim(
        F.col("order_id")
    ).alias("order_id"),

    F.trim(
        F.col("customer_id")
    ).alias("customer_id"),

    F.lower(
        F.trim(
            F.col("order_status")
        )
    ).alias("order_status"),

    F.to_timestamp(
        F.col("order_purchase_timestamp")
    ).alias(
        "order_purchase_timestamp"
    ),

    F.to_timestamp(
        F.col("order_approved_at")
    ).alias(
        "order_approved_at"
    ),

    F.to_timestamp(
        F.col("order_delivered_carrier_date")
    ).alias(
        "order_delivered_carrier_date"
    ),

    F.to_timestamp(
        F.col("order_delivered_customer_date")
    ).alias(
        "order_delivered_customer_date"
    ),

    F.to_timestamp(
        F.col("order_estimated_delivery_date")
    ).alias(
        "order_estimated_delivery_date"
    ),

    (
        F.col("order_approved_at").isNull()
        & (
            F.lower(
                F.trim(
                    F.col("order_status")
                )
            ) == "delivered"
        )
    ).alias(
        "approval_missing_flag"
    ),

    (
        (
            F.col("order_approved_at").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull()
            & (
                F.to_timestamp(
                    F.col("order_approved_at")
                )
                <
                F.to_timestamp(
                    F.col("order_purchase_timestamp")
                )
            )
        )
        |
        (
            F.col("order_delivered_carrier_date").isNotNull()
            & F.col("order_approved_at").isNotNull()
            & (
                F.to_timestamp(
                    F.col("order_delivered_carrier_date")
                )
                <
                F.to_timestamp(
                    F.col("order_approved_at")
                )
            )
        )
        |
        (
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_delivered_carrier_date").isNotNull()
            & (
                F.to_timestamp(
                    F.col("order_delivered_customer_date")
                )
                <
                F.to_timestamp(
                    F.col("order_delivered_carrier_date")
                )
            )
        )
    ).alias(
        "timeline_issue_flag"
    ),

    F.current_timestamp().alias(
        "silver_load_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Preview Silver Orders

# COMMAND ----------

silver_orders_df.printSchema()

display(
    silver_orders_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Write Silver Orders
# MAGIC
# MAGIC Persist the corrected Silver Orders DataFrame as a managed Delta table.
# MAGIC
# MAGIC All Bronze order records are retained.

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.orders
""")

(
    silver_orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.silver.orders"
    )
)

print(
    "Silver Orders table created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 8 — Read Persisted Silver Orders
# MAGIC
# MAGIC Reload the persisted Delta table so that all subsequent validations
# MAGIC operate on the actual Silver table.

# COMMAND ----------

silver_orders_df = spark.table(
    "workspace.silver.orders"
)

silver_order_count = silver_orders_df.count()

print(
    f"Silver Orders rows: "
    f"{silver_order_count:,}"
)

silver_orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 9 — Validate Bronze-to-Silver Row Count
# MAGIC
# MAGIC Every Bronze order must be retained in Silver.
# MAGIC
# MAGIC No order records are intentionally filtered during the Silver
# MAGIC transformation.

# COMMAND ----------

print(
    f"Bronze Orders rows : {bronze_order_count:,}"
)

print(
    f"Silver Orders rows : {silver_order_count:,}"
)

if bronze_order_count != silver_order_count:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "Bronze and Silver row counts do not match."
    )

print(
    "PASS — Bronze and Silver order counts match."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 10 — Validate Silver Order IDs
# MAGIC
# MAGIC `order_id` is the business key for the Orders table.
# MAGIC
# MAGIC It must be non-NULL and unique in the persisted Silver table.

# COMMAND ----------

silver_null_order_ids = silver_orders_df.filter(
    F.col("order_id").isNull()
).count()

silver_duplicate_order_ids = (
    silver_orders_df
    .groupBy("order_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"NULL order IDs      : "
    f"{silver_null_order_ids}"
)

print(
    f"Duplicate order IDs : "
    f"{silver_duplicate_order_ids}"
)

if silver_null_order_ids != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "NULL order_id values found."
    )

if silver_duplicate_order_ids != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "Duplicate order_id values found."
    )

print(
    "PASS — Silver order identifier validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 11 — Validate Required Order Fields
# MAGIC
# MAGIC The following fields are required for downstream order analytics and
# MAGIC relationships:
# MAGIC
# MAGIC - `order_id`
# MAGIC - `customer_id`
# MAGIC - `order_status`
# MAGIC - `order_purchase_timestamp`

# COMMAND ----------

null_customer_ids = silver_orders_df.filter(
    F.col("customer_id").isNull()
).count()

null_order_statuses = silver_orders_df.filter(
    F.col("order_status").isNull()
).count()

null_purchase_timestamps = silver_orders_df.filter(
    F.col("order_purchase_timestamp").isNull()
).count()

print(
    f"NULL customer IDs        : "
    f"{null_customer_ids}"
)

print(
    f"NULL order statuses      : "
    f"{null_order_statuses}"
)

print(
    f"NULL purchase timestamps : "
    f"{null_purchase_timestamps}"
)

if null_customer_ids != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "NULL customer_id values found."
    )

if null_order_statuses != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "NULL order_status values found."
    )

if null_purchase_timestamps != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "NULL order_purchase_timestamp values found."
    )

print(
    "PASS — Required Silver Order fields are populated."
)

# COMMAND ----------

expected_order_types = {
    "order_id": "string",
    "customer_id": "string",
    "order_status": "string",
    "order_purchase_timestamp": "timestamp",
    "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp",
    "approval_missing_flag": "boolean",
    "timeline_issue_flag": "boolean",
    "silver_load_timestamp": "timestamp"
}

actual_order_types = dict(
    silver_orders_df.dtypes
)

datatype_errors = {}

for column, expected_type in expected_order_types.items():
    actual_type = actual_order_types.get(column)

    if actual_type != expected_type:
        datatype_errors[column] = {
            "expected": expected_type,
            "actual": actual_type
        }

if datatype_errors:
    raise ValueError(
        "Silver Orders datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Silver Orders datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 12 — Validate Persisted Order Business Key
# MAGIC
# MAGIC Validate the order identifier on the persisted Silver table.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 13 — Validate Silver Load Timestamp
# MAGIC
# MAGIC Every persisted Silver order must contain a Silver load timestamp for
# MAGIC pipeline traceability and operational monitoring.

# COMMAND ----------

null_silver_timestamps = silver_orders_df.filter(
    F.col("silver_load_timestamp").isNull()
).count()

print(
    f"NULL Silver load timestamps: "
    f"{null_silver_timestamps}"
)

if null_silver_timestamps != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Silver load timestamp is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 14 — Validate Order Status Domain
# MAGIC
# MAGIC Order status is standardized to lowercase in Silver.
# MAGIC
# MAGIC Only known source order statuses are accepted.

# COMMAND ----------

valid_order_statuses = {
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved"
}

invalid_status_count = silver_orders_df.filter(
    F.col("order_status").isNull()
    |
    ~F.col("order_status").isin(
        list(valid_order_statuses)
    )
).count()

print(
    f"Invalid order statuses: "
    f"{invalid_status_count}"
)

if invalid_status_count != 0:
    display(
        silver_orders_df
        .filter(
            F.col("order_status").isNull()
            |
            ~F.col("order_status").isin(
                list(valid_order_statuses)
            )
        )
        .select(
            "order_id",
            "order_status"
        )
        .distinct()
    )

    raise ValueError(
        "Silver Orders quality gate failed: "
        "Invalid order_status values found."
    )

print(
    "PASS — All Silver order statuses are valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 15 — Validate Approval Missing Flag
# MAGIC
# MAGIC A delivered order without an approval timestamp is flagged as a data
# MAGIC quality issue.
# MAGIC
# MAGIC The order itself is retained.

# COMMAND ----------

expected_approval_missing_flag = (
    F.col("order_status") == "delivered"
) & F.col("order_approved_at").isNull()

incorrect_approval_flags = silver_orders_df.filter(
    F.col("approval_missing_flag")
    != expected_approval_missing_flag
).count()

approval_missing_count = silver_orders_df.filter(
    F.col("approval_missing_flag")
).count()

print(
    f"Orders with approval missing flag: "
    f"{approval_missing_count}"
)

print(
    f"Incorrect approval flags        : "
    f"{incorrect_approval_flags}"
)

if incorrect_approval_flags != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "approval_missing_flag does not match "
        "the defined business rule."
    )

print(
    "PASS — Approval missing flag is logically correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 16 — Analyze Order Lifecycle Rules
# MAGIC
# MAGIC The expected lifecycle is:
# MAGIC
# MAGIC Purchase → Approval → Carrier Delivery → Customer Delivery
# MAGIC
# MAGIC Each rule is evaluated independently.
# MAGIC
# MAGIC Records violating a rule are retained and identified through the
# MAGIC existing `timeline_issue_flag`.

# COMMAND ----------

approval_before_purchase_count = silver_orders_df.filter(
    F.col("order_approved_at").isNotNull()
    & F.col("order_purchase_timestamp").isNotNull()
    & (
        F.col("order_approved_at")
        < F.col("order_purchase_timestamp")
    )
).count()

carrier_before_approval_count = silver_orders_df.filter(
    F.col("order_delivered_carrier_date").isNotNull()
    & F.col("order_approved_at").isNotNull()
    & (
        F.col("order_delivered_carrier_date")
        < F.col("order_approved_at")
    )
).count()

delivery_before_carrier_count = silver_orders_df.filter(
    F.col("order_delivered_customer_date").isNotNull()
    & F.col("order_delivered_carrier_date").isNotNull()
    & (
        F.col("order_delivered_customer_date")
        < F.col("order_delivered_carrier_date")
    )
).count()

print(
    f"Approval before purchase        : "
    f"{approval_before_purchase_count}"
)

print(
    f"Carrier before approval         : "
    f"{carrier_before_approval_count}"
)

print(
    f"Customer delivery before carrier: "
    f"{delivery_before_carrier_count}"
)

carrier_before_approval_df = silver_orders_df.filter(
    F.col("order_delivered_carrier_date").isNotNull()
    & F.col("order_approved_at").isNotNull()
    & (
        F.col("order_delivered_carrier_date")
        < F.col("order_approved_at")
    )
)

print(
    "Carrier-before-approval records: "
    f"{carrier_before_approval_df.count():,}"
)

display(
    carrier_before_approval_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "timeline_issue_flag"
    ).orderBy(
        "order_delivered_carrier_date"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 17 — Investigate Carrier-Before-Approval Records
# MAGIC
# MAGIC Inspect orders where the carrier delivery timestamp occurs before the
# MAGIC approval timestamp.
# MAGIC
# MAGIC These records are retained in Silver and identified through
# MAGIC `timeline_issue_flag`.
# MAGIC
# MAGIC This investigation is performed to understand the source-data quality
# MAGIC issue without removing valid business records.

# COMMAND ----------

carrier_before_approval_df = silver_orders_df.filter(
    (F.col("order_delivered_carrier_date").isNotNull()) &
    (F.col("order_approved_at").isNotNull()) &
    (
        F.col("order_delivered_carrier_date")
        < F.col("order_approved_at")
    )
)

carrier_before_approval_count = (
    carrier_before_approval_df.count()
)

print(
    f"Carrier-before-approval records: "
    f"{carrier_before_approval_count:,}"
)

display(
    carrier_before_approval_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "timeline_issue_flag"
    ).orderBy(
        "order_delivered_carrier_date"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 18 — Investigate Customer-Delivery-Before-Carrier Records
# MAGIC
# MAGIC Inspect orders where customer delivery occurs before carrier delivery.
# MAGIC
# MAGIC These records are retained in Silver and identified through
# MAGIC `timeline_issue_flag`.

# COMMAND ----------

delivery_before_carrier_df = silver_orders_df.filter(
    (F.col("order_delivered_customer_date").isNotNull()) &
    (F.col("order_delivered_carrier_date").isNotNull()) &
    (
        F.col("order_delivered_customer_date")
        < F.col("order_delivered_carrier_date")
    )
)

delivery_before_carrier_count = (
    delivery_before_carrier_df.count()
)

print(
    f"Customer-delivery-before-carrier records: "
    f"{delivery_before_carrier_count:,}"
)

display(
    delivery_before_carrier_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "timeline_issue_flag"
    ).orderBy(
        "order_delivered_customer_date"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 19 — Validate Timeline Quality Flag
# MAGIC
# MAGIC The `timeline_issue_flag` must be TRUE whenever an order violates
# MAGIC any defined lifecycle rule:
# MAGIC
# MAGIC - Approval before purchase
# MAGIC - Carrier delivery before approval
# MAGIC - Customer delivery before carrier delivery
# MAGIC
# MAGIC The flag must be FALSE when none of these rules are violated.
# MAGIC
# MAGIC Lifecycle anomalies are retained in Silver and are not removed.

# COMMAND ----------

expected_timeline_issue = (
    (
        F.col("order_approved_at").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()
        & (
            F.col("order_approved_at")
            < F.col("order_purchase_timestamp")
        )
    )
    |
    (
        F.col("order_delivered_carrier_date").isNotNull()
        & F.col("order_approved_at").isNotNull()
        & (
            F.col("order_delivered_carrier_date")
            < F.col("order_approved_at")
        )
    )
    |
    (
        F.col("order_delivered_customer_date").isNotNull()
        & F.col("order_delivered_carrier_date").isNotNull()
        & (
            F.col("order_delivered_customer_date")
            < F.col("order_delivered_carrier_date")
        )
    )
)

incorrect_timeline_flags = silver_orders_df.filter(
    F.col("timeline_issue_flag")
    != expected_timeline_issue
).count()

timeline_issue_count = silver_orders_df.filter(
    F.col("timeline_issue_flag") == True
).count()

print(
    f"Orders with timeline issues : "
    f"{timeline_issue_count:,}"
)

print(
    f"Incorrect timeline flags    : "
    f"{incorrect_timeline_flags}"
)

if incorrect_timeline_flags != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "timeline_issue_flag does not match "
        "the defined lifecycle rules."
    )

print(
    "PASS — Timeline quality flag is logically correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 20 — Validate Bronze-to-Silver Order Population
# MAGIC
# MAGIC Verify that the exact set of Bronze `order_id` values is preserved in
# MAGIC Silver.
# MAGIC
# MAGIC This prevents a row-count match from hiding simultaneous record loss
# MAGIC and record creation during the Silver transformation.

# COMMAND ----------

bronze_order_ids_df = bronze_orders_df.select(
    "order_id"
).distinct()

silver_order_ids_df = silver_orders_df.select(
    "order_id"
).distinct()


bronze_ids_missing_in_silver = (
    bronze_order_ids_df
    .join(
        silver_order_ids_df,
        on="order_id",
        how="left_anti"
    )
    .count()
)


silver_ids_not_in_bronze = (
    silver_order_ids_df
    .join(
        bronze_order_ids_df,
        on="order_id",
        how="left_anti"
    )
    .count()
)


print(
    f"Bronze order IDs missing in Silver : "
    f"{bronze_ids_missing_in_silver}"
)

print(
    f"Silver order IDs not in Bronze     : "
    f"{silver_ids_not_in_bronze}"
)


if bronze_ids_missing_in_silver != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "Bronze order IDs are missing from Silver."
    )


if silver_ids_not_in_bronze != 0:
    raise ValueError(
        "Silver Orders quality gate failed: "
        "Silver contains order IDs not present in Bronze."
    )


print(
    "PASS — Bronze and Silver order populations match exactly."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 21 — Validate Timestamp Conversion
# MAGIC
# MAGIC Verify that the Bronze-to-Silver timestamp conversion preserves the
# MAGIC expected NULL pattern.
# MAGIC
# MAGIC A NULL timestamp in Silver is valid when the corresponding Bronze
# MAGIC timestamp was already NULL or blank.
# MAGIC
# MAGIC Unexpected timestamp conversion failures must fail the pipeline.

# COMMAND ----------

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

timestamp_conversion_errors = {}

for column in timestamp_columns:

    bronze_expected_null_count = bronze_orders_df.filter(
        F.col(column).isNull()
        | (F.trim(F.col(column)) == "")
    ).count()

    silver_null_count = silver_orders_df.filter(
        F.col(column).isNull()
    ).count()

    if silver_null_count != bronze_expected_null_count:
        timestamp_conversion_errors[column] = {
            "Bronze expected NULL": bronze_expected_null_count,
            "Silver actual NULL": silver_null_count
        }


if timestamp_conversion_errors:

    raise ValueError(
        "Silver Orders timestamp conversion validation failed: "
        f"{timestamp_conversion_errors}"
    )


print(
    "PASS — Timestamp conversion preserved the expected NULL pattern."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 22 — Final Silver Orders Quality Gate
# MAGIC
# MAGIC Validate the persisted Silver Orders Delta table against all critical
# MAGIC structural, business, transformation, and operational quality rules.
# MAGIC
# MAGIC Lifecycle anomalies are treated as data-quality observations and do not
# MAGIC fail the pipeline when they are correctly identified by
# MAGIC `timeline_issue_flag`.

# COMMAND ----------

final_silver_orders_df = spark.table(
    "workspace.silver.orders"
)

final_silver_order_count = (
    final_silver_orders_df.count()
)

final_null_order_ids = (
    final_silver_orders_df
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)

final_duplicate_order_ids = (
    final_silver_orders_df
    .groupBy("order_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

final_null_customer_ids = (
    final_silver_orders_df
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)

final_null_statuses = (
    final_silver_orders_df
    .filter(
        F.col("order_status").isNull()
    )
    .count()
)

final_invalid_statuses = (
    final_silver_orders_df
    .filter(
        ~F.col("order_status").isin(
            list(valid_order_statuses)
        )
    )
    .count()
)

final_null_purchase_timestamps = (
    final_silver_orders_df
    .filter(
        F.col("order_purchase_timestamp").isNull()
    )
    .count()
)

final_null_silver_timestamps = (
    final_silver_orders_df
    .filter(
        F.col("silver_load_timestamp").isNull()
    )
    .count()
)

final_incorrect_approval_flags = (
    final_silver_orders_df
    .filter(
        F.col("approval_missing_flag")
        != expected_approval_missing_flag
    )
    .count()
)

final_incorrect_timeline_flags = (
    final_silver_orders_df
    .filter(
        F.col("timeline_issue_flag")
        != expected_timeline_issue
    )
    .count()
)

print("=" * 75)
print("SILVER ORDERS QUALITY GATE")
print("=" * 75)

print(
    f"Bronze rows                    : "
    f"{bronze_order_count:,}"
)

print(
    f"Silver rows                    : "
    f"{final_silver_order_count:,}"
)

print(
    f"NULL order IDs                 : "
    f"{final_null_order_ids}"
)

print(
    f"Duplicate order IDs            : "
    f"{final_duplicate_order_ids}"
)

print(
    f"NULL customer IDs              : "
    f"{final_null_customer_ids}"
)

print(
    f"NULL order statuses            : "
    f"{final_null_statuses}"
)

print(
    f"Invalid order statuses         : "
    f"{final_invalid_statuses}"
)

print(
    f"NULL purchase timestamps       : "
    f"{final_null_purchase_timestamps}"
)

print(
    f"NULL Silver load timestamps    : "
    f"{final_null_silver_timestamps}"
)

print(
    f"Incorrect approval flags       : "
    f"{final_incorrect_approval_flags}"
)

print(
    f"Incorrect timeline flags       : "
    f"{final_incorrect_timeline_flags}"
)

print("=" * 75)

if (
    final_silver_order_count == bronze_order_count
    and final_null_order_ids == 0
    and final_duplicate_order_ids == 0
    and final_null_customer_ids == 0
    and final_null_statuses == 0
    and final_invalid_statuses == 0
    and final_null_purchase_timestamps == 0
    and final_null_silver_timestamps == 0
    and final_incorrect_approval_flags == 0
    and final_incorrect_timeline_flags == 0
):
    print(
        "STATUS: ALL SILVER ORDERS CHECKS PASSED"
    )
else:
    raise ValueError(
        "SILVER ORDERS QUALITY GATE FAILED."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 23 — Validate Silver Orders Delta Table
# MAGIC
# MAGIC Verify that `workspace.silver.orders` is correctly persisted as a
# MAGIC managed Delta table.
# MAGIC
# MAGIC The table must exist and contain the expected Silver dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.silver.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 24 — Validate Silver Orders Delta History
# MAGIC
# MAGIC Verify the Delta transaction history for the Silver Orders table.
# MAGIC
# MAGIC This provides operational traceability for table creation and future
# MAGIC pipeline executions.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.silver.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 25 — Verify Silver Orders Table Registration
# MAGIC
# MAGIC Confirm that the Silver Orders table is registered in the
# MAGIC `workspace.silver` schema and available for downstream workloads.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver Orders — Execution Summary
# MAGIC
# MAGIC ## Execution Status
# MAGIC
# MAGIC **Status:** `SUCCESS`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC **Bronze Table:**
# MAGIC
# MAGIC `workspace.bronze.orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Target
# MAGIC
# MAGIC `workspace.silver.orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC One record represents one customer order.
# MAGIC
# MAGIC **Business Key:**
# MAGIC
# MAGIC `order_id`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Transformations
# MAGIC
# MAGIC The Silver Orders layer performs the following transformations:
# MAGIC
# MAGIC - Trims `order_id`.
# MAGIC - Trims `customer_id`.
# MAGIC - Standardizes `order_status` to lowercase.
# MAGIC - Converts order lifecycle fields from STRING to TIMESTAMP.
# MAGIC - Creates `approval_missing_flag`.
# MAGIC - Creates `timeline_issue_flag`.
# MAGIC - Adds `silver_load_timestamp`.
# MAGIC - Preserves all Bronze order records.
# MAGIC
# MAGIC No order records are removed because of lifecycle anomalies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Validation
# MAGIC
# MAGIC The following validations were completed successfully:
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |------------|--------|
# MAGIC | Bronze schema validation | PASS |
# MAGIC | Bronze order ID NULL validation | PASS |
# MAGIC | Bronze order ID uniqueness | PASS |
# MAGIC | Bronze/Silver row-count reconciliation | PASS |
# MAGIC | Silver order ID NULL validation | PASS |
# MAGIC | Silver order ID uniqueness | PASS |
# MAGIC | Required customer ID validation | PASS |
# MAGIC | Required order status validation | PASS |
# MAGIC | Required purchase timestamp validation | PASS |
# MAGIC | Order status domain validation | PASS |
# MAGIC | Silver datatype validation | PASS |
# MAGIC | Silver load timestamp validation | PASS |
# MAGIC | Approval flag validation | PASS |
# MAGIC | Timeline flag validation | PASS |
# MAGIC | Bronze/Silver order ID reconciliation | PASS |
# MAGIC | Timestamp conversion validation | PASS |
# MAGIC | Final persisted Silver quality gate | PASS |
# MAGIC | Delta metadata validation | PASS |
# MAGIC | Delta history validation | PASS |
# MAGIC | Silver table registration | PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Lifecycle Quality Findings
# MAGIC
# MAGIC The expected order lifecycle is:
# MAGIC
# MAGIC `Purchase → Approval → Carrier Delivery → Customer Delivery`
# MAGIC
# MAGIC The following source-data anomalies were identified:
# MAGIC
# MAGIC - Approval before purchase: `0`
# MAGIC - Carrier delivery before approval: `1,359`
# MAGIC - Customer delivery before carrier delivery: `23`
# MAGIC - Total orders with timeline issues: `1,382`
# MAGIC - Delivered orders with missing approval: `14`
# MAGIC
# MAGIC These records were **not deleted**.
# MAGIC
# MAGIC Instead, the anomalies are represented through:
# MAGIC
# MAGIC - `approval_missing_flag`
# MAGIC - `timeline_issue_flag`
# MAGIC
# MAGIC This preserves the source business records while making the data-quality issues available for downstream analytics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Population Reconciliation
# MAGIC
# MAGIC **Bronze Orders rows:** `99,441`
# MAGIC
# MAGIC **Silver Orders rows:** `99,441`
# MAGIC
# MAGIC **Bronze order IDs missing from Silver:** `0`
# MAGIC
# MAGIC **Silver order IDs not present in Bronze:** `0`
# MAGIC
# MAGIC Therefore, the Bronze and Silver order populations match exactly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Automation Readiness
# MAGIC
# MAGIC Critical data-quality failures raise exceptions and are therefore
# MAGIC capable of failing the corresponding Databricks Workflow task.
# MAGIC
# MAGIC Known source-data lifecycle anomalies are retained and flagged rather
# MAGIC than causing unnecessary record deletion.
# MAGIC
# MAGIC The persisted Silver table is a managed Delta table registered as:
# MAGIC
# MAGIC `workspace.silver.orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Downstream Dependencies
# MAGIC
# MAGIC The Silver Orders table will participate in downstream relationships
# MAGIC with:
# MAGIC
# MAGIC - `workspace.silver.customers`
# MAGIC - `workspace.silver.order_items`
# MAGIC - `workspace.silver.order_payments`
# MAGIC - `workspace.silver.reviews`
# MAGIC
# MAGIC Referential-integrity checks will be performed once the required
# MAGIC related Silver tables are available.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Status
# MAGIC
# MAGIC **SUCCESS — Silver Orders transformation, validation, persistence,
# MAGIC and technical quality checks completed successfully.**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 27 — Final Silver Orders Preview
# MAGIC
# MAGIC Review the final persisted Silver Orders records.

# COMMAND ----------

final_orders_preview_df = spark.table(
    "workspace.silver.orders"
)

display(
    final_orders_preview_df.limit(20)
)

# COMMAND ----------

