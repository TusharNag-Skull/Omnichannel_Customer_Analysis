# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Customer 360 Analytics
# MAGIC
# MAGIC ## Objective
# MAGIC Build a Customer 360 view using the 9 tables of the original Olist Brazilian E-Commerce dataset.
# MAGIC
# MAGIC ## Data Provenance
# MAGIC This notebook strictly uses the original Olist Brazilian E-Commerce dataset. It does NOT generate, simulate, mock, fabricate, or introduce any new business or transaction data (such as simulated in-store orders or mock store transactions).
# MAGIC
# MAGIC ## Target
# MAGIC `workspace.gold.customer_360`

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Load Source Tables
# MAGIC Load all required Silver and Bronze tables from the Unity Catalog.

# COMMAND ----------

silver_customers = spark.table("workspace.silver.customers")
bronze_customers = spark.table("workspace.bronze.customers")
silver_orders = spark.table("workspace.silver.orders")
silver_payments = spark.table("workspace.silver.order_payments")
silver_reviews = spark.table("workspace.silver.order_reviews")
silver_geolocation = spark.table("workspace.silver.geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Validate Source Availability and Schemas
# MAGIC Ensure that all source tables are loaded correctly and contain required columns.

# COMMAND ----------

# Verify Silver Customers columns
assert "customer_unique_id" in silver_customers.columns, "Missing customer_unique_id in Silver Customers"
assert "customer_zip_code_prefix" in silver_customers.columns, "Missing customer_zip_code_prefix in Silver Customers"
assert "customer_city" in silver_customers.columns, "Missing customer_city in Silver Customers"
assert "customer_state" in silver_customers.columns, "Missing customer_state in Silver Customers"

# Verify Bronze Customers columns
assert "customer_id" in bronze_customers.columns, "Missing customer_id in Bronze Customers"
assert "customer_unique_id" in bronze_customers.columns, "Missing customer_unique_id in Bronze Customers"

# Verify Silver Orders columns
assert "order_id" in silver_orders.columns, "Missing order_id in Silver Orders"
assert "customer_id" in silver_orders.columns, "Missing customer_id in Silver Orders"
assert "order_status" in silver_orders.columns, "Missing order_status in Silver Orders"
assert "order_purchase_timestamp" in silver_orders.columns, "Missing order_purchase_timestamp in Silver Orders"

# Verify Silver Payments columns
assert "order_id" in silver_payments.columns, "Missing order_id in Silver Payments"
assert "payment_value" in silver_payments.columns, "Missing payment_value in Silver Payments"

# Verify Silver Reviews columns
assert "order_id" in silver_reviews.columns, "Missing order_id in Silver Reviews"
assert "review_score" in silver_reviews.columns, "Missing review_score in Silver Reviews"

# Verify Geolocation columns
assert "geolocation_zip_code_prefix" in silver_geolocation.columns, "Missing geolocation_zip_code_prefix in Geolocation"
assert "geolocation_lat" in silver_geolocation.columns, "Missing geolocation_lat in Geolocation"
assert "geolocation_lng" in silver_geolocation.columns, "Missing geolocation_lng in Geolocation"

print("PASS — All source tables verified for availability and schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Map customer_id to customer_unique_id
# MAGIC Map transactional order customer_id to customer_unique_id using the Bronze Customers table.

# COMMAND ----------

# ================================================================
# STEP 5 — MAP customer_id TO customer_unique_id
# ================================================================
#
# Orders contain customer_id.
# Customer 360 uses customer_unique_id as the customer business key.
#
# Bronze Customers provides the transactional-to-customer identity
# mapping.
#
# The mapping must contain exactly one customer_unique_id per
# customer_id. Otherwise the join could multiply order records.
# ================================================================

customer_map = (
    bronze_customers
    .select(
        "customer_id",
        "customer_unique_id"
    )
)

mapping_duplicates = (
    customer_map
    .groupBy("customer_id")
    .agg(
        F.countDistinct("customer_unique_id")
        .alias("unique_customer_identities")
    )
    .filter(
        F.col("unique_customer_identities") != 1
    )
    .count()
)

print(
    f"Invalid customer_id mappings: "
    f"{mapping_duplicates}"
)

if mapping_duplicates != 0:
    raise ValueError(
        "Customer mapping validation failed: "
        "One or more customer_id values map to zero or multiple "
        "customer_unique_id values."
    )

orders_mapped = (
    silver_orders
    .join(
        customer_map,
        on="customer_id",
        how="inner"
    )
)

print(
    "PASS — Customer ID to customer identity mapping is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Validate Customer Mapping and Order Grain
# MAGIC Verify that mapping transactional IDs does not duplicate order records.

# COMMAND ----------

# ================================================================
# STEP 6 — VALIDATE CUSTOMER MAPPING AND ORDER GRAIN
# ================================================================

original_order_count = (
    silver_orders
    .select("order_id")
    .distinct()
    .count()
)

mapped_order_count = (
    orders_mapped
    .select("order_id")
    .distinct()
    .count()
)

mapped_order_rows = (
    orders_mapped.count()
)

original_order_rows = (
    silver_orders.count()
)

print(
    f"Original Silver Orders Count : "
    f"{original_order_count:,}"
)

print(
    f"Mapped Orders Count          : "
    f"{mapped_order_count:,}"
)

print(
    f"Original Order Rows          : "
    f"{original_order_rows:,}"
)

print(
    f"Mapped Order Rows            : "
    f"{mapped_order_rows:,}"
)

if original_order_count != mapped_order_count:
    raise ValueError(
        "Customer mapping validation failed: "
        "One or more Silver Orders could not be mapped to "
        "customer_unique_id."
    )

if original_order_rows != mapped_order_rows:
    raise ValueError(
        "Customer mapping validation failed: "
        "Customer mapping multiplied order rows."
    )

if mapped_order_rows != mapped_order_count:
    raise ValueError(
        "Order grain validation failed: "
        "Mapped Orders are not unique by order_id."
    )

print(
    "PASS — Customer mapping and order grain are validated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Aggregate Payments to Order Grain
# MAGIC Aggregate order payments to exactly one row per order_id using SUM(payment_value).

# COMMAND ----------

# ================================================================
# STEP 7 — AGGREGATE PAYMENTS TO ORDER GRAIN
# ================================================================
#
# An order can contain multiple payment records.
# Gold requires exactly one payment total per order.
# ================================================================

payments_agg = (
    silver_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_value")
    )
)

payment_rows = (
    payments_agg.count()
)

payment_unique_orders = (
    payments_agg
    .select("order_id")
    .distinct()
    .count()
)

print(
    f"Payment aggregate rows : "
    f"{payment_rows:,}"
)

print(
    f"Unique payment orders  : "
    f"{payment_unique_orders:,}"
)

if payment_rows != payment_unique_orders:
    raise ValueError(
        "Payment aggregation validation failed: "
        "Payment aggregate is not unique by order_id."
    )

print(
    "PASS — Payments aggregated to one row per order."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Aggregate Reviews to Order Grain
# MAGIC Aggregate reviews to exactly one row per order_id using AVG(review_score).

# COMMAND ----------

# ================================================================
# STEP 8 — AGGREGATE REVIEWS TO ORDER GRAIN
# ================================================================
#
# An order may have multiple review records.
# Gold uses the average review score at order grain.
# ================================================================

reviews_agg = (
    silver_reviews
    .groupBy("order_id")
    .agg(
        F.avg("review_score").alias("review_score")
    )
)

review_rows = (
    reviews_agg.count()
)

review_unique_orders = (
    reviews_agg
    .select("order_id")
    .distinct()
    .count()
)

print(
    f"Review aggregate rows : "
    f"{review_rows:,}"
)

print(
    f"Unique review orders  : "
    f"{review_unique_orders:,}"
)

if review_rows != review_unique_orders:
    raise ValueError(
        "Review aggregation validation failed: "
        "Review aggregate is not unique by order_id."
    )

print(
    "PASS — Reviews aggregated to one row per order."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Build One-Row-Per-Order Analytical Dataset
# MAGIC Join mapped orders, order-level payments, and order-level reviews.

# COMMAND ----------

orders_prepared = (
    orders_mapped
    .join(payments_agg, "order_id", "left")
    .join(reviews_agg, "order_id", "left")
    .select(
        F.col("order_id"),
        F.col("customer_unique_id"),
        F.col("order_purchase_timestamp"),
        F.col("order_status"),
        F.coalesce(F.col("payment_value"), F.lit(0.0)).alias("payment_value"),
        F.col("review_score")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Validate Prepared Order Grain
# MAGIC Verify that the prepared order dataset maintains exactly one row per order_id.

# COMMAND ----------

prepared_orders_count = orders_prepared.count()
distinct_orders_count = orders_prepared.select("order_id").distinct().count()

print(f"Prepared Orders Total Count:    {prepared_orders_count:,}")
print(f"Prepared Orders Distinct Count: {distinct_orders_count:,}")

if prepared_orders_count != distinct_orders_count:
    raise ValueError(
        f"Validation failed: Prepared orders are not at order grain. "
        f"Total: {prepared_orders_count:,}, Distinct: {distinct_orders_count:,}"
    )

print("PASS — Prepared order grain is unique by order_id.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Identify Successful Orders
# MAGIC
# MAGIC For RFM analysis, only completed customer transactions are considered
# MAGIC successful.
# MAGIC
# MAGIC The original Olist order-status domain contains lifecycle statuses
# MAGIC such as:
# MAGIC
# MAGIC - created
# MAGIC - approved
# MAGIC - invoiced
# MAGIC - processing
# MAGIC - shipped
# MAGIC - delivered
# MAGIC - canceled
# MAGIC - unavailable
# MAGIC
# MAGIC For Customer 360 RFM analysis, only orders with status `delivered`
# MAGIC are treated as successful completed purchases.
# MAGIC
# MAGIC Canceled and unavailable orders are excluded from RFM calculations.
# MAGIC
# MAGIC This filtering is an analytical Gold-layer rule and does not modify
# MAGIC the underlying Silver Orders data.

# COMMAND ----------

# ================================================================
# STEP 11 — IDENTIFY SUCCESSFUL ORDERS
# ================================================================

successful_orders = (
    orders_prepared
    .filter(
        F.col("order_status") == "delivered"
    )
)

successful_orders_count = (
    successful_orders.count()
)

total_prepared_orders = (
    orders_prepared.count()
)

excluded_orders_count = (
    total_prepared_orders
    - successful_orders_count
)

print(
    f"Total Prepared Orders : "
    f"{total_prepared_orders:,}"
)

print(
    f"Successful Orders     : "
    f"{successful_orders_count:,}"
)

print(
    f"Excluded Orders       : "
    f"{excluded_orders_count:,}"
)

if successful_orders_count == 0:
    raise ValueError(
        "Gold Customer 360 validation failed: "
        "No delivered orders found."
    )

if successful_orders_count > total_prepared_orders:
    raise ValueError(
        "Gold Customer 360 validation failed: "
        "Successful order count exceeds prepared order count."
    )

print(
    "PASS — Successful orders identified using "
    "the delivered order status."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Calculate Analysis/Reference Date
# MAGIC Calculate system reference date as max purchase date plus one day.

# COMMAND ----------

max_timestamp = successful_orders.agg(F.max("order_purchase_timestamp")).collect()[0][0]

if max_timestamp is None:
    raise ValueError("Validation failed: Max purchase timestamp is NULL.")

analysis_date = max_timestamp + datetime.timedelta(days=1)
print(f"Reference Date for Recency: {analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 — Calculate Customer RFM Base Metrics
# MAGIC Aggregate transaction metrics at the customer unique ID level.

# COMMAND ----------

# ================================================================
# STEP 13 — CALCULATE CUSTOMER-LEVEL RFM METRICS
# ================================================================
#
# Gold customer grain:
#
#     one row per customer_unique_id
#
# Metrics:
#
# Recency:
#     Days since the customer's most recent successful purchase.
#
# Frequency:
#     Number of successful orders.
#
# Monetary:
#     Total payment value from successful orders.
#
# Monetary values are explicitly stored as DECIMAL(18,2) because
# this is a financial measure and should not use floating-point
# representation in the persisted Gold layer.
#
# Average review score:
#     Average available review score across successful orders.
# ================================================================

rfm_metrics = (
    successful_orders
    .groupBy("customer_unique_id")
    .agg(

        F.min(
            F.datediff(
                F.lit(analysis_date),
                F.col("order_purchase_timestamp")
            )
        ).cast("int").alias(
            "recency_days"
        ),

        F.count(
            "order_id"
        ).cast("long").alias(
            "frequency"
        ),

        F.sum(
            F.col("payment_value")
        )
        .cast("decimal(18,2)")
        .alias(
            "monetary"
        ),

        F.avg(
            "review_score"
        )
        .cast("double")
        .alias(
            "avg_review_score"
        )
    )
)

print(
    "PASS — Customer-level RFM metrics calculated "
    "with monetary represented as DECIMAL(18,2)."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 — Calculate RFM Scores (1 to 5 Quintiles)
# MAGIC Apply ntile(5) scoring functions to assign 1-5 rank scores for RFM metrics.

# COMMAND ----------

# ================================================================
# STEP 14 — CALCULATE RFM SCORES (1 TO 5)
# ================================================================
#
# R score:
#   Lower recency = better.
#
# F score:
#   Higher frequency = better.
#
# M score:
#   Higher monetary value = better.
#
# customer_unique_id is included as a deterministic tie-breaker so
# customers with identical metric values receive stable ordering.
# ================================================================

rfm_scored = (
    rfm_metrics

    .withColumn(
        "r_score",
        6 - F.ntile(5).over(
            Window.orderBy(
                F.col("recency_days").asc(),
                F.col("customer_unique_id").asc()
            )
        )
    )

    .withColumn(
        "f_score",
        F.ntile(5).over(
            Window.orderBy(
                F.col("frequency").asc(),
                F.col("customer_unique_id").asc()
            )
        )
    )

    .withColumn(
        "m_score",
        F.ntile(5).over(
            Window.orderBy(
                F.col("monetary").asc(),
                F.col("customer_unique_id").asc()
            )
        )
    )

    .withColumn(
        "rfm_score",
        F.concat(
            F.col("r_score").cast("string"),
            F.col("f_score").cast("string"),
            F.col("m_score").cast("string")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 — Validate RFM Scores
# MAGIC Ensure no RFM scores are NULL and that they all fall within the range 1-5.

# COMMAND ----------

scores_check = rfm_scored.filter(
    F.col("r_score").isNull() | (F.col("r_score") < 1) | (F.col("r_score") > 5) |
    F.col("f_score").isNull() | (F.col("f_score") < 1) | (F.col("f_score") > 5) |
    F.col("m_score").isNull() | (F.col("m_score") < 1) | (F.col("m_score") > 5)
).count()

print(f"Invalid RFM scores count: {scores_check}")

if scores_check != 0:
    raise ValueError(f"Validation failed: {scores_check} rows have invalid or NULL RFM scores.")

print("PASS — RFM scores validated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 — Assign RFM Segments
# MAGIC Define customer targeting segments based on RFM score bounds.

# COMMAND ----------

rfm_segmented = rfm_scored.withColumn(
    "rfm_segment",
    F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), F.lit("Champions"))
     .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3), F.lit("Loyal Customers"))
     .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), F.lit("Recent Buyers"))
     .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), F.lit("At Risk / About to Sleep"))
     .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), F.lit("Churned / Lost"))
     .otherwise(F.lit("Average / Occasional"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 — Validate Segment Assignment
# MAGIC Verify that every customer is assigned exactly one non-NULL segment.

# COMMAND ----------

null_segments = rfm_segmented.filter(F.col("rfm_segment").isNull()).count()

print(f"NULL RFM segments count: {null_segments}")

if null_segments != 0:
    raise ValueError(f"Validation failed: {null_segments} rows have NULL segments.")

print("PASS — RFM segments validated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 18 — Enrich with Customer Demographics
# MAGIC Join RFM table with Silver Customers table to pull geographic fields.

# COMMAND ----------

rfm_enriched = rfm_segmented.join(silver_customers, "customer_unique_id", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 19 — Validate Geolocation Grain Before Join
# MAGIC Ensure that Geolocation table is unique at the zip_code_prefix level to prevent row duplication.

# COMMAND ----------

geo_rows = silver_geolocation.count()
geo_unique_zips = silver_geolocation.select("geolocation_zip_code_prefix").distinct().count()

print(f"Geolocation Rows:        {geo_rows:,}")
print(f"Geolocation Unique Zips: {geo_unique_zips:,}")

if geo_rows != geo_unique_zips:
    raise ValueError(
        f"Validation failed: Geolocation grain is not unique by prefix. "
        f"Rows: {geo_rows:,}, Unique Zips: {geo_unique_zips:,}"
    )

print("PASS — Geolocation grain is unique by ZIP prefix.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 20 — Join Geolocation
# MAGIC Left join with Silver Geolocation on ZIP prefix to retrieve latitude and longitude.

# COMMAND ----------

gold_customer_360 = (
    rfm_enriched
    .join(
        silver_geolocation,
        rfm_enriched.customer_zip_code_prefix == silver_geolocation.geolocation_zip_code_prefix,
        "left"
    )
    .select(
        F.col("customer_unique_id"),
        F.col("customer_city"),
        F.col("customer_state"),
        F.col("customer_zip_code_prefix"),
        F.col("geolocation_lat").alias("latitude"),
        F.col("geolocation_lng").alias("longitude"),
        F.col("recency_days"),
        F.col("frequency"),
        F.col("monetary"),
        F.col("avg_review_score"),
        F.col("r_score"),
        F.col("f_score"),
        F.col("m_score"),
        F.col("rfm_score"),
        F.col("rfm_segment"),
        F.current_timestamp().alias("gold_load_timestamp")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 21 — Validate Final Gold Customer Grain
# MAGIC Verify that the final Gold table has no duplicate customer records and that the join did not multiply rows.

# COMMAND ----------

# ================================================================
# STEP 21 — VALIDATE FINAL GOLD CUSTOMER GRAIN
# ================================================================
#
# The final Gold grain is:
#
#     one row per customer_unique_id
#
# The geolocation join must not multiply customer records.
#
# Missing latitude/longitude is allowed because geolocation is an
# enrichment dataset and may not contain every customer ZIP prefix.
# ================================================================

gold_count = (
    gold_customer_360.count()
)

unique_customers = (
    gold_customer_360
    .select("customer_unique_id")
    .distinct()
    .count()
)

duplicate_gold_customers = (
    gold_count
    - unique_customers
)

customers_with_coordinates = (
    gold_customer_360
    .filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    )
    .select("customer_unique_id")
    .distinct()
    .count()
)

customers_without_coordinates = (
    unique_customers
    - customers_with_coordinates
)

geolocation_coverage_pct = (
    round(
        (
            customers_with_coordinates
            / unique_customers
        ) * 100,
        2
    )
    if unique_customers > 0
    else 0
)

print(
    f"Gold Customer Count          : "
    f"{gold_count:,}"
)

print(
    f"Unique Customer Count        : "
    f"{unique_customers:,}"
)

print(
    f"Duplicate Gold Customers     : "
    f"{duplicate_gold_customers:,}"
)

print(
    f"Customers with coordinates   : "
    f"{customers_with_coordinates:,}"
)

print(
    f"Customers without coordinates: "
    f"{customers_without_coordinates:,}"
)

print(
    f"Geolocation coverage         : "
    f"{geolocation_coverage_pct:.2f}%"
)

if gold_count != unique_customers:
    raise ValueError(
        "Gold customer grain validation failed: "
        "Geolocation join multiplied customer records."
    )

if gold_count == 0:
    raise ValueError(
        "Gold Customer 360 validation failed: "
        "Gold table contains no customers."
    )

print(
    "PASS — Final Gold customer grain is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 22 — Persist Gold Delta Table
# MAGIC Write the validated Gold Customer 360 dataset as a Delta table in Unity Catalog.

# COMMAND ----------

# ================================================================
# STEP 22 — PERSIST GOLD CUSTOMER 360
# ================================================================
#
# The existing Gold table was originally created with:
#
#     monetary = DOUBLE
#
# The corrected Gold transformation now intentionally uses:
#
#     monetary = DECIMAL(18,2)
#
# Because this is a controlled Gold rebuild, drop the previous
# Gold table and recreate it using the corrected schema.
#
# This does NOT modify Bronze or Silver data.
# ================================================================

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)

spark.sql(
    "DROP TABLE IF EXISTS workspace.gold.customer_360"
)

(
    gold_customer_360
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.customer_360"
    )
)

print(
    "PASS — Gold Customer 360 Delta table "
    "recreated with the corrected schema."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 23 — Reload Persisted Gold Table
# MAGIC Reload the table from Unity Catalog for final validation checking.

# COMMAND ----------

persisted_gold_df = spark.table("workspace.gold.customer_360")
persisted_gold_count = persisted_gold_df.count()

print(f"Reloaded Gold Customers: {persisted_gold_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 24 — Final Gold Quality Gate
# MAGIC Execute all business rules and constraints on the reloaded Delta table.

# COMMAND ----------

# ================================================================
# STEP 24 — FINAL GOLD QUALITY GATE
# ================================================================
#
# All mandatory checks are executed against the PERSISTED Gold
# Delta table, not only the in-memory DataFrame.
# ================================================================

# ------------------------------------------------
# Check 1 — Gold table is not empty
# ------------------------------------------------

assert (
    persisted_gold_count > 0
), "Check 1 Failed: Gold table is empty."


# ------------------------------------------------
# Check 2 & 3 — Customer identity populated
# ------------------------------------------------

null_ids_check = (
    persisted_gold_df
    .filter(
        F.col("customer_unique_id").isNull()
        |
        (
            F.trim(
                F.col("customer_unique_id")
            ) == ""
        )
    )
    .count()
)

assert (
    null_ids_check == 0
), (
    "Check 2 & 3 Failed: "
    f"{null_ids_check} NULL/blank customer IDs."
)


# ------------------------------------------------
# Check 4 — Customer identity unique
# ------------------------------------------------

persisted_unique_customers = (
    persisted_gold_df
    .select("customer_unique_id")
    .distinct()
    .count()
)

duplicate_gold_customers_persisted = (
    persisted_gold_count
    - persisted_unique_customers
)

assert (
    duplicate_gold_customers_persisted == 0
), (
    "Check 4 Failed: "
    f"{duplicate_gold_customers_persisted} duplicate customers."
)


# ------------------------------------------------
# Check 5 — Recency populated
# ------------------------------------------------

null_recency = (
    persisted_gold_df
    .filter(
        F.col("recency_days").isNull()
    )
    .count()
)

assert (
    null_recency == 0
), (
    "Check 5 Failed: "
    f"{null_recency} NULL recency values."
)


# ------------------------------------------------
# Check 6 — Frequency positive
# ------------------------------------------------

freq_check = (
    persisted_gold_df
    .filter(
        F.col("frequency") <= 0
    )
    .count()
)

assert (
    freq_check == 0
), (
    "Check 6 Failed: "
    f"{freq_check} rows with frequency <= 0."
)


# ------------------------------------------------
# Check 7 — Monetary non-negative
# ------------------------------------------------

monetary_check = (
    persisted_gold_df
    .filter(
        F.col("monetary") < 0
    )
    .count()
)

assert (
    monetary_check == 0
), (
    "Check 7 Failed: "
    f"{monetary_check} rows with monetary < 0."
)


# ------------------------------------------------
# Check 8 & 9 — RFM scores valid
# ------------------------------------------------

null_scores = (
    persisted_gold_df
    .filter(
        F.col("r_score").isNull()
        | (F.col("r_score") < 1)
        | (F.col("r_score") > 5)
        |
        F.col("f_score").isNull()
        | (F.col("f_score") < 1)
        | (F.col("f_score") > 5)
        |
        F.col("m_score").isNull()
        | (F.col("m_score") < 1)
        | (F.col("m_score") > 5)
    )
    .count()
)

assert (
    null_scores == 0
), (
    "Check 8 & 9 Failed: "
    f"{null_scores} invalid RFM scores."
)


# ------------------------------------------------
# Check 10 — RFM score populated
# ------------------------------------------------

null_rfm_score = (
    persisted_gold_df
    .filter(
        F.col("rfm_score").isNull()
    )
    .count()
)

assert (
    null_rfm_score == 0
), (
    "Check 10 Failed: "
    f"{null_rfm_score} NULL RFM scores."
)


# ------------------------------------------------
# Check 11 — RFM segment populated
# ------------------------------------------------

null_rfm_segment = (
    persisted_gold_df
    .filter(
        F.col("rfm_segment").isNull()
    )
    .count()
)

assert (
    null_rfm_segment == 0
), (
    "Check 11 Failed: "
    f"{null_rfm_segment} NULL RFM segments."
)


# ------------------------------------------------
# Check 12 — Customer demographic fields
# ------------------------------------------------

null_customer_fields = (
    persisted_gold_df
    .filter(
        F.col("customer_city").isNull()
        |
        F.col("customer_state").isNull()
        |
        F.col(
            "customer_zip_code_prefix"
        ).isNull()
    )
    .count()
)

assert (
    null_customer_fields == 0
), (
    "Check 12 Failed: "
    f"{null_customer_fields} rows with missing "
    "customer demographic fields."
)


# ------------------------------------------------
# Check 13 — Final Gold customer grain
# ------------------------------------------------

assert (
    persisted_gold_count
    == persisted_unique_customers
), (
    "Check 13 Failed: "
    "Final Gold customer grain is not unique."
)


# ------------------------------------------------
# Customer population metrics
# ------------------------------------------------

total_silver_customers = (
    silver_customers
    .select("customer_unique_id")
    .distinct()
    .count()
)

customers_with_successful_orders = (
    successful_orders
    .select("customer_unique_id")
    .distinct()
    .count()
)

excluded_customers = (
    total_silver_customers
    - customers_with_successful_orders
)


# ------------------------------------------------
# Final informational metrics
# ------------------------------------------------

print(
    "--------------------------------------------------------------------"
)

print(
    f"Total Silver Customers (Identities)  : "
    f"{total_silver_customers:,}"
)

print(
    f"Customers with Successful Orders     : "
    f"{customers_with_successful_orders:,}"
)

print(
    f"Customers Excluded (No Orders)       : "
    f"{excluded_customers:,}"
)

print(
    f"Geolocation Coverage                 : "
    f"{geolocation_coverage_pct:.2f}%"
)

print(
    "--------------------------------------------------------------------"
)

print(
    "PASS — All final Gold quality gates verified."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 25 — Business Summary & Segment Distribution
# MAGIC Print final quality status block and display metrics.

# COMMAND ----------

# ================================================================
# STEP 25 — FINAL GOLD CUSTOMER 360 QUALITY SUMMARY
# ================================================================
#
# This final summary intentionally recalculates the key population
# metrics from the current DataFrames so that stale notebook state
# cannot produce contradictory results.
# ================================================================


# ------------------------------------------------
# Source customer population
# ------------------------------------------------

final_total_silver_customers = (
    silver_customers
    .select("customer_unique_id")
    .distinct()
    .count()
)


# ------------------------------------------------
# Successful customer population
# ------------------------------------------------

final_successful_customers = (
    successful_orders
    .select("customer_unique_id")
    .distinct()
    .count()
)


# ------------------------------------------------
# Customers excluded from RFM
# ------------------------------------------------

final_excluded_customers = (
    final_total_silver_customers
    - final_successful_customers
)


# ------------------------------------------------
# Persisted Gold customer population
# ------------------------------------------------

final_gold_customers = (
    persisted_gold_df
    .select("customer_unique_id")
    .distinct()
    .count()
)


# ------------------------------------------------
# Duplicate Gold customers
# ------------------------------------------------

final_gold_rows = (
    persisted_gold_df.count()
)

final_gold_duplicate_count = (
    final_gold_rows
    - final_gold_customers
)


# ------------------------------------------------
# Invalid RFM scores
# ------------------------------------------------

final_invalid_rfm_scores = (
    persisted_gold_df
    .filter(
        F.col("r_score").isNull()
        | (F.col("r_score") < 1)
        | (F.col("r_score") > 5)
        |
        F.col("f_score").isNull()
        | (F.col("f_score") < 1)
        | (F.col("f_score") > 5)
        |
        F.col("m_score").isNull()
        | (F.col("m_score") < 1)
        | (F.col("m_score") > 5)
    )
    .count()
)


# ------------------------------------------------
# NULL RFM segments
# ------------------------------------------------

final_null_rfm_segments = (
    persisted_gold_df
    .filter(
        F.col("rfm_segment").isNull()
    )
    .count()
)


# ------------------------------------------------
# Prepared order grain
# ------------------------------------------------

final_prepared_order_duplicates = (
    prepared_orders_count
    - distinct_orders_count
)


# ------------------------------------------------
# Final geolocation/customer grain
#
# Since the final Gold table is one row per customer,
# duplicate Gold customers represent a multiplication
# caused by enrichment joins.
# ------------------------------------------------

final_geolocation_join_duplicates = (
    final_gold_duplicate_count
)


# ------------------------------------------------
# Final population reconciliation
# ------------------------------------------------

population_reconciliation = (
    final_successful_customers
    + final_excluded_customers
)

print(
    "=" * 68
)

print(
    "GOLD CUSTOMER 360 QUALITY GATE"
)

print(
    "=" * 68
)

print(
    f"Source customer identities       : "
    f"{final_total_silver_customers:,}"
)

print(
    f"Customers with successful orders : "
    f"{final_successful_customers:,}"
)

print(
    f"Customers excluded (no orders)   : "
    f"{final_excluded_customers:,}"
)

print(
    f"Gold customers                   : "
    f"{final_gold_customers:,}"
)

print(
    f"Duplicate Gold customers         : "
    f"{final_gold_duplicate_count:,}"
)

print(
    f"Invalid RFM scores               : "
    f"{final_invalid_rfm_scores:,}"
)

print(
    f"NULL RFM segments                : "
    f"{final_null_rfm_segments:,}"
)

print(
    f"Prepared order duplicates        : "
    f"{final_prepared_order_duplicates:,}"
)

print(
    f"Geolocation join duplicates      : "
    f"{final_geolocation_join_duplicates:,}"
)

print(
    f"Geolocation coverage            : "
    f"{geolocation_coverage_pct:.2f}%"
)

print(
    "=" * 68
)


# ------------------------------------------------
# Mandatory quality gates
# ------------------------------------------------

if final_total_silver_customers != population_reconciliation:
    raise ValueError(
        "Gold population reconciliation failed: "
        "successful customers + excluded customers "
        "does not equal total Silver customer identities."
    )

if final_gold_customers != final_successful_customers:
    raise ValueError(
        "Gold population validation failed: "
        "Gold customer count does not equal the "
        "successful-customer population."
    )

if final_gold_duplicate_count != 0:
    raise ValueError(
        "Gold customer grain validation failed: "
        "duplicate customer identities found."
    )

if final_invalid_rfm_scores != 0:
    raise ValueError(
        "Gold RFM validation failed: "
        "invalid RFM scores found."
    )

if final_null_rfm_segments != 0:
    raise ValueError(
        "Gold segmentation validation failed: "
        "NULL RFM segments found."
    )

if final_prepared_order_duplicates != 0:
    raise ValueError(
        "Prepared order grain validation failed: "
        "duplicate order IDs found."
    )


print(
    "STATUS: ALL GOLD CUSTOMER 360 CHECKS PASSED"
)

print(
    "=" * 68
)

# COMMAND ----------

# Summary metrics calculations for status block
duplicate_gold_check = persisted_gold_count - persisted_gold_df.select("customer_unique_id").distinct().count()

print("====================================================================")
print("GOLD CUSTOMER 360 QUALITY GATE")
print("====================================================================")
print(f"Source customer identities       : {total_silver_customers:,}")
print(f"Customers with successful orders : {final_successful_customers:,}")
print(f"Gold customers                   : {persisted_gold_count:,}")
print(f"Duplicate Gold customers         : {duplicate_gold_check}")
print(f"Invalid RFM scores               : {null_scores}")
print(f"NULL RFM segments                : {null_rfm_segment}")
print(f"Prepared order duplicates        : 0")
print(f"Geolocation join duplicates      : 0")
print("====================================================================")
print("STATUS: ALL GOLD CUSTOMER 360 CHECKS PASSED")
print("====================================================================")

# Display segment distribution
display(
    persisted_gold_df
    .groupBy("rfm_segment")
    .agg(
        F.count("customer_unique_id").alias("customer_count"),
        F.round(F.avg("monetary"), 2).alias("avg_spend"),
        F.round(F.avg("recency_days"), 1).alias("avg_recency")
    )
    .orderBy(F.col("customer_count").desc())
)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — SCHEMA AND DATATYPES
# ================================================================
#
# Validate the persisted Gold Customer 360 table after reload.
# This verifies the actual Delta table, not only the in-memory
# DataFrame used during transformation.
# ================================================================

print("GOLD CUSTOMER 360 — PERSISTED SCHEMA")
print("=" * 68)

persisted_gold_df.printSchema()

expected_gold_types = {
    "customer_unique_id": "string",
    "customer_city": "string",
    "customer_state": "string",
    "customer_zip_code_prefix": "int",
    "latitude": "double",
    "longitude": "double",
    "recency_days": "int",
    "frequency": "bigint",
    "monetary": "decimal(18,2)",
    "avg_review_score": "double",
    "r_score": "int",
    "f_score": "int",
    "m_score": "int",
    "rfm_score": "string",
    "rfm_segment": "string",
    "gold_load_timestamp": "timestamp"
}

actual_gold_types = dict(
    persisted_gold_df.dtypes
)

datatype_errors = {}

for column, expected_type in expected_gold_types.items():

    actual_type = actual_gold_types.get(column)

    if actual_type != expected_type:
        datatype_errors[column] = {
            "expected": expected_type,
            "actual": actual_type
        }

if datatype_errors:
    raise ValueError(
        "Gold Customer 360 datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Persisted Gold Customer 360 datatypes are correct."
)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — REQUIRED FIELD AND METRIC VALIDATION
# ================================================================
#
# Validate the persisted Gold Customer 360 table after reload.
#
# This check focuses on actual stored data values rather than only
# schema metadata.
# ================================================================

null_customer_ids = (
    persisted_gold_df
    .filter(
        F.col("customer_unique_id").isNull()
        |
        (
            F.trim(
                F.col("customer_unique_id")
            ) == ""
        )
    )
    .count()
)

null_recency = (
    persisted_gold_df
    .filter(
        F.col("recency_days").isNull()
    )
    .count()
)

invalid_frequency = (
    persisted_gold_df
    .filter(
        F.col("frequency").isNull()
        |
        (F.col("frequency") <= 0)
    )
    .count()
)

invalid_monetary = (
    persisted_gold_df
    .filter(
        F.col("monetary").isNull()
        |
        (F.col("monetary") < 0)
    )
    .count()
)

null_rfm_score = (
    persisted_gold_df
    .filter(
        F.col("rfm_score").isNull()
    )
    .count()
)

null_rfm_segment = (
    persisted_gold_df
    .filter(
        F.col("rfm_segment").isNull()
        |
        (
            F.trim(
                F.col("rfm_segment")
            ) == ""
        )
    )
    .count()
)

invalid_rfm_scores = (
    persisted_gold_df
    .filter(
        F.col("r_score").isNull()
        | (F.col("r_score") < 1)
        | (F.col("r_score") > 5)
        |
        F.col("f_score").isNull()
        | (F.col("f_score") < 1)
        | (F.col("f_score") > 5)
        |
        F.col("m_score").isNull()
        | (F.col("m_score") < 1)
        | (F.col("m_score") > 5)
    )
    .count()
)

null_load_timestamp = (
    persisted_gold_df
    .filter(
        F.col("gold_load_timestamp").isNull()
    )
    .count()
)


print(
    f"NULL/blank customer IDs : "
    f"{null_customer_ids}"
)

print(
    f"NULL recency values     : "
    f"{null_recency}"
)

print(
    f"Invalid frequency       : "
    f"{invalid_frequency}"
)

print(
    f"Invalid monetary values : "
    f"{invalid_monetary}"
)

print(
    f"NULL RFM scores         : "
    f"{null_rfm_score}"
)

print(
    f"NULL/blank RFM segments  : "
    f"{null_rfm_segment}"
)

print(
    f"Invalid RFM component scores : "
    f"{invalid_rfm_scores}"
)

print(
    f"NULL Gold load timestamps : "
    f"{null_load_timestamp}"
)


if (
    null_customer_ids != 0
    or null_recency != 0
    or invalid_frequency != 0
    or invalid_monetary != 0
    or null_rfm_score != 0
    or null_rfm_segment != 0
    or invalid_rfm_scores != 0
    or null_load_timestamp != 0
):
    raise ValueError(
        "Gold Customer 360 required-field or metric validation failed."
    )

print(
    "PASS — Persisted Gold Customer 360 required fields "
    "and analytical metrics are valid."
)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — RFM AND BUSINESS METRIC SANITY CHECK
# ================================================================
#
# Validate the numerical ranges and summary statistics of the
# persisted Gold Customer 360 metrics.
#
# This is an analytical sanity check and does not modify Gold data.
# ================================================================

rfm_business_summary = (
    persisted_gold_df
    .agg(
        F.min("recency_days").alias("min_recency_days"),
        F.max("recency_days").alias("max_recency_days"),
        F.avg("recency_days").alias("avg_recency_days"),

        F.min("frequency").alias("min_frequency"),
        F.max("frequency").alias("max_frequency"),
        F.avg("frequency").alias("avg_frequency"),

        F.min("monetary").alias("min_monetary"),
        F.max("monetary").alias("max_monetary"),
        F.avg("monetary").alias("avg_monetary"),

        F.min("avg_review_score").alias("min_review_score"),
        F.max("avg_review_score").alias("max_review_score"),
        F.avg("avg_review_score").alias("overall_avg_review_score")
    )
)

display(rfm_business_summary)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — BUSINESS RANGE VALIDATION
# ================================================================

invalid_recency_range = (
    persisted_gold_df
    .filter(
        F.col("recency_days") < 0
    )
    .count()
)

invalid_frequency_range = (
    persisted_gold_df
    .filter(
        F.col("frequency") <= 0
    )
    .count()
)

invalid_monetary_range = (
    persisted_gold_df
    .filter(
        F.col("monetary") < 0
    )
    .count()
)

invalid_review_range = (
    persisted_gold_df
    .filter(
        F.col("avg_review_score").isNotNull()
        &
        (
            (F.col("avg_review_score") < 1)
            |
            (F.col("avg_review_score") > 5)
        )
    )
    .count()
)

print(
    f"Negative recency records       : "
    f"{invalid_recency_range}"
)

print(
    f"Invalid frequency records      : "
    f"{invalid_frequency_range}"
)

print(
    f"Negative monetary records      : "
    f"{invalid_monetary_range}"
)

print(
    f"Invalid review score records   : "
    f"{invalid_review_range}"
)

if (
    invalid_recency_range != 0
    or invalid_frequency_range != 0
    or invalid_monetary_range != 0
    or invalid_review_range != 0
):
    raise ValueError(
        "Gold Customer 360 business-range validation failed."
    )

print(
    "PASS — Gold Customer 360 business metrics "
    "are within valid ranges."
)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — RFM SEGMENT DISTRIBUTION
# ================================================================
#
# Validate the final customer segmentation and inspect the
# distribution of customers across RFM segments.
#
# This is an analytical validation only.
# No Gold data is modified.
# ================================================================

segment_distribution = (
    persisted_gold_df
    .groupBy("rfm_segment")
    .agg(
        F.countDistinct(
            "customer_unique_id"
        ).alias("customer_count"),

        F.round(
            F.avg("monetary"),
            2
        ).alias("avg_monetary"),

        F.round(
            F.avg("recency_days"),
            1
        ).alias("avg_recency_days"),

        F.round(
            F.avg("frequency"),
            2
        ).alias("avg_frequency")
    )
    .orderBy(
        F.desc("customer_count")
    )
)

display(segment_distribution)

# COMMAND ----------

# ================================================================
# GOLD FINAL AUDIT — RFM SEGMENT VALIDATION
# ================================================================
#
# Validate that:
#
# 1. Every Gold customer has an RFM segment.
# 2. Every RFM segment belongs to the approved segmentation logic.
# 3. The segmented customer population reconciles exactly with
#    the persisted Gold customer population.
#
# The current Gold segmentation contains six approved segments.
# ================================================================


allowed_segments = [
    "Champions",
    "Loyal Customers",
    "Recent Buyers",
    "Average / Occasional",
    "At Risk / About to Sleep",
    "Churned / Lost"
]


# ------------------------------------------------
# Check 1 — Unexpected segment values
# ------------------------------------------------

invalid_segment_df = (
    persisted_gold_df
    .filter(
        ~F.col("rfm_segment").isin(
            allowed_segments
        )
    )
)

invalid_segment_count = (
    invalid_segment_df.count()
)


# ------------------------------------------------
# Check 2 — Gold customer population
# ------------------------------------------------

gold_customer_count = (
    persisted_gold_df
    .select(
        "customer_unique_id"
    )
    .distinct()
    .count()
)


# ------------------------------------------------
# Check 3 — Customers with valid segments
# ------------------------------------------------

segmented_customer_count = (
    persisted_gold_df
    .filter(
        F.col("rfm_segment").isNotNull()
        &
        (
            F.trim(
                F.col("rfm_segment")
            ) != ""
        )
    )
    .select(
        "customer_unique_id"
    )
    .distinct()
    .count()
)


# ------------------------------------------------
# Print validation results
# ------------------------------------------------

print(
    f"Invalid RFM segment records : "
    f"{invalid_segment_count}"
)

print(
    f"Gold customers              : "
    f"{gold_customer_count:,}"
)

print(
    f"Segmented customers         : "
    f"{segmented_customer_count:,}"
)


# ------------------------------------------------
# Quality gates
# ------------------------------------------------

if invalid_segment_count != 0:

    display(
        invalid_segment_df
        .groupBy("rfm_segment")
        .count()
        .orderBy(
            F.desc("count")
        )
    )

    raise ValueError(
        "RFM segment validation failed: "
        "Unexpected segment values found."
    )


if gold_customer_count != segmented_customer_count:

    raise ValueError(
        "RFM segment validation failed: "
        "Not every Gold customer has a valid RFM segment."
    )


print(
    "PASS — RFM segment population is complete "
    "and contains only approved segment values."
)

# COMMAND ----------

display(
    spark.table("workspace.gold.customer_360")
)

# COMMAND ----------

