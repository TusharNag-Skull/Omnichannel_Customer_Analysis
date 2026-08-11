# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Orders
# MAGIC
# MAGIC ### Notebook: 02_Bronze_Orders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist Orders dataset from AWS S3 into the Databricks
# MAGIC Bronze layer.
# MAGIC
# MAGIC The Bronze layer is responsible for preserving the source dataset
# MAGIC with minimal intervention.
# MAGIC
# MAGIC No business transformations, cleaning, standardization, filtering,
# MAGIC deduplication, or derived columns are applied.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC AWS S3
# MAGIC
# MAGIC File:
# MAGIC
# MAGIC olist_orders_dataset.csv
# MAGIC
# MAGIC S3 Location:
# MAGIC
# MAGIC s3://olist-retail-project/olist_orders_dataset.csv
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC Catalog: workspace
# MAGIC
# MAGIC Schema: bronze
# MAGIC
# MAGIC Table: orders
# MAGIC
# MAGIC Fully Qualified Table:
# MAGIC
# MAGIC workspace.bronze.orders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Principles
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - Reads the source CSV from S3.
# MAGIC - Preserves the original column names.
# MAGIC - Preserves the source values.
# MAGIC - Preserves timestamp fields as STRING.
# MAGIC - Stores the dataset as a Delta table.
# MAGIC - Performs validation and profiling only.
# MAGIC
# MAGIC This notebook does NOT:
# MAGIC
# MAGIC - Clean NULL values.
# MAGIC - Remove duplicates.
# MAGIC - Rename columns.
# MAGIC - Standardize text.
# MAGIC - Convert timestamps to TIMESTAMP.
# MAGIC - Apply business rules.
# MAGIC - Create derived business columns.
# MAGIC - Add ingestion metadata.
# MAGIC - Export another copy of the Bronze table to S3.
# MAGIC
# MAGIC All data transformations will be performed in the Silver layer.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_PATH ='s3://omnicapstone/raw_data/olist_orders_dataset.csv'

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "orders"

BRONZE_FULL_TABLE = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read the Raw Orders Dataset
# MAGIC
# MAGIC The source CSV is read directly from AWS S3.
# MAGIC
# MAGIC Schema inference is intentionally disabled.
# MAGIC
# MAGIC The source dataset contains timestamp-looking values, but these
# MAGIC remain STRING in Bronze because Bronze is intended to preserve the
# MAGIC source representation.
# MAGIC
# MAGIC The Silver layer will perform explicit datatype conversion and
# MAGIC timestamp validation.

# COMMAND ----------

orders_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

# COMMAND ----------

orders_raw_df.printSchema()

# COMMAND ----------

display(orders_raw_df.limit(20))

# COMMAND ----------

source_count = orders_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create the Bronze Schema
# MAGIC
# MAGIC The Bronze schema is created inside the existing Unity Catalog
# MAGIC workspace catalog.
# MAGIC
# MAGIC Unity Catalog namespace:
# MAGIC
# MAGIC workspace.bronze.orders
# MAGIC
# MAGIC The Bronze schema contains the raw source tables for the project.

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("Bronze schema is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create the Bronze Orders Table
# MAGIC
# MAGIC The Bronze table is created as a Delta table.
# MAGIC
# MAGIC The table structure explicitly defines all source columns as STRING.
# MAGIC
# MAGIC This is intentional.
# MAGIC
# MAGIC Although several source fields contain timestamps, the raw textual
# MAGIC representation is preserved in Bronze.
# MAGIC
# MAGIC Timestamp conversion will occur in Silver.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_FULL_TABLE} (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp STRING,
    order_approved_at STRING,
    order_delivered_carrier_date STRING,
    order_delivered_customer_date STRING,
    order_estimated_delivery_date STRING
)
USING DELTA
""")

print(f"Created table: {BRONZE_FULL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Load the Source Data
# MAGIC
# MAGIC The source dataset is written into the Bronze Delta table.
# MAGIC
# MAGIC No filtering or transformation is performed.
# MAGIC
# MAGIC The source DataFrame and Bronze table therefore remain at the same
# MAGIC source grain and record count.

# COMMAND ----------

(
    orders_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_FULL_TABLE)
)

print(f"Successfully loaded {BRONZE_FULL_TABLE}")

# COMMAND ----------

bronze_orders_df = spark.table(BRONZE_FULL_TABLE)

display(bronze_orders_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 — Bronze Validation
# MAGIC
# MAGIC The following checks verify that the Bronze table was loaded
# MAGIC correctly.
# MAGIC
# MAGIC Validation does not modify the data.
# MAGIC
# MAGIC Any data quality issue identified here is documented for downstream
# MAGIC Silver-layer handling.

# COMMAND ----------

bronze_count = bronze_orders_df.count()

print(f"Source records : {source_count:,}")
print(f"Bronze records : {bronze_count:,}")

if source_count == bronze_count:
    print("PASS — Source and Bronze row counts match.")
else:
    print("FAIL — Source and Bronze row counts do not match.")

# COMMAND ----------

bronze_orders_df.printSchema()

# COMMAND ----------

expected_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

actual_columns = bronze_orders_df.columns

missing_columns = [c for c in expected_columns if c not in actual_columns]
unexpected_columns = [c for c in actual_columns if c not in expected_columns]

print("Expected columns :", len(expected_columns))
print("Actual columns   :", len(actual_columns))

print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if not missing_columns and not unexpected_columns:
    print("PASS — Column structure matches the expected source structure.")
else:
    print("FAIL — Column structure requires investigation.")

# COMMAND ----------

bronze_orders_df.select(
    F.sum(
        F.when(F.col("order_id").isNull(), 1).otherwise(0)
    ).alias("null_order_id")
).show()

# COMMAND ----------

duplicate_order_ids = (
    bronze_orders_df
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_count = duplicate_order_ids.count()

print(f"Duplicate order_id groups: {duplicate_count}")

if duplicate_count == 0:
    print("PASS — order_id is unique.")
else:
    print("FAIL — Duplicate order_id values detected.")

display(duplicate_order_ids.limit(20))

# COMMAND ----------

bronze_orders_df.select(
    F.sum(
        F.when(F.col("customer_id").isNull(), 1).otherwise(0)
    ).alias("null_customer_id")
).show()

# COMMAND ----------

order_status_distribution = (
    bronze_orders_df
        .groupBy("order_status")
        .count()
        .orderBy(F.desc("count"))
)

display(order_status_distribution)

# COMMAND ----------

null_profile = bronze_orders_df.select(
    F.count("*").alias("total_rows"),

    F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0))
        .alias("null_order_id"),

    F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0))
        .alias("null_customer_id"),

    F.sum(F.when(F.col("order_status").isNull(), 1).otherwise(0))
        .alias("null_order_status"),

    F.sum(F.when(F.col("order_purchase_timestamp").isNull(), 1).otherwise(0))
        .alias("null_order_purchase_timestamp"),

    F.sum(F.when(F.col("order_approved_at").isNull(), 1).otherwise(0))
        .alias("null_order_approved_at"),

    F.sum(F.when(F.col("order_delivered_carrier_date").isNull(), 1).otherwise(0))
        .alias("null_order_delivered_carrier_date"),

    F.sum(F.when(F.col("order_delivered_customer_date").isNull(), 1).otherwise(0))
        .alias("null_order_delivered_customer_date"),

    F.sum(F.when(F.col("order_estimated_delivery_date").isNull(), 1).otherwise(0))
        .alias("null_order_estimated_delivery_date")
)

display(null_profile)

# COMMAND ----------

blank_profile = bronze_orders_df.select(
    F.sum(
        F.when(F.trim(F.col("order_id")) == "", 1).otherwise(0)
    ).alias("blank_order_id"),

    F.sum(
        F.when(F.trim(F.col("customer_id")) == "", 1).otherwise(0)
    ).alias("blank_customer_id"),

    F.sum(
        F.when(F.trim(F.col("order_status")) == "", 1).otherwise(0)
    ).alias("blank_order_status"),

    F.sum(
        F.when(F.trim(F.col("order_purchase_timestamp")) == "", 1).otherwise(0)
    ).alias("blank_order_purchase_timestamp"),

    F.sum(
        F.when(F.trim(F.col("order_approved_at")) == "", 1).otherwise(0)
    ).alias("blank_order_approved_at"),

    F.sum(
        F.when(F.trim(F.col("order_delivered_carrier_date")) == "", 1).otherwise(0)
    ).alias("blank_order_delivered_carrier_date"),

    F.sum(
        F.when(F.trim(F.col("order_delivered_customer_date")) == "", 1).otherwise(0)
    ).alias("blank_order_delivered_customer_date"),

    F.sum(
        F.when(F.trim(F.col("order_estimated_delivery_date")) == "", 1).otherwise(0)
    ).alias("blank_order_estimated_delivery_date")
)

display(blank_profile)

# COMMAND ----------

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in timestamp_columns:

    invalid_count = (
        bronze_orders_df
            .filter(
                F.col(column_name).isNotNull()
                & (
                    F.to_timestamp(
                        F.col(column_name),
                        "yyyy-MM-dd HH:mm:ss"
                    ).isNull()
                )
            )
            .count()
    )

    print(f"{column_name}: invalid timestamp strings = {invalid_count}")

# COMMAND ----------

for column_name in timestamp_columns:

    print(f"\n===== {column_name} =====")

    (
        bronze_orders_df
            .select(
                F.min(column_name).alias("minimum_value"),
                F.max(column_name).alias("maximum_value")
            )
            .show()
    )

# COMMAND ----------

orders_with_timestamps = bronze_orders_df.select(
    "*",

    F.to_timestamp(
        "order_purchase_timestamp",
        "yyyy-MM-dd HH:mm:ss"
    ).alias("_purchase_ts"),

    F.to_timestamp(
        "order_approved_at",
        "yyyy-MM-dd HH:mm:ss"
    ).alias("_approved_ts"),

    F.to_timestamp(
        "order_delivered_carrier_date",
        "yyyy-MM-dd HH:mm:ss"
    ).alias("_carrier_ts"),

    F.to_timestamp(
        "order_delivered_customer_date",
        "yyyy-MM-dd HH:mm:ss"
    ).alias("_customer_delivery_ts"),

    F.to_timestamp(
        "order_estimated_delivery_date",
        "yyyy-MM-dd HH:mm:ss"
    ).alias("_estimated_delivery_ts")
)

# COMMAND ----------

lifecycle_validation = orders_with_timestamps.select(
    F.sum(
        F.when(
            F.col("_approved_ts").isNotNull()
            & (F.col("_approved_ts") < F.col("_purchase_ts")),
            1
        ).otherwise(0)
    ).alias("approval_before_purchase"),

    F.sum(
        F.when(
            F.col("_carrier_ts").isNotNull()
            & (F.col("_carrier_ts") < F.col("_purchase_ts")),
            1
        ).otherwise(0)
    ).alias("carrier_before_purchase"),

    F.sum(
        F.when(
            F.col("_customer_delivery_ts").isNotNull()
            & (F.col("_customer_delivery_ts") < F.col("_purchase_ts")),
            1
        ).otherwise(0)
    ).alias("customer_delivery_before_purchase"),

    F.sum(
        F.when(
            F.col("_customer_delivery_ts").isNotNull()
            & F.col("_carrier_ts").isNotNull()
            & (F.col("_customer_delivery_ts") < F.col("_carrier_ts")),
            1
        ).otherwise(0)
    ).alias("customer_delivery_before_carrier")
)

display(lifecycle_validation)

# COMMAND ----------

display(
    bronze_orders_df
        .groupBy("order_status")
        .agg(
            F.count("*").alias("total_orders"),

            F.sum(
                F.when(
                    F.col("order_delivered_customer_date").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("orders_with_delivery_date"),

            F.sum(
                F.when(
                    F.col("order_delivered_carrier_date").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("orders_with_carrier_date")
        )
        .orderBy(F.desc("total_orders"))
)

# COMMAND ----------

bronze_orders_df.select(
    F.countDistinct("customer_id").alias("unique_customer_ids")
).show()

# COMMAND ----------

spark.sql(f"""
DESCRIBE DETAIL {BRONZE_FULL_TABLE}
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
DESCRIBE HISTORY {BRONZE_FULL_TABLE}
""").show(truncate=False)

# COMMAND ----------

display(
    spark.table(BRONZE_FULL_TABLE).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Orders — Execution Summary
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Source:
# MAGIC
# MAGIC olist_orders_dataset.csv
# MAGIC
# MAGIC S3:
# MAGIC
# MAGIC s3://olist-retail-project/olist_orders_dataset.csv
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC workspace.bronze.orders
# MAGIC
# MAGIC ## Source Characteristics
# MAGIC
# MAGIC Records:
# MAGIC
# MAGIC 99,441
# MAGIC
# MAGIC Columns:
# MAGIC
# MAGIC 8
# MAGIC
# MAGIC Primary Business Key:
# MAGIC
# MAGIC order_id
# MAGIC
# MAGIC Customer Reference:
# MAGIC
# MAGIC customer_id
# MAGIC
# MAGIC ## Validation Completed
# MAGIC
# MAGIC - Source row count reconciliation
# MAGIC - Schema validation
# MAGIC - Column validation
# MAGIC - Primary key NULL validation
# MAGIC - Primary key duplicate validation
# MAGIC - Customer ID NULL validation
# MAGIC - Order status profiling
# MAGIC - NULL profiling
# MAGIC - Blank string analysis
# MAGIC - Timestamp format profiling
# MAGIC - Timestamp range profiling
# MAGIC - Order lifecycle anomaly analysis
# MAGIC - Order status versus delivery profiling
# MAGIC - Distinct customer analysis
# MAGIC - Delta table metadata inspection
# MAGIC - Delta history inspection
# MAGIC
# MAGIC ## Timestamp Handling
# MAGIC
# MAGIC The following columns remain STRING in Bronze:
# MAGIC
# MAGIC - order_purchase_timestamp
# MAGIC - order_approved_at
# MAGIC - order_delivered_carrier_date
# MAGIC - order_delivered_customer_date
# MAGIC - order_estimated_delivery_date
# MAGIC
# MAGIC This is intentional.
# MAGIC
# MAGIC The raw source representation is preserved in Bronze.
# MAGIC
# MAGIC Explicit timestamp conversion, invalid-date handling, lifecycle
# MAGIC validation, delivery-duration calculations, and business rules will
# MAGIC be implemented in the Silver layer.
# MAGIC
# MAGIC ## Actual Profiling Results
# MAGIC
# MAGIC ### Order Status Distribution
# MAGIC
# MAGIC | Order Status | Records |
# MAGIC |--------------|--------:|
# MAGIC | delivered | 96,478 |
# MAGIC | shipped | 1,107 |
# MAGIC | canceled | 625 |
# MAGIC | unavailable | 609 |
# MAGIC | invoiced | 314 |
# MAGIC | processing | 301 |
# MAGIC | created | 5 |
# MAGIC | approved | 2 |
# MAGIC | **Total** | **99,441** |
# MAGIC
# MAGIC The `delivered` status represents the overwhelming majority of the
# MAGIC dataset, followed by `shipped`.
# MAGIC
# MAGIC The less frequent statuses are retained because Bronze preserves the
# MAGIC complete source dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### NULL Analysis
# MAGIC
# MAGIC | Column | NULL Records |
# MAGIC |--------|-------------:|
# MAGIC | order_id | 0 |
# MAGIC | customer_id | 0 |
# MAGIC | order_status | 0 |
# MAGIC | order_purchase_timestamp | 0 |
# MAGIC | order_approved_at | 160 |
# MAGIC | order_delivered_carrier_date | 1,783 |
# MAGIC | order_delivered_customer_date | 2,965 |
# MAGIC | order_estimated_delivery_date | 0 |
# MAGIC
# MAGIC The NULL values occur primarily in order lifecycle timestamps.
# MAGIC
# MAGIC These values are retained in Bronze and will be handled according to
# MAGIC Silver-layer business rules.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Order Lifecycle Analysis
# MAGIC
# MAGIC | Validation | Records |
# MAGIC |------------|--------:|
# MAGIC | Approval before purchase | 0 |
# MAGIC | Carrier before purchase | 166 |
# MAGIC | Customer delivery before purchase | 0 |
# MAGIC | Customer delivery before carrier | 23 |
# MAGIC
# MAGIC No orders were identified with approval occurring before purchase.
# MAGIC
# MAGIC A small number of chronological anomalies were identified involving
# MAGIC carrier and customer delivery timestamps.
# MAGIC
# MAGIC These records are retained in Bronze because Bronze does not correct
# MAGIC source data.
# MAGIC
# MAGIC They require investigation during Silver processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Order Status and Delivery Analysis
# MAGIC
# MAGIC | Status | Total Orders | With Delivery Date | With Carrier Date |
# MAGIC |--------|-------------:|-------------------:|------------------:|
# MAGIC | delivered | 96,478 | 96,470 | 96,476 |
# MAGIC | shipped | 1,107 | 0 | 1,107 |
# MAGIC | canceled | 625 | 6 | 75 |
# MAGIC | unavailable | 609 | 0 | 0 |
# MAGIC | invoiced | 314 | 0 | 0 |
# MAGIC | processing | 301 | 0 | 0 |
# MAGIC | created | 5 | 0 | 0 |
# MAGIC | approved | 2 | 0 | 0 |
# MAGIC
# MAGIC The relationship between order status and lifecycle timestamps should
# MAGIC be considered when defining Silver-layer business rules.
# MAGIC
# MAGIC For example, not every `delivered` order has a populated customer
# MAGIC delivery timestamp. Therefore, Silver validation must identify such
# MAGIC records rather than assuming that every status has complete lifecycle
# MAGIC timestamps.
# MAGIC
# MAGIC ## Bronze Layer Conclusion
# MAGIC
# MAGIC The raw Orders dataset has been successfully ingested into the
# MAGIC Unity Catalog Bronze layer as a Delta table.
# MAGIC
# MAGIC No business transformations have been applied.
# MAGIC
# MAGIC The table is ready to serve as the source for Silver Orders
# MAGIC transformation.

# COMMAND ----------

