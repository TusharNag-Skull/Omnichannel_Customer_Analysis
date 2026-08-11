# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Order Items
# MAGIC
# MAGIC ### Notebook: 03_Bronze_Order_Items
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist Order Items dataset from AWS S3 into the
# MAGIC Databricks Bronze layer.
# MAGIC
# MAGIC The Bronze layer preserves the source dataset and does not apply
# MAGIC business transformations.
# MAGIC
# MAGIC This notebook performs ingestion, structural validation, and source
# MAGIC data profiling only.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC AWS S3
# MAGIC
# MAGIC File:
# MAGIC
# MAGIC olist_order_items_dataset.csv
# MAGIC
# MAGIC S3 Location:
# MAGIC
# MAGIC s3://olist-retail-project/olist_order_items_dataset.csv
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC Catalog: workspace
# MAGIC
# MAGIC Schema: bronze
# MAGIC
# MAGIC Table: order_items
# MAGIC
# MAGIC Fully Qualified Table:
# MAGIC
# MAGIC workspace.bronze.order_items
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC The natural grain of this dataset is:
# MAGIC
# MAGIC     order_id + order_item_id
# MAGIC
# MAGIC A single order can contain multiple order items.
# MAGIC
# MAGIC Therefore, `order_id` alone is NOT the primary key of this dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Responsibilities
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - Reads the raw CSV from S3.
# MAGIC - Preserves source column names.
# MAGIC - Preserves source values.
# MAGIC - Preserves the source timestamp representation.
# MAGIC - Stores the data as Delta.
# MAGIC - Performs validation and profiling.
# MAGIC
# MAGIC This notebook does NOT:
# MAGIC
# MAGIC - Remove duplicates.
# MAGIC - Rename columns.
# MAGIC - Clean NULL values.
# MAGIC - Standardize text.
# MAGIC - Convert `shipping_limit_date` to TIMESTAMP.
# MAGIC - Create `total_item_value`.
# MAGIC - Apply business rules.
# MAGIC - Add ingestion metadata.
# MAGIC - Export a second Bronze copy to S3.
# MAGIC
# MAGIC All transformation and business logic belongs in Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_PATH = 's3://omnicapstone/raw_data/olist_order_items_dataset.csv'

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "order_items"

BRONZE_FULL_TABLE = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read the Raw Dataset
# MAGIC
# MAGIC The source CSV is read directly from AWS S3.
# MAGIC
# MAGIC Schema inference is disabled so that Bronze preserves the raw CSV
# MAGIC representation consistently.
# MAGIC
# MAGIC The `shipping_limit_date` field will remain STRING in Bronze.
# MAGIC
# MAGIC Datatype standardization will be performed in Silver.

# COMMAND ----------

order_items_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

# COMMAND ----------

order_items_raw_df.printSchema()

# COMMAND ----------

display(order_items_raw_df.limit(20))

# COMMAND ----------

source_count = order_items_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("Bronze schema is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create the Bronze Delta Table
# MAGIC
# MAGIC The Bronze table is created using the source column structure.
# MAGIC
# MAGIC All columns are stored as STRING because this layer is intended to
# MAGIC preserve the raw source representation.
# MAGIC
# MAGIC No transformation is performed during table creation.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_FULL_TABLE} (
    order_id STRING,
    order_item_id STRING,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date STRING,
    price STRING,
    freight_value STRING
)
USING DELTA
""")

print(f"Created table: {BRONZE_FULL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Load the Raw Data
# MAGIC
# MAGIC The source DataFrame is written directly to the Bronze Delta table.
# MAGIC
# MAGIC No filtering, cleaning, casting, deduplication, or enrichment is
# MAGIC performed.

# COMMAND ----------

(
    order_items_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_FULL_TABLE)
)

print(f"Successfully loaded {BRONZE_FULL_TABLE}")

# COMMAND ----------

bronze_order_items_df = spark.table(BRONZE_FULL_TABLE)

display(bronze_order_items_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4 — Bronze Validation
# MAGIC
# MAGIC The following checks validate the structural integrity of the Bronze
# MAGIC table.
# MAGIC
# MAGIC These checks identify issues but do not modify the source data.
# MAGIC
# MAGIC The Bronze layer preserves the original dataset.

# COMMAND ----------

bronze_count = bronze_order_items_df.count()

print(f"Source records : {source_count:,}")
print(f"Bronze records : {bronze_count:,}")

if source_count == bronze_count:
    print("PASS — Source and Bronze row counts match.")
else:
    print("FAIL — Source and Bronze row counts do not match.")

# COMMAND ----------

bronze_order_items_df.printSchema()

# COMMAND ----------

expected_columns = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
]

actual_columns = bronze_order_items_df.columns

missing_columns = [
    column for column in expected_columns
    if column not in actual_columns
]

unexpected_columns = [
    column for column in actual_columns
    if column not in expected_columns
]

print("Expected columns :", len(expected_columns))
print("Actual columns   :", len(actual_columns))
print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if not missing_columns and not unexpected_columns:
    print("PASS — Source column structure is correct.")
else:
    print("FAIL — Column structure requires investigation.")

# COMMAND ----------

key_nulls = bronze_order_items_df.select(
    F.sum(
        F.when(F.col("order_id").isNull(), 1).otherwise(0)
    ).alias("null_order_id"),

    F.sum(
        F.when(F.col("order_item_id").isNull(), 1).otherwise(0)
    ).alias("null_order_item_id")
)

display(key_nulls)

# COMMAND ----------

duplicate_item_groups = (
    bronze_order_items_df
        .groupBy("order_id", "order_item_id")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_item_count = duplicate_item_groups.count()

print(
    f"Duplicate (order_id, order_item_id) groups: "
    f"{duplicate_item_count}"
)

if duplicate_item_count == 0:
    print("PASS — Composite business key is unique.")
else:
    print("FAIL — Duplicate composite keys detected.")

display(duplicate_item_groups.limit(20))

# COMMAND ----------

null_profile = bronze_order_items_df.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(F.col("order_id").isNull(), 1).otherwise(0)
    ).alias("null_order_id"),

    F.sum(
        F.when(F.col("order_item_id").isNull(), 1).otherwise(0)
    ).alias("null_order_item_id"),

    F.sum(
        F.when(F.col("product_id").isNull(), 1).otherwise(0)
    ).alias("null_product_id"),

    F.sum(
        F.when(F.col("seller_id").isNull(), 1).otherwise(0)
    ).alias("null_seller_id"),

    F.sum(
        F.when(F.col("shipping_limit_date").isNull(), 1).otherwise(0)
    ).alias("null_shipping_limit_date"),

    F.sum(
        F.when(F.col("price").isNull(), 1).otherwise(0)
    ).alias("null_price"),

    F.sum(
        F.when(F.col("freight_value").isNull(), 1).otherwise(0)
    ).alias("null_freight_value")
)

display(null_profile)

# COMMAND ----------

blank_profile = bronze_order_items_df.select(
    F.sum(
        F.when(F.trim(F.col("order_id")) == "", 1).otherwise(0)
    ).alias("blank_order_id"),

    F.sum(
        F.when(F.trim(F.col("order_item_id")) == "", 1).otherwise(0)
    ).alias("blank_order_item_id"),

    F.sum(
        F.when(F.trim(F.col("product_id")) == "", 1).otherwise(0)
    ).alias("blank_product_id"),

    F.sum(
        F.when(F.trim(F.col("seller_id")) == "", 1).otherwise(0)
    ).alias("blank_seller_id"),

    F.sum(
        F.when(F.trim(F.col("shipping_limit_date")) == "", 1).otherwise(0)
    ).alias("blank_shipping_limit_date"),

    F.sum(
        F.when(F.trim(F.col("price")) == "", 1).otherwise(0)
    ).alias("blank_price"),

    F.sum(
        F.when(F.trim(F.col("freight_value")) == "", 1).otherwise(0)
    ).alias("blank_freight_value")
)

display(blank_profile)

# COMMAND ----------

price_profile = bronze_order_items_df.select(
    F.min(F.col("price").cast("double")).alias("minimum_price"),
    F.max(F.col("price").cast("double")).alias("maximum_price"),
    F.avg(F.col("price").cast("double")).alias("average_price")
)

display(price_profile)

# COMMAND ----------

freight_profile = bronze_order_items_df.select(
    F.min(F.col("freight_value").cast("double")).alias("minimum_freight"),
    F.max(F.col("freight_value").cast("double")).alias("maximum_freight"),
    F.avg(F.col("freight_value").cast("double")).alias("average_freight")
)

display(freight_profile)

# COMMAND ----------

negative_values = bronze_order_items_df.select(
    F.sum(
        F.when(
            F.col("price").cast("double") < 0,
            1
        ).otherwise(0)
    ).alias("negative_price"),

    F.sum(
        F.when(
            F.col("freight_value").cast("double") < 0,
            1
        ).otherwise(0)
    ).alias("negative_freight_value")
)

display(negative_values)

# COMMAND ----------

zero_values = bronze_order_items_df.select(
    F.sum(
        F.when(
            F.col("price").cast("double") == 0,
            1
        ).otherwise(0)
    ).alias("zero_price"),

    F.sum(
        F.when(
            F.col("freight_value").cast("double") == 0,
            1
        ).otherwise(0)
    ).alias("zero_freight_value")
)

display(zero_values)

# COMMAND ----------

items_per_order = (
    bronze_order_items_df
        .groupBy("order_id")
        .count()
        .select(
            F.min("count").alias("minimum_items_per_order"),
            F.max("count").alias("maximum_items_per_order"),
            F.avg("count").alias("average_items_per_order")
        )
)

display(items_per_order)

# COMMAND ----------

display(
    bronze_order_items_df
        .groupBy("order_item_id")
        .count()
        .orderBy("order_item_id")
)

# COMMAND ----------

invalid_shipping_dates = (
    bronze_order_items_df
        .filter(
            F.col("shipping_limit_date").isNotNull()
            &
            F.to_timestamp(
                F.col("shipping_limit_date"),
                "yyyy-MM-dd HH:mm:ss"
            ).isNull()
        )
        .count()
)

print(
    f"Invalid shipping_limit_date values: "
    f"{invalid_shipping_dates}"
)

# COMMAND ----------

display(
    bronze_order_items_df.select(
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("seller_id").alias("unique_sellers"),
        F.countDistinct("order_id").alias("unique_orders")
    )
)

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
# MAGIC # Bronze Order Items — Execution Summary
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Source:
# MAGIC
# MAGIC `olist_order_items_dataset.csv`
# MAGIC
# MAGIC S3:
# MAGIC
# MAGIC `s3://olist-retail-project/olist_order_items_dataset.csv`
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.order_items`
# MAGIC
# MAGIC ## Dataset Profile
# MAGIC
# MAGIC | Attribute | Value |
# MAGIC |---|---:|
# MAGIC | Source Records | 112,650 |
# MAGIC | Columns | 7 |
# MAGIC | Unique Orders | Validated during execution |
# MAGIC | Unique Products | Validated during execution |
# MAGIC | Unique Sellers | Validated during execution |
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC The natural grain of the table is:
# MAGIC
# MAGIC `order_id + order_item_id`
# MAGIC
# MAGIC `order_id` alone is not unique because one order can contain multiple
# MAGIC items.
# MAGIC
# MAGIC `order_item_id` alone is also not globally unique because item numbers
# MAGIC restart for each order.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Completed
# MAGIC
# MAGIC - Source-to-Bronze row count reconciliation
# MAGIC - Schema validation
# MAGIC - Column validation
# MAGIC - Composite key NULL validation
# MAGIC - Composite key duplicate validation
# MAGIC - Complete NULL analysis
# MAGIC - Blank string analysis
# MAGIC - Price profiling
# MAGIC - Freight profiling
# MAGIC - Negative price validation
# MAGIC - Negative freight validation
# MAGIC - Zero-value profiling
# MAGIC - Items-per-order profiling
# MAGIC - Order item ID distribution
# MAGIC - Shipping timestamp format validation
# MAGIC - Product/Seller/Order relationship profiling
# MAGIC - Delta table metadata inspection
# MAGIC - Delta table history inspection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Data Quality Findings
# MAGIC
# MAGIC The source Order Items dataset contains no NULL values in the seven
# MAGIC source columns.
# MAGIC
# MAGIC No duplicate `(order_id, order_item_id)` combinations were identified.
# MAGIC
# MAGIC No negative price or freight values were identified.
# MAGIC
# MAGIC The source timestamp field:
# MAGIC
# MAGIC `shipping_limit_date`
# MAGIC
# MAGIC is preserved as STRING in Bronze.
# MAGIC
# MAGIC Timestamp conversion will be performed in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important Downstream Relationships
# MAGIC
# MAGIC The Order Items table will later connect to:
# MAGIC
# MAGIC `orders.order_id`
# MAGIC
# MAGIC `products.product_id`
# MAGIC
# MAGIC `sellers.seller_id`
# MAGIC
# MAGIC These relationships will be validated in the dedicated cross-table
# MAGIC Bronze/Silver validation layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Layer Conclusion
# MAGIC
# MAGIC The raw Order Items dataset has been successfully ingested into:
# MAGIC
# MAGIC `workspace.bronze.order_items`
# MAGIC
# MAGIC The source grain and source values are preserved.
# MAGIC
# MAGIC No business transformations, cleaning, deduplication, metadata
# MAGIC columns, or derived metrics have been added.
# MAGIC
# MAGIC The table is ready for Silver-layer processing.

# COMMAND ----------

