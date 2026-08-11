# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Order Items
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook transforms the Bronze Order Items dataset into the Silver layer.
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - Standardize identifiers and source fields.
# MAGIC - Convert source fields to appropriate analytical datatypes.
# MAGIC - Preserve the source business grain.
# MAGIC - Validate required identifiers and attributes.
# MAGIC - Validate numeric values.
# MAGIC - Validate timestamp conversion.
# MAGIC - Validate relationships with Silver Orders, Products, and Sellers.
# MAGIC - Persist the cleaned dataset as a managed Delta table.
# MAGIC - Perform final quality and technical validation.
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.order_items`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.order_items`
# MAGIC
# MAGIC ## Business Grain
# MAGIC
# MAGIC One row represents one product item within an order.
# MAGIC
# MAGIC Business key:
# MAGIC
# MAGIC `(order_id, order_item_id)`
# MAGIC
# MAGIC ## Silver Transformations
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - Trim identifier fields.
# MAGIC - Convert `order_item_id` to integer.
# MAGIC - Convert `shipping_limit_date` to timestamp.
# MAGIC - Convert `price` to `DECIMAL(18,2)`.
# MAGIC - Convert `freight_value` to `DECIMAL(18,2)`.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC
# MAGIC No Gold-level aggregations or business metrics are created in this notebook.
# MAGIC
# MAGIC ## Quality Principle
# MAGIC
# MAGIC Source records are not removed simply because they contain unusual but valid source values.
# MAGIC
# MAGIC Examples such as zero price or zero freight are investigated separately rather than automatically deleted.
# MAGIC
# MAGIC Invalid structural or business-key conditions will fail the Silver quality gate.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

BRONZE_TABLE = "workspace.bronze.order_items"
SILVER_TABLE = "workspace.silver.order_items"

ORDERS_TABLE = "workspace.silver.orders"
PRODUCTS_TABLE = "workspace.silver.products"
SELLERS_TABLE = "workspace.silver.sellers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Inspect Bronze Order Items## Step 1 — Read Bronze Order Items
# MAGIC
# MAGIC Silver reads from the existing Bronze Delta table.
# MAGIC
# MAGIC We do not read directly from S3 because Bronze is the controlled source
# MAGIC for the Silver transformation.
# MAGIC
# MAGIC The Bronze table is inspected before transformation to confirm that the expected source structure is available.
# MAGIC
# MAGIC No business transformation is performed at this stage.

# COMMAND ----------

bronze_order_items_df = spark.table(
    BRONZE_TABLE
)

bronze_order_items_count = (
    bronze_order_items_df.count()
)

print(
    f"Bronze Order Items rows: "
    f"{bronze_order_items_count:,}"
)

bronze_order_items_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Validate Bronze Order Items Structure
# MAGIC
# MAGIC The Bronze table must contain all columns required to construct the
# MAGIC Silver Order Items table.

# COMMAND ----------

required_order_item_columns = {
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
}

actual_order_item_columns = set(
    bronze_order_items_df.columns
)

missing_order_item_columns = (
    required_order_item_columns
    - actual_order_item_columns
)

if missing_order_item_columns:
    raise ValueError(
        "Bronze Order Items schema validation failed. "
        f"Missing columns: {sorted(missing_order_item_columns)}"
    )

print(
    "PASS — Bronze Order Items contains all required columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Validate Bronze Business Key
# MAGIC
# MAGIC The business grain is:
# MAGIC
# MAGIC (order_id, order_item_id)
# MAGIC
# MAGIC Both fields must be populated and the combination must be unique.

# COMMAND ----------

bronze_null_keys = (
    bronze_order_items_df
    .filter(
        F.col("order_id").isNull()
        | F.col("order_item_id").isNull()
        | (F.trim(F.col("order_id")) == "")
        | (F.trim(F.col("order_item_id")) == "")
    )
    .count()
)

bronze_duplicate_keys = (
    bronze_order_items_df
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"NULL/blank business keys : {bronze_null_keys}"
)

print(
    f"Duplicate business keys  : {bronze_duplicate_keys}"
)

if bronze_null_keys != 0:
    raise ValueError(
        "Bronze Order Items quality gate failed: "
        "NULL or blank business keys found."
    )

if bronze_duplicate_keys != 0:
    raise ValueError(
        "Bronze Order Items quality gate failed: "
        "Duplicate (order_id, order_item_id) keys found."
    )

print(
    "PASS — Bronze Order Items business key is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Validate Bronze-to-Silver Type Conversions
# MAGIC
# MAGIC Bronze stores several fields as STRING.
# MAGIC
# MAGIC Silver requires:
# MAGIC
# MAGIC order_item_id       → INT
# MAGIC shipping_limit_date → TIMESTAMP
# MAGIC price               → DECIMAL(18,2)
# MAGIC freight_value       → DECIMAL(18,2)
# MAGIC
# MAGIC try_cast is used to detect malformed numeric values safely.

# COMMAND ----------

conversion_check_df = (
    bronze_order_items_df
    .select(
        "order_id",
        "order_item_id",
        "shipping_limit_date",
        "price",
        "freight_value",

        F.expr(
            "try_cast(trim(order_item_id) AS INT)"
        ).alias(
            "order_item_id_cast"
        ),

        F.to_timestamp(
            F.trim(
                F.col("shipping_limit_date")
            )
        ).alias(
            "shipping_limit_date_cast"
        ),

        F.expr(
            "try_cast(trim(price) AS DECIMAL(18,2))"
        ).alias(
            "price_cast"
        ),

        F.expr(
            "try_cast(trim(freight_value) AS DECIMAL(18,2))"
        ).alias(
            "freight_value_cast"
        )
    )
)

# COMMAND ----------

conversion_error_df = (
    conversion_check_df
    .filter(
        (
            F.col("order_item_id").isNotNull()
            & (F.trim(F.col("order_item_id")) != "")
            & F.col("order_item_id_cast").isNull()
        )
        |
        (
            F.col("shipping_limit_date").isNotNull()
            & (F.trim(F.col("shipping_limit_date")) != "")
            & F.col("shipping_limit_date_cast").isNull()
        )
        |
        (
            F.col("price").isNotNull()
            & (F.trim(F.col("price")) != "")
            & F.col("price_cast").isNull()
        )
        |
        (
            F.col("freight_value").isNotNull()
            & (F.trim(F.col("freight_value")) != "")
            & F.col("freight_value_cast").isNull()
        )
    )
)

conversion_error_count = (
    conversion_error_df.count()
)

print(
    f"Invalid conversion records: "
    f"{conversion_error_count:,}"
)

# COMMAND ----------

if conversion_error_count != 0:

    display(
        conversion_error_df
        .select(
            "order_id",
            "order_item_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        )
        .limit(50)
    )

    raise ValueError(
        "Silver Order Items conversion validation failed."
    )

print(
    "PASS — All Order Items fields convert successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create Silver Order Items
# MAGIC
# MAGIC Apply Silver-level standardization.
# MAGIC
# MAGIC No business records are intentionally filtered.

# COMMAND ----------

silver_order_items_df = (
    bronze_order_items_df
    .select(

        F.trim(
            F.col("order_id")
        ).alias(
            "order_id"
        ),

        F.expr(
            "try_cast(trim(order_item_id) AS INT)"
        ).alias(
            "order_item_id"
        ),

        F.trim(
            F.col("product_id")
        ).alias(
            "product_id"
        ),

        F.trim(
            F.col("seller_id")
        ).alias(
            "seller_id"
        ),

        F.to_timestamp(
            F.trim(
                F.col("shipping_limit_date")
            )
        ).alias(
            "shipping_limit_date"
        ),

        F.expr(
            "try_cast(trim(price) AS DECIMAL(18,2))"
        ).alias(
            "price"
        ),

        F.expr(
            "try_cast(trim(freight_value) AS DECIMAL(18,2))"
        ).alias(
            "freight_value"
        )
    )
    .withColumn(
        "silver_load_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

silver_order_items_df.printSchema()

# COMMAND ----------

display(
    silver_order_items_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Validate Silver Business Rules
# MAGIC
# MAGIC Rules:
# MAGIC
# MAGIC - order_item_id must be positive.
# MAGIC - price cannot be negative.
# MAGIC - freight_value cannot be negative.
# MAGIC - shipping_limit_date must be populated.

# COMMAND ----------

silver_business_rule_errors = (
    silver_order_items_df
    .filter(
        F.col("order_item_id").isNull()
        | (F.col("order_item_id") < 1)
        | F.col("price").isNull()
        | (F.col("price") < 0)
        | F.col("freight_value").isNull()
        | (F.col("freight_value") < 0)
        | F.col("shipping_limit_date").isNull()
    )
    .count()
)

print(
    f"Silver business-rule violations: "
    f"{silver_business_rule_errors:,}"
)

# COMMAND ----------

if silver_business_rule_errors != 0:

    display(
        silver_order_items_df
        .filter(
            F.col("order_item_id").isNull()
            | (F.col("order_item_id") < 1)
            | F.col("price").isNull()
            | (F.col("price") < 0)
            | F.col("freight_value").isNull()
            | (F.col("freight_value") < 0)
            | F.col("shipping_limit_date").isNull()
        )
        .limit(50)
    )

    raise ValueError(
        "Silver Order Items business-rule validation failed."
    )

print(
    "PASS — Silver Order Items business rules are valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

print(
    "Silver schema is ready."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Write Silver Order Items
# MAGIC
# MAGIC The existing table is replaced with the corrected Silver definition.

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.order_items
""")

(
    silver_order_items_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.silver.order_items"
    )
)

print(
    "Silver Order Items table created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Read Persisted Silver Order Items
# MAGIC
# MAGIC All important validations should operate on the persisted Delta table.

# COMMAND ----------

final_silver_order_items_df = spark.table(
    SILVER_TABLE
)

silver_order_items_count = (
    final_silver_order_items_df.count()
)

print(
    f"Silver Order Items rows: "
    f"{silver_order_items_count:,}"
)

final_silver_order_items_df.printSchema()

# COMMAND ----------

print(
    f"Bronze Order Items rows : "
    f"{bronze_order_items_count:,}"
)

print(
    f"Silver Order Items rows : "
    f"{silver_order_items_count:,}"
)

if (
    bronze_order_items_count
    != silver_order_items_count
):
    raise ValueError(
        "Silver Order Items quality gate failed: "
        "Bronze and Silver row counts do not match."
    )

print(
    "PASS — Bronze and Silver Order Items counts match."
)

# COMMAND ----------

silver_null_keys = (
    final_silver_order_items_df
    .filter(
        F.col("order_id").isNull()
        | F.col("order_item_id").isNull()
    )
    .count()
)

silver_duplicate_keys = (
    final_silver_order_items_df
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"NULL business keys : {silver_null_keys}"
)

print(
    f"Duplicate keys     : {silver_duplicate_keys}"
)

if silver_null_keys != 0:
    raise ValueError(
        "Silver Order Items quality gate failed: "
        "NULL business keys found."
    )

if silver_duplicate_keys != 0:
    raise ValueError(
        "Silver Order Items quality gate failed: "
        "Duplicate business keys found."
    )

print(
    "PASS — Silver Order Items grain is valid."
)

# COMMAND ----------

required_field_errors = (
    final_silver_order_items_df
    .filter(
        F.col("order_id").isNull()
        | F.col("order_item_id").isNull()
        | F.col("product_id").isNull()
        | F.col("seller_id").isNull()
        | F.col("shipping_limit_date").isNull()
        | F.col("price").isNull()
        | F.col("freight_value").isNull()
        | F.col("silver_load_timestamp").isNull()
    )
    .count()
)

print(
    f"Required-field errors: "
    f"{required_field_errors}"
)

if required_field_errors != 0:
    raise ValueError(
        "Silver Order Items required-field validation failed."
    )

print(
    "PASS — Required Silver fields are populated."
)

# COMMAND ----------

invalid_numeric_records = (
    final_silver_order_items_df
    .filter(
        (F.col("order_item_id") < 1)
        | (F.col("price") < 0)
        | (F.col("freight_value") < 0)
    )
    .count()
)

print(
    f"Invalid numeric records: "
    f"{invalid_numeric_records}"
)

if invalid_numeric_records != 0:
    raise ValueError(
        "Silver Order Items numeric validation failed."
    )

print(
    "PASS — Numeric and monetary values are valid."
)

# COMMAND ----------

expected_order_item_types = {
    "order_id": "string",
    "order_item_id": "int",
    "product_id": "string",
    "seller_id": "string",
    "shipping_limit_date": "timestamp",
    "price": "decimal(18,2)",
    "freight_value": "decimal(18,2)",
    "silver_load_timestamp": "timestamp"
}

actual_order_item_types = dict(
    final_silver_order_items_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_order_item_types.get(column)
    }
    for column, expected_type
    in expected_order_item_types.items()
    if actual_order_item_types.get(column)
    != expected_type
}

if datatype_errors:
    raise ValueError(
        "Silver Order Items datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Silver Order Items datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Validate Exact Bronze/Silver Population
# MAGIC
# MAGIC The complete business-key population must be identical between Bronze
# MAGIC and Silver.

# COMMAND ----------

bronze_order_item_keys = (
    bronze_order_items_df
    .select(
        F.trim(
            F.col("order_id")
        ).alias("order_id"),

        F.expr(
            "try_cast(trim(order_item_id) AS INT)"
        ).alias("order_item_id")
    )
    .distinct()
)

silver_order_item_keys = (
    final_silver_order_items_df
    .select(
        "order_id",
        "order_item_id"
    )
    .distinct()
)

# COMMAND ----------

bronze_keys_missing_in_silver = (
    bronze_order_item_keys
    .join(
        silver_order_item_keys,
        ["order_id", "order_item_id"],
        "left_anti"
    )
    .count()
)

silver_keys_not_in_bronze = (
    silver_order_item_keys
    .join(
        bronze_order_item_keys,
        ["order_id", "order_item_id"],
        "left_anti"
    )
    .count()
)

print(
    f"Bronze keys missing in Silver : "
    f"{bronze_keys_missing_in_silver}"
)

print(
    f"Silver keys not in Bronze     : "
    f"{silver_keys_not_in_bronze}"
)

if bronze_keys_missing_in_silver != 0:
    raise ValueError(
        "Bronze Order Items keys are missing from Silver."
    )

if silver_keys_not_in_bronze != 0:
    raise ValueError(
        "Silver Order Items contains keys not present in Bronze."
    )

print(
    "PASS — Bronze and Silver populations match exactly."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Load Related Silver Tables
# MAGIC
# MAGIC Order Items has relationships with:
# MAGIC
# MAGIC - Silver Orders
# MAGIC - Silver Products
# MAGIC - Silver Sellers

# COMMAND ----------

silver_orders_df = (
    spark.table(
        ORDERS_TABLE
    )
    .select(
        "order_id"
    )
    .distinct()
)

silver_products_df = (
    spark.table(
        PRODUCTS_TABLE
    )
    .select(
        "product_id"
    )
    .distinct()
)

silver_sellers_df = (
    spark.table(
        SELLERS_TABLE
    )
    .select(
        "seller_id"
    )
    .distinct()
)

# COMMAND ----------

orphan_orders = (
    final_silver_order_items_df
    .join(
        silver_orders_df,
        "order_id",
        "left_anti"
    )
    .count()
)

orphan_products = (
    final_silver_order_items_df
    .join(
        silver_products_df,
        "product_id",
        "left_anti"
    )
    .count()
)

orphan_sellers = (
    final_silver_order_items_df
    .join(
        silver_sellers_df,
        "seller_id",
        "left_anti"
    )
    .count()
)

print(
    f"Orphan order references   : {orphan_orders}"
)

print(
    f"Orphan product references : {orphan_products}"
)

print(
    f"Orphan seller references  : {orphan_sellers}"
)

# COMMAND ----------

if (
    orphan_orders != 0
    or orphan_products != 0
    or orphan_sellers != 0
):
    raise ValueError(
        "Silver Order Items referential integrity failed."
    )

print(
    "PASS — Order Items referential integrity is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Analyze Shipping Timeline
# MAGIC
# MAGIC The shipping limit date should normally occur on or after the order
# MAGIC purchase timestamp.
# MAGIC
# MAGIC Source anomalies are retained rather than silently deleted.

# COMMAND ----------

shipping_before_purchase_df = (
    final_silver_order_items_df
    .join(
        spark.table(
            ORDERS_TABLE
        ).select(
            "order_id",
            "order_purchase_timestamp"
        ),
        "order_id",
        "left"
    )
    .filter(
        F.col("order_purchase_timestamp").isNotNull()
        & F.col("shipping_limit_date").isNotNull()
        & (
            F.col("shipping_limit_date")
            < F.col("order_purchase_timestamp")
        )
    )
)

shipping_before_purchase_count = (
    shipping_before_purchase_df.count()
)

print(
    f"Shipping limit before purchase: "
    f"{shipping_before_purchase_count:,}"
)

if shipping_before_purchase_count > 0:
    print(
        "WARNING — Timeline anomalies detected."
    )
else:
    print(
        "PASS — No shipping timeline anomalies detected."
    )

# COMMAND ----------

display(
    shipping_before_purchase_df
    .select(
        "order_id",
        "order_item_id",
        "shipping_limit_date",
        "order_purchase_timestamp",
        "price",
        "freight_value"
    )
    .orderBy(
        "shipping_limit_date"
    )
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 — Final Silver Order Items Quality Gate
# MAGIC
# MAGIC All critical validations are evaluated against the persisted Silver table.
# MAGIC
# MAGIC If any critical validation fails, the notebook fails.

# COMMAND ----------

print("=" * 75)
print("SILVER ORDER ITEMS QUALITY GATE")
print("=" * 75)

print(
    f"Bronze rows               : "
    f"{bronze_order_items_count:,}"
)

print(
    f"Silver rows               : "
    f"{silver_order_items_count:,}"
)

print(
    f"NULL business keys        : "
    f"{silver_null_keys}"
)

print(
    f"Duplicate business keys   : "
    f"{silver_duplicate_keys}"
)

print(
    f"Required-field errors     : "
    f"{required_field_errors}"
)

print(
    f"Numeric errors            : "
    f"{invalid_numeric_records}"
)

print(
    f"Datatype errors           : "
    f"{len(datatype_errors)}"
)

print(
    f"Missing Bronze keys       : "
    f"{bronze_keys_missing_in_silver}"
)

print(
    f"Unexpected Silver keys    : "
    f"{silver_keys_not_in_bronze}"
)

print(
    f"Orphan Orders             : "
    f"{orphan_orders}"
)

print(
    f"Orphan Products           : "
    f"{orphan_products}"
)

print(
    f"Orphan Sellers            : "
    f"{orphan_sellers}"
)

print("=" * 75)

# COMMAND ----------

if (
    silver_order_items_count
    != bronze_order_items_count

    or silver_null_keys
    != 0

    or silver_duplicate_keys
    != 0

    or required_field_errors
    != 0

    or invalid_numeric_records
    != 0

    or len(datatype_errors)
    != 0

    or bronze_keys_missing_in_silver
    != 0

    or silver_keys_not_in_bronze
    != 0

    or orphan_orders
    != 0

    or orphan_products
    != 0

    or orphan_sellers
    != 0
):
    raise ValueError(
        "SILVER ORDER ITEMS QUALITY GATE FAILED."
    )

print(
    "STATUS: ALL SILVER ORDER ITEMS CHECKS PASSED"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 — Silver Order Items Monetary Profile
# MAGIC
# MAGIC Descriptive analysis only. This does not modify the Silver table.

# COMMAND ----------

display(
    final_silver_order_items_df
    .select(
        F.round(
            F.min("price"),
            2
        ).alias("min_price"),

        F.round(
            F.max("price"),
            2
        ).alias("max_price"),

        F.round(
            F.avg("price"),
            2
        ).alias("avg_price"),

        F.round(
            F.min("freight_value"),
            2
        ).alias("min_freight"),

        F.round(
            F.max("freight_value"),
            2
        ).alias("max_freight"),

        F.round(
            F.avg("freight_value"),
            2
        ).alias("avg_freight")
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.silver.order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.silver.order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

display(
    spark.table(
        "workspace.silver.order_items"
    ).limit(20)
)

# COMMAND ----------

