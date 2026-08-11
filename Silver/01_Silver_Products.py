# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Silver Layer — Products
# MAGIC
# MAGIC ### Notebook: 01_Silver_Products
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create a clean, standardized, analytics-ready Products dataset from
# MAGIC the Bronze Products and Category Translation tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Tables
# MAGIC
# MAGIC `workspace.bronze.products`
# MAGIC
# MAGIC `workspace.bronze.category_translation`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target Table
# MAGIC
# MAGIC `workspace.silver.products`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Purpose
# MAGIC
# MAGIC The Silver Products table will provide the standardized product
# MAGIC attributes required for downstream product analytics and Customer 360
# MAGIC analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Transformations
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - Join product categories with the English category translation.
# MAGIC - Handle the two source categories without English translations.
# MAGIC - Rename incorrectly spelled source column names.
# MAGIC - Convert numeric source fields from STRING to appropriate numeric
# MAGIC   datatypes.
# MAGIC - Preserve NULL product categories.
# MAGIC - Preserve product records.
# MAGIC - Preserve product dimensions.
# MAGIC - Add a Silver load timestamp.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1 — Preserve Products
# MAGIC
# MAGIC Every Bronze product must remain in Silver.
# MAGIC
# MAGIC No product is intentionally filtered out.
# MAGIC
# MAGIC ### Rule 2 — Category Translation
# MAGIC
# MAGIC Join:
# MAGIC
# MAGIC `product_category_name`
# MAGIC
# MAGIC with:
# MAGIC
# MAGIC `workspace.bronze.category_translation.product_category_name`
# MAGIC
# MAGIC to obtain:
# MAGIC
# MAGIC `product_category_name_english`
# MAGIC
# MAGIC ### Rule 3 — Manual Category Mapping
# MAGIC
# MAGIC `pc_gamer`
# MAGIC
# MAGIC → `Gaming PC`
# MAGIC
# MAGIC ### Rule 4 — Manual Category Mapping
# MAGIC
# MAGIC `portateis_cozinha_e_preparadores_de_alimentos`
# MAGIC
# MAGIC → `Portable Kitchen Appliances`
# MAGIC
# MAGIC ### Rule 5 — Missing Category
# MAGIC
# MAGIC Products with no product category are retained.
# MAGIC
# MAGIC Their English category is represented as:
# MAGIC
# MAGIC `Unknown Category`
# MAGIC
# MAGIC ### Rule 6 — Column Standardization
# MAGIC
# MAGIC Rename:
# MAGIC
# MAGIC `product_name_lenght`
# MAGIC
# MAGIC → `product_name_length`
# MAGIC
# MAGIC `product_description_lenght`
# MAGIC
# MAGIC → `product_description_length`
# MAGIC
# MAGIC ### Rule 7 — Datatype Standardization
# MAGIC
# MAGIC Numeric product attributes are converted from Bronze STRING
# MAGIC representation to appropriate numeric datatypes.
# MAGIC
# MAGIC ### Rule 8 — Audit Metadata
# MAGIC
# MAGIC Add:
# MAGIC
# MAGIC `silver_load_timestamp`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Quality Requirements
# MAGIC
# MAGIC The final Silver table must satisfy:
# MAGIC
# MAGIC - Bronze product count = Silver product count.
# MAGIC - `product_id` must not be NULL.
# MAGIC - `product_id` must be unique.
# MAGIC - Category translation must not create duplicate products.
# MAGIC - Numeric product fields must contain valid numeric values.
# MAGIC - The two manual category mappings must be correct.
# MAGIC - Missing product categories must remain represented.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ## Step 1 — Read Bronze Sources
# MAGIC
# MAGIC The Silver transformation starts from the Bronze Delta tables.
# MAGIC
# MAGIC The raw CSV files are not read again.
# MAGIC
# MAGIC This ensures that Silver depends on the completed Bronze layer.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_products_df = spark.table(
    "workspace.bronze.products"
)

bronze_category_translation_df = spark.table(
    "workspace.bronze.category_translation"
)

print(
    f"Bronze Products rows: "
    f"{bronze_products_df.count():,}"
)

print(
    f"Bronze Category Translation rows: "
    f"{bronze_category_translation_df.count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 — Validate Category Translation Grain
# MAGIC
# MAGIC The category translation table is used as a lookup table.
# MAGIC
# MAGIC Each source category must occur only once.
# MAGIC
# MAGIC This check is required because duplicate translation records would
# MAGIC multiply product records during the Silver join.

# COMMAND ----------

translation_duplicates = (
    bronze_category_translation_df
        .groupBy("product_category_name")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_translation_count = translation_duplicates.count()

print(
    f"Duplicate source-category mappings: "
    f"{duplicate_translation_count}"
)

if duplicate_translation_count != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "Category Translation contains duplicate source categories."
    )

print("PASS — Category Translation has one mapping per source category.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Validate Bronze Product Identifier
# MAGIC
# MAGIC Before transformation, verify that the Bronze Products table contains
# MAGIC one record per `product_id`.
# MAGIC
# MAGIC The Silver table must not introduce or remove product records.

# COMMAND ----------

bronze_product_count = bronze_products_df.count()

null_product_ids = (
    bronze_products_df
        .filter(F.col("product_id").isNull())
        .count()
)

duplicate_product_ids = (
    bronze_products_df
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

print(f"Bronze product records : {bronze_product_count:,}")
print(f"NULL product IDs       : {null_product_ids}")
print(f"Duplicate product IDs  : {duplicate_product_ids}")

if null_product_ids != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "NULL product_id values exist in Bronze."
    )

if duplicate_product_ids != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "Duplicate product_id values exist in Bronze."
    )

print("PASS — Bronze product identifier is valid.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Numeric Source Fields
# MAGIC
# MAGIC Bronze stores the source fields as STRING.
# MAGIC
# MAGIC Before converting them to numeric datatypes, verify that non-NULL
# MAGIC values can be interpreted as numbers.
# MAGIC
# MAGIC Fields checked:
# MAGIC
# MAGIC - `product_name_lenght`
# MAGIC - `product_description_lenght`
# MAGIC - `product_photos_qty`
# MAGIC - `product_weight_g`
# MAGIC - `product_length_cm`
# MAGIC - `product_height_cm`
# MAGIC - `product_width_cm`

# COMMAND ----------

numeric_columns = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

numeric_validation = bronze_products_df.select(
    *[
        F.sum(
            F.when(
                F.col(column).isNotNull()
                & F.col(column).cast("double").isNull(),
                1
            ).otherwise(0)
        ).alias(f"invalid_{column}")
        for column in numeric_columns
    ]
)

display(numeric_validation)

# COMMAND ----------

invalid_numeric_values = (
    numeric_validation
        .collect()[0]
        .asDict()
)

invalid_columns = {
    column: count
    for column, count in invalid_numeric_values.items()
    if count != 0
}

if invalid_columns:
    raise ValueError(
        "Silver Products quality gate failed. "
        f"Invalid numeric values found: {invalid_columns}"
    )

print("PASS — Numeric source fields can be converted safely.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Create Silver Products
# MAGIC
# MAGIC Apply the required Silver transformations.
# MAGIC
# MAGIC The transformation:
# MAGIC
# MAGIC - joins category translation,
# MAGIC - handles the two manual mappings,
# MAGIC - converts numeric fields,
# MAGIC - renames incorrectly spelled columns,
# MAGIC - preserves all product records,
# MAGIC - preserves NULL physical attributes,
# MAGIC - adds Silver load metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.silver.products
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC
# MAGIC     p.product_category_name,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN p.product_category_name = 'pc_gamer'
# MAGIC             THEN 'Gaming PC'
# MAGIC
# MAGIC         WHEN p.product_category_name =
# MAGIC              'portateis_cozinha_e_preparadores_de_alimentos'
# MAGIC             THEN 'Portable Kitchen Appliances'
# MAGIC
# MAGIC         WHEN p.product_category_name IS NULL
# MAGIC             THEN 'Unknown Category'
# MAGIC
# MAGIC         ELSE COALESCE(
# MAGIC             ct.product_category_name_english,
# MAGIC             'Unknown Category'
# MAGIC         )
# MAGIC     END AS product_category_name_english,
# MAGIC
# MAGIC     CAST(p.product_name_lenght AS INT)
# MAGIC         AS product_name_length,
# MAGIC
# MAGIC     CAST(p.product_description_lenght AS INT)
# MAGIC         AS product_description_length,
# MAGIC
# MAGIC     CAST(p.product_photos_qty AS INT)
# MAGIC         AS product_photos_qty,
# MAGIC
# MAGIC     CAST(p.product_weight_g AS INT)
# MAGIC         AS product_weight_g,
# MAGIC
# MAGIC     CAST(p.product_length_cm AS INT)
# MAGIC         AS product_length_cm,
# MAGIC
# MAGIC     CAST(p.product_height_cm AS INT)
# MAGIC         AS product_height_cm,
# MAGIC
# MAGIC     CAST(p.product_width_cm AS INT)
# MAGIC         AS product_width_cm,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP()
# MAGIC         AS silver_load_timestamp
# MAGIC
# MAGIC FROM workspace.bronze.products p
# MAGIC
# MAGIC LEFT JOIN workspace.bronze.category_translation ct
# MAGIC
# MAGIC     ON p.product_category_name =
# MAGIC        ct.product_category_name;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Read the Final Silver Table
# MAGIC
# MAGIC Read the completed Silver Products table and verify the resulting
# MAGIC schema and data.

# COMMAND ----------

silver_products_df = spark.table(
    "workspace.silver.products"
)

silver_products_df.printSchema()

display(
    silver_products_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Validate Silver Row Count
# MAGIC
# MAGIC Every Bronze product must remain in Silver.
# MAGIC
# MAGIC The category translation is a many-to-one lookup from the product
# MAGIC perspective and must not increase or decrease the product count.

# COMMAND ----------

silver_product_count = silver_products_df.count()

print(f"Bronze Products : {bronze_product_count:,}")
print(f"Silver Products : {silver_product_count:,}")

if silver_product_count != bronze_product_count:
    raise ValueError(
        "Silver Products quality gate failed: "
        f"Bronze={bronze_product_count:,}, "
        f"Silver={silver_product_count:,}"
    )

print("PASS — Bronze and Silver product counts match.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 8 — Validate Product Identifier
# MAGIC
# MAGIC `product_id` is the product business key.
# MAGIC
# MAGIC The final Silver table must contain:
# MAGIC
# MAGIC - No NULL product IDs.
# MAGIC - No duplicate product IDs.

# COMMAND ----------

silver_null_product_ids = (
    silver_products_df
        .filter(F.col("product_id").isNull())
        .count()
)

silver_duplicate_product_ids = (
    silver_products_df
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

print(
    f"NULL product IDs      : "
    f"{silver_null_product_ids}"
)

print(
    f"Duplicate product IDs : "
    f"{silver_duplicate_product_ids}"
)

if silver_null_product_ids != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "NULL product_id exists."
    )

if silver_duplicate_product_ids != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "Duplicate product_id exists."
    )

print("PASS — Product identifier validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 9 — Validate Category Translation
# MAGIC
# MAGIC Verify the two manually mapped categories and confirm that the final
# MAGIC English category is not NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     product_category_name,
# MAGIC     product_category_name_english,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM workspace.silver.products
# MAGIC WHERE product_category_name IN (
# MAGIC     'pc_gamer',
# MAGIC     'portateis_cozinha_e_preparadores_de_alimentos'
# MAGIC )
# MAGIC GROUP BY
# MAGIC     product_category_name,
# MAGIC     product_category_name_english
# MAGIC ORDER BY
# MAGIC     product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS null_english_category
# MAGIC FROM workspace.silver.products
# MAGIC WHERE product_category_name_english IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     product_category_name_english,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM workspace.silver.products
# MAGIC WHERE product_category_name_english = 'Unknown Category'
# MAGIC GROUP BY product_category_name_english;

# COMMAND ----------

null_english_category_count = (
    silver_products_df
        .filter(
            F.col("product_category_name_english").isNull()
        )
        .count()
)

print(
    f"NULL English categories: {null_english_category_count}"
)

if null_english_category_count != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "NULL English product categories remain."
    )

print(
    "PASS — Every Silver product has an English category value."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 10 — Validate Product Datatypes
# MAGIC
# MAGIC Verify that the Bronze STRING fields were correctly converted to the
# MAGIC required Silver datatypes.

# COMMAND ----------

expected_silver_types = {
    "product_id": "string",
    "product_category_name": "string",
    "product_category_name_english": "string",
    "product_name_length": "int",
    "product_description_length": "int",
    "product_photos_qty": "int",
    "product_weight_g": "int",
    "product_length_cm": "int",
    "product_height_cm": "int",
    "product_width_cm": "int",
    "silver_load_timestamp": "timestamp"
}

actual_silver_types = dict(
    silver_products_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_silver_types.get(column)
    }
    for column, expected_type in expected_silver_types.items()
    if actual_silver_types.get(column) != expected_type
}

if datatype_errors:
    raise ValueError(
        f"Silver datatype validation failed: {datatype_errors}"
    )

print("PASS — Silver Products datatypes are correct.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 11 — Validate Product Attributes
# MAGIC
# MAGIC Check for negative values in product attributes where negative values
# MAGIC are not valid.
# MAGIC
# MAGIC NULL values are allowed because they are present in the source data.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN product_name_length < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_name_length,
# MAGIC
# MAGIC     SUM(CASE WHEN product_description_length < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_description_length,
# MAGIC
# MAGIC     SUM(CASE WHEN product_photos_qty < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_photos,
# MAGIC
# MAGIC     SUM(CASE WHEN product_weight_g < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_weight,
# MAGIC
# MAGIC     SUM(CASE WHEN product_length_cm < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_length,
# MAGIC
# MAGIC     SUM(CASE WHEN product_height_cm < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_height,
# MAGIC
# MAGIC     SUM(CASE WHEN product_width_cm < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_width
# MAGIC
# MAGIC FROM workspace.silver.products;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 12 — Validate Silver Product Attribute Quality
# MAGIC
# MAGIC The negative-value validation must pass before the Silver table is
# MAGIC considered complete.

# COMMAND ----------

negative_counts = (
    silver_products_df.select(
        F.sum(
            F.when(
                F.col("product_name_length") < 0,
                1
            ).otherwise(0)
        ).alias("negative_name_length"),

        F.sum(
            F.when(
                F.col("product_description_length") < 0,
                1
            ).otherwise(0)
        ).alias("negative_description_length"),

        F.sum(
            F.when(
                F.col("product_photos_qty") < 0,
                1
            ).otherwise(0)
        ).alias("negative_photos"),

        F.sum(
            F.when(
                F.col("product_weight_g") < 0,
                1
            ).otherwise(0)
        ).alias("negative_weight"),

        F.sum(
            F.when(
                F.col("product_length_cm") < 0,
                1
            ).otherwise(0)
        ).alias("negative_length"),

        F.sum(
            F.when(
                F.col("product_height_cm") < 0,
                1
            ).otherwise(0)
        ).alias("negative_height"),

        F.sum(
            F.when(
                F.col("product_width_cm") < 0,
                1
            ).otherwise(0)
        ).alias("negative_width")
    )
    .collect()[0]
    .asDict()
)

print(negative_counts)

invalid_negative_values = {
    column: count
    for column, count in negative_counts.items()
    if count != 0
}

if invalid_negative_values:
    raise ValueError(
        "Silver Products quality gate failed. "
        f"Negative values found: {invalid_negative_values}"
    )

print("PASS — No negative product attribute values found.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 13 — Validate Silver Load Timestamp
# MAGIC
# MAGIC Every Silver record must contain a Silver load timestamp.

# COMMAND ----------

null_silver_timestamps = (
    silver_products_df
        .filter(
            F.col("silver_load_timestamp").isNull()
        )
        .count()
)

print(
    f"NULL Silver load timestamps: {null_silver_timestamps}"
)

if null_silver_timestamps != 0:
    raise ValueError(
        "Silver Products quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print("PASS — Silver load timestamp is populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 14 — Final Silver Quality Gate
# MAGIC
# MAGIC All critical Silver Products validations are checked together.
# MAGIC
# MAGIC The notebook fails if any critical validation fails.

# COMMAND ----------

final_silver_df = spark.table(
    "workspace.silver.products"
)

final_silver_count = final_silver_df.count()

final_null_product_ids = (
    final_silver_df
        .filter(
            F.col("product_id").isNull()
        )
        .count()
)

final_duplicate_product_ids = (
    final_silver_df
        .groupBy("product_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

final_null_english_categories = (
    final_silver_df
        .filter(
            F.col("product_category_name_english").isNull()
        )
        .count()
)

final_null_timestamps = (
    final_silver_df
        .filter(
            F.col("silver_load_timestamp").isNull()
        )
        .count()
)

print("=" * 70)
print("SILVER PRODUCTS QUALITY GATE")
print("=" * 70)

print(
    f"Bronze rows              : "
    f"{bronze_product_count:,}"
)

print(
    f"Silver rows              : "
    f"{final_silver_count:,}"
)

print(
    f"NULL product IDs         : "
    f"{final_null_product_ids}"
)

print(
    f"Duplicate product IDs    : "
    f"{final_duplicate_product_ids}"
)

print(
    f"NULL English categories  : "
    f"{final_null_english_categories}"
)

print(
    f"NULL load timestamps     : "
    f"{final_null_timestamps}"
)

print("=" * 70)

if (
    final_silver_count == bronze_product_count
    and final_null_product_ids == 0
    and final_duplicate_product_ids == 0
    and final_null_english_categories == 0
    and final_null_timestamps == 0
):
    print("STATUS: ALL SILVER PRODUCTS CHECKS PASSED")
else:
    raise ValueError(
        "SILVER PRODUCTS QUALITY GATE FAILED."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 15 — Verify Silver Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.silver.products;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 16 — Verify Silver Delta History

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.silver.products;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 17 — Verify Silver Table Registration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Silver Products Preview

# COMMAND ----------

display(
    spark.table(
        "workspace.silver.products"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver Products — Execution Summary
# MAGIC
# MAGIC ## Source Tables
# MAGIC
# MAGIC - `workspace.bronze.products`
# MAGIC - `workspace.bronze.category_translation`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.products`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Transformation Summary
# MAGIC
# MAGIC The Silver Products table:
# MAGIC
# MAGIC - Preserves all Bronze products.
# MAGIC - Adds English product category information.
# MAGIC - Applies the required manual category mappings.
# MAGIC - Represents products without a category as `Unknown Category`.
# MAGIC - Renames incorrectly spelled source columns.
# MAGIC - Converts product attributes to standardized numeric datatypes.
# MAGIC - Preserves NULL physical attributes.
# MAGIC - Adds `silver_load_timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Results
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |---|---|
# MAGIC | Bronze → Silver row count | PASS |
# MAGIC | Product ID NULL validation | PASS |
# MAGIC | Product ID uniqueness | PASS |
# MAGIC | Category translation validation | PASS |
# MAGIC | English category NULL validation | PASS |
# MAGIC | Numeric datatype validation | PASS |
# MAGIC | Negative attribute validation | PASS |
# MAGIC | Silver timestamp validation | PASS |
# MAGIC | Final Silver quality gate | PASS |
# MAGIC | Delta metadata validation | PASS |
# MAGIC | Delta history validation | PASS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Results
# MAGIC
# MAGIC | Metric | Result |
# MAGIC |---|---:|
# MAGIC | Bronze products | 32,951 |
# MAGIC | Silver products | 32,951 |
# MAGIC | Category translation mappings | 71 |
# MAGIC | Duplicate category mappings | 0 |
# MAGIC | Duplicate product IDs | 0 |
# MAGIC | NULL product IDs | 0 |
# MAGIC | NULL English categories | 0 |
# MAGIC | Unknown Category products | 610 |
# MAGIC | `pc_gamer` → `Gaming PC` | 3 |
# MAGIC | Portable Kitchen Appliances mapping | 10 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Status
# MAGIC
# MAGIC **SUCCESS — Silver Products transformation and quality validation
# MAGIC completed successfully.**

# COMMAND ----------

