# Databricks notebook source
# MAGIC %md
# MAGIC # Customer360 Retail Analytics
# MAGIC
# MAGIC ## Bronze Layer — Category Translation
# MAGIC
# MAGIC ### Notebook: 05_Bronze_Category_Translation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Ingest the raw Olist product category translation dataset from AWS S3
# MAGIC into the Databricks Bronze layer.
# MAGIC
# MAGIC The Bronze layer preserves the source dataset without applying
# MAGIC business transformations.
# MAGIC
# MAGIC This notebook performs:
# MAGIC
# MAGIC - Source ingestion
# MAGIC - Structural validation
# MAGIC - Row-count reconciliation
# MAGIC - NULL analysis
# MAGIC - Blank-string analysis
# MAGIC - Duplicate analysis
# MAGIC - Mapping integrity analysis
# MAGIC - Delta table validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC AWS S3
# MAGIC
# MAGIC File:
# MAGIC
# MAGIC product_category_name_translation.csv
# MAGIC
# MAGIC S3 Location:
# MAGIC
# MAGIC s3://olist-retail-project/product_category_name_translation.csv
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC Catalog:
# MAGIC
# MAGIC workspace
# MAGIC
# MAGIC Schema:
# MAGIC
# MAGIC bronze
# MAGIC
# MAGIC Table:
# MAGIC
# MAGIC category_translation
# MAGIC
# MAGIC Fully Qualified Table:
# MAGIC
# MAGIC workspace.bronze.category_translation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC The expected source grain is one record per Portuguese product
# MAGIC category.
# MAGIC
# MAGIC The source mapping is:
# MAGIC
# MAGIC product_category_name
# MAGIC         ↓
# MAGIC product_category_name_english
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Principles
# MAGIC
# MAGIC This notebook does NOT:
# MAGIC
# MAGIC - Rename source columns
# MAGIC - Translate values
# MAGIC - Remove duplicates
# MAGIC - Replace NULL values
# MAGIC - Standardize category names
# MAGIC - Join with Products
# MAGIC - Add metadata columns
# MAGIC - Create derived columns
# MAGIC
# MAGIC All business transformations are reserved for Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_PATH = 's3://omnicapstone/raw_data/product_category_name_translation.csv'

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "category_translation"

BRONZE_FULL_TABLE = (
    f"{BRONZE_CATALOG}."
    f"{BRONZE_SCHEMA}."
    f"{BRONZE_TABLE}"
)

print("Source:", SOURCE_PATH)
print("Target:", BRONZE_FULL_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read Raw Source
# MAGIC
# MAGIC The CSV is read directly from S3.
# MAGIC
# MAGIC Schema inference is disabled so that Bronze preserves the source
# MAGIC representation.
# MAGIC
# MAGIC No transformations are applied during ingestion.

# COMMAND ----------

category_translation_raw_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
)

# COMMAND ----------

category_translation_raw_df.printSchema()

# COMMAND ----------

display(
    category_translation_raw_df.limit(20)
)

# COMMAND ----------

source_count = category_translation_raw_df.count()

print(f"Source row count: {source_count:,}")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("workspace.bronze is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Bronze Delta Table
# MAGIC
# MAGIC The source column names are preserved exactly.
# MAGIC
# MAGIC No translation or standardization is performed.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_FULL_TABLE} (
    product_category_name STRING,
    product_category_name_english STRING
)
USING DELTA
""")

print(f"Created: {BRONZE_FULL_TABLE}")

# COMMAND ----------

(
    category_translation_raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_FULL_TABLE)
)

print(
    f"Successfully loaded {BRONZE_FULL_TABLE}"
)

# COMMAND ----------

bronze_category_translation_df = (
    spark.table(BRONZE_FULL_TABLE)
)

display(
    bronze_category_translation_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3 — Bronze Validation
# MAGIC
# MAGIC The first critical validation confirms that the Bronze table contains
# MAGIC the same number of records as the source.

# COMMAND ----------

bronze_count = bronze_category_translation_df.count()

print(f"Source records : {source_count:,}")
print(f"Bronze records : {bronze_count:,}")

if source_count != bronze_count:
    raise ValueError(
        f"BRONZE LOAD FAILED: "
        f"Source has {source_count:,} rows but "
        f"Bronze has {bronze_count:,} rows."
    )

print("PASS — Source and Bronze row counts match.")

# COMMAND ----------

expected_columns = [
    "product_category_name",
    "product_category_name_english"
]

actual_columns = bronze_category_translation_df.columns

missing_columns = [
    column
    for column in expected_columns
    if column not in actual_columns
]

unexpected_columns = [
    column
    for column in actual_columns
    if column not in expected_columns
]

print("Expected columns :", expected_columns)
print("Actual columns   :", actual_columns)
print("Missing columns  :", missing_columns)
print("Unexpected cols  :", unexpected_columns)

if missing_columns or unexpected_columns:
    raise ValueError(
        "BRONZE SCHEMA VALIDATION FAILED."
    )

print("PASS — Column structure is correct.")

# COMMAND ----------

bronze_category_translation_df.printSchema()

# COMMAND ----------

null_profile = bronze_category_translation_df.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("product_category_name").isNull(),
            1
        ).otherwise(0)
    ).alias("null_product_category_name"),

    F.sum(
        F.when(
            F.col("product_category_name_english").isNull(),
            1
        ).otherwise(0)
    ).alias("null_product_category_name_english")
)

display(null_profile)

# COMMAND ----------

critical_nulls = (
    bronze_category_translation_df
        .filter(
            F.col("product_category_name").isNull()
            |
            F.col("product_category_name_english").isNull()
        )
        .count()
)

print(
    f"Rows with NULL mapping values: {critical_nulls}"
)

if critical_nulls > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "Category translation contains NULL mapping values."
    )

print(
    "PASS — No NULL mapping values detected."
)

# COMMAND ----------

blank_profile = bronze_category_translation_df.select(
    F.sum(
        F.when(
            F.trim(
                F.col("product_category_name")
            ) == "",
            1
        ).otherwise(0)
    ).alias("blank_product_category_name"),

    F.sum(
        F.when(
            F.trim(
                F.col("product_category_name_english")
            ) == "",
            1
        ).otherwise(0)
    ).alias(
        "blank_product_category_name_english"
    )
)

display(blank_profile)

# COMMAND ----------

blank_mapping_count = (
    bronze_category_translation_df
        .filter(
            (F.trim(
                F.col("product_category_name")
            ) == "")
            |
            (F.trim(
                F.col("product_category_name_english")
            ) == "")
        )
        .count()
)

print(
    f"Rows with blank mapping values: "
    f"{blank_mapping_count}"
)

if blank_mapping_count > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "Blank category mapping values detected."
    )

print(
    "PASS — No blank mapping values detected."
)

# COMMAND ----------

duplicate_source_categories = (
    bronze_category_translation_df
        .groupBy("product_category_name")
        .count()
        .filter(F.col("count") > 1)
)

duplicate_source_count = (
    duplicate_source_categories.count()
)

print(
    "Duplicate source category groups:",
    duplicate_source_count
)

display(
    duplicate_source_categories
)

# COMMAND ----------

if duplicate_source_count > 0:
    raise ValueError(
        "BRONZE VALIDATION FAILED: "
        "A product category maps to multiple "
        "translation records."
    )

print(
    "PASS — Each source category occurs once."
)

# COMMAND ----------

duplicate_english_categories = (
    bronze_category_translation_df
        .groupBy(
            "product_category_name_english"
        )
        .count()
        .filter(F.col("count") > 1)
)

duplicate_english_count = (
    duplicate_english_categories.count()
)

print(
    "Duplicate English category groups:",
    duplicate_english_count
)

display(
    duplicate_english_categories
)

# COMMAND ----------

mapping_summary = bronze_category_translation_df.select(
    F.count("*").alias("total_mappings"),
    F.countDistinct(
        "product_category_name"
    ).alias("unique_source_categories"),
    F.countDistinct(
        "product_category_name_english"
    ).alias("unique_english_categories")
)

display(mapping_summary)

# COMMAND ----------

display(
    bronze_category_translation_df
        .orderBy("product_category_name")
)

# COMMAND ----------

translation_length_profile = (
    bronze_category_translation_df
        .select(
            F.min(
                F.length(
                    "product_category_name"
                )
            ).alias(
                "min_source_category_length"
            ),

            F.max(
                F.length(
                    "product_category_name"
                )
            ).alias(
                "max_source_category_length"
            ),

            F.min(
                F.length(
                    "product_category_name_english"
                )
            ).alias(
                "min_english_category_length"
            ),

            F.max(
                F.length(
                    "product_category_name_english"
                )
            ).alias(
                "max_english_category_length"
            )
        )
)

display(translation_length_profile)

# COMMAND ----------

products_table_exists = spark.catalog.tableExists(
    "workspace.bronze.products"
)

print(
    "Bronze Products available:",
    products_table_exists
)

# COMMAND ----------

if products_table_exists:

    bronze_products_df = spark.table(
        "workspace.bronze.products"
    )

    product_categories = (
        bronze_products_df
            .select(
                "product_category_name"
            )
            .where(
                F.col(
                    "product_category_name"
                ).isNotNull()
            )
            .distinct()
    )

    translation_categories = (
        bronze_category_translation_df
            .select(
                "product_category_name"
            )
            .distinct()
    )

    unmapped_categories = (
        product_categories
            .join(
                translation_categories,
                on="product_category_name",
                how="left_anti"
            )
    )

    unmapped_count = unmapped_categories.count()

    print(
        "Product categories without translation:",
        unmapped_count
    )

    display(unmapped_categories)

else:

    print(
        "Bronze Products table is not available."
    )

    print(
        "Cross-table category coverage will be "
        "performed later by the Bronze integration "
        "validation task."
    )

# COMMAND ----------

spark.sql(
    f"""
    DESCRIBE DETAIL {BRONZE_FULL_TABLE}
    """
).show(
    truncate=False
)

# COMMAND ----------

spark.sql(
    f"""
    DESCRIBE HISTORY {BRONZE_FULL_TABLE}
    """
).show(
    truncate=False
)

# COMMAND ----------

display(
    spark.table(
        BRONZE_FULL_TABLE
    ).limit(20)
)

# COMMAND ----------

final_count = spark.table(
    BRONZE_FULL_TABLE
).count()

if final_count != source_count:
    raise ValueError(
        "FINAL BRONZE QUALITY GATE FAILED: "
        f"Expected {source_count:,} rows, "
        f"found {final_count:,}."
    )

print("=" * 70)
print("BRONZE CATEGORY TRANSLATION PIPELINE SUCCESS")
print("=" * 70)
print(f"Source rows : {source_count:,}")
print(f"Bronze rows : {final_count:,}")
print(f"Target      : {BRONZE_FULL_TABLE}")
print("Status      : SUCCESS")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Category Translation — Execution Summary
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Source:
# MAGIC
# MAGIC `product_category_name_translation.csv`
# MAGIC
# MAGIC S3:
# MAGIC
# MAGIC `s3://olist-retail-project/product_category_name_translation.csv`
# MAGIC
# MAGIC ## Bronze Target
# MAGIC
# MAGIC `workspace.bronze.category_translation`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This dataset provides the mapping between the original Portuguese
# MAGIC product category name and its English translation.
# MAGIC
# MAGIC The mapping will be used during Silver Products processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC Expected grain:
# MAGIC
# MAGIC `product_category_name`
# MAGIC
# MAGIC Each source category should map to a single translation record.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Performed
# MAGIC
# MAGIC - Source row count
# MAGIC - Bronze row count
# MAGIC - Row-count reconciliation
# MAGIC - Column validation
# MAGIC - Schema validation
# MAGIC - NULL analysis
# MAGIC - Blank-string analysis
# MAGIC - Duplicate source-category validation
# MAGIC - Duplicate English-category profiling
# MAGIC - Mapping cardinality analysis
# MAGIC - Translation length profiling
# MAGIC - Product-category coverage profiling
# MAGIC - Delta metadata validation
# MAGIC - Delta transaction history validation
# MAGIC - Final automation quality gate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Critical Validation Rules
# MAGIC
# MAGIC The pipeline fails if:
# MAGIC
# MAGIC 1. Source and Bronze row counts do not match.
# MAGIC 2. Required columns are missing.
# MAGIC 3. Unexpected columns are present.
# MAGIC 4. A source category is NULL.
# MAGIC 5. An English translation is NULL.
# MAGIC 6. A source category is blank.
# MAGIC 7. An English translation is blank.
# MAGIC 8. A source category occurs more than once.
# MAGIC 9. Final Bronze row count does not match source row count.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Principles
# MAGIC
# MAGIC No transformations are applied.
# MAGIC
# MAGIC The following are intentionally NOT performed:
# MAGIC
# MAGIC - Category translation
# MAGIC - Category renaming
# MAGIC - Category standardization
# MAGIC - NULL replacement
# MAGIC - Duplicate removal
# MAGIC - Product joins
# MAGIC - Derived columns
# MAGIC - Metadata columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Downstream Usage
# MAGIC
# MAGIC Silver Products will use this table to enrich:
# MAGIC
# MAGIC `product_category_name`
# MAGIC
# MAGIC with:
# MAGIC
# MAGIC `product_category_name_english`
# MAGIC
# MAGIC The join will be performed in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Automation
# MAGIC
# MAGIC The notebook contains explicit validation gates that raise exceptions
# MAGIC when critical data-quality conditions fail.
# MAGIC
# MAGIC This allows the notebook to be executed as a task inside a Databricks
# MAGIC Workflow without requiring manual inspection.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC The category translation source is ingested into:
# MAGIC
# MAGIC `workspace.bronze.category_translation`
# MAGIC
# MAGIC The table is ready for Bronze integration validation and downstream
# MAGIC Silver Products transformation.

# COMMAND ----------

