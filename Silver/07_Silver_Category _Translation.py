# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Category Translation
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create a clean, standardized and validated Silver lookup table
# MAGIC from the Bronze Category Translation dataset.
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC workspace.bronze.category_translation
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC workspace.silver.category_translation
# MAGIC
# MAGIC ## Business Grain
# MAGIC
# MAGIC One row represents one source product category.
# MAGIC
# MAGIC Business key:
# MAGIC
# MAGIC product_category_name
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This table provides the mapping between the original product category
# MAGIC name and its English translation.
# MAGIC
# MAGIC It is a reference/lookup dataset and will support downstream
# MAGIC Product analytics and Silver/Gold transformations.
# MAGIC
# MAGIC ## Silver Responsibilities
# MAGIC
# MAGIC - Preserve the Bronze category mappings.
# MAGIC - Validate the source-category business key.
# MAGIC - Validate translation completeness.
# MAGIC - Validate blank values.
# MAGIC - Validate duplicate mappings.
# MAGIC - Standardize string values.
# MAGIC - Add Silver audit metadata.
# MAGIC - Preserve the exact source population.
# MAGIC - Persist the result as a Delta table.
# MAGIC
# MAGIC ## Important Design Principle
# MAGIC
# MAGIC This table is a reference dataset.
# MAGIC
# MAGIC No product-level joins, aggregations or business metrics are performed here.
# MAGIC
# MAGIC Those operations belong to downstream Silver/Gold processing.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

BRONZE_TABLE = "workspace.bronze.category_translation"

SILVER_TABLE = "workspace.silver.category_translation"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read Bronze Category Translation
# MAGIC
# MAGIC Silver reads from the completed Bronze Delta table.
# MAGIC
# MAGIC The raw source file is not read again.

# COMMAND ----------

bronze_category_translation_df = spark.table(
    BRONZE_TABLE
)

bronze_category_translation_count = (
    bronze_category_translation_df.count()
)

print(
    f"Bronze Category Translation rows: "
    f"{bronze_category_translation_count:,}"
)

bronze_category_translation_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Validate Bronze Schema
# MAGIC
# MAGIC The lookup table must contain the source category and its English
# MAGIC translation.

# COMMAND ----------

required_columns = {
    "product_category_name",
    "product_category_name_english"
}

actual_columns = set(
    bronze_category_translation_df.columns
)

missing_columns = (
    required_columns - actual_columns
)

if missing_columns:
    raise ValueError(
        "Bronze Category Translation schema validation failed. "
        f"Missing columns: {sorted(missing_columns)}"
    )

print(
    "PASS — Bronze Category Translation contains all required columns."
)

# COMMAND ----------

unexpected_columns = (
    actual_columns - required_columns
)

print(
    f"Unexpected columns: "
    f"{sorted(unexpected_columns)}"
)

if unexpected_columns:
    raise ValueError(
        "Bronze Category Translation schema validation failed. "
        f"Unexpected columns found: {sorted(unexpected_columns)}"
    )

print(
    "PASS — Category Translation schema contains exactly the expected columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

null_source_categories = (
    bronze_category_translation_df
    .filter(
        F.col("product_category_name").isNull()
    )
    .count()
)

blank_source_categories = (
    bronze_category_translation_df
    .filter(
        F.col("product_category_name").isNotNull()
        & (
            F.trim(
                F.col("product_category_name")
            ) == ""
        )
    )
    .count()
)

print(
    f"NULL source categories   : "
    f"{null_source_categories}"
)

print(
    f"Blank source categories  : "
    f"{blank_source_categories}"
)

if null_source_categories != 0:
    raise ValueError(
        "Category Translation quality gate failed: "
        "NULL source categories found."
    )

if blank_source_categories != 0:
    raise ValueError(
        "Category Translation quality gate failed: "
        "Blank source categories found."
    )

print(
    "PASS — Source category values are populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Validate English Translation
# MAGIC
# MAGIC Every source category should have an English translation in this
# MAGIC reference dataset.

# COMMAND ----------

null_english_categories = (
    bronze_category_translation_df
    .filter(
        F.col("product_category_name_english").isNull()
    )
    .count()
)

blank_english_categories = (
    bronze_category_translation_df
    .filter(
        F.col("product_category_name_english").isNotNull()
        & (
            F.trim(
                F.col("product_category_name_english")
            ) == ""
        )
    )
    .count()
)

print(
    f"NULL English categories  : "
    f"{null_english_categories}"
)

print(
    f"Blank English categories : "
    f"{blank_english_categories}"
)

if null_english_categories != 0:
    raise ValueError(
        "Category Translation quality gate failed: "
        "NULL English translations found."
    )

if blank_english_categories != 0:
    raise ValueError(
        "Category Translation quality gate failed: "
        "Blank English translations found."
    )

print(
    "PASS — English category translations are populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Validate Reference Table Grain
# MAGIC
# MAGIC The business key is:
# MAGIC
# MAGIC product_category_name
# MAGIC
# MAGIC Each source category must have exactly one translation.

# COMMAND ----------

duplicate_source_categories_df = (
    bronze_category_translation_df
    .groupBy("product_category_name")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_source_category_count = (
    duplicate_source_categories_df.count()
)

print(
    f"Duplicate source categories: "
    f"{duplicate_source_category_count}"
)

if duplicate_source_category_count != 0:
    display(
        duplicate_source_categories_df
        .orderBy(F.desc("count"))
    )

    raise ValueError(
        "Category Translation quality gate failed: "
        "Multiple translations exist for a source category."
    )

print(
    "PASS — Each source category has one translation."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Profile English Category Cardinality
# MAGIC
# MAGIC Multiple source-language categories may legitimately map to the same
# MAGIC English category.
# MAGIC
# MAGIC Therefore, duplicate English category values are NOT treated as a
# MAGIC data-quality failure.
# MAGIC
# MAGIC This is an informational profiling check only.

# COMMAND ----------

duplicate_english_categories_df = (
    bronze_category_translation_df
    .groupBy("product_category_name_english")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

display(
    duplicate_english_categories_df
)

print(
    "INFO — Multiple source categories mapping to the same "
    "English category are allowed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Validate Bronze Datatypes
# MAGIC
# MAGIC Both category fields are expected to be strings.
# MAGIC
# MAGIC This validation confirms that the Bronze reference data has the
# MAGIC expected structure before Silver transformation.

# COMMAND ----------

expected_bronze_types = {
    "product_category_name": "string",
    "product_category_name_english": "string"
}

actual_bronze_types = dict(
    bronze_category_translation_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_bronze_types.get(column)
    }
    for column, expected_type in expected_bronze_types.items()
    if actual_bronze_types.get(column) != expected_type
}

if datatype_errors:
    raise ValueError(
        "Bronze Category Translation datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Bronze Category Translation datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Create Silver Category Translation
# MAGIC
# MAGIC Transform the validated Bronze reference data into the Silver layer.
# MAGIC
# MAGIC Transformations:
# MAGIC
# MAGIC - Trim leading/trailing whitespace.
# MAGIC - Preserve the source category.
# MAGIC - Preserve the English translation.
# MAGIC - Add Silver load timestamp.
# MAGIC - Do not filter or aggregate records.
# MAGIC - Preserve the one-row-per-source-category grain.
# MAGIC
# MAGIC No business mapping is invented in this layer.

# COMMAND ----------

silver_category_translation_df = (
    bronze_category_translation_df
    .select(
        F.trim(
            F.col("product_category_name")
        ).alias("product_category_name"),

        F.trim(
            F.col("product_category_name_english")
        ).alias("product_category_name_english")
    )
    .withColumn(
        "silver_load_timestamp",
        F.current_timestamp()
    )
)

print(
    "Silver Category Translation transformation created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Validate Silver Transformation Schema
# MAGIC
# MAGIC The Silver reference table should contain:
# MAGIC
# MAGIC - product_category_name
# MAGIC - product_category_name_english
# MAGIC - silver_load_timestamp

# COMMAND ----------

silver_category_translation_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Preview Silver Category Translation
# MAGIC
# MAGIC Review the transformed reference data before persistence.

# COMMAND ----------

display(
    silver_category_translation_df
    .orderBy("product_category_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Validate Silver Business Rules
# MAGIC
# MAGIC The Silver reference table must not contain:
# MAGIC
# MAGIC - NULL source categories
# MAGIC - Blank source categories
# MAGIC - NULL English translations
# MAGIC - Blank English translations
# MAGIC
# MAGIC The business grain must remain one row per source category.

# COMMAND ----------

silver_invalid_records = (
    silver_category_translation_df
    .filter(
        F.col("product_category_name").isNull()
        | (
            F.trim(
                F.col("product_category_name")
            ) == ""
        )
        | F.col("product_category_name_english").isNull()
        | (
            F.trim(
                F.col("product_category_name_english")
            ) == ""
        )
    )
    .count()
)

silver_duplicate_source_categories = (
    silver_category_translation_df
    .groupBy("product_category_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Invalid Silver records        : "
    f"{silver_invalid_records}"
)

print(
    f"Duplicate Silver source keys  : "
    f"{silver_duplicate_source_categories}"
)

if silver_invalid_records != 0:
    raise ValueError(
        "Silver Category Translation business-rule validation failed: "
        "Invalid category values found."
    )

if silver_duplicate_source_categories != 0:
    raise ValueError(
        "Silver Category Translation business-rule validation failed: "
        "Duplicate source categories found."
    )

print(
    "PASS — Silver Category Translation business rules are valid."
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
# MAGIC ## Step 13 — Persist Silver Category Translation
# MAGIC
# MAGIC Persist the validated reference dataset as a managed Delta table.
# MAGIC
# MAGIC Target:
# MAGIC
# MAGIC workspace.silver.category_translation

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.category_translation
""")

(
    silver_category_translation_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.silver.category_translation"
    )
)

print(
    "Silver Category Translation table created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 — Read the Persisted Silver Table
# MAGIC
# MAGIC All final quality checks should operate against the persisted table,
# MAGIC not only the in-memory transformation DataFrame.
# MAGIC
# MAGIC This validates what was actually written to Delta.

# COMMAND ----------

final_silver_category_translation_df = (
    spark.table(
        "workspace.silver.category_translation"
    )
)

final_silver_category_translation_count = (
    final_silver_category_translation_df.count()
)

print(
    f"Persisted Silver Category Translation rows: "
    f"{final_silver_category_translation_count:,}"
)

final_silver_category_translation_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 — Reconcile Bronze and Silver Populations
# MAGIC
# MAGIC Category Translation is a reference dataset.
# MAGIC
# MAGIC No records should be added, removed, or aggregated during Silver
# MAGIC transformation.
# MAGIC
# MAGIC Therefore Bronze and Silver row counts must match exactly.

# COMMAND ----------

print(
    f"Bronze rows : "
    f"{bronze_category_translation_count:,}"
)

print(
    f"Silver rows : "
    f"{final_silver_category_translation_count:,}"
)

if (
    bronze_category_translation_count
    != final_silver_category_translation_count
):
    raise ValueError(
        "Silver Category Translation quality gate failed: "
        "Bronze and Silver row counts do not match."
    )

print(
    "PASS — Bronze and Silver Category Translation counts match."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 — Validate Persisted Silver Grain
# MAGIC
# MAGIC The persisted Silver table must maintain the reference-table grain:
# MAGIC
# MAGIC     one product_category_name
# MAGIC     → one product_category_name_english
# MAGIC
# MAGIC This is critical because Products will eventually use this table as
# MAGIC a lookup. Duplicate source keys could multiply product rows during a
# MAGIC join.

# COMMAND ----------

final_duplicate_source_categories = (
    final_silver_category_translation_df
    .groupBy("product_category_name")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

final_null_source_categories = (
    final_silver_category_translation_df
    .filter(
        F.col("product_category_name").isNull()
    )
    .count()
)

final_blank_source_categories = (
    final_silver_category_translation_df
    .filter(
        F.trim(
            F.col("product_category_name")
        ) == ""
    )
    .count()
)

final_null_english_categories = (
    final_silver_category_translation_df
    .filter(
        F.col("product_category_name_english").isNull()
    )
    .count()
)

final_blank_english_categories = (
    final_silver_category_translation_df
    .filter(
        F.trim(
            F.col("product_category_name_english")
        ) == ""
    )
    .count()
)

print(
    f"NULL source categories      : "
    f"{final_null_source_categories}"
)

print(
    f"Blank source categories     : "
    f"{final_blank_source_categories}"
)

print(
    f"NULL English categories     : "
    f"{final_null_english_categories}"
)

print(
    f"Blank English categories    : "
    f"{final_blank_english_categories}"
)

print(
    f"Duplicate source categories : "
    f"{final_duplicate_source_categories}"
)

if final_null_source_categories != 0:
    raise ValueError(
        "Persisted Silver Category Translation contains "
        "NULL source categories."
    )

if final_blank_source_categories != 0:
    raise ValueError(
        "Persisted Silver Category Translation contains "
        "blank source categories."
    )

if final_null_english_categories != 0:
    raise ValueError(
        "Persisted Silver Category Translation contains "
        "NULL English categories."
    )

if final_blank_english_categories != 0:
    raise ValueError(
        "Persisted Silver Category Translation contains "
        "blank English categories."
    )

if final_duplicate_source_categories != 0:
    raise ValueError(
        "Persisted Silver Category Translation contains "
        "duplicate source categories."
    )

print(
    "PASS — Persisted Silver translation grain is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 — Validate Persisted Silver Datatypes
# MAGIC
# MAGIC Confirm that the Delta table was persisted with the intended
# MAGIC production schema.

# COMMAND ----------

expected_silver_types = {
    "product_category_name": "string",
    "product_category_name_english": "string",
    "silver_load_timestamp": "timestamp"
}

actual_silver_types = dict(
    final_silver_category_translation_df.dtypes
)

silver_datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_silver_types.get(column)
    }
    for column, expected_type in expected_silver_types.items()
    if actual_silver_types.get(column) != expected_type
}

if silver_datatype_errors:
    raise ValueError(
        "Silver Category Translation datatype validation failed: "
        f"{silver_datatype_errors}"
    )

print(
    "PASS — Silver Category Translation datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 18 — Validate Silver Audit Metadata
# MAGIC
# MAGIC Every persisted Silver record should have a Silver load timestamp.
# MAGIC
# MAGIC This metadata will later be useful for:
# MAGIC
# MAGIC - pipeline monitoring
# MAGIC - debugging
# MAGIC - lineage
# MAGIC - incremental processing
# MAGIC - auditability

# COMMAND ----------

null_silver_timestamps = (
    final_silver_category_translation_df
    .filter(
        F.col("silver_load_timestamp").isNull()
    )
    .count()
)

print(
    f"NULL Silver load timestamps: "
    f"{null_silver_timestamps}"
)

if null_silver_timestamps != 0:
    raise ValueError(
        "Silver Category Translation quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Silver load timestamp is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

bronze_translation_keys = (
    bronze_category_translation_df
    .select(
        F.trim(
            F.col("product_category_name")
        ).alias("product_category_name"),

        F.trim(
            F.col("product_category_name_english")
        ).alias("product_category_name_english")
    )
    .distinct()
)

silver_translation_keys = (
    final_silver_category_translation_df
    .select(
        "product_category_name",
        "product_category_name_english"
    )
    .distinct()
)

print(
    f"Distinct Bronze mappings: "
    f"{bronze_translation_keys.count():,}"
)

print(
    f"Distinct Silver mappings: "
    f"{silver_translation_keys.count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

bronze_mappings_missing_in_silver = (
    bronze_translation_keys
    .join(
        silver_translation_keys,
        on=[
            "product_category_name",
            "product_category_name_english"
        ],
        how="left_anti"
    )
    .count()
)

silver_mappings_not_in_bronze = (
    silver_translation_keys
    .join(
        bronze_translation_keys,
        on=[
            "product_category_name",
            "product_category_name_english"
        ],
        how="left_anti"
    )
    .count()
)

print(
    f"Bronze mappings missing in Silver : "
    f"{bronze_mappings_missing_in_silver}"
)

print(
    f"Silver mappings not in Bronze     : "
    f"{silver_mappings_not_in_bronze}"
)

if bronze_mappings_missing_in_silver != 0:
    raise ValueError(
        "Silver Category Translation is missing Bronze mappings."
    )

if silver_mappings_not_in_bronze != 0:
    raise ValueError(
        "Silver Category Translation contains mappings "
        "not present in Bronze."
    )

print(
    "PASS — Bronze and Silver category mappings match exactly."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 21 — Product Category Translation Coverage
# MAGIC
# MAGIC The Category Translation table is a reference/lookup table.
# MAGIC
# MAGIC A separate coverage test is required to determine whether every
# MAGIC product category appearing in Silver Products has a corresponding
# MAGIC translation.
# MAGIC
# MAGIC Categories are classified into three groups:
# MAGIC
# MAGIC 1. Official translation
# MAGIC    - Category exists in the translation reference table.
# MAGIC
# MAGIC 2. Approved manual mapping
# MAGIC    - Category is intentionally handled by an explicit business rule
# MAGIC      in the Silver Products transformation.
# MAGIC
# MAGIC 3. Untranslated / unknown
# MAGIC    - No official translation or manual mapping exists.
# MAGIC    - These products are represented as `Unknown Category`.
# MAGIC
# MAGIC This analysis does not modify the Category Translation table.

# COMMAND ----------

# ================================================================
# STEP 21 — PRODUCT CATEGORY TRANSLATION COVERAGE ANALYSIS
# ================================================================

silver_products_df = spark.table(
    "workspace.silver.products"
)

silver_category_translation_df = spark.table(
    "workspace.silver.category_translation"
)

print(
    f"Silver Products rows              : "
    f"{silver_products_df.count():,}"
)

print(
    f"Silver Category Translation rows  : "
    f"{silver_category_translation_df.count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 22 — Identify Categories Missing from Translation Reference
# MAGIC
# MAGIC Compare the distinct product categories used by Silver Products against
# MAGIC the official Category Translation reference table.
# MAGIC
# MAGIC This identifies categories that require either:
# MAGIC
# MAGIC - an approved manual mapping, or
# MAGIC - `Unknown Category`.

# COMMAND ----------

# ================================================================
# STEP 22 — FIND PRODUCT CATEGORIES WITHOUT OFFICIAL TRANSLATION
# ================================================================

product_categories = (
    silver_products_df
    .filter(
        F.col("product_category_name").isNotNull()
    )
    .select(
        "product_category_name"
    )
    .distinct()
)

translated_categories = (
    silver_category_translation_df
    .select(
        "product_category_name"
    )
    .distinct()
)

untranslated_product_categories_df = (
    product_categories
    .join(
        translated_categories,
        on="product_category_name",
        how="left_anti"
    )
    .orderBy(
        "product_category_name"
    )
)

untranslated_category_count = (
    untranslated_product_categories_df.count()
)

print(
    f"Product categories without official translation: "
    f"{untranslated_category_count}"
)

display(
    untranslated_product_categories_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 23 — Quantify Products Affected by Missing Translation
# MAGIC
# MAGIC A category-level gap is useful, but Gold-layer planning requires the
# MAGIC number of actual products affected.
# MAGIC
# MAGIC This measures the business impact of missing category translations.

# COMMAND ----------

# ================================================================
# STEP 23 — QUANTIFY PRODUCTS AFFECTED BY MISSING TRANSLATIONS
# ================================================================

untranslated_product_count = (
    silver_products_df
    .join(
        translated_categories,
        on="product_category_name",
        how="left_anti"
    )
    .count()
)

print(
    f"Products without official category translation: "
    f"{untranslated_product_count:,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 24 — Validate Unknown Category Handling
# MAGIC
# MAGIC Products without an official category translation must not have a NULL
# MAGIC English category.
# MAGIC
# MAGIC The current Silver Products business rule represents these products as:
# MAGIC
# MAGIC     Unknown Category
# MAGIC
# MAGIC This test confirms that the fallback was actually applied.

# COMMAND ----------

# ================================================================
# STEP 24 — VALIDATE UNKNOWN CATEGORY FALLBACK
# ================================================================

unknown_category_products = (
    silver_products_df
    .filter(
        F.col("product_category_name_english")
        == "Unknown Category"
    )
    .count()
)

null_english_categories = (
    silver_products_df
    .filter(
        F.col("product_category_name_english").isNull()
    )
    .count()
)

print(
    f"Products classified as Unknown Category : "
    f"{unknown_category_products:,}"
)

print(
    f"Products with NULL English category      : "
    f"{null_english_categories}"
)

if null_english_categories != 0:
    raise ValueError(
        "Silver Products translation quality gate failed: "
        "NULL English categories remain."
    )

print(
    "PASS — Missing translations are represented "
    "without NULL English categories."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 25 — Validate Approved Manual Category Mappings
# MAGIC
# MAGIC Some product categories are intentionally handled by explicit business
# MAGIC rules rather than the reference translation table.
# MAGIC
# MAGIC These mappings must be validated in Silver Products, not inserted into
# MAGIC the Category Translation reference table.

# COMMAND ----------

# ================================================================
# STEP 25 — VALIDATE APPROVED MANUAL CATEGORY MAPPINGS
# ================================================================

expected_manual_mappings = {
    "pc_gamer": "Gaming PC",
    "portateis_cozinha_e_preparadores_de_alimentos":
        "Portable Kitchen Appliances"
}

manual_mapping_errors = []

for source_category, expected_english in expected_manual_mappings.items():

    result = (
        silver_products_df
        .filter(
            F.col("product_category_name")
            == source_category
        )
        .select(
            "product_category_name",
            "product_category_name_english"
        )
        .groupBy(
            "product_category_name",
            "product_category_name_english"
        )
        .count()
        .collect()
    )

    if not result:
        continue

    invalid_mapping = any(
        row["product_category_name_english"]
        != expected_english
        for row in result
    )

    if invalid_mapping:

        manual_mapping_errors.append(
            {
                "source_category": source_category,
                "expected": expected_english,
                "actual": [
                    row["product_category_name_english"]
                    for row in result
                ]
            }
        )

if manual_mapping_errors:

    raise ValueError(
        "Silver Products manual category mapping validation failed: "
        f"{manual_mapping_errors}"
    )

print(
    "PASS — Approved manual category mappings are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 26 — Validate Unknown Category Classification
# MAGIC
# MAGIC An important integrity rule is:
# MAGIC
# MAGIC If a product category has an official translation, the resulting
# MAGIC Silver Products English category must NOT be `Unknown Category`.
# MAGIC
# MAGIC This prevents a valid translation from being accidentally lost.

# COMMAND ----------

# ================================================================
# STEP 26 — VALIDATE UNKNOWN CATEGORY CLASSIFICATION
# ================================================================

translated_product_categories = (
    silver_category_translation_df
    .select(
        "product_category_name",
        "product_category_name_english"
    )
)

incorrect_unknown_categories_df = (
    silver_products_df.alias("p")
    .join(
        translated_product_categories.alias("t"),
        on="product_category_name",
        how="inner"
    )
    .filter(
        F.col("p.product_category_name_english")
        == "Unknown Category"
    )
    .select(
        F.col("p.product_category_name"),
        F.col("t.product_category_name_english")
            .alias("expected_english_category"),
        F.col("p.product_category_name_english")
            .alias("actual_english_category")
    )
)

incorrect_unknown_category_count = (
    incorrect_unknown_categories_df.count()
)

print(
    f"Translated products incorrectly classified "
    f"as Unknown Category: "
    f"{incorrect_unknown_category_count:,}"
)

if incorrect_unknown_category_count != 0:

    display(
        incorrect_unknown_categories_df
    )

    raise ValueError(
        "Silver Products translation quality gate failed: "
        "Products with official translations were classified "
        "as Unknown Category."
    )

print(
    "PASS — Officially translated categories are not "
    "incorrectly classified as Unknown Category."
)

# COMMAND ----------

# ================================================================
# LOCATE PRODUCTS WITHOUT OFFICIAL TRANSLATION
# AND EXPLAIN THEIR FINAL CATEGORY
# ================================================================

# Categories that do not exist in the official translation table
untranslated_products_df = (
    silver_products_df
    .join(
        translated_categories,
        on="product_category_name",
        how="left_anti"
    )
)

# Profile the final English category assigned to these products
untranslated_products_profile_df = (
    untranslated_products_df
    .groupBy(
        "product_category_name",
        "product_category_name_english"
    )
    .count()
    .orderBy(
        "product_category_name",
        "product_category_name_english"
    )
)

display(
    untranslated_products_profile_df
)

# COMMAND ----------

# ================================================================
# RECONCILIATION OF UNTRANSLATED PRODUCT CATEGORIES
# ================================================================

manual_mapping_product_count = (
    untranslated_products_df
    .filter(
        F.col("product_category_name").isin(
            list(expected_manual_mappings.keys())
        )
    )
    .count()
)

unknown_category_count = (
    untranslated_products_df
    .filter(
        F.col("product_category_name_english")
        == "Unknown Category"
    )
    .count()
)

total_untranslated_products = (
    untranslated_products_df.count()
)

print(
    f"Total products without official translation : "
    f"{total_untranslated_products:,}"
)

print(
    f"Products handled by manual mappings         : "
    f"{manual_mapping_product_count:,}"
)

print(
    f"Products handled as Unknown Category       : "
    f"{unknown_category_count:,}"
)

print(
    f"Reconciled total                            : "
    f"{manual_mapping_product_count + unknown_category_count:,}"
)

if (
    manual_mapping_product_count
    + unknown_category_count
    != total_untranslated_products
):
    raise ValueError(
        "Translation coverage reconciliation failed."
    )

print(
    "PASS — All products without official translations "
    "are fully accounted for."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 27 — Final Translation Coverage Profile
# MAGIC
# MAGIC Summarize the translation situation before proceeding to Gold.
# MAGIC
# MAGIC This profile distinguishes:
# MAGIC
# MAGIC - official translations
# MAGIC - manual mappings
# MAGIC - unknown/untranslated categories
# MAGIC
# MAGIC The purpose is transparency and downstream business interpretation.

# COMMAND ----------

# ================================================================
# STEP 27 — TRANSLATION COVERAGE PROFILE
# ================================================================

total_product_categories = (
    product_categories.count()
)

officially_translated_categories = (
    product_categories
    .join(
        translated_categories,
        on="product_category_name",
        how="inner"
    )
    .count()
)

print(
    f"Total product categories             : "
    f"{total_product_categories:,}"
)

print(
    f"Categories with official translation : "
    f"{officially_translated_categories:,}"
)

print(
    f"Categories without official mapping  : "
    f"{untranslated_category_count:,}"
)

print(
    f"Products classified as Unknown       : "
    f"{unknown_category_products:,}"
)

print(
    f"NULL English categories              : "
    f"{null_english_categories}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 28 — Final Category Translation Integration Gate
# MAGIC
# MAGIC The reference table itself must be structurally correct.
# MAGIC
# MAGIC The Products integration must additionally satisfy:
# MAGIC
# MAGIC 1. No NULL English categories.
# MAGIC 2. No officially translated category may become Unknown Category.
# MAGIC 3. Approved manual mappings must remain correct.
# MAGIC 4. Untranslated categories must be explicitly measurable.
# MAGIC
# MAGIC Untranslated categories are not automatically a pipeline failure because
# MAGIC the source data may legitimately contain categories without an available
# MAGIC translation.
# MAGIC
# MAGIC They are instead surfaced as a documented business-quality condition.

# COMMAND ----------

# ================================================================
# STEP 28 — FINAL CATEGORY TRANSLATION INTEGRATION GATE
# ================================================================

if (
    null_english_categories != 0
    or incorrect_unknown_category_count != 0
    or len(manual_mapping_errors) != 0
):

    raise ValueError(
        "CATEGORY TRANSLATION INTEGRATION QUALITY GATE FAILED."
    )

print(
    "STATUS: CATEGORY TRANSLATION INTEGRATION CHECKS PASSED"
)

print(
    f"INFO — {untranslated_category_count:,} product categories "
    "have no official translation."
)

print(
    f"INFO — {unknown_category_products:,} products are represented "
    "as 'Unknown Category'."
)

# COMMAND ----------

# ================================================================
# CATEGORY TRANSLATION EXCEPTION REVIEW
# ================================================================

manual_mapping_categories = [
    "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos"
]

# Profile the two manually handled categories
manual_mapping_profile = (
    silver_products_df
    .filter(
        F.col("product_category_name").isin(
            manual_mapping_categories
        )
    )
    .groupBy(
        "product_category_name",
        "product_category_name_english"
    )
    .count()
    .orderBy(
        "product_category_name"
    )
)

display(manual_mapping_profile)

# Count pc_gamer products
pc_gamer_count = (
    silver_products_df
    .filter(
        F.col("product_category_name") == "pc_gamer"
    )
    .count()
)

# Count portable kitchen products
portable_kitchen_count = (
    silver_products_df
    .filter(
        F.col("product_category_name")
        == "portateis_cozinha_e_preparadores_de_alimentos"
    )
    .count()
)

print(
    f"pc_gamer products              : {pc_gamer_count}"
)

print(
    f"Portable kitchen products      : {portable_kitchen_count}"
)

print(
    f"Total manual-mapping products  : "
    f"{pc_gamer_count + portable_kitchen_count}"
)

# COMMAND ----------

