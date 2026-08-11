# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Customers
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create the standardized Silver Customers dataset from Bronze Customers.
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - Standardize customer attributes.
# MAGIC - Preserve customer identity information.
# MAGIC - Deduplicate customer records at the customer identity level.
# MAGIC - Standardize city and state values.
# MAGIC - Convert ZIP prefix to an appropriate numeric type.
# MAGIC - Validate customer identifiers.
# MAGIC - Validate the resulting persisted Delta table.
# MAGIC - Prepare the dataset for downstream Orders and Gold customer analytics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `workspace.bronze.customers`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `workspace.silver.customers`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Grain
# MAGIC
# MAGIC Bronze contains customer-order-level customer records.
# MAGIC
# MAGIC A `customer_id` identifies a customer record associated with an order,
# MAGIC while `customer_unique_id` identifies the underlying customer.
# MAGIC
# MAGIC Therefore, Silver Customers will use:
# MAGIC
# MAGIC `customer_unique_id`
# MAGIC
# MAGIC as the customer-level business key.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Transformation Policy
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - Preserve customer identity information.
# MAGIC - Deduplicate records by `customer_unique_id`.
# MAGIC - Retain a representative `customer_id`.
# MAGIC - Trim string fields.
# MAGIC - Standardize city and state values.
# MAGIC - Cast `customer_zip_code_prefix` to INTEGER.
# MAGIC - Add `silver_load_timestamp`.
# MAGIC
# MAGIC No unrelated business enrichment is performed in this notebook.
# MAGIC
# MAGIC Geolocation enrichment will be handled separately.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important Identity Rule
# MAGIC
# MAGIC `customer_id` and `customer_unique_id` are not interchangeable.
# MAGIC
# MAGIC Multiple `customer_id` values may belong to the same
# MAGIC `customer_unique_id`.
# MAGIC
# MAGIC This distinction must be preserved because Gold customer analytics will
# MAGIC operate at the actual customer level.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Automation
# MAGIC
# MAGIC Critical validation failures will raise exceptions so that the notebook
# MAGIC can fail automatically when executed as a Databricks Workflow task.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Downstream Usage
# MAGIC
# MAGIC Silver Customers will support:
# MAGIC
# MAGIC - Customer 360 analytics.
# MAGIC - Repeat-purchase analysis.
# MAGIC - Customer lifetime value.
# MAGIC - Customer segmentation.
# MAGIC - Geographic customer analysis.
# MAGIC - Orders-to-customer relationships.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3 — Read Bronze Customers
# MAGIC
# MAGIC Load the persisted Bronze Customers Delta table.
# MAGIC
# MAGIC All Silver transformations and validations will operate from this
# MAGIC Bronze source.

# COMMAND ----------

bronze_customers_df = spark.table(
    "workspace.bronze.customers"
)

bronze_customer_count = bronze_customers_df.count()

print(
    f"Bronze Customers rows: "
    f"{bronze_customer_count:,}"
)

bronze_customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 — Validate Bronze Customers Schema
# MAGIC
# MAGIC Verify that the Bronze Customers table contains the required source
# MAGIC columns before transformation.

# COMMAND ----------

required_customer_columns = {
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
}

actual_customer_columns = set(
    bronze_customers_df.columns
)

missing_customer_columns = (
    required_customer_columns
    - actual_customer_columns
)

if missing_customer_columns:
    raise ValueError(
        "Bronze Customers schema validation failed. "
        f"Missing columns: {sorted(missing_customer_columns)}"
    )

print(
    "PASS — Bronze Customers contains all required columns."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 — Profile Bronze Customer Identity
# MAGIC
# MAGIC Compare `customer_id` and `customer_unique_id` populations.
# MAGIC
# MAGIC This identifies repeat customer records before customer-level
# MAGIC deduplication.

# COMMAND ----------

unique_customer_ids = (
    bronze_customers_df
    .select("customer_id")
    .distinct()
    .count()
)

unique_customer_unique_ids = (
    bronze_customers_df
    .select("customer_unique_id")
    .distinct()
    .count()
)

print(
    f"Bronze Customers rows          : "
    f"{bronze_customer_count:,}"
)

print(
    f"Unique customer_id values      : "
    f"{unique_customer_ids:,}"
)

print(
    f"Unique customer_unique_id values: "
    f"{unique_customer_unique_ids:,}"
)

print(
    f"Customer identity reduction    : "
    f"{unique_customer_ids - unique_customer_unique_ids:,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 6 — Validate Bronze Customer Identifiers
# MAGIC
# MAGIC Customer identity identifiers must not contain NULL or blank values.
# MAGIC
# MAGIC `customer_unique_id` is the primary customer-level business key for
# MAGIC Silver Customers.

# COMMAND ----------

null_customer_ids = bronze_customers_df.filter(
    F.col("customer_id").isNull()
).count()

blank_customer_ids = bronze_customers_df.filter(
    F.trim(F.col("customer_id")) == ""
).count()

null_customer_unique_ids = bronze_customers_df.filter(
    F.col("customer_unique_id").isNull()
).count()

blank_customer_unique_ids = bronze_customers_df.filter(
    F.trim(F.col("customer_unique_id")) == ""
).count()

print(
    f"NULL customer IDs              : "
    f"{null_customer_ids}"
)

print(
    f"Blank customer IDs             : "
    f"{blank_customer_ids}"
)

print(
    f"NULL customer unique IDs       : "
    f"{null_customer_unique_ids}"
)

print(
    f"Blank customer unique IDs      : "
    f"{blank_customer_unique_ids}"
)

if null_customer_unique_ids != 0:
    raise ValueError(
        "Bronze Customers quality gate failed: "
        "NULL customer_unique_id values found."
    )

if blank_customer_unique_ids != 0:
    raise ValueError(
        "Bronze Customers quality gate failed: "
        "Blank customer_unique_id values found."
    )

print(
    "PASS — Bronze customer identity key is valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 7 — Create Silver Customers
# MAGIC
# MAGIC Create one Silver record per `customer_unique_id`.
# MAGIC
# MAGIC When multiple Bronze records belong to the same customer identity,
# MAGIC select a deterministic representative record.
# MAGIC
# MAGIC No customer identity is removed because of data quality issues.
# MAGIC The transformation reduces repeated customer records to the customer
# MAGIC level.

# COMMAND ----------

from pyspark.sql.window import Window

customer_window = Window.partitionBy(
    "customer_unique_id"
).orderBy(
    F.col("customer_id").asc()
)

silver_customers_df = (
    bronze_customers_df
    .select(
        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.trim(
            F.col("customer_unique_id")
        ).alias("customer_unique_id"),

        F.col("customer_zip_code_prefix")
        .cast("integer")
        .alias("customer_zip_code_prefix"),

        F.lower(
            F.trim(
                F.col("customer_city")
            )
        ).alias("customer_city"),

        F.upper(
            F.trim(
                F.col("customer_state")
            )
        ).alias("customer_state")
    )
    .withColumn(
        "_customer_row_number",
        F.row_number().over(
            customer_window
        )
    )
    .filter(
        F.col("_customer_row_number") == 1
    )
    .drop(
        "_customer_row_number"
    )
    .withColumn(
        "silver_load_timestamp",
        F.current_timestamp()
    )
)

silver_customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 8 — Preview Silver Customers
# MAGIC
# MAGIC Review the transformed customer-level dataset before persistence.

# COMMAND ----------

display(
    silver_customers_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 9 — Validate Silver Customer Grain
# MAGIC
# MAGIC Silver Customers must contain exactly one record per
# MAGIC `customer_unique_id`.
# MAGIC
# MAGIC The Silver row count is expected to decrease from the Bronze row count
# MAGIC because multiple Bronze customer records can represent the same
# MAGIC underlying customer.

# COMMAND ----------

silver_customer_count = silver_customers_df.count()

duplicate_customer_unique_ids = (
    silver_customers_df
        .groupBy("customer_unique_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
)

print(
    f"Bronze Customers rows       : "
    f"{bronze_customer_count:,}"
)

print(
    f"Silver Customers rows       : "
    f"{silver_customer_count:,}"
)

print(
    f"Duplicate customer_unique_id: "
    f"{duplicate_customer_unique_ids}"
)

if duplicate_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Duplicate customer_unique_id values found."
    )

print(
    "PASS — Silver Customers contains one record per customer identity."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 10 — Validate Required Silver Customer Fields
# MAGIC
# MAGIC Validate that the customer-level business key and required geographic
# MAGIC attributes are populated after transformation.

# COMMAND ----------

null_customer_unique_ids = silver_customers_df.filter(
    F.col("customer_unique_id").isNull()
).count()

null_customer_ids = silver_customers_df.filter(
    F.col("customer_id").isNull()
).count()

null_customer_cities = silver_customers_df.filter(
    F.col("customer_city").isNull()
).count()

null_customer_states = silver_customers_df.filter(
    F.col("customer_state").isNull()
).count()

print(
    f"NULL customer_unique_id : "
    f"{null_customer_unique_ids}"
)

print(
    f"NULL customer_id        : "
    f"{null_customer_ids}"
)

print(
    f"NULL customer_city      : "
    f"{null_customer_cities}"
)

print(
    f"NULL customer_state     : "
    f"{null_customer_states}"
)

if null_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL customer_unique_id values found."
    )

if null_customer_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL customer_id values found."
    )

print(
    "PASS — Silver customer identity fields are populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 11 — Validate Blank Customer Attributes
# MAGIC
# MAGIC Check for empty or whitespace-only values in the standardized string
# MAGIC attributes.

# COMMAND ----------

blank_customer_unique_ids = silver_customers_df.filter(
    F.trim(
        F.col("customer_unique_id")
    ) == ""
).count()

blank_customer_ids = silver_customers_df.filter(
    F.trim(
        F.col("customer_id")
    ) == ""
).count()

blank_customer_cities = silver_customers_df.filter(
    F.trim(
        F.col("customer_city")
    ) == ""
).count()

blank_customer_states = silver_customers_df.filter(
    F.trim(
        F.col("customer_state")
    ) == ""
).count()

print(
    f"Blank customer_unique_id : "
    f"{blank_customer_unique_ids}"
)

print(
    f"Blank customer_id        : "
    f"{blank_customer_ids}"
)

print(
    f"Blank customer_city      : "
    f"{blank_customer_cities}"
)

print(
    f"Blank customer_state     : "
    f"{blank_customer_states}"
)

if blank_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Blank customer_unique_id values found."
    )

if blank_customer_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Blank customer_id values found."
    )

print(
    "PASS — No blank customer identity values found."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 12 — Validate Customer ZIP Prefix
# MAGIC
# MAGIC Validate the standardized customer ZIP prefix.
# MAGIC
# MAGIC ZIP prefixes are geographic identifiers rather than measures, so negative
# MAGIC values are invalid.

# COMMAND ----------

invalid_customer_zip_prefixes = silver_customers_df.filter(
    F.col("customer_zip_code_prefix").isNotNull()
    & (
        F.col("customer_zip_code_prefix") < 0
    )
).count()

print(
    f"Invalid customer ZIP prefixes: "
    f"{invalid_customer_zip_prefixes}"
)

if invalid_customer_zip_prefixes != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Negative customer ZIP prefixes found."
    )

print(
    "PASS — Customer ZIP prefix validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 13 — Validate Silver Customer Datatypes
# MAGIC
# MAGIC Confirm that the persisted Silver design uses the expected datatypes
# MAGIC for customer identity, geographic attributes, and load metadata.

# COMMAND ----------

expected_customer_types = {
    "customer_id": "string",
    "customer_unique_id": "string",
    "customer_zip_code_prefix": "int",
    "customer_city": "string",
    "customer_state": "string",
    "silver_load_timestamp": "timestamp"
}

actual_customer_types = dict(
    silver_customers_df.dtypes
)

datatype_errors = {
    column: {
        "expected": expected_type,
        "actual": actual_customer_types.get(column)
    }
    for column, expected_type in expected_customer_types.items()
    if actual_customer_types.get(column) != expected_type
}

if datatype_errors:
    raise ValueError(
        "Silver Customers datatype validation failed: "
        f"{datatype_errors}"
    )

print(
    "PASS — Silver Customers datatypes are correct."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 14 — Analyze Customer Identity Relationships
# MAGIC
# MAGIC Analyze the relationship between `customer_unique_id` and `customer_id`.
# MAGIC
# MAGIC A customer may have multiple `customer_id` values because the source
# MAGIC dataset can contain multiple customer records for the same underlying
# MAGIC customer.
# MAGIC
# MAGIC This is an expected source-data relationship and is not treated as a
# MAGIC Silver quality failure.

# COMMAND ----------

customer_identity_relationship_df = (
    bronze_customers_df
    .groupBy("customer_unique_id")
    .agg(
        F.countDistinct("customer_id").alias(
            "customer_id_count"
        )
    )
)

customers_with_multiple_ids = (
    customer_identity_relationship_df
    .filter(
        F.col("customer_id_count") > 1
    )
    .count()
)

maximum_customer_ids_per_identity = (
    customer_identity_relationship_df
    .agg(
        F.max("customer_id_count")
        .alias("max_customer_id_count")
    )
    .collect()[0]["max_customer_id_count"]
)

print(
    f"Customer identities with multiple customer IDs : "
    f"{customers_with_multiple_ids:,}"
)

print(
    f"Maximum customer IDs per customer identity      : "
    f"{maximum_customer_ids_per_identity}"
)

print(
    "PASS — Customer identity relationship analyzed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 15 — Profile Silver Customer Geography
# MAGIC
# MAGIC Profile customer city and state distributions after standardization.
# MAGIC
# MAGIC This provides a baseline for downstream geographic customer analytics.

# COMMAND ----------

silver_customer_state_count = (
    silver_customers_df
    .select("customer_state")
    .distinct()
    .count()
)

silver_customer_city_count = (
    silver_customers_df
    .select("customer_city")
    .distinct()
    .count()
)

print(
    f"Distinct customer states : "
    f"{silver_customer_state_count:,}"
)

print(
    f"Distinct customer cities  : "
    f"{silver_customer_city_count:,}"
)

print(
    "PASS — Silver customer geographic attributes are profiled."
)

# COMMAND ----------

display(
    silver_customers_df
    .groupBy("customer_state")
    .count()
    .orderBy(
        F.col("count").desc()
    )
)

# COMMAND ----------

display(
    silver_customers_df
    .groupBy("customer_city")
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 16 — Persist Silver Customers
# MAGIC
# MAGIC Write the validated Silver Customers dataset as a managed Delta table.
# MAGIC
# MAGIC The target table represents one record per `customer_unique_id`.

# COMMAND ----------

(
    silver_customers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.silver.customers"
    )
)

print(
    "Silver Customers table created successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 17 — Reload Persisted Silver Customers
# MAGIC
# MAGIC Reload the managed Delta table and validate the actual persisted
# MAGIC dataset.

# COMMAND ----------

final_silver_customers_df = spark.table(
    "workspace.silver.customers"
)

final_silver_customer_count = (
    final_silver_customers_df.count()
)

print(
    f"Persisted Silver Customers rows: "
    f"{final_silver_customer_count:,}"
)

final_silver_customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 18 — Validate Persisted Silver Customer Grain
# MAGIC
# MAGIC Verify that the persisted Silver Customers table contains exactly one
# MAGIC record per `customer_unique_id`.
# MAGIC
# MAGIC Because the Silver transformation changes the dataset grain from
# MAGIC customer-order records to customer identities, the expected Silver row
# MAGIC count is the number of distinct `customer_unique_id` values in Bronze.

# COMMAND ----------

expected_silver_customer_count = (
    bronze_customers_df
    .select("customer_unique_id")
    .distinct()
    .count()
)

actual_silver_customer_count = (
    final_silver_customers_df.count()
)

duplicate_silver_customer_unique_ids = (
    final_silver_customers_df
    .groupBy("customer_unique_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Expected Silver customer rows : "
    f"{expected_silver_customer_count:,}"
)

print(
    f"Actual Silver customer rows   : "
    f"{actual_silver_customer_count:,}"
)

print(
    f"Duplicate customer identities  : "
    f"{duplicate_silver_customer_unique_ids}"
)

if actual_silver_customer_count != expected_silver_customer_count:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Silver customer count does not match the number of "
        "distinct Bronze customer_unique_id values."
    )

if duplicate_silver_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Duplicate customer_unique_id values found."
    )

print(
    "PASS — Persisted Silver Customers has the correct customer grain."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 19 — Validate Customer Identity Reconciliation
# MAGIC
# MAGIC Verify that the exact set of `customer_unique_id` values is preserved
# MAGIC between Bronze and Silver.
# MAGIC
# MAGIC The transformation may reduce the number of rows, but it must not
# MAGIC remove or create customer identities.

# COMMAND ----------

bronze_customer_identity_df = (
    bronze_customers_df
    .select("customer_unique_id")
    .distinct()
)

silver_customer_identity_df = (
    final_silver_customers_df
    .select("customer_unique_id")
    .distinct()
)

bronze_identities_missing_in_silver = (
    bronze_customer_identity_df
    .join(
        silver_customer_identity_df,
        on="customer_unique_id",
        how="left_anti"
    )
    .count()
)

silver_identities_not_in_bronze = (
    silver_customer_identity_df
    .join(
        bronze_customer_identity_df,
        on="customer_unique_id",
        how="left_anti"
    )
    .count()
)

print(
    f"Bronze customer identities missing in Silver : "
    f"{bronze_identities_missing_in_silver}"
)

print(
    f"Silver identities not present in Bronze      : "
    f"{silver_identities_not_in_bronze}"
)

if bronze_identities_missing_in_silver != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Bronze customer identities are missing from Silver."
    )

if silver_identities_not_in_bronze != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Silver contains customer identities not present in Bronze."
    )

print(
    "PASS — Bronze and Silver customer identities match exactly."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 20 — Validate Persisted Silver Customer Fields
# MAGIC
# MAGIC Validate required identifiers, standardized geographic fields, and
# MAGIC Silver load metadata in the persisted Silver Customers table.

# COMMAND ----------

final_null_customer_unique_ids = (
    final_silver_customers_df
    .filter(
        F.col("customer_unique_id").isNull()
    )
    .count()
)

final_duplicate_customer_ids = (
    final_silver_customers_df
    .groupBy("customer_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

final_null_customer_ids = (
    final_silver_customers_df
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)

final_blank_customer_unique_ids = (
    final_silver_customers_df
    .filter(
        F.trim(
            F.col("customer_unique_id")
        ) == ""
    )
    .count()
)

final_blank_customer_ids = (
    final_silver_customers_df
    .filter(
        F.trim(
            F.col("customer_id")
        ) == ""
    )
    .count()
)

final_null_customer_cities = (
    final_silver_customers_df
    .filter(
        F.col("customer_city").isNull()
    )
    .count()
)

final_null_customer_states = (
    final_silver_customers_df
    .filter(
        F.col("customer_state").isNull()
    )
    .count()
)

final_null_silver_timestamps = (
    final_silver_customers_df
    .filter(
        F.col("silver_load_timestamp").isNull()
    )
    .count()
)

print(
    f"NULL customer_unique_id       : "
    f"{final_null_customer_unique_ids}"
)

print(
    f"Duplicate customer_id values  : "
    f"{final_duplicate_customer_ids}"
)

print(
    f"NULL customer_id              : "
    f"{final_null_customer_ids}"
)

print(
    f"Blank customer_unique_id      : "
    f"{final_blank_customer_unique_ids}"
)

print(
    f"Blank customer_id             : "
    f"{final_blank_customer_ids}"
)

print(
    f"NULL customer_city            : "
    f"{final_null_customer_cities}"
)

print(
    f"NULL customer_state           : "
    f"{final_null_customer_states}"
)

print(
    f"NULL Silver load timestamps   : "
    f"{final_null_silver_timestamps}"
)

if final_null_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL customer_unique_id values found."
    )

if final_duplicate_customer_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Duplicate customer_id values found."
    )

if final_null_customer_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL customer_id values found."
    )

if final_blank_customer_unique_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Blank customer_unique_id values found."
    )

if final_blank_customer_ids != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Blank customer_id values found."
    )

if final_null_silver_timestamps != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Persisted Silver customer fields are valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 21 — Validate Persisted Customer ZIP Prefix
# MAGIC
# MAGIC Validate the persisted customer ZIP prefix values.
# MAGIC
# MAGIC Negative ZIP prefixes are invalid. NULL values are retained only when
# MAGIC the source value was unavailable.

# COMMAND ----------

final_invalid_zip_prefixes = (
    final_silver_customers_df
    .filter(
        F.col("customer_zip_code_prefix").isNotNull()
        & (
            F.col("customer_zip_code_prefix") < 0
        )
    )
    .count()
)

print(
    f"Invalid customer ZIP prefixes: "
    f"{final_invalid_zip_prefixes}"
)

if final_invalid_zip_prefixes != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "Invalid negative ZIP prefixes found."
    )

print(
    "PASS — Persisted customer ZIP prefixes are valid."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 22 — Validate Silver Load Timestamp
# MAGIC
# MAGIC Every persisted Silver Customers record must contain a load timestamp
# MAGIC so that pipeline execution can be traced.

# COMMAND ----------

final_null_silver_timestamps = (
    final_silver_customers_df
    .filter(
        F.col("silver_load_timestamp").isNull()
    )
    .count()
)

print(
    f"NULL Silver load timestamps: "
    f"{final_null_silver_timestamps}"
)

if final_null_silver_timestamps != 0:
    raise ValueError(
        "Silver Customers quality gate failed: "
        "NULL silver_load_timestamp values found."
    )

print(
    "PASS — Silver Customers load timestamp is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 23 — Final Silver Customers Quality Gate
# MAGIC
# MAGIC Validate the persisted Silver Customers Delta table against all
# MAGIC critical identity, grain, geographic, and operational quality rules.
# MAGIC
# MAGIC The expected Silver grain is one record per `customer_unique_id`.
# MAGIC
# MAGIC The reduction from Bronze rows to Silver rows is intentional because
# MAGIC Bronze contains multiple records for the same underlying customer.

# COMMAND ----------

final_customers_df = spark.table(
    "workspace.silver.customers"
)

final_customer_count = (
    final_customers_df.count()
)

expected_customer_count = (
    bronze_customers_df
    .select("customer_unique_id")
    .distinct()
    .count()
)

final_duplicate_customer_unique_ids = (
    final_customers_df
    .groupBy("customer_unique_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

final_null_customer_unique_ids = (
    final_customers_df
    .filter(
        F.col("customer_unique_id").isNull()
    )
    .count()
)

final_null_customer_ids = (
    final_customers_df
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)

final_duplicate_customer_ids = (
    final_customers_df
    .groupBy("customer_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

final_blank_customer_unique_ids = (
    final_customers_df
    .filter(
        F.trim(
            F.col("customer_unique_id")
        ) == ""
    )
    .count()
)

final_blank_customer_ids = (
    final_customers_df
    .filter(
        F.trim(
            F.col("customer_id")
        ) == ""
    )
    .count()
)

final_invalid_zip_prefixes = (
    final_customers_df
    .filter(
        F.col("customer_zip_code_prefix").isNotNull()
        & (
            F.col("customer_zip_code_prefix") < 0
        )
    )
    .count()
)

final_null_silver_timestamps = (
    final_customers_df
    .filter(
        F.col("silver_load_timestamp").isNull()
    )
    .count()
)

print("=" * 75)
print("SILVER CUSTOMERS QUALITY GATE")
print("=" * 75)

print(
    f"Bronze customer rows              : "
    f"{bronze_customer_count:,}"
)

print(
    f"Expected Silver customer rows     : "
    f"{expected_customer_count:,}"
)

print(
    f"Actual Silver customer rows       : "
    f"{final_customer_count:,}"
)

print(
    f"Duplicate customer identities     : "
    f"{final_duplicate_customer_unique_ids}"
)

print(
    f"NULL customer_unique_id           : "
    f"{final_null_customer_unique_ids}"
)

print(
    f"NULL customer_id                  : "
    f"{final_null_customer_ids}"
)

print(
    f"Duplicate customer_id values      : "
    f"{final_duplicate_customer_ids}"
)

print(
    f"Blank customer_unique_id           : "
    f"{final_blank_customer_unique_ids}"
)

print(
    f"Blank customer_id                  : "
    f"{final_blank_customer_ids}"
)

print(
    f"Invalid ZIP prefixes               : "
    f"{final_invalid_zip_prefixes}"
)

print(
    f"NULL Silver load timestamps        : "
    f"{final_null_silver_timestamps}"
)

print("=" * 75)

if (
    final_customer_count == expected_customer_count
    and final_duplicate_customer_unique_ids == 0
    and final_null_customer_unique_ids == 0
    and final_null_customer_ids == 0
    and final_duplicate_customer_ids == 0
    and final_blank_customer_unique_ids == 0
    and final_blank_customer_ids == 0
    and final_invalid_zip_prefixes == 0
    and final_null_silver_timestamps == 0
):
    print(
        "STATUS: ALL SILVER CUSTOMERS CHECKS PASSED"
    )
else:
    raise ValueError(
        "SILVER CUSTOMERS QUALITY GATE FAILED."
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL workspace.silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 25 — Validate Silver Customers Delta History
# MAGIC
# MAGIC Verify the Delta transaction history for the persisted Silver Customers
# MAGIC table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 26 — Verify Silver Customers Table Registration
# MAGIC
# MAGIC Confirm that the Silver Customers table is registered in the
# MAGIC `workspace.silver` schema and available for downstream workloads.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 27 — Final Silver Customers Preview
# MAGIC
# MAGIC Review the final persisted Silver Customers dataset.

# COMMAND ----------

final_customers_preview_df = spark.table(
    "workspace.silver.customers"
)

display(
    final_customers_preview_df.limit(20)
)

# COMMAND ----------

