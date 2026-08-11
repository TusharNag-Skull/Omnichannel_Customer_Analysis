# Databricks notebook source
# Check whether dbutils secrets are available

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# ================================================================
# STEP 7 — CHECK DATABRICKS SDK
# ================================================================

try:
    from databricks.sdk import WorkspaceClient

    print("PASS — Databricks SDK is available.")

except Exception as e:
    raise RuntimeError(
        "Databricks SDK is not available in this environment. "
        f"Error: {e}"
    )

# COMMAND ----------

# ================================================================
# STEP 8 — INITIALIZE DATABRICKS WORKSPACE CLIENT
# ================================================================

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print(
    "PASS — Databricks WorkspaceClient initialized successfully."
)

# COMMAND ----------

# ================================================================
# STEP 10 — STORE SNOWFLAKE ACCOUNT IDENTIFIER
# ================================================================

w.secrets.put_secret(
    scope="snowflake-secrets",
    key="account",
    string_value="ZRNMXCD-IK77161"
)

print(
    "PASS — Snowflake account identifier stored."
)

# COMMAND ----------

# ================================================================
# STEP 10 — STORE SNOWFLAKE ACCOUNT IDENTIFIER
# ================================================================

w.secrets.put_secret(
    scope="snowflake-secrets",
    key="account",
    string_value="ZRNMXCD-IK77161"
)

print(
    "PASS — Snowflake account identifier stored."
)

# COMMAND ----------

# ================================================================
# STEP 7.5 — SECURELY CAPTURE SNOWFLAKE PASSWORD
# ================================================================

from getpass import getpass

snowflake_password_input = getpass(
    "Enter your Snowflake password: "
)

if not snowflake_password_input:
    raise ValueError(
        "Snowflake password cannot be empty."
    )

print(
    "PASS — Snowflake password was entered securely."
)

# COMMAND ----------

# ================================================================
# STEP 7.6 — STORE SNOWFLAKE PASSWORD
# ================================================================

w.secrets.put_secret(
    scope="snowflake-secrets",
    key="snowflake-password",
    string_value=snowflake_password_input
)

print(
    "PASS — Snowflake password stored in Databricks secret scope."
)

# Remove the local Python variable after storing it
del snowflake_password_input

# COMMAND ----------

# ================================================================
# STEP 8 — VERIFY SECRET KEYS
# ================================================================

SECRET_SCOPE = "snowflake-secrets"

secret_metadata = dbutils.secrets.list(
    SECRET_SCOPE
)

available_keys = {
    secret.key
    for secret in secret_metadata
}

required_keys = {
    "account",
    "snowflake-password"
}

missing_keys = required_keys - available_keys

print(
    f"Secret scope    : {SECRET_SCOPE}"
)

print(
    f"Available keys  : {sorted(available_keys)}"
)

print(
    f"Missing keys    : {sorted(missing_keys)}"
)

if missing_keys:
    raise ValueError(
        "Missing required Snowflake secrets: "
        f"{sorted(missing_keys)}"
    )

print(
    "PASS — All required Snowflake secret keys are present."
)

# COMMAND ----------

# ================================================================
# STEP 9 — LOAD SNOWFLAKE CREDENTIALS
# ================================================================

SECRET_SCOPE = "snowflake-secrets"

sf_password = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="snowflake-password"
)

sf_account = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="account"
)

# Snowflake username is known from the Snowflake setup.
sf_user = "COLLEGE"

if sf_account != "ZRNMXCD-IK77161":
    raise ValueError(
        "Snowflake account secret is incorrect."
    )

if not sf_password:
    raise ValueError(
        "Snowflake password secret is empty."
    )

print(
    f"Snowflake account : {sf_account}"
)

print(
    f"Snowflake username: {sf_user}"
)

print(
    "Snowflake password: ********"
)

print(
    "PASS — Snowflake credentials loaded successfully."
)

# COMMAND ----------

# ================================================================
# STEP 9 — LOAD SNOWFLAKE CREDENTIALS
# ================================================================

SECRET_SCOPE = "snowflake-secrets"

sf_password = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="snowflake-password"
)

sf_account = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="account"
)

# Snowflake username verified from Snowflake
sf_user = "COLLEGE"

if sf_account != "ZRNMXCD-IK77161":
    raise ValueError(
        "Snowflake account secret is incorrect."
    )

if not sf_password:
    raise ValueError(
        "Snowflake password secret is empty."
    )

print(f"Snowflake account : {sf_account}")
print(f"Snowflake username: {sf_user}")
print("Snowflake password: ********")
print("PASS — Snowflake credentials loaded successfully.")

# COMMAND ----------

# ================================================================
# STEP 10A — SNOWFLAKE SERVERLESS WRITE CONFIGURATION
# ================================================================

sf_write_options = {
    "sfAccount": sf_account,
    "sfUser": sf_user,
    "sfPassword": sf_password,

    "sfDatabase": "RETAIL_CUSTOMER360",
    "sfSchema": "ANALYTICS_GOLD",

    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "CUSTOMER360_DATABRICKS_ROLE"
}

print(
    f"Snowflake account  : {sf_account}"
)

print(
    "Snowflake database : RETAIL_CUSTOMER360"
)

print(
    "Snowflake schema   : ANALYTICS_GOLD"
)

print(
    "Snowflake warehouse: COMPUTE_WH"
)

print(
    "Snowflake role     : CUSTOMER360_DATABRICKS_ROLE"
)

print(
    "PASS — Serverless Snowflake WRITE configuration prepared."
)

# COMMAND ----------

# ================================================================
# STEP 11 — DATABRICKS → SNOWFLAKE CONNECTION TEST
# ================================================================

sf_options = {
    "sfURL": "ZRNMXCD-IK77161.snowflakecomputing.com",

    "sfUser": sf_user,
    "sfPassword": sf_password,

    "sfDatabase": "RETAIL_CUSTOMER360",
    "sfSchema": "ANALYTICS_GOLD",

    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "CUSTOMER360_DATABRICKS_ROLE"
}

connection_test_df = (
    spark.read
    .format("snowflake")
    .options(**sf_options)
    .option(
        "query",
        """
        SELECT
            CURRENT_VERSION() AS snowflake_version,
            CURRENT_USER() AS snowflake_user,
            CURRENT_ROLE() AS snowflake_role,
            CURRENT_DATABASE() AS snowflake_database,
            CURRENT_SCHEMA() AS snowflake_schema,
            CURRENT_WAREHOUSE() AS snowflake_warehouse
        """
    )
    .load()
)

display(connection_test_df)

# COMMAND ----------

# ================================================================
# STEP 12 — SNOWFLAKE CONNECTION QUALITY GATE
# ================================================================

# Retrieve the first row returned by the connection test

result = (
    connection_test_df
    .first()
    .asDict()
)

print("Connection test result:")
print(result)

# ---------------------------------------------------------------
# Extract returned values safely
# ---------------------------------------------------------------

actual_database = result.get("SNOWFLAKE_DATABASE")
actual_schema = result.get("SNOWFLAKE_SCHEMA")
actual_role = result.get("SNOWFLAKE_ROLE")
actual_warehouse = result.get("SNOWFLAKE_WAREHOUSE")

# ---------------------------------------------------------------
# Display values
# ---------------------------------------------------------------

print()
print(f"Database  : {actual_database}")
print(f"Schema    : {actual_schema}")
print(f"Role      : {actual_role}")
print(f"Warehouse : {actual_warehouse}")

# ---------------------------------------------------------------
# Validate database
# ---------------------------------------------------------------

if actual_database != "RETAIL_CUSTOMER360":
    raise ValueError(
        "Snowflake database validation failed. "
        f"Expected RETAIL_CUSTOMER360, got {actual_database}."
    )

# ---------------------------------------------------------------
# Validate schema
# ---------------------------------------------------------------

if actual_schema != "ANALYTICS_GOLD":
    raise ValueError(
        "Snowflake schema validation failed. "
        f"Expected ANALYTICS_GOLD, got {actual_schema}."
    )

# ---------------------------------------------------------------
# Validate role
# ---------------------------------------------------------------

if actual_role != "CUSTOMER360_DATABRICKS_ROLE":
    raise ValueError(
        "Snowflake role validation failed. "
        f"Expected CUSTOMER360_DATABRICKS_ROLE, got {actual_role}."
    )

# ---------------------------------------------------------------
# Validate warehouse
# ---------------------------------------------------------------

if actual_warehouse != "COMPUTE_WH":
    raise ValueError(
        "Snowflake warehouse validation failed. "
        f"Expected COMPUTE_WH, got {actual_warehouse}."
    )

print()
print(
    "PASS — Databricks is successfully connected "
    "to the correct Snowflake environment."
)

# COMMAND ----------

# ================================================================
# STEP 13 — CREATE TEMPORARY CONNECTIVITY TEST DATA
# ================================================================

# This is ONLY a technical integration test.
# It is NOT project data.
# It will be deleted after validation.

TEST_TABLE = "DATABRICKS_SNOWFLAKE_CONNECTION_TEST"

connection_test_data_df = spark.createDataFrame(
    [
        (
            "CONNECTION_TEST_001",
            "Databricks to Snowflake connectivity test"
        )
    ],
    [
        "test_id",
        "test_message"
    ]
)

display(connection_test_data_df)

# COMMAND ----------

# ================================================================
# STEP 14 — SERVERLESS SNOWFLAKE WRITE CONFIGURATION
# ================================================================

sf_write_options = {
    "host": "ZRNMXCD-IK77161.snowflakecomputing.com",
    "port": "443",

    "sfUser": sf_user,
    "sfPassword": sf_password,

    "sfDatabase": "RETAIL_CUSTOMER360",
    "sfSchema": "ANALYTICS_GOLD",

    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "CUSTOMER360_DATABRICKS_ROLE"
}

print(
    "Snowflake host      : "
    "ZRNMXCD-IK77161.snowflakecomputing.com"
)

print(
    "Snowflake port      : 443"
)

print(
    "Snowflake database  : RETAIL_CUSTOMER360"
)

print(
    "Snowflake schema    : ANALYTICS_GOLD"
)

print(
    "Snowflake warehouse : COMPUTE_WH"
)

print(
    "Snowflake role      : CUSTOMER360_DATABRICKS_ROLE"
)

print(
    "PASS — Serverless Snowflake write configuration prepared."
)

# COMMAND ----------

# ================================================================
# STEP 15 — TEMPORARY SNOWFLAKE WRITE TEST
# ================================================================

# This is ONLY an integration test.
# It is not project data.
# It will be deleted after validation.

TEST_TABLE = "DATABRICKS_SNOWFLAKE_CONNECTION_TEST"

(
    connection_test_data_df
    .write
    .format("snowflake")
    .options(**sf_write_options)
    .option(
        "dbtable",
        TEST_TABLE
    )
    .mode("overwrite")
    .save()
)

print(
    "PASS — Databricks successfully wrote the "
    "temporary test table to Snowflake."
)

# COMMAND ----------

# ================================================================
# STEP 16 — READ TEMPORARY TEST TABLE FROM SNOWFLAKE
# ================================================================

# Purpose:
# Confirm that the data written by Databricks can be read back
# correctly from Snowflake.

roundtrip_test_df = (
    spark.read
    .format("snowflake")
    .options(**sf_options)
    .option(
        "dbtable",
        TEST_TABLE
    )
    .load()
)

display(roundtrip_test_df)

# COMMAND ----------

# ================================================================
# STEP 17 — ROUND-TRIP QUALITY GATE
# ================================================================

expected_rows = 1

actual_rows = roundtrip_test_df.count()

print(f"Expected rows : {expected_rows}")
print(f"Actual rows   : {actual_rows}")

if actual_rows != expected_rows:
    raise ValueError(
        "Databricks → Snowflake → Databricks "
        "round-trip validation failed."
    )

expected_test_id = "CONNECTION_TEST_001"

actual_test_id = (
    roundtrip_test_df
    .select("test_id")
    .first()["test_id"]
)

if actual_test_id != expected_test_id:
    raise ValueError(
        "Round-trip data validation failed. "
        f"Expected {expected_test_id}, got {actual_test_id}."
    )

print(
    "PASS — Databricks successfully wrote, "
    "read, and validated data in Snowflake."
)

# COMMAND ----------

# ================================================================
# STEP 18 — REMOVE TEMPORARY TEST TABLE
# ================================================================

# The table contains synthetic connectivity-test data only.
# It must be removed after successful validation.

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    RETAIL_CUSTOMER360.ANALYTICS_GOLD.{TEST_TABLE}
    """
)

print(
    "PASS — Temporary connectivity test table removed."
)

# COMMAND ----------

# ================================================================
# STEP 19 — VERIFY TEMPORARY TEST CLEANUP
# ================================================================

cleanup_query = f"""
SELECT COUNT(*) AS TABLE_COUNT
FROM RETAIL_CUSTOMER360.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
AND TABLE_NAME = '{TEST_TABLE}'
"""

cleanup_df = (
    spark.read
    .format("snowflake")
    .options(**sf_options)
    .option("query", cleanup_query)
    .load()
)

# Spark Connect-safe extraction
cleanup_result = cleanup_df.first().asDict()

print("Cleanup verification result:")
print(cleanup_result)

# Find TABLE_COUNT without assuming column-case
remaining_test_tables = None

for key, value in cleanup_result.items():
    if key.lower() == "table_count":
        remaining_test_tables = int(value)
        break

if remaining_test_tables is None:
    raise ValueError(
        "Cleanup verification failed: TABLE_COUNT was not returned."
    )

print(f"Remaining test tables : {remaining_test_tables}")

if remaining_test_tables != 0:
    raise ValueError(
        "Temporary connectivity test table still exists."
    )

print(
    "PASS — Temporary connectivity test data "
    "has been completely removed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing
# MAGIC

# COMMAND ----------

# # ================================================================
# # STEP 20 — CHECK TEMPORARY TEST TABLE
# # ================================================================

# check_test_table_query = f"""
# SELECT
#     TABLE_CATALOG,
#     TABLE_SCHEMA,
#     TABLE_NAME,
#     TABLE_TYPE
# FROM RETAIL_CUSTOMER360.INFORMATION_SCHEMA.TABLES
# WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
# AND TABLE_NAME = '{TEST_TABLE}'
# """

# check_test_table_df = (
#     spark.read
#     .format("snowflake")
#     .options(**sf_options)
#     .option("query", check_test_table_query)
#     .load()
# )

# display(check_test_table_df)

# COMMAND ----------

# # ================================================================
# # STEP 21 — DROP TEMPORARY TEST TABLE FROM SNOWFLAKE
# # ================================================================

# drop_test_table_query = f"""
# DROP TABLE IF EXISTS
# RETAIL_CUSTOMER360.ANALYTICS_GOLD.{TEST_TABLE}
# """

# drop_result_df = (
#     spark.read
#     .format("snowflake")
#     .options(**sf_options)
#     .option("query", drop_test_table_query)
#     .load()
# )

# print(
#     "PASS — DROP TABLE command sent to Snowflake."
# )

# COMMAND ----------

# # ================================================================
# # STEP 22 — VERIFY TEMPORARY TEST TABLE WAS DELETED
# # ================================================================

# cleanup_query = f"""
# SELECT COUNT(*) AS TABLE_COUNT
# FROM RETAIL_CUSTOMER360.INFORMATION_SCHEMA.TABLES
# WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
# AND TABLE_NAME = '{TEST_TABLE}'
# """

# cleanup_df = (
#     spark.read
#     .format("snowflake")
#     .options(**sf_options)
#     .option("query", cleanup_query)
#     .load()
# )

# cleanup_result = cleanup_df.first().asDict()

# print("Cleanup verification result:")
# print(cleanup_result)

# COMMAND ----------

# ================================================================
# STEP 19 — TWO-WAY CLEANUP VERIFICATION
# ================================================================

cleanup_query = """
SELECT
    COUNT(*) AS TABLE_COUNT
FROM RETAIL_CUSTOMER360.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
AND TABLE_NAME = 'DATABRICKS_SNOWFLAKE_CONNECTION_TEST'
"""

cleanup_df = (
    spark.read
    .format("snowflake")
    .options(**sf_options)
    .option("query", cleanup_query)
    .load()
)

cleanup_result = cleanup_df.first().asDict()

print("Databricks → Snowflake cleanup verification:")
print(cleanup_result)

remaining_test_tables = None

for key, value in cleanup_result.items():
    if key.lower() == "table_count":
        remaining_test_tables = int(value)
        break

if remaining_test_tables is None:
    raise ValueError(
        "TABLE_COUNT was not returned by Snowflake."
    )

print(
    f"Remaining test tables in Snowflake : "
    f"{remaining_test_tables}"
)

if remaining_test_tables != 0:
    raise ValueError(
        "Temporary connectivity test table still exists "
        "in Snowflake."
    )

print(
    "PASS — Databricks confirms the temporary "
    "Snowflake test table has been removed."
)

# COMMAND ----------

# ================================================================
# STEP 21 — LOAD VALIDATED GOLD CUSTOMER 360
# ================================================================

# Source:
# workspace.gold.customer_360
#
# This is the already-validated Gold dataset.
# No transformations are performed here.

gold_customer_360_source = (
    spark.table(
        "workspace.gold.customer_360"
    )
)

print(
    f"Gold Customer 360 source rows : "
    f"{gold_customer_360_source.count():,}"
)

gold_customer_360_source.printSchema()

# COMMAND ----------

# ================================================================
# STEP 22 — GOLD SOURCE GRAIN AUDIT
# ================================================================

source_row_count = (
    gold_customer_360_source.count()
)

source_customer_count = (
    gold_customer_360_source
    .select("customer_unique_id")
    .distinct()
    .count()
)

source_duplicate_customer_count = (
    gold_customer_360_source
    .groupBy("customer_unique_id")
    .count()
    .filter("count > 1")
    .count()
)

print(
    f"Source rows             : {source_row_count:,}"
)

print(
    f"Distinct customers      : "
    f"{source_customer_count:,}"
)

print(
    f"Duplicate customer keys : "
    f"{source_duplicate_customer_count:,}"
)

# ---------------------------------------------------------------
# Validate expected Gold row count
# ---------------------------------------------------------------

if source_row_count != 93358:
    raise ValueError(
        "Gold source row-count validation failed. "
        f"Expected 93,358, got {source_row_count:,}."
    )

# ---------------------------------------------------------------
# Validate customer grain
# ---------------------------------------------------------------

if source_duplicate_customer_count != 0:
    raise ValueError(
        "Gold source grain validation failed: "
        "duplicate customer_unique_id values found."
    )

# ---------------------------------------------------------------
# Validate one row per customer
# ---------------------------------------------------------------

if source_row_count != source_customer_count:
    raise ValueError(
        "Gold source grain validation failed: "
        "rows do not equal distinct customer identities."
    )

print(
    "PASS — Gold Customer 360 source grain is valid."
)

# COMMAND ----------

# ================================================================
# STEP 23 — RELOAD GOLD SOURCE + DATATYPE AUDIT
# ================================================================

# Purpose:
# Reload the already-validated Gold Customer 360 table and verify
# that its schema is exactly what we expect before publishing it
# to Snowflake.
#
# No transformation is performed.

from pyspark.sql import functions as F

gold_customer_360_source = (
    spark.table(
        "workspace.gold.customer_360"
    )
)

print(
    f"Gold Customer 360 source rows : "
    f"{gold_customer_360_source.count():,}"
)

print()
print("Gold Customer 360 source schema:")

gold_customer_360_source.printSchema()

# ---------------------------------------------------------------
# Expected Gold datatypes
# ---------------------------------------------------------------

expected_gold_datatypes = {
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

# ---------------------------------------------------------------
# Actual datatypes
# ---------------------------------------------------------------

actual_gold_datatypes = {
    field.name: field.dataType.simpleString()
    for field in gold_customer_360_source.schema.fields
}

# ---------------------------------------------------------------
# Check for missing/unexpected columns
# ---------------------------------------------------------------

expected_columns = set(
    expected_gold_datatypes.keys()
)

actual_columns = set(
    actual_gold_datatypes.keys()
)

missing_columns = (
    expected_columns - actual_columns
)

unexpected_columns = (
    actual_columns - expected_columns
)

# ---------------------------------------------------------------
# Check datatypes
# ---------------------------------------------------------------

datatype_errors = {}

for column_name, expected_type in expected_gold_datatypes.items():

    actual_type = actual_gold_datatypes.get(
        column_name
    )

    if actual_type != expected_type:

        datatype_errors[column_name] = {
            "expected": expected_type,
            "actual": actual_type
        }

# ---------------------------------------------------------------
# Report
# ---------------------------------------------------------------

print()
print(
    f"Missing columns    : {len(missing_columns)}"
)

print(
    f"Unexpected columns : {len(unexpected_columns)}"
)

print(
    f"Datatype errors    : {len(datatype_errors)}"
)

# ---------------------------------------------------------------
# Quality gate
# ---------------------------------------------------------------

if missing_columns:

    raise ValueError(
        "Gold Customer 360 schema validation failed. "
        f"Missing columns: {sorted(missing_columns)}"
    )

if unexpected_columns:

    raise ValueError(
        "Gold Customer 360 schema validation failed. "
        f"Unexpected columns: {sorted(unexpected_columns)}"
    )

if datatype_errors:

    print("Datatype errors:")
    print(datatype_errors)

    raise ValueError(
        "Gold Customer 360 source datatype validation failed."
    )

print(
    "PASS — Gold Customer 360 source schema and "
    "datatypes are correct."
)

# COMMAND ----------

# ================================================================
# STEP 24 — GOLD SOURCE REQUIRED-FIELD AUDIT
# ================================================================

required_columns = [
    "customer_unique_id",
    "recency_days",
    "frequency",
    "monetary",
    "r_score",
    "f_score",
    "m_score",
    "rfm_score",
    "rfm_segment",
    "gold_load_timestamp"
]

required_field_errors = {}

for column_name in required_columns:

    invalid_count = (
        gold_customer_360_source
        .filter(
            F.col(column_name).isNull()
        )
        .count()
    )

    if invalid_count != 0:

        required_field_errors[column_name] = (
            invalid_count
        )

print(
    f"Required-field errors : "
    f"{len(required_field_errors)}"
)

if required_field_errors:

    print(required_field_errors)

    raise ValueError(
        "Gold Customer 360 required-field validation failed."
    )

print(
    "PASS — Gold Customer 360 required fields are populated."
)

# COMMAND ----------

# ================================================================
# STEP 25 — PUBLISH GOLD CUSTOMER 360 TO SNOWFLAKE
# ================================================================

# SOURCE:
# workspace.gold.customer_360
#
# TARGET:
# RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360
#
# No transformation is performed.
# Existing validated Gold data is published as-is.

from pyspark.sql import functions as F

# ---------------------------------------------------------------
# 1. LOAD SNOWFLAKE CREDENTIALS FROM DATABRICKS SECRET SCOPE
# ---------------------------------------------------------------

SECRET_SCOPE = "snowflake-secrets"

sf_password = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="snowflake-password"
)

sf_account = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="account"
)

# Snowflake username verified earlier
sf_user = "COLLEGE"

# ---------------------------------------------------------------
# 2. BUILD SERVERLESS SNOWFLAKE WRITE OPTIONS
# ---------------------------------------------------------------

sf_write_options = {
    "host": f"{sf_account}.snowflakecomputing.com",
    "port": "443",

    "sfUser": sf_user,
    "sfPassword": sf_password,

    "sfDatabase": "RETAIL_CUSTOMER360",
    "sfSchema": "ANALYTICS_GOLD",

    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "CUSTOMER360_DATABRICKS_ROLE"
}

# ---------------------------------------------------------------
# 3. LOAD EXISTING VALIDATED GOLD TABLE
# ---------------------------------------------------------------

gold_customer_360_source = (
    spark.table(
        "workspace.gold.customer_360"
    )
)

source_row_count = (
    gold_customer_360_source.count()
)

print(
    f"Gold Customer 360 source rows : "
    f"{source_row_count:,}"
)

# ---------------------------------------------------------------
# 4. SOURCE ROW-COUNT QUALITY GATE
# ---------------------------------------------------------------

EXPECTED_GOLD_ROWS = 93358

if source_row_count != EXPECTED_GOLD_ROWS:
    raise ValueError(
        "Gold Customer 360 source row-count validation failed. "
        f"Expected {EXPECTED_GOLD_ROWS:,}, "
        f"got {source_row_count:,}."
    )

print(
    "PASS — Gold Customer 360 source row count is correct."
)

# ---------------------------------------------------------------
# 5. SOURCE CUSTOMER-GRAIN QUALITY GATE
# ---------------------------------------------------------------

duplicate_customer_count = (
    gold_customer_360_source
    .groupBy("customer_unique_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_customer_count != 0:
    raise ValueError(
        "Gold Customer 360 source contains duplicate "
        "customer_unique_id values."
    )

print(
    "PASS — Gold Customer 360 customer grain is unique."
)

# ---------------------------------------------------------------
# 6. TARGET TABLE
# ---------------------------------------------------------------

TARGET_TABLE = "CUSTOMER_360"

# ---------------------------------------------------------------
# 7. PUBLISH TO SNOWFLAKE
# ---------------------------------------------------------------

(
    gold_customer_360_source
    .write
    .format("snowflake")
    .options(**sf_write_options)
    .option(
        "dbtable",
        TARGET_TABLE
    )
    .mode("overwrite")
    .save()
)

# ---------------------------------------------------------------
# 8. FINAL WRITE RESULT
# ---------------------------------------------------------------

print("=" * 72)
print("GOLD CUSTOMER 360 PUBLICATION")
print("=" * 72)

print(
    "Source : workspace.gold.customer_360"
)

print(
    "Target : "
    "RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360"
)

print(
    f"Rows published : {source_row_count:,}"
)

print(
    "PASS — Gold Customer 360 published successfully to Snowflake."
)

print("=" * 72)

# COMMAND ----------

# ================================================================
# STEP 26 — GOLD ROW COUNT RECONCILIATION
# ================================================================

# Purpose:
# Verify that Snowflake contains exactly the same number of
# Gold Customer 360 records as Databricks.

from pyspark.sql import functions as F

SECRET_SCOPE = "snowflake-secrets"

# ---------------------------------------------------------------
# Load credentials
# ---------------------------------------------------------------

sf_password = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="snowflake-password"
)

sf_account = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="account"
)

# Snowflake username verified earlier
sf_user = "COLLEGE"

# ---------------------------------------------------------------
# Snowflake READ configuration
# ---------------------------------------------------------------

sf_read_options = {
    "sfURL": f"{sf_account}.snowflakecomputing.com",
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": "RETAIL_CUSTOMER360",
    "sfSchema": "ANALYTICS_GOLD",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "CUSTOMER360_DATABRICKS_ROLE"
}

# ---------------------------------------------------------------
# Databricks source count
# ---------------------------------------------------------------

databricks_gold_count = (
    spark.table(
        "workspace.gold.customer_360"
    )
    .count()
)

# ---------------------------------------------------------------
# Snowflake target count
# ---------------------------------------------------------------

snowflake_count_df = (
    spark.read
    .format("snowflake")
    .options(**sf_read_options)
    .option(
        "query",
        """
        SELECT COUNT(*) AS ROW_COUNT
        FROM RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360
        """
    )
    .load()
)

snowflake_result = (
    snowflake_count_df
    .first()
    .asDict()
)

snowflake_gold_count = None

for key, value in snowflake_result.items():

    if key.lower() == "row_count":
        snowflake_gold_count = int(value)
        break

if snowflake_gold_count is None:
    raise ValueError(
        "Snowflake row count was not returned."
    )

# ---------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------

print(
    f"Databricks Gold rows : {databricks_gold_count:,}"
)

print(
    f"Snowflake Gold rows  : {snowflake_gold_count:,}"
)

if databricks_gold_count != snowflake_gold_count:

    raise ValueError(
        "Gold row-count reconciliation FAILED. "
        f"Databricks={databricks_gold_count:,}, "
        f"Snowflake={snowflake_gold_count:,}"
    )

print(
    "PASS — Databricks and Snowflake Gold row counts "
    "match exactly."
)

# COMMAND ----------

# ================================================================
# STEP 27 — CUSTOMER KEY RECONCILIATION
# ================================================================

# Purpose:
# Verify that Databricks and Snowflake contain the exact same
# customer_unique_id population.
#
# Checks both directions:
#
# Databricks → Snowflake
# Snowflake → Databricks
#
# No data is modified.

# ---------------------------------------------------------------
# 1. Load Databricks Gold keys
# ---------------------------------------------------------------

databricks_keys_df = (
    spark.table(
        "workspace.gold.customer_360"
    )
    .select(
        "customer_unique_id"
    )
    .distinct()
)

databricks_key_count = (
    databricks_keys_df.count()
)

# ---------------------------------------------------------------
# 2. Load Snowflake Gold keys
# ---------------------------------------------------------------

snowflake_keys_df = (
    spark.read
    .format("snowflake")
    .options(**sf_read_options)
    .option(
        "dbtable",
        "CUSTOMER_360"
    )
    .load()
    .select(
        "customer_unique_id"
    )
    .distinct()
)

snowflake_key_count = (
    snowflake_keys_df.count()
)

# ---------------------------------------------------------------
# 3. Databricks keys missing in Snowflake
# ---------------------------------------------------------------

databricks_missing_in_snowflake = (
    databricks_keys_df
    .join(
        snowflake_keys_df,
        on="customer_unique_id",
        how="left_anti"
    )
)

missing_in_snowflake_count = (
    databricks_missing_in_snowflake.count()
)

# ---------------------------------------------------------------
# 4. Snowflake keys missing in Databricks
# ---------------------------------------------------------------

snowflake_missing_in_databricks = (
    snowflake_keys_df
    .join(
        databricks_keys_df,
        on="customer_unique_id",
        how="left_anti"
    )
)

missing_in_databricks_count = (
    snowflake_missing_in_databricks.count()
)

# ---------------------------------------------------------------
# 5. Results
# ---------------------------------------------------------------

print(
    f"Databricks distinct customers       : "
    f"{databricks_key_count:,}"
)

print(
    f"Snowflake distinct customers        : "
    f"{snowflake_key_count:,}"
)

print(
    f"Databricks keys missing in Snowflake: "
    f"{missing_in_snowflake_count:,}"
)

print(
    f"Snowflake keys missing in Databricks: "
    f"{missing_in_databricks_count:,}"
)

# ---------------------------------------------------------------
# 6. Quality gate
# ---------------------------------------------------------------

if missing_in_snowflake_count != 0:
    raise ValueError(
        "Customer key reconciliation failed: "
        "Databricks customers are missing in Snowflake."
    )

if missing_in_databricks_count != 0:
    raise ValueError(
        "Customer key reconciliation failed: "
        "Snowflake contains unexpected customers."
    )

if databricks_key_count != snowflake_key_count:
    raise ValueError(
        "Distinct customer counts do not match."
    )

print(
    "PASS — Databricks and Snowflake contain "
    "the exact same customer population."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINAL CHECK

# COMMAND ----------

# ================================================================
# SECTION 15 — FINAL DATABRICKS ↔ SNOWFLAKE ROW RECONCILIATION
# ================================================================

# ---------------------------------------------------------------
# 1. Databricks Gold row count
# ---------------------------------------------------------------

databricks_gold_count = (
    spark.table(
        "workspace.gold.customer_360"
    )
    .count()
)

# ---------------------------------------------------------------
# 2. Snowflake Customer 360 row count
# ---------------------------------------------------------------

snowflake_count_df = (
    spark.read
    .format("snowflake")
    .options(**sf_read_options)
    .option(
        "query",
        """
        SELECT COUNT(*) AS ROW_COUNT
        FROM RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360
        """
    )
    .load()
)

snowflake_result = (
    snowflake_count_df
    .first()
    .asDict()
)

snowflake_gold_count = None

for key, value in snowflake_result.items():

    if key.lower() == "row_count":
        snowflake_gold_count = int(value)
        break

if snowflake_gold_count is None:
    raise ValueError(
        "Snowflake row count was not returned."
    )

# ---------------------------------------------------------------
# 3. Compare
# ---------------------------------------------------------------

print("=" * 72)
print("FINAL DATABRICKS ↔ SNOWFLAKE ROW RECONCILIATION")
print("=" * 72)

print(
    f"Databricks Gold rows : {databricks_gold_count:,}"
)

print(
    f"Snowflake Gold rows  : {snowflake_gold_count:,}"
)

if databricks_gold_count != snowflake_gold_count:

    raise ValueError(
        "FINAL ROW RECONCILIATION FAILED. "
        f"Databricks={databricks_gold_count:,}, "
        f"Snowflake={snowflake_gold_count:,}"
    )

print(
    "PASS — Databricks and Snowflake contain "
    "the exact same number of Gold Customer 360 rows."
)

print("=" * 72)

# COMMAND ----------

# ================================================================
# SECTION 16 — CUSTOMER KEY RECONCILIATION
# ================================================================

# ---------------------------------------------------------------
# 1. Load Databricks Gold customer IDs
# ---------------------------------------------------------------

databricks_keys_df = (
    spark.table(
        "workspace.gold.customer_360"
    )
    .select(
        "customer_unique_id"
    )
    .distinct()
)

databricks_key_count = (
    databricks_keys_df.count()
)

# ---------------------------------------------------------------
# 2. Load Snowflake Customer 360 customer IDs
# ---------------------------------------------------------------

snowflake_keys_df = (
    spark.read
    .format("snowflake")
    .options(**sf_read_options)
    .option(
        "dbtable",
        "CUSTOMER_360"
    )
    .load()
    .select(
        "customer_unique_id"
    )
    .distinct()
)

snowflake_key_count = (
    snowflake_keys_df.count()
)

# ---------------------------------------------------------------
# 3. Databricks customers missing in Snowflake
# ---------------------------------------------------------------

databricks_missing_in_snowflake = (
    databricks_keys_df
    .join(
        snowflake_keys_df,
        on="customer_unique_id",
        how="left_anti"
    )
)

missing_in_snowflake_count = (
    databricks_missing_in_snowflake.count()
)

# ---------------------------------------------------------------
# 4. Snowflake customers missing in Databricks
# ---------------------------------------------------------------

snowflake_missing_in_databricks = (
    snowflake_keys_df
    .join(
        databricks_keys_df,
        on="customer_unique_id",
        how="left_anti"
    )
)

missing_in_databricks_count = (
    snowflake_missing_in_databricks.count()
)

# ---------------------------------------------------------------
# 5. Display results
# ---------------------------------------------------------------

print("=" * 72)
print("CUSTOMER KEY RECONCILIATION")
print("=" * 72)

print(
    f"Databricks distinct customers        : "
    f"{databricks_key_count:,}"
)

print(
    f"Snowflake distinct customers         : "
    f"{snowflake_key_count:,}"
)

print(
    f"Databricks keys missing in Snowflake : "
    f"{missing_in_snowflake_count:,}"
)

print(
    f"Snowflake keys missing in Databricks : "
    f"{missing_in_databricks_count:,}"
)

# ---------------------------------------------------------------
# 6. Quality gate
# ---------------------------------------------------------------

if missing_in_snowflake_count != 0:

    raise ValueError(
        "Customer key reconciliation failed: "
        "Databricks customers are missing in Snowflake."
    )

if missing_in_databricks_count != 0:

    raise ValueError(
        "Customer key reconciliation failed: "
        "Snowflake contains unexpected customers."
    )

if databricks_key_count != snowflake_key_count:

    raise ValueError(
        "Distinct customer counts do not match."
    )

print(
    "PASS — Databricks and Snowflake contain "
    "the exact same customer population."
)

print("=" * 72)

# COMMAND ----------

# ================================================================
# SECTION 17 — FINAL GOLD CUSTOMER 360 DATA SAMPLE
# ================================================================

final_customer_360_df = (
    spark.table(
        "workspace.gold.customer_360"
    )
)

display(
    final_customer_360_df
    .orderBy("customer_unique_id")
    .limit(20)
)

# COMMAND ----------

