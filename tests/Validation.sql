-- ============================================================
-- STEP 1: CHECK AVAILABLE SNOWFLAKE ROLES
-- ============================================================
-- This lets us identify the role available to your user
-- before making any database or warehouse changes.
-- ============================================================

SHOW ROLES;



-- ============================================================
-- STEP 2: CHECK CUSTOMER 360 ROLE PRIVILEGES
-- ============================================================
-- We first inspect the existing custom role before granting
-- anything new. This prevents unnecessary privilege changes.
-- ============================================================

SHOW GRANTS TO ROLE CUSTOMER360_DATABRICKS_ROLE;


-- ============================================================
-- STEP 3: USE THE CUSTOMER 360 PROJECT ROLE
-- ============================================================
-- We switch from ACCOUNTADMIN to the dedicated project role
-- so the notebook runs using the intended permissions.
-- ============================================================

USE ROLE CUSTOMER360_DATABRICKS_ROLE;


SELECT CURRENT_ROLE();


-- ============================================================
-- STEP 4: SET PROJECT WAREHOUSE
-- ============================================================
-- COMPUTE_WH is the warehouse already granted to our
-- CUSTOMER360_DATABRICKS_ROLE.
-- ============================================================

USE WAREHOUSE COMPUTE_WH;

SELECT CURRENT_WAREHOUSE();



-- ============================================================
-- STEP 5: SET DATABASE AND SCHEMA
-- ============================================================
-- The Gold Customer 360 table is stored under this database
-- and schema. Setting the context simplifies later queries.
-- ============================================================

USE DATABASE RETAIL_CUSTOMER360;

USE SCHEMA ANALYTICS_GOLD;

SELECT
    CURRENT_DATABASE() AS CURRENT_DATABASE,
    CURRENT_SCHEMA() AS CURRENT_SCHEMA;



-- ============================================================
-- STEP 6: VERIFY GOLD TABLE
-- ============================================================
-- We confirm that the Gold table loaded from Databricks is
-- available in Snowflake before building dashboard queries.
-- ============================================================

SHOW TABLES;


-- ------------------------------------------------------ DASHBOARD 1 --------------------------------------------------------------------------------



-- ============================================================
-- STEP 7: TOTAL CUSTOMERS
-- ============================================================
-- Power BI uses Count (Distinct) on CUSTOMER_UNIQUE_ID.
-- We reproduce the same calculation in Snowflake.
-- ============================================================

SELECT
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS TOTAL_CUSTOMERS
FROM CUSTOMER_360;


-- ============================================================
-- STEP 8: AVERAGE PURCHASE FREQUENCY
-- ============================================================
-- FREQUENCY represents the number of successful orders
-- made by each customer.
-- Power BI displays the average FREQUENCY across customers.
-- ============================================================

SELECT
    ROUND(AVG(FREQUENCY), 2) AS AVERAGE_PURCHASE_FREQUENCY
FROM CUSTOMER_360;



-- ============================================================
-- STEP 9: AVERAGE CUSTOMER VALUE
-- ============================================================
-- MONETARY represents the total value associated with each
-- customer. We calculate the average customer value across
-- all customers to match the Power BI KPI.
-- ============================================================

SELECT
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE
FROM CUSTOMER_360;



-- ============================================================
-- STEP 10: TOTAL CUSTOMER VALUE
-- ============================================================
-- MONETARY contains the total customer value for each customer.
-- Summing it gives the total value across all customers.
-- ============================================================

SELECT
    ROUND(SUM(MONETARY), 2) AS TOTAL_CUSTOMER_VALUE
FROM CUSTOMER_360;



-- ============================================================
-- STEP 11A: IDENTIFY REVIEW COLUMN IN GOLD TABLE
-- ============================================================
-- We check the actual Snowflake column name instead of
-- assuming it matches the Power BI field name exactly.
-- ============================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
  AND TABLE_NAME = 'CUSTOMER_360'
  AND UPPER(COLUMN_NAME) LIKE '%REVIEW%'
ORDER BY ORDINAL_POSITION;



-- ============================================================
-- STEP 11B: AVERAGE REVIEW SCORE
-- ============================================================
-- The Gold table already stores the average review score
-- for each customer in AVG_REVIEW_SCORE.
-- We calculate the overall average to match Power BI.
-- ============================================================

SELECT
    ROUND(AVG(AVG_REVIEW_SCORE), 2) AS AVERAGE_REVIEW_SCORE
FROM CUSTOMER_360;



-- ============================================================
-- STEP 12A: IDENTIFY RECENCY COLUMN
-- ============================================================
-- We check the actual Gold table column used for customer
-- recency before writing the KPI query.
-- ============================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
  AND TABLE_NAME = 'CUSTOMER_360'
  AND UPPER(COLUMN_NAME) LIKE '%RECENCY%'
ORDER BY ORDINAL_POSITION;



-- ============================================================
-- STEP 12B: AVERAGE RECENCY
-- ============================================================
-- RECENCY_DAYS stores the number of days since each customer's
-- most recent successful purchase.
-- We calculate the average across all customers to match Power BI.
-- ============================================================

SELECT
    ROUND(AVG(RECENCY_DAYS), 2) AS AVERAGE_RECENCY_DAYS
FROM CUSTOMER_360;



-- ============================================================
-- STEP 13A: VERIFY RFM SEGMENT COLUMN
-- ============================================================
-- We confirm the exact Gold-table column used for customer
-- segmentation before building the distribution query.
-- ============================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
  AND TABLE_NAME = 'CUSTOMER_360'
  AND UPPER(COLUMN_NAME) LIKE '%RFM%'
ORDER BY ORDINAL_POSITION;


-- ============================================================
-- STEP 13B: CUSTOMER DISTRIBUTION BY RFM SEGMENT
-- ============================================================
-- Each row in CUSTOMER_360 represents one customer.
-- We count customers in each RFM segment and calculate their
-- percentage of the total customer population.
-- ============================================================

SELECT
    RFM_SEGMENT,
    COUNT(*) AS CUSTOMER_COUNT,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS CUSTOMER_PERCENTAGE
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY CUSTOMER_COUNT DESC;



-- ============================================================
-- STEP 14: TOTAL CUSTOMER VALUE BY RFM SEGMENT
-- ============================================================
-- Each customer has a MONETARY value and an RFM segment.
-- We sum MONETARY for each segment to calculate the total
-- customer value contributed by that segment.
-- ============================================================

SELECT
    RFM_SEGMENT,
    ROUND(SUM(MONETARY), 2) AS TOTAL_CUSTOMER_VALUE
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY TOTAL_CUSTOMER_VALUE DESC;



-- -------------------------------------------------------- DASHBOARD 2 ----------------------------------------------------------------------------



-- ============================================================
-- STEP 15A: VERIFY CUSTOMER ID COLUMN
-- ============================================================
-- We confirm the exact customer identifier used in the Gold
-- table before reproducing the Power BI customer-count visual.
-- ============================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ANALYTICS_GOLD'
  AND TABLE_NAME = 'CUSTOMER_360'
  AND (
        UPPER(COLUMN_NAME) LIKE '%CUSTOMER%'
        OR UPPER(COLUMN_NAME) LIKE '%UID%'
      )
ORDER BY ORDINAL_POSITION;


-- ============================================================
-- STEP 15B: CUSTOMER COUNT BY RFM SEGMENT
-- ============================================================
-- Power BI counts customers for each RFM segment.
-- CUSTOMER_UNIQUE_ID is the actual customer identifier
-- available in the Snowflake Gold table.
-- ============================================================

SELECT
    RFM_SEGMENT,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY CUSTOMER_COUNT DESC;




-- ============================================================
-- STEP 16: AVERAGE CUSTOMER VALUE BY RFM SEGMENT
-- ============================================================
-- MONETARY represents the value associated with each customer.
-- We calculate the average monetary value within each
-- RFM segment to reproduce the Power BI bar chart.
-- ============================================================

SELECT
    RFM_SEGMENT,
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY AVERAGE_CUSTOMER_VALUE DESC;




-- ============================================================
-- STEP 17: CUSTOMER VALUE & COUNT BY RFM SCORE
-- ============================================================
-- For every RFM score, calculate:
-- 1. Average customer monetary value
-- 2. Number of unique customers
--
-- These correspond to the bar and line in the Power BI
-- combination chart.
-- ============================================================

SELECT
    RFM_SCORE,
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY RFM_SCORE
ORDER BY RFM_SCORE;




-- ============================================================
-- STEP 18: CUSTOMER DISTRIBUTION BY RFM SCORE
-- ============================================================
-- This counts the number of unique customers belonging to
-- each RFM score.
-- ============================================================

SELECT
    RFM_SCORE,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY RFM_SCORE
ORDER BY CUSTOMER_COUNT DESC;


-- ============================================================
-- STEP 19: RFM SEGMENT — RECENCY VS FREQUENCY
-- ============================================================
-- Each point in the Power BI scatter chart represents an
-- RFM segment.
--
-- X-axis  -> Average recency
-- Y-axis  -> Average purchase frequency
-- ============================================================

SELECT
    RFM_SEGMENT,
    ROUND(AVG(RECENCY_DAYS), 2) AS AVERAGE_RECENCY_DAYS,
    ROUND(AVG(FREQUENCY), 2) AS AVERAGE_FREQUENCY
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY RFM_SEGMENT;

-- ---------------------------------------------- DASHBOARD 3 -------------------------------------------------------------------------------

-- ============================================================
-- STEP 20: TOP 10 ZIP PREFIXES — CUSTOMER PERFORMANCE
-- ============================================================
-- We identify the 10 ZIP prefixes having the highest
-- number of unique customers.
--
-- For those ZIP prefixes, we also calculate:
-- 1. Average customer monetary value
-- 2. Average review score
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,
    
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT,
    
    ROUND(AVG(MONETARY), 2) AS AVERAGE_MONETARY,
    
    ROUND(AVG(AVG_REVIEW_SCORE), 2) AS AVERAGE_REVIEW_SCORE

FROM CUSTOMER_360

GROUP BY CUSTOMER_ZIP_CODE_PREFIX

ORDER BY CUSTOMER_COUNT DESC

LIMIT 10;



-- ============================================================
-- STEP 21: TOP 10 ZIP PREFIXES BY AVERAGE CUSTOMER VALUE
-- ============================================================
-- Here we rank ZIP prefixes based on the average monetary
-- value generated by customers in each ZIP prefix.
--
-- Unlike Step 20, the ranking is based on AVERAGE(MONETARY),
-- not customer count.
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,
    
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE

FROM CUSTOMER_360

GROUP BY CUSTOMER_ZIP_CODE_PREFIX

ORDER BY AVERAGE_CUSTOMER_VALUE DESC

LIMIT 10;




-- ============================================================
-- STEP 22: CUSTOMER COUNT BY ZIP PREFIX
-- ============================================================
-- Count the number of unique customers belonging to each
-- ZIP prefix.
--
-- The result reproduces the Power BI customer distribution
-- bar chart.
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY CUSTOMER_ZIP_CODE_PREFIX
ORDER BY CUSTOMER_COUNT DESC
LIMIT 10;


-- ============================================================
-- STEP 23: TOP 10 ZIP PREFIXES — CUSTOMER PERFORMANCE
-- ============================================================
-- Identify the 10 ZIP prefixes with the highest average
-- customer monetary value.
--
-- For those ZIP prefixes, calculate:
-- 1. Unique customer count
-- 2. Average monetary value
-- 3. Average review score
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,

    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT,

    ROUND(AVG(MONETARY), 2) AS AVERAGE_MONETARY,

    ROUND(AVG(AVG_REVIEW_SCORE), 2) AS AVERAGE_REVIEW_SCORE

FROM CUSTOMER_360

GROUP BY CUSTOMER_ZIP_CODE_PREFIX

ORDER BY AVERAGE_MONETARY DESC

LIMIT 10;




-- ============================================================
-- STEP 24: AVERAGE CUSTOMER VALUE BY TOP 10 ZIP PREFIXES
-- ============================================================
-- Identify the ZIP prefixes with the highest average
-- customer monetary value.
--
-- The result is used to reproduce the Power BI bar chart.
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE

FROM CUSTOMER_360

GROUP BY CUSTOMER_ZIP_CODE_PREFIX

ORDER BY AVERAGE_CUSTOMER_VALUE DESC

LIMIT 10;




-- ============================================================
-- STEP 25A: VERIFY GEOLOCATION COLUMNS
-- ============================================================
-- The Power BI map requires geographic coordinates.
-- We first confirm the exact latitude and longitude columns
-- available in the Gold Customer 360 table.
-- ============================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CUSTOMER_360'
  AND (
        COLUMN_NAME LIKE '%LAT%'
        OR COLUMN_NAME LIKE '%LNG%'
        OR COLUMN_NAME LIKE '%LONG%'
        OR COLUMN_NAME LIKE '%GEO%'
      )
ORDER BY ORDINAL_POSITION;




-- ============================================================
-- STEP 25B: GEOLOCATION DISTRIBUTION
-- ============================================================
-- The Power BI map plots customers geographically using
-- latitude and longitude.
--
-- RFM_SEGMENT is used to distinguish customer segments
-- across the geographic locations.
-- ============================================================

SELECT
    CUSTOMER_UNIQUE_ID,
    RFM_SEGMENT,
    LATITUDE,
    LONGITUDE
FROM CUSTOMER_360
WHERE LATITUDE IS NOT NULL
  AND LONGITUDE IS NOT NULL;







-- ----------------------------------------------------- DASHBOARD 4 ---------------------------------------------------------------------------------


-- ============================================================
-- STEP 26: SELECTED CUSTOMER COUNT
-- ============================================================
-- Count the unique customers available in the Gold table.
-- With all Power BI filters set to "All", this should match
-- the total customer count shown in the dashboard.
-- ============================================================

SELECT
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS SELECTED_CUSTOMERS
FROM CUSTOMER_360;



-- ============================================================
-- STEP 27: AVERAGE CUSTOMER VALUE
-- ============================================================
-- Calculate the average monetary value across customers.
-- This should match the "Average Customer Value" KPI in
-- Power BI when all filters are set to "All".
-- ============================================================

SELECT
    ROUND(AVG(MONETARY), 2) AS AVERAGE_CUSTOMER_VALUE
FROM CUSTOMER_360;


-- ============================================================
-- STEP 28: AVERAGE RECENCY
-- ============================================================
-- Calculate the average number of days since the customer's
-- most recent purchase.
-- This should match the Power BI "Average Recency (Days)" KPI.
-- ============================================================

SELECT
    ROUND(AVG(RECENCY_DAYS), 2) AS AVERAGE_RECENCY_DAYS
FROM CUSTOMER_360;



-- ============================================================
-- STEP 29: CUSTOMER-LEVEL RFM DETAILS
-- ============================================================
-- Retrieve the customer-level RFM attributes used by the
-- Power BI detail table.
-- Since all dashboard filters are currently set to "All",
-- we do not apply any filtering here.
-- ============================================================

SELECT
    CUSTOMER_UNIQUE_ID AS CUSTOMER_UID,
    RFM_SEGMENT,
    RFM_SCORE,
    RECENCY_DAYS,
    FREQUENCY,
    MONETARY,
    AVG_REVIEW_SCORE AS REVIEW_SCORE,
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX
FROM CUSTOMER_360;



-- ============================================================
-- STEP 30: RFM SCORE FILTER VALUES
-- ============================================================
-- Check the available RFM scores and the number of customers
-- belonging to each score.
-- This represents the data behind the RFM_SCORE filter.
-- ============================================================

SELECT
    RFM_SCORE,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY RFM_SCORE
ORDER BY RFM_SCORE;



-- ============================================================
-- STEP 31: ZIP PREFIX FILTER VALUES
-- ============================================================
-- Check the available ZIP prefixes and the number of unique
-- customers associated with each geographic area.
-- This represents the data behind the ZIP_PREFIX filter.
-- ============================================================

SELECT
    CUSTOMER_ZIP_CODE_PREFIX AS ZIP_PREFIX,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY CUSTOMER_ZIP_CODE_PREFIX
ORDER BY ZIP_PREFIX;




-- ============================================================
-- STEP 32: RFM SEGMENT FILTER VALUES
-- ============================================================
-- Check each RFM segment and its customer population.
-- This represents the data available to the RFM_SEGMENT filter.
-- ============================================================

SELECT
    RFM_SEGMENT,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS CUSTOMER_COUNT
FROM CUSTOMER_360
GROUP BY RFM_SEGMENT
ORDER BY CUSTOMER_COUNT DESC;



