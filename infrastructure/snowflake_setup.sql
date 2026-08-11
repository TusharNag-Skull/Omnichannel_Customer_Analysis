-- =============================================================================
-- Snowflake Setup Script
-- =============================================================================
-- Run this script in the Snowflake UI (Worksheets) to create the database,
-- schemas, warehouse, and raw staging tables.
--
-- WHAT THIS SCRIPT DOES:
-- 1. Creates the RETAIL_ANALYTICS database (our single project database)
-- 2. Creates the RAW schema (where Silver data from Databricks lands)
-- 3. Creates the ANALYTICS schema (where Gold tables live — Customer 360, RFM)
-- 4. Creates a virtual warehouse (compute engine for running queries)
-- 5. Creates all raw staging tables with explicit column definitions
--
-- WHY SEPARATE SCHEMAS:
-- Separating RAW and ANALYTICS ensures that:
-- - Analysts never accidentally query uncleaned staging data
-- - We can set different access controls per schema
-- - The lineage is clear: RAW feeds ANALYTICS, never the reverse
-- This is standard practice at companies like Walmart and JPMorgan.
--
-- WAREHOUSE SIZING:
-- We use X-Small because our dataset is ~120MB. Snowflake charges by
-- compute time, so X-Small saves cost while being fast enough for our scale.
-- Auto-suspend after 5 minutes of idle means we stop paying when not querying.
--
-- USAGE:
-- Copy this entire script into a Snowflake Worksheet and run it.
-- =============================================================================


-- ============================================
-- Step 1: Create Database
-- ============================================
-- A database in Snowflake is a container for schemas, tables, and views.
-- One database per project is the recommended practice for isolation.

CREATE DATABASE IF NOT EXISTS RETAIL_ANALYTICS;

USE DATABASE RETAIL_ANALYTICS;


-- ============================================
-- Step 2: Create Schemas
-- ============================================
-- RAW schema: receives cleaned data from Databricks Silver layer
-- ANALYTICS schema: contains business-ready tables (Customer 360, RFM)

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;


-- ============================================
-- Step 3: Create Virtual Warehouse
-- ============================================
-- A "warehouse" in Snowflake is NOT storage — it is a compute cluster.
-- This is one of the most commonly misunderstood concepts.
-- Storage and compute are completely separated in Snowflake.
--
-- X-Small = 1 credit/hour (smallest available)
-- AUTO_SUSPEND = 300 seconds (5 minutes of idle = warehouse shuts off)
-- AUTO_RESUME = TRUE (warehouse starts automatically when a query runs)

CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE RETAIL_WH;


-- ============================================
-- Step 4: Create Raw Staging Tables
-- ============================================
-- These tables receive Silver-layer data from Databricks.
-- Column types match what the Silver layer produces (cleaned and cast).

USE SCHEMA RAW;

-- Customers table
-- One row per unique customer (after deduplication in Silver)
CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    customer_unique_id    VARCHAR(50)    NOT NULL,
    customer_id           VARCHAR(50)    NOT NULL,
    customer_zip_code_prefix  INT,
    customer_city         VARCHAR(100),
    customer_state        VARCHAR(5)
);

-- Orders table
-- One row per order, linked to customer via customer_unique_id
CREATE TABLE IF NOT EXISTS RAW_ORDERS (
    order_id                    VARCHAR(50)    NOT NULL,
    customer_unique_id          VARCHAR(50)    NOT NULL,
    customer_id                 VARCHAR(50),
    order_status                VARCHAR(20),
    order_purchase_timestamp    TIMESTAMP,
    order_approved_at           TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    delivery_days               INT
);

-- Order Items table
-- One row per item in an order (an order can have multiple items)
CREATE TABLE IF NOT EXISTS RAW_ORDER_ITEMS (
    order_id            VARCHAR(50)    NOT NULL,
    order_item_id       INT            NOT NULL,
    product_id          VARCHAR(50)    NOT NULL,
    seller_id           VARCHAR(50)    NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    total_price         DECIMAL(10,2)
);

-- Order Payments table
-- One row per payment for an order (some orders have split payments)
CREATE TABLE IF NOT EXISTS RAW_ORDER_PAYMENTS (
    order_id            VARCHAR(50)    NOT NULL,
    payment_sequential  INT,
    payment_type        VARCHAR(30),
    payment_installments INT,
    payment_value       DECIMAL(10,2)
);

-- Order Reviews table
-- One row per review, linked to an order
CREATE TABLE IF NOT EXISTS RAW_ORDER_REVIEWS (
    review_id               VARCHAR(50)    NOT NULL,
    order_id                VARCHAR(50)    NOT NULL,
    review_score            INT,
    review_comment_title    VARCHAR(500),
    review_comment_message  VARCHAR(5000),
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- Products table
-- One row per product, with English category name
CREATE TABLE IF NOT EXISTS RAW_PRODUCTS (
    product_id                  VARCHAR(50)    NOT NULL,
    product_category_name       VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT
);

-- Sellers table
-- One row per seller
CREATE TABLE IF NOT EXISTS RAW_SELLERS (
    seller_id               VARCHAR(50)    NOT NULL,
    seller_zip_code_prefix  INT,
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5)
);

-- Geolocation table (deduplicated: one row per zip code)
CREATE TABLE IF NOT EXISTS RAW_GEOLOCATION (
    geolocation_zip_code_prefix  INT        NOT NULL,
    geolocation_lat              FLOAT,
    geolocation_lng              FLOAT,
    geolocation_city             VARCHAR(100),
    geolocation_state            VARCHAR(5)
);


-- ============================================
-- Step 5: Verify Setup
-- ============================================

SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA ANALYTICS;
SHOW WAREHOUSES LIKE 'RETAIL_WH';

-- Expected output:
-- RAW schema: 8 tables
-- ANALYTICS schema: 0 tables (will be created after Gold layer build)
-- Warehouse: RETAIL_WH, X-Small, initially suspended