# Data Model Documentation — Retail Customer 360

## Overview

This document details the data modeling design, entity relationships, schema specifications, and Medallion transformations across Bronze, Silver, and Gold layers.

---

## 1. Source Data Model (Olist E-Commerce + In-Store Omnichannel)

The operational source dataset models the end-to-end e-commerce order lifecycle and simulated physical retail branch transactions.

### Entity Relationship Diagram (Source Level)

```
customers ──< orders ──< order_items >── products >── category_translation
                │                  >── sellers
                ├──< order_payments
                └──< order_reviews

geolocation ── (linked via zip_code_prefix to customers/sellers)
in_store_orders ── (linked via customer_unique_id to customers)
```

### Critical Identity Resolution
* `customer_id`: Transient order-level key generated per transaction (99,441 records).
* `customer_unique_id`: Permanent human customer key (~96,096 unique individuals).
* **Rule**: All Silver and Gold downstream analytics use `customer_unique_id` as the primary customer entity key.

---

## 2. Bronze Layer (`workspace.bronze` Catalog)

**Policy**: Source Preservation. All columns stored as `STRING`. No data modifications or filtering performed.

| Table Name | Source File | Record Count | Primary Key / Grain | Key Columns |
|---|---|---|---|---|
| `workspace.bronze.customers` | `olist_customers_dataset.csv` | 99,441 | `customer_id` | `customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state` |
| `workspace.bronze.geolocation` | `olist_geolocation_dataset.csv` | 1,000,163 | ZIP Prefix Readings | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_city`, `geolocation_state` |
| `workspace.bronze.orders` | `olist_orders_dataset.csv` | 99,441 | `order_id` | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_customer_date` |
| `workspace.bronze.order_payments` | `olist_order_payments_dataset.csv` | 103,886 | (`order_id`, `payment_sequential`) | `order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value` |
| `workspace.bronze.order_items` | `olist_order_items_dataset.csv` | 112,650 | (`order_id`, `order_item_id`) | `order_id`, `order_item_id`, `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value` |
| `workspace.bronze.products` | `olist_products_dataset.csv` | 32,951 | `product_id` | `product_id`, `product_category_name`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` |
| `workspace.bronze.category_translation` | `product_category_name_translation.csv` | 71 | `product_category_name` | `product_category_name`, `product_category_name_english` |
| `workspace.bronze.sellers` | `olist_sellers_dataset.csv` | 3,095 | `seller_id` | `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state` |
| `workspace.bronze.order_reviews` | `olist_order_reviews_dataset.csv` | 99,224 | `review_id` | `review_id`, `order_id`, `review_score`, `review_comment_title`, `review_comment_message`, `review_creation_date` |
| `workspace.bronze.in_store_orders` | `in_store_orders_dataset.csv` | 10,000 | `order_id` | `order_id`, `customer_unique_id`, `store_id`, `order_purchase_timestamp`, `payment_value`, `payment_type` |

---

## 3. Silver Layer (`workspace.silver` Catalog)

**Policy**: Cleaned, Typed, Deduplicated Domain Models.

### 3.1 `silver.customers`
* **Grain**: 1 Row per `customer_unique_id` (~96,096 rows).
* **Deduplication**: PySpark Window function `partitionBy("customer_unique_id").orderBy("customer_id")` filtering `row_number() == 1`.
* **Attributes**: `customer_unique_id` (STRING), `customer_id` (STRING), `customer_zip_code_prefix` (INT), `customer_city` (STRING, lowercase trimmed), `customer_state` (STRING, uppercase trimmed), `silver_load_timestamp` (TIMESTAMP).

### 3.2 `silver.geolocation`
* **Grain**: 1 Row per `geolocation_zip_code_prefix` (19,015 rows).
* **Aggregation**: `groupBy("geolocation_zip_code_prefix")` computing `AVG(lat)`, `AVG(lng)`, `FIRST(city, ignorenulls=True)`, `FIRST(state, ignorenulls=True)`.
* **Attributes**: `geolocation_zip_code_prefix` (INT), `geolocation_lat` (DOUBLE), `geolocation_lng` (DOUBLE), `geolocation_city` (STRING), `geolocation_state` (STRING), `silver_load_timestamp` (TIMESTAMP).

### 3.3 `silver.orders`
* **Grain**: 1 Row per `order_id` (99,441 rows).
* **Attributes**: `order_id` (STRING), `customer_id` (STRING), `order_status` (STRING), `order_purchase_timestamp` (TIMESTAMP), `order_approved_at` (TIMESTAMP), `order_delivered_carrier_date` (TIMESTAMP), `order_delivered_customer_date` (TIMESTAMP), `order_estimated_delivery_date` (TIMESTAMP), `approval_missing_flag` (BOOLEAN), `timeline_issue_flag` (BOOLEAN), `silver_load_timestamp` (TIMESTAMP).

### 3.4 `silver.order_payments`
* **Grain**: 1 Row per `order_id` + `payment_sequential` (103,886 rows).
* **Attributes**: `order_id` (STRING), `payment_sequential` (INT), `payment_type` (STRING), `payment_installments` (INT), `payment_value` (DOUBLE), `invalid_payment_value_flag` (BOOLEAN), `invalid_installments_flag` (BOOLEAN), `silver_load_timestamp` (TIMESTAMP).

### 3.5 `silver.order_items`
* **Grain**: 1 Row per `order_id` + `order_item_id` (112,650 rows).
* **Attributes**: `order_id` (STRING), `order_item_id` (INT), `product_id` (STRING), `seller_id` (STRING), `shipping_limit_date` (TIMESTAMP), `price` (DOUBLE), `freight_value` (DOUBLE), `invalid_price_flag` (BOOLEAN), `silver_load_timestamp` (TIMESTAMP).

### 3.6 `silver.products`
* **Grain**: 1 Row per `product_id` (32,951 rows).
* **Enrichment**: Left Joined with `category_translation` to append `product_category_name_english`.
* **Attributes**: `product_id` (STRING), `product_category_name` (STRING), `product_category_name_english` (STRING), `product_weight_g` (DOUBLE), `product_length_cm` (DOUBLE), `product_height_cm` (DOUBLE), `product_width_cm` (DOUBLE), `silver_load_timestamp` (TIMESTAMP).

### 3.7 `silver.sellers`
* **Grain**: 1 Row per `seller_id` (3,095 rows).
* **Attributes**: `seller_id` (STRING), `seller_zip_code_prefix` (INT), `seller_city` (STRING, lowercase trimmed), `seller_state` (STRING, uppercase trimmed), `silver_load_timestamp` (TIMESTAMP).

### 3.8 `silver.order_reviews`
* **Grain**: 1 Row per `review_id` (99,224 rows).
* **Attributes**: `review_id` (STRING), `order_id` (STRING), `review_score` (INT), `review_comment_title` (STRING), `review_comment_message` (STRING), `review_creation_date` (TIMESTAMP), `review_answer_timestamp` (TIMESTAMP), `silver_load_timestamp` (TIMESTAMP).

### 3.9 `silver.in_store_orders`
* **Grain**: 1 Row per `order_id` (10,000 rows).
* **Attributes**: `order_id` (STRING), `customer_unique_id` (STRING), `store_id` (STRING), `order_purchase_timestamp` (TIMESTAMP), `payment_value` (DOUBLE), `payment_type` (STRING), `silver_load_timestamp` (TIMESTAMP).

---

## 4. Gold Layer (`ANALYTICS_GOLD` Schema in Snowflake)

### Table: `ANALYTICS_GOLD.CUSTOMER_360`

**Grain**: 1 Row per `customer_unique_id`

| Column Name | Data Type | Formula / Description |
|---|---|---|
| `CUSTOMER_UNIQUE_ID` | VARCHAR(50) (PK) | Primary Customer Identity Key |
| `CUSTOMER_CITY` | VARCHAR(100) | Standardized Customer City |
| `CUSTOMER_STATE` | VARCHAR(5) | Standardized Customer State |
| `CUSTOMER_ZIP_CODE_PREFIX` | INT | Customer ZIP Prefix |
| `LATITUDE` | FLOAT | Geographic Centroid Latitude |
| `LONGITUDE` | FLOAT | Geographic Centroid Longitude |
| `RECENCY_DAYS` | INT | `DATEDIFF('day', MAX(last_order_date), CURRENT_DATE())` |
| `FREQUENCY` | INT | `COUNT(online_orders) + COUNT(in_store_orders)` |
| `MONETARY` | FLOAT | `SUM(online_payment_value) + SUM(in_store_payment_value)` |
| `ONLINE_ORDERS_COUNT` | INT | Count of online orders |
| `IN_STORE_ORDERS_COUNT` | INT | Count of physical store orders |
| `AVG_REVIEW_SCORE` | FLOAT | Average review score awarded by customer |
| `R_SCORE` | INT | `NTILE(5) OVER (ORDER BY recency_days DESC)` (1-5) |
| `F_SCORE` | INT | `NTILE(5) OVER (ORDER BY frequency ASC)` (1-5) |
| `M_SCORE` | INT | `NTILE(5) OVER (ORDER BY monetary ASC)` (1-5) |
| `RFM_SCORE` | VARCHAR(3) | Concatenated RFM score string (e.g., '555', '111') |
| `RFM_SEGMENT` | VARCHAR(50) | Marketing segment ('Champions', 'Loyal Customers', 'New Customers', 'At Risk', 'Hibernating') |
| `GOLD_LOAD_TIMESTAMP` | TIMESTAMP_NTZ | Execution load timestamp |

### 4.1 Automated Snowflake Publishing (`T01_Publish_Gold_Customer360`)
The final Gold `CUSTOMER_360` analytical view is published from Databricks to Snowflake via the automated Databricks Workflow task `T01_Publish_Gold_Customer360` (`.../01_Publish_Gold_Customer_360`). It uses the Spark Snowflake Connector to write the aggregated dataset directly into Snowflake's `ANALYTICS_GOLD` schema.