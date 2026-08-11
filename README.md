# Omnichannel Customer 360 & RFM Analytics Platform

**Architecture**: Medallion Architecture (Bronze → Silver → Gold)  
**Tech Stack**: AWS S3 | Databricks PySpark | Delta Lake | Snowflake Data Warehouse  
**Target Domain**: Retail E-Commerce & In-Store Omnichannel Analytics  

---

## 1. Project Overview

This enterprise data engineering platform ingests, cleanses, standardizes, and unifies online e-commerce transactions (Olist Brazilian E-Commerce dataset) and brick-and-mortar physical store purchases into a single, analytics-ready **Customer 360 View** with **RFM (Recency, Frequency, Monetary) Segmentation**.

### Business Problem
Retail organizations frequently suffer from customer identity fragmentation across digital and physical touchpoints:
1. **Per-Order Customer Identifiers**: Transactional systems generate new customer identifiers per order, masking repeat purchase behavior and under-counting true customer retention.
2. **Geographic Fan-Out Joins**: Raw geolocation records contain multiple GPS readings per postal prefix (~53 coordinate readings per ZIP), causing catastrophic row duplication during analytical joins.
3. **Data Quality & Schema Drift**: Operational data suffers from missing timestamps, inconsistent city/state casing, unvalidated payment amounts, and non-standard strings.

### The Engineering Solution
A robust, automated ELT pipeline built on the **Medallion Architecture**:
* **Bronze Layer**: Raw payload preservation (`inferSchema=false`, zero-data-loss policy).
* **Silver Layer**: Production-grade data cleansing, deterministic windowed deduplication, centroid coordinate aggregation, and automated quality assertion gates.
* **Gold Layer (Snowflake Warehouse)**: High-concurrency analytical serving layer implementing Customer 360 aggregation and $NTILE(5)$ quintile RFM segmentation.

---

## 2. System Architecture & Workflows DAG

```
[AWS S3 Raw Storage Bucket] (s3://olist-retail-project/)
         │
         ▼
[Databricks Workflows DAG (Serverless Compute)]
   ├── BRONZE LAYER (workspace.bronze)  ──► 10 Parallel Raw Payload Tasks
   ├── SILVER LAYER (workspace.silver)  ──► 10 Cleaned Domain Tasks (Window Dedup & Centroid Agg)
   └── GOLD LAYER (Gold_Customer_360)   ──► Consolidated Customer 360 Aggregation
         │
         ▼ (T01_Publish_Gold_Customer360 Task / Spark Snowflake Connector)
[Snowflake Data Warehouse (RETAIL_CUSTOMER360)]
   ├── RAW_BRONZE Schema                 ──► Staging Tables
   ├── ANALYTICS_SILVER Schema           ──► Clean Domain Models
   └── ANALYTICS_GOLD Schema             ──► CUSTOMER_360 View (RFM Segmented)
```

---

## 3. Dataset Summary

The platform processes **1.3+ million operational records** across 10 distinct datasets:

| # | Dataset | Source File | Records | Core Entity & Description |
|---|---|---|---|---|
| 1 | **Customers** | `olist_customers_dataset.csv` | 99,441 | Customer profiles (`customer_id` vs `customer_unique_id`) |
| 2 | **Geolocation** | `olist_geolocation_dataset.csv` | 1,000,163 | ZIP code prefix latitude and longitude readings |
| 3 | **Orders** | `olist_orders_dataset.csv` | 99,441 | Online order status, purchase & delivery timestamps |
| 4 | **Order Payments** | `olist_order_payments_dataset.csv` | 103,886 | Transaction payment methods, installments & values |
| 5 | **Order Items** | `olist_order_items_dataset.csv` | 112,650 | Product items purchased, seller keys & prices |
| 6 | **Products** | `olist_products_dataset.csv` | 32,951 | Product catalog attributes & category names |
| 7 | **Category Translation** | `product_category_name_translation.csv` | 71 | Portuguese to English category mappings |
| 8 | **Sellers** | `olist_sellers_dataset.csv` | 3,095 | Seller locations & identifiers |
| 9 | **Order Reviews** | `olist_order_reviews_dataset.csv` | 99,224 | Customer review scores (1-5) & feedback comments |
| 10 | **In-Store Orders** | `in_store_orders_dataset.csv` | 10,000 | Synthetic brick-and-mortar retail transactions |

---

## 4. Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| **Cloud Storage** | AWS S3 | Central landing zone for raw source CSV datasets |
| **Orchestration** | Databricks Workflows | DAG task dependency management & automated pipeline execution |
| **Compute Mode** | Serverless Compute | High-performance, zero-startup-latency distributed execution |
| **Processing Engine** | Databricks PySpark | Distributed data extraction, cleaning & transformation |
| **Storage Format** | Delta Lake | ACID transactions, time travel & schema enforcement |
| **Data Warehouse** | Snowflake | Serving warehouse (`RAW_BRONZE`, `ANALYTICS_SILVER`, `ANALYTICS_GOLD`) |
| **Analytics Engine** | Snowflake SQL | Gold-layer aggregation, windowed RFM scoring (`NTILE(5)`) |
| **Documentation** | Markdown & Mermaid | Enterprise technical reference & architecture diagrams |

---

## 5. Repository Structure

```
├── README.md                                  # Executive platform overview
├── docs/
│   ├── architecture_diagram.md                # One-Page Architecture Specification & DAG Topology
│   ├── architecture_diagram.png                # Visual architecture graphic
│   ├── end_to_end_pipeline_docs.md            # Comprehensive technical pipeline reference
│   ├── presentation_and_interview_defense.md  # 12-slide script & 25 Q&A defense bank
│   ├── data_model.md                          # Schema definitions & medallion lineage
│   └── ai_usage.md                            # AI tools & engineering workflow log
│
├── infrastructure/
│   ├── s3_upload.py                           # AWS S3 automated upload utility
│   └── snowflake_setup.sql                    # Initial Snowflake DDL script
│
├── databricks/
│   ├── config/
│   │   └── pipeline_config.py                 # Centralized pipeline configuration
│   └── notebooks/
│       ├── 01_bronze_ingestion.py             # Pilot Bronze ingestion script
│       └── 02_silver_transformation.py        # Pilot Silver transformation script
│
└── Retail-Customer360/                        # Complete Team Codebase
    ├── Bronze/
    │   └── Final_bronze/                      # 10 Final Bronze PySpark Notebooks
    ├── Silver/
    │   └── final_silver/                      # 10 Final Silver PySpark Notebooks
    ├── Snowflake/
    │   ├── 01_snowflake_ddl.sql               # Snowflake 3-Schema Architecture DDL
    │   └── 02_snowflake_copy.sql              # S3-to-Snowflake Ingestion Script
    └── Gold/
        ├── 01_Gold_Customer_360.py            # Databricks Customer 360 notebook
        └── 01_Publish_Gold_Customer_360.py    # Snowflake publishing workflow task
```

---

## 6. Execution & Deployment Workflow

### Step 1: Land Operational Datasets in AWS S3
Execute the python ingestion utility to populate `s3://olist-retail-project/`:
```bash
python infrastructure/s3_upload.py
```

### Step 2: Trigger Databricks Workflows DAG (Serverless Compute)
In Databricks, run the **Databricks Workflows DAG** which automatically executes:
1. **Bronze Tasks**: Ingests all 10 raw datasets in parallel (`01_Bronze_Customers` through `10_Bronze_In_Store_Orders`).
2. **Silver Tasks**: Cleans, deduplicates (`customer_unique_id`), and calculates geospatial centroids (`01_Silver_Products` through `10_Silver_In_Store_Orders`).
3. **Gold Task**: Aggregates `Gold_Customer_360` customer profile and RFM scores.
4. **Publish Task**: Executes `T01_Publish_Gold_Customer360` to transfer final Gold datasets directly into Snowflake.

### Step 3: Snowflake Warehouse Provisioning
1. Log into Snowflake and execute `Retail-Customer360/Snowflake/01_snowflake_ddl.sql` to provision database `RETAIL_CUSTOMER360` and schemas (`RAW_BRONZE`, `ANALYTICS_SILVER`, `ANALYTICS_GOLD`).
2. Execute `Retail-Customer360/Snowflake/02_snowflake_copy.sql` to stage and load raw/silver data into Snowflake schemas.

### Step 4: Validate Gold Customer 360 Analytics
Query the final Customer 360 view in Snowflake:
```sql
SELECT 
    rfm_segment,
    COUNT(*) AS total_customers,
    ROUND(AVG(monetary), 2) AS avg_spend,
    ROUND(AVG(recency_days), 0) AS avg_recency_days
FROM RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360
GROUP BY rfm_segment
ORDER BY total_customers DESC;
```

---

## 7. Key Engineering Principles Enforced

1. **Identity Resolution (`customer_unique_id`)**: `customer_id` is a transient transaction key generated per order. `customer_unique_id` represents the human entity. Grouping by `customer_unique_id` resolves repeat buyers and prevents customer over-counting.
2. **Centroid Geolocation Aggregation**: Raw geolocation contains ~53 coordinate points per ZIP prefix. Silver aggregates lat/lng by ZIP prefix centroid (`AVG(lat)`, `AVG(lng)`), eliminating 53x fan-out join multiplication.
3. **Deterministic Deduplication**: Uses PySpark Window functions (`partitionBy("customer_unique_id").orderBy("customer_id")`) with `row_number() == 1` instead of non-deterministic `first()` aggregations.
4. **Automated Quality Gates**: Every Silver job enforces 4 automated quality assertions (Schema Check, Key Uniqueness, Null Assertions, Range Validation). Any gate failure raises a pipeline-blocking exception.

---

## 8. Team Members

* Asvin Nigam
* Tushar Nag
* Harshraj Parmar
* Preeti Vala