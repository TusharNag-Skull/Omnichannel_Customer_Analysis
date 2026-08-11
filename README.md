# Omnichannel Customer 360 & RFM Analytics Platform

**Architecture**: Medallion Architecture (Bronze → Silver → Gold)
**Tech Stack**: AWS S3 | Databricks PySpark | Delta Lake | Snowflake Data Warehouse | Power BI
**Target Domain**: Retail E-Commerce Analytics (Olist Brazilian E-Commerce Dataset)

---

## 1. Project Overview

This project ingests, cleans, standardizes, and unifies the **Olist Brazilian E-Commerce dataset** into a single, analytics-ready **Customer 360 view** with **RFM (Recency, Frequency, Monetary) segmentation**, following the industry-standard **Medallion Architecture**.

### Business Problem

Retail organizations frequently struggle to build a single view of the customer because of:

1. **Per-order customer identifiers** — Transactional systems generate a new customer ID for every order, hiding repeat-purchase behavior.
2. **Duplicate geolocation data** — Raw geolocation records contain many GPS readings per ZIP prefix (~50+ readings per prefix), which multiplies rows if joined naively.
3. **Data quality and schema drift** — Missing timestamps, inconsistent casing, and unvalidated payment values are common in raw operational exports.

### The Engineering Solution

An automated ELT pipeline built on the Medallion Architecture:

* **Bronze Layer** — Raw payload preserved exactly as received (`inferSchema=false`, all columns as `STRING`, zero data loss).
* **Silver Layer** — Cleansing, deterministic deduplication, geolocation centroid aggregation, and automated quality gates that fail the pipeline on critical errors.
* **Gold Layer** — Customer 360 aggregation with `NTILE(5)`-based RFM scoring, published to Snowflake for BI consumption.

---

## 2. System Architecture

```
[AWS S3 Raw Storage]  (s3://omnicapstone/raw_data/)
        │
        ▼
[Databricks PySpark Engine — Unity Catalog: "workspace"]
   ├── BRONZE LAYER (workspace.bronze)   ──►  10 raw Delta tables
   │
   ├── SILVER LAYER (workspace.silver)   ──►  10 cleaned & typed Delta tables
   │
   └── GOLD LAYER (workspace.gold)       ──►  customer_360 (RFM segmented)
         │
         ▼  (Snowflake Spark Connector)
[Snowflake — RETAIL_CUSTOMER360 database]
   └── ANALYTICS_GOLD.CUSTOMER_360       ──►  Serving table for BI

         │
         ▼
[Power BI]  ──►  Executive dashboards (RFM segments, revenue, geography)
```

---

## 3. Dataset Summary

| # | Dataset | Source File | Records | Grain |
|---|---|---|---|---|
| 1 | Customers | `olist_customers_dataset.csv` | 99,441 | `customer_id` |
| 2 | Geolocation | `olist_geolocation_dataset.csv` | 1,000,163 | ZIP prefix readings |
| 3 | Orders | `olist_orders_dataset.csv` | 99,441 | `order_id` |
| 4 | Order Payments | `olist_order_payments_dataset.csv` | 103,886 | `order_id` + `payment_sequential` |
| 5 | Order Items | `olist_order_items_dataset.csv` | 112,650 | `order_id` + `order_item_id` |
| 6 | Products | `olist_products_dataset.csv` | 32,951 | `product_id` |
| 7 | Category Translation | `product_category_name_translation.csv` | 71 | `product_category_name` |
| 8 | Sellers | `olist_sellers_dataset.csv` | 3,095 | `seller_id` |
| 9 | Order Reviews | `olist_order_reviews_dataset.csv` | 99,224 | `review_id` |

> Note: the original blueprint also referenced a synthetic "In-Store Orders" dataset (#10) for an omnichannel extension. The current Gold layer (`01_Gold_Customer_360_CORRECTED.py`) intentionally uses **only the 9 real Olist tables** and does not fabricate in-store transaction data — see [Data Provenance](#7-data-provenance--integrity-notes) below.

---

## 4. Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Cloud Storage | AWS S3 | Landing zone for raw source CSVs |
| Compute Engine | Databricks (PySpark) | Distributed extraction, cleaning, transformation |
| Storage Format | Delta Lake | ACID transactions, schema enforcement, time travel |
| Data Warehouse | Snowflake | Serving layer (`RAW`, `ANALYTICS_GOLD` schemas) |
| BI / Reporting | Power BI | Executive dashboards on top of Snowflake |
| Orchestration | Databricks Workflows | End-to-end scheduled pipeline runs *(in progress)* |
| Documentation | Markdown & Mermaid | Technical reference and diagrams |

---

## 5. Repository Structure

```
├── README.md
├── docs/
│   ├── data_model.md                          # Schema definitions & medallion lineage
│   └── ai_usage.md                             # AI tool usage & review governance log
│
├── infrastructure/
│   ├── s3_upload.py                            # Local → S3 upload utility
│   └── snowflake_setup.sql                     # Snowflake database/schema/warehouse DDL
│
├── Bronze/
│   ├── 01_Bronze_Customers.py
│   ├── 02_Bronze_Orders.py
│   ├── 03_Bronze_Order_Items.py
│   ├── 04_Bronze_Products.py
│   ├── 05_Bronze_Category_Translation.py
│   ├── 06_Bronze_Sellers.py
│   ├── 07_Bronze_Payments.py
│   ├── 08_Bronze_Reviews.py
│   └── 09_Bronze_Geolocation.py
│
├── Silver/
│   ├── 01_Silver_Products.py
│   ├── 02_Silver_Sellers.py
│   ├── 03_Silver_Payments.py
│   ├── 04_Silver_Orders.py
│   ├── 05_Silver_Customers.py
│   ├── 06_Silver_Orderitems.py
│   ├── 07_Silver_Category_Translation.py
│   ├── 08_Silver_Reviews.py
│   └── 09_Silver_Geolocation.py
│
├── Gold/
│   └── 01_Gold_Customer_360_CORRECTED.py       # Customer 360 + RFM segmentation
│
└── databricks snowflake connction.py           # Databricks ↔ Snowflake integration & publish
```

---

## 6. Execution & Deployment Workflow

### Step 1 — Land raw data in AWS S3
```bash
python infrastructure/s3_upload.py
```
Uploads all 9 Olist CSVs to the configured S3 bucket.

### Step 2 — Run the Databricks Medallion Pipeline
1. **Bronze**: Run all notebooks in `Bronze/` (`01_Bronze_Customers.py` → `09_Bronze_Geolocation.py`).
2. **Silver**: Run all notebooks in `Silver/` (`01_Silver_Products.py` → `09_Silver_Geolocation.py`).
3. **Gold**: Run `Gold/01_Gold_Customer_360_CORRECTED.py`.

> Each notebook is self-validating — it raises a `ValueError` and stops execution if a critical data-quality gate fails, so a failed run is loud and visible rather than silently producing bad data.

### Step 3 — Provision Snowflake & Publish Gold Data
1. Run `infrastructure/snowflake_setup.sql` to create the database, schemas, and warehouse.
2. Run `databricks snowflake connction.py` to securely load credentials from Databricks Secrets, verify connectivity, and publish `workspace.gold.customer_360` to `RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360`.

### Step 4 — Validate in Snowflake
```sql
SELECT
    rfm_segment,
    COUNT(*) AS total_customers,
    ROUND(AVG(monetary), 2) AS avg_spend,
    ROUND(AVG(recency_days), 0) AS avg_recency
FROM RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360
GROUP BY rfm_segment
ORDER BY total_customers DESC;
```

### Step 5 — Connect Power BI *(in progress)*
Point Power BI's native Snowflake connector at `RETAIL_CUSTOMER360.ANALYTICS_GOLD.CUSTOMER_360` and build the RFM segment, revenue, and geographic dashboards.

---

## 7. Data Provenance & Integrity Notes

* All Gold-layer metrics are computed **strictly from the 9 real Olist datasets**. No transaction data is simulated, mocked, or fabricated anywhere in the pipeline.
* `customer_id` is a **transient, per-order** identifier. `customer_unique_id` is the **permanent human customer key**. All Silver and Gold analytics use `customer_unique_id` — this is the single most important identity-resolution decision in the project (see `docs/data_model.md`).
* Geolocation is deduplicated to **one row per ZIP prefix** using coordinate averaging (`AVG(lat)`, `AVG(lng)`) before any join, preventing the ~50x row-multiplication that raw geolocation data would otherwise cause.
* Only `delivered` orders are treated as "successful" purchases for RFM calculation; `canceled` and `unavailable` orders are excluded from Gold RFM metrics but are **not deleted** anywhere upstream — this is an analytical Gold-layer filter, not a data-cleaning decision.

---

## 8. Key Engineering Principles Enforced

1. **Identity Resolution** — `customer_unique_id`, not `customer_id`, is the customer entity key throughout Silver and Gold.
2. **Deterministic Deduplication** — Uses PySpark `Window.partitionBy(...).orderBy(...)` with `row_number() == 1`, never non-deterministic `first()` aggregations, so reruns produce identical results.
3. **Automated Quality Gates** — Every Silver and Gold notebook enforces schema checks, key-uniqueness checks, null assertions, and range validation. A failed gate raises an exception and stops the pipeline.
4. **Bronze Immutability** — Bronze never renames columns, casts types, removes nulls, or filters rows. All business logic lives in Silver/Gold, keeping Bronze a faithful, replayable copy of the source.

---

## 9. Team Members

* Asvin Nigam
* Tushar Nag
* Harshraj Parmar
* Preeti Vala

---

## 10. Related Documentation

| Document | Purpose |
|---|---|
| [`docs/data_model.md`](docs/data_model.md) | Full schema definitions for Bronze, Silver, and Gold tables |
| [`docs/ai_usage.md`](docs/ai_usage.md) | AI tool usage log and human review governance (project deliverable) |