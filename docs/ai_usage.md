# AI Tool Usage Report — Retail Customer 360

## Overview

This report documents how Generative AI tools (Antigravity AI Coding Assistant) were utilized throughout the capstone project for architecture design, PySpark code optimization, technical documentation, and presentation coaching.

---

## 1. AI Tool Summary & Review Governance

| Tool / Model | Primary Capabilities Used | Human Review & Validation Process |
|---|---|---|
| **Antigravity AI (Gemini 3.6 & Claude 4.6)** | Architecture blueprinting, PySpark refactoring, Mermaid diagram generation, SQL optimization, Interview coaching | Every line of AI-assisted code was manually inspected, cross-referenced with PySpark/Snowflake official docs, and validated via execution test runs. |

---

## 2. Granular Daily Usage Log

### Day 1 — Setup & Architecture
* **AI Contribution**: Assisted in structuring the Medallion Architecture blueprint (S3 → Databricks → Snowflake), generating the initial `.gitignore`, `pipeline_config.py`, and base `data_model.md`.
* **Human Review & Refinement**: Verified S3 URI mappings, adjusted database naming conventions, and verified schema designs against the 9 Olist source datasets.

### Day 2 — Bronze Ingestion & Silver Deduplication Architecture
* **AI Contribution**: Helped design the zero-data-loss Bronze policy (`inferSchema=false`) and provided deep-dive learning guides explaining why `customer_unique_id` is the canonical human key.
* **Human Review & Refinement**: Tested initial PySpark ingestion scripts on Databricks clusters and confirmed row reconciliation against CSV source files.

### Days 3–5 — Full Team Pipeline Assembly & Snowflake Integration
* **AI Contribution**: Analyzed the 10 Bronze and 10 Silver team notebooks. Identified non-deterministic `first()` aggregations in deduplication logic and recommended upgrading to `Window.partitionBy()` with `row_number()`.
* **Human Review & Refinement**: Adopted the team's expanded validation gates and verified Snowflake DDL schemas (`RAW_BRONZE`, `ANALYTICS_SILVER`, `ANALYTICS_GOLD`).

### Day 6 — Enterprise Documentation & 30-Minute Presentation Prep
* **AI Contribution**: Generated publication-grade Mermaid diagrams (System Architecture Flow, ERD, Identity Resolution Sequence, Centroid Aggregation Flowchart) and drafted a 12-slide presentation script with a 25-question interview defense manual.
* **Human Review & Refinement**: Verified that all 10 datasets across Bronze, Silver, and Gold were accurately documented in `docs/end_to_end_pipeline_docs.md`.

---

## 3. Engineering Quality & Governance Rules

All AI-assisted outputs adhered to strict engineering quality rules:
1. **Zero Black-Box Code**: Every PySpark syntax decision (e.g., `ignorenulls=True`, `cast("integer")`, `NTILE(5)`) was fully documented and understood by the engineering lead.
2. **Deterministic Processing**: Replaced non-deterministic PySpark functions with explicit window ordering.
3. **No Hallucinated Schemas**: All table definitions were validated directly against the raw CSV column headers and Silver output DataFrames.
