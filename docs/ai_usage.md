# Deliverable 6 — AI Tool Usage & Verification Report

**Project**: Omnichannel Customer 360 & RFM Analytics  
**Document**: Deliverable 6 — AI Usage & Human Verification Report  
**Tools Employed**: Antigravity AI Coding Assistant (Gemini 3.6 Flash & Claude 4.6)  

---

## 1. Overview & AI Governance Policy

This report documents how AI tools were integrated into the capstone project lifecycle. AI was used as a **co-pilot and architecture advisor** for system design, PySpark transformation optimization, SQL warehouse DDL drafting, and documentation generation. 

To maintain enterprise quality and zero black-box code:
1. **100% Code Understanding**: Every AI-suggested function (`Window.partitionBy()`, `ignorenulls=True`, `NTILE(5)`, `COALESCE()`) was reviewed line-by-line and understood by the engineering lead.
2. **Empirical Validation**: AI outputs were executed on active Databricks and Snowflake environments to confirm row reconciliation and zero schema corruption.

---

## 2. Task-by-Task AI Usage & Review Matrix

| Task Area | AI Tool Contribution | Human Review & Validation Process | Action Taken / Changes Made |
|---|---|---|---|
| **1. Pipeline Architecture Design** | Suggested Medallion pattern (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) separating Databricks ETL from Snowflake serving. | Reviewed against capstone requirements and multi-table operational constraints. | Approved architecture; established 3-schema layout in Snowflake. |
| **2. Bronze Ingestion Strategy** | Drafted raw PySpark CSV readers (`inferSchema=false`, `mode='PERMISSIVE'`). | Verified that disabling schema inference prevents silent nullification of corrupt strings. | Standardized `inferSchema=false` across all 10 Bronze notebooks for zero data loss. |
| **3. Customer Identity Resolution** | Recommended `Window.partitionBy("customer_unique_id").orderBy("customer_id")` over non-deterministic `first()`. | Compared `groupBy().agg(first())` vs Window functions across cluster runs to test determinism. | Adopted Window `row_number() == 1` filtering to guarantee 100% reproducible deduplication. |
| **4. Geolocation Centroid Math** | Identified fan-out join risk (~53 readings per ZIP prefix) and proposed `AVG(lat)` and `AVG(lng)` aggregation. | Calculated post-join row counts to confirm fan-out elimination (1M rows reduced to 19K centroids). | Implemented ZIP prefix centroid grouping in `09_Silver_Geolocation.py`. |
| **5. Order Timestamps & Quality Flags** | Generated PySpark `to_timestamp()` casting and boolean flag logic (`approval_missing_flag`, `timeline_issue_flag`). | Checked order lifecycle sequences (`purchase` $\rightarrow$ `approval` $\rightarrow$ `carrier` $\rightarrow$ `customer delivery`). | Retained anomaly rows with audit flags instead of dropping them, preserving revenue accounting. |
| **6. Snowflake DDL & COPY Scripts** | Drafted `01_snowflake_ddl.sql` and `02_snowflake_copy.sql` (stages, file formats, schemas). | Inspected file format settings (`FIELD_OPTIONALLY_ENCLOSED_BY = '"'`) against CSV text quotes. | Verified staging setup and successfully populated `RAW_BRONZE` and `ANALYTICS_SILVER` tables. |
| **7. Gold Customer 360 & RFM Engine** | Formulated $NTILE(5)$ quintile scoring SQL and `CASE WHEN` segment mapping rules. | Validated mathematical boundary conditions for Recency ($R$), Frequency ($F$), and Monetary ($M$). | Verified dynamic quintile scoring in Snowflake `ANALYTICS_GOLD.CUSTOMER_360`. |
| **8. Documentation & Defense Prep** | Generated Mermaid flowcharts, ERD diagrams, 12-slide presentation script, and 25 Q&A defense questions. | Verified all column names and table counts across documentation artifacts. | Saved to `docs/end_to_end_pipeline_docs.md` and `presentation_and_interview_defense.md`. |

---

## 3. Detailed Verification & Validation Methodology

All AI-generated outputs underwent a 4-step validation protocol prior to final inclusion:

```
[1. Static Code Analysis]
       │ (Inspect line-by-line for black-box functions, non-deterministic logic, or hardcoded credentials)
       ▼
[2. Documentation Cross-Reference]
       │ (Verify PySpark APIs & Snowflake SQL syntax against official vendor documentation)
       ▼
[3. Execution Cluster Testing]
       │ (Execute PySpark notebooks on Databricks cluster; verify row counts & schema types)
       ▼
[4. Defensive Exception Testing]
       │ (Inject mock missing columns and null keys to verify quality gate ValueError triggers)
```

### Specific Validation Case Studies:
1. **Non-Deterministic `first()` vs Window Function**:
   * *AI Proposal*: Initial code draft used `groupBy("customer_unique_id").agg(first("customer_id"))`.
   * *Human Audit*: Identified that `first()` without explicit ordering is non-deterministic in Spark across distributed partitions.
   * *Resolution*: Updated to `Window.partitionBy("customer_unique_id").orderBy(col("customer_id").asc())` with `row_number() == 1` filter to guarantee 100% reproducible execution.

2. **Quoted Strings in Review & Product Texts**:
   * *AI Proposal*: Initial Snowflake file format lacked quote enclosing options.
   * *Human Audit*: Review comments containing commas inside quotes broke column alignment during test COPY operations.
   * *Resolution*: Added `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` and `NULL_IF = ('', 'NULL')` to `RAW_BRONZE.CSV_FILE_FORMAT`.

---

## 4. Conclusion

Generative AI accelerated structural drafting, Mermaid diagram creation, and presentation defense preparation. The engineering team retained full oversight, enforcing zero-data-loss ingestion, deterministic deduplication, centroid aggregation, and multi-gate quality assertions across the platform.
