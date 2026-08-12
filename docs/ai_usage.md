# Deliverable 6 — AI Tool Usage & Verification Report

**Project Name**: Retail Omnichannel Customer Analytics (Group 3)
**Document**: Deliverable 6 — AI Usage & Human Verification Report 
**Tools Employed**: Antigravity AI Coding Assistant (Gemini 3.6 Flash & Claude 4.6) 

---

## 1. Overview & AI Governance Policy

This report outlines how our engineering team strategically utilized generative AI tools to enhance productivity during the capstone project lifecycle. The core architecture, business logic, and problem-solving strategies were entirely human-driven. AI was strictly utilized as a supplementary research assistant to accelerate structural documentation, reference specific PySpark syntax, and aid in rapid error diagnosis.

To maintain enterprise-grade quality and ensure zero black-box code implementation, our team adhered to a strict governance policy:
* **Complete Code Ownership**: Every architectural decision and data modeling rule was established by the engineering team prior to utilizing AI for syntax formatting.
* **Empirical Validation**: All AI-assisted code snippets were manually tested and rigorously reviewed against our Medallion architecture guidelines in our Databricks environment prior to implementation.

---

## 2. Task-by-Task AI Assistance Matrix

The following table summarizes the specific areas where AI was used to accelerate our team's engineering efforts:

| Task Area | Team Execution & Strategy | AI Assistance Role |
| :--- | :--- | :--- |
| **Pipeline Architecture** | Designed the end-to-end Medallion pipeline flowing from AWS S3 to Databricks (Bronze/Silver) and Snowflake (Gold). | Accelerated the translation of our structural requirements into Mermaid.js flowcharts for documentation. |
| **Bronze Ingestion** | Identified severe data corruption in the raw reviews dataset caused by hidden line-breaks and commas. | Provided rapid syntax referencing for implementing the PySpark `multiLine` parsing option to resolve the delimiter issue. |
| **Silver Cleansing & Entity Resolution** | Strategized the use of `customer_unique_id` instead of `customer_id` for core joins to prevent duplication bugs and ensure accurate Frequency metrics. | Assisted with boilerplate formatting for strict data validation, schema enforcement, and timestamp casting. |
| **Troubleshooting & Infrastructure** | Led the configuration of the Databricks cluster, AWS S3 storage integration, and Snowflake data warehouse staging. | Acted as a pair-programming resource to rapidly diagnose and decode complex AWS IAM configuration blocks, specifically 403 Forbidden errors. |

---

## 3. Detailed Verification & Validation Methodology

Our team ensured that AI-assisted outputs were heavily scrutinized and tested to prevent schema corruption or data loss. We followed a streamlined validation protocol:

1.  **Static Code Analysis**: The engineering team manually inspected all syntax suggestions to ensure they aligned with our specific business requirements (e.g., preserving anomalous rows using Boolean quality flags instead of dropping data).
2.  **Execution Cluster Testing**: Code was executed in isolated Databricks cells to physically verify row-count reconciliation between the Bronze and Silver layers.
3.  **Defensive Exception Testing**: We validated that our primary key constraints (e.g., null checks on `review_id` and `order_id`) correctly triggered our engineered quality gates.

---

## 4. Conclusion

By intelligently integrating AI as a supportive productivity tool, our team was able to accelerate syntax writing and diagram generation while maintaining complete authoritative control over the project. The engineering team successfully enforced zero-data-loss ingestion, accurate entity resolution, and robust data modeling across the entire omnichannel pipeline.
