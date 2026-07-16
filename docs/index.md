# OpenLake: A Retail Lakehouse - Technical Report

> A first-person account of designing, building, debugging, and understanding
> a production-grade data engineering system on a student budget.

This document is not a tutorial. It's the write-up I wish existed when I started -
the kind that explains *why* every decision was made, *what broke* when I made it,
and *how to defend it* in an interview room.

---

## What is OpenLake & A Lakehouse?

*   **OpenLake** is the name of this project: an end-to-end, production-grade retail data engineering system. It serves as a comprehensive technical showcase of modern data architecture, built on a local open-source stack (Track A) and ported to a fully-managed Azure cloud environment (Track B).
*   **Lakehouse** is a modern data management architecture that combines the low-cost, highly-scalable storage of a **Data Lake** (like MinIO, Amazon S3, or Azure Data Lake Storage Gen2) with the structure, reliability, and transaction guarantees of a **Data Warehouse** (ACID transactions, schema enforcement, data versioning, and high-performance SQL querying). By using open table formats like **Delta Lake** directly on top of cheap object storage, it eliminates the need to run and sync separate platforms for raw storage and analytical queries.
*   **Open Stack / Open Standards**: The "Open" in OpenLake indicates our reliance on open protocols and formats (Delta Lake table format, S3 storage API, Kafka wire API). By avoiding proprietary, coupled warehouse tools, we keep the core compute/pipeline logic fully portable, allowing the same code to run both locally and in the cloud.

---

## Table of Contents

| Chapter | Topic | Key Questions Covered |
|---|---|---|
| [1. Architecture & The Medallion Pattern](./ch1_architecture.md) | Why we built it this way | Medallion vs Postgres, data temperature, Bronze/Silver/Gold, schema drift, system of record |
| [2. Object Storage & Infrastructure](./ch2_storage_infrastructure.md) | MinIO, Docker, and the S3-compatible layer | Object storage semantics, S3 API portability, Docker Compose design, secrets, healthchecks |
| [3. Orchestration with Apache Airflow](./ch3_airflow.md) | Scheduling, dependencies, and idempotency | DAGs, task state, XCom, execution_date, idempotency, backfill |
| [4. Distributed Compute with Apache Spark](./ch4_spark.md) | How the processing engine works | Driver/Executor, lazy eval, shuffle, partitioning, Parquet vs CSV, SparkSession |
| [5. Delta Lake - ACID on Object Storage](./ch5_delta_lake.md) | Why plain Parquet isn't enough | `_delta_log`, MERGE vs overwrite, time travel, schema enforcement, small file problem |
| [6. Streaming - Redpanda + Spark Structured Streaming](./ch6_streaming.md) | The live order pipeline | Broker internals, watermarking, exactly-once, Lambda vs Kappa, output modes |
| [7. The ML Pipeline - RFM, Churn, Reverse ETL](./ch7_ml_pipeline.md) | From Gold features to CRM action | RFM design, data leakage, model lifecycle, Reverse ETL, precision/recall tradeoff |
| [8. Governance, Lineage & System Design](./ch8_governance_system_design.md) | The big picture and hard questions | OpenMetadata, PII, data contracts, real-time churn, SPOF, scalability |
| [9. Porting Transformations to dbt & Azure SQL](./ch9_dbt_azure_sql.md) | Moving to dbt and SQL Server database connectivity | dbt profiles, pyodbc, unixODBC on Linux, Microsoft ODBC driver registration, environment variable loaders |
| [10. Cloud Orchestration, Hybrid Spark Compute & Reverse ETL Optimizations](./ch10_cloud_orchestration.md) | Cloud Ingestion, local-cloud Spark, Reverse ETL batching, testing, and CI/CD guardrails | Server-side copy, resource locks, batched MERGE, dynamic open mocks, git history pruning, and Great Expectations 1.x |
| [11. Infrastructure as Code - Terraform & Azure Core Resources](./ch11_terraform_iac.md) | Why IaC, Terraform internals, RBAC 403 bug, state file, auth | Declarative vs imperative, providers.tf/variables.tf/main.tf, state drift, control vs data plane RBAC, az login vs Service Principal |
| [12. Architecture Decision - Option B, Protocols & dbt](./ch12_architecture_decision.md) | The Databricks quota deadlock, compute/storage decoupling, s3a→abfss, dbt, MERGE optimization | vCPU quota math, hybrid compute tradeoffs, hadoop-azure connector, ON CONFLICT vs MERGE, batching speedup, Key Vault vs .env, CI status |

---

## How to Read This

Each chapter is self-contained but assumes you've read the ones before it.
The architecture chapter (1) is the foundation - if something in a later chapter
seems unmotivated, the answer is usually in chapter 1.

**Callout boxes** appear throughout, formatted like this:

> [!TIP]
> **Answer**: This is the crisp, 60-second version of the concept.
> Memorise the shape of this answer, not the exact words.

**Problem stories** are woven into each chapter where they naturally occurred
during the build. They're not a troubleshooting appendix - they're part of the
engineering story.

**Screenshot TODOs** appear as:

> [!NOTE]
> **📸 TODO Screenshot**: Description of what to capture and where.

---

## The Stack at a Glance

| Layer | Local (Track A) | Azure (Track B) |
|---|---|---|
| Object Storage | MinIO | ADLS Gen2 |
| Table Format | Delta Lake | Delta Lake |
| Batch Orchestration | Apache Airflow | Azure Data Factory |
| Streaming Broker | Redpanda (Kafka-API) | Azure Event Hubs |
| Compute Engine | Apache Spark | Synapse Spark |
| Transformation | dbt-core | dbt-core |
| Data Quality | Great Expectations | Great Expectations |
| Catalog & Lineage | OpenMetadata | OpenMetadata |
| BI / Dashboards | Apache Superset | Power BI |
| ML Serving | FastAPI + scikit-learn | FastAPI + scikit-learn |
| IaC | Terraform | Terraform |
| CI | GitHub Actions | GitHub Actions |

---

*Report written as of: July 2026 - v1 temporal split baseline complete. Chapters 11–12 added for Phase 5 prep.*
