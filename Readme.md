# Retail Lakehouse: End-to-End Data Engineering Project

## 1. The Problem We're Solving

A mid-size online retailer has order, customer, and product data trapped in
operational systems. Nobody can answer basic questions fast: what's our
monthly revenue trend, which customers are about to churn, what does live
order volume look like right now. The business needs three things data
engineering exists to provide:

- **Historical analytics** — dashboards on sales, customer value, product
  performance, refreshed daily.
- **Near-real-time visibility** — an operations view of orders as they
  happen, not next-day.
- **A feedback loop** — insights (like a churn score) pushed back into
  systems the business actually uses, not stranded in a BI tool.

This is deliberately a *generic, well-understood domain* (retail/e-commerce).
That's intentional — interviewers can sanity-check your decisions without
learning a new domain first, and the dataset is rich enough to touch
batch, streaming, dimensional modeling, and ML without inventing fake
complexity.

**Datasets**: Kaggle's "Online Retail II" (real UK e-commerce transactions,
~1M rows, has customers/products/orders) as the historical seed, plus a
Python event generator that produces synthetic "live" order events to
simulate an operational stream. Using a real historical dataset plus a
synthetic streaming layer gives you both batch and streaming without
needing access to a live production system.

---

## 2. Architecture Overview

Two parallel tracks, sharing the same data model:

**Track A — Local/open-source (always running, $0, your durable demo)**
**Track B — Azure for Students (cloud deployment, proof you can operate
in a managed platform, runs for the life of your $100/12mo credit)**

You build the pipeline logic once conceptually, implement it twice
(local stack first, then port the storage/orchestration/serving layers to
Azure), and document the tradeoffs you observe between them. That
comparison write-up is itself a strong interview artifact — most
candidates have only ever touched one stack.

                 ┌─────────────────────────────────────┐
                 │           SOURCE DATA                 │
                 │  Kaggle CSV (historical) + Python      │
                 │  event generator (synthetic streaming) │
                 └───────────────┬─────────────────────┘
                                 │
                ┌────────────────┴────────────────┐
                │                                  │
          BATCH INGESTION                  STREAMING INGESTION
        (Airflow + ADF, daily)            (Kafka/Event Hubs, live)
                │                                  │
                └────────────────┬────────────────┘
                                 ▼
                      BRONZE (raw, immutable,
                      Delta Lake on MinIO/ADLS Gen2)
                                 ▼
                    SILVER (cleaned, conformed,
                    deduplicated, typed, Delta Lake)
                                 ▼
                    GOLD (business-level aggregates,
                    dimensional model, ML features)
                                 ▼
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                   ▼
        BI / Dashboards   ML (churn model)     Reverse ETL
        (Superset/Power BI) (scikit-learn)     (push churn score
                                                 back to "CRM" table)

Cross-cutting, touching every layer: **orchestration** (Airflow/ADF),
**data quality** (Great Expectations), **catalog & lineage**
(OpenMetadata), **IaC** (Terraform), **CI** (GitHub Actions).

---

## 3. Stack, and Why Each Piece

| Layer | Tool | Why this one |
|---|---|---|
| Object storage (local) | **MinIO** | S3-compatible API, free, lets you write code once that works against MinIO and S3/ADLS with no changes — this is the actual skill (object storage semantics), not the vendor. |
| Object storage (cloud) | **ADLS Gen2** | Azure for Students credit covers it at portfolio scale for months; hierarchical namespace is a relevant detail to know vs flat S3. |
| Table format | **Delta Lake** | This is the answer to "don't commit object-storage antipatterns." Plain Parquet on object storage means every update rewrites whole files — fine for append-only logs, terrible for upserts (e.g. correcting a customer record). Delta gives ACID transactions and MERGE on top of object storage, so you get database-like upsert semantics without hammering the object store with random small writes. You'll explicitly demo this: a customer-dedup job that does a Delta `MERGE` instead of a naive overwrite, and you'll be able to explain *why* that distinction matters in an interview. |
| Batch orchestration | **Apache Airflow** (local) / **Azure Data Factory** (cloud) | Airflow is the de facto open-source standard — most JDs ask for it by name. ADF gives you the managed-platform equivalent so you can speak to both DAG-as-code and GUI/managed orchestration. |
| Streaming | **Kafka (Redpanda)** locally, **Event Hubs** in Azure | Same reasoning as storage: same protocol, two implementations. Redpanda is a lighter Kafka-API-compatible broker, easier to run in Docker than real Kafka. |
| Processing | **Apache Spark** (local + Databricks Community Edition / Synapse Spark) | Spark is the dominant batch/streaming engine in the field; Structured Streaming covers the live-order pipeline. |
| Transformation/SQL | **dbt-core** | Industry-standard for the transform layer — version-controlled, testable SQL with built-in documentation and lineage generation. Using dbt also forces you to write transformations as simple, modular, testable models rather than one giant script, which directly demonstrates the "simplicity + business rules" transformation principles from the article. |
| Data quality | **Great Expectations** | Gate between bronze→silver→gold: a batch doesn't get promoted unless it passes expectations (no nulls in order_id, totals reconcile, etc.). This is what makes "data quality" a real engineering control instead of a slide bullet. |
| Catalog & lineage | **OpenMetadata** | Free, open-source, and it auto-ingests lineage from dbt and Airflow — this directly satisfies the "metadata management / schema evolution / lineage" requirement from the article without paying for Purview. |
| Serving — BI | **Apache Superset** (local) / **Power BI Desktop** (free, unpublished) | Two different audiences: Superset is what a startup/eng-heavy company runs themselves; Power BI is what you'll meet at any Microsoft-stack enterprise. |
| Serving — ML | **scikit-learn** + a small **FastAPI** wrapper | A churn-prediction model trained on gold-layer features, served via a simple API — demonstrates the "ML model training + real-time prediction" serving pattern without needing a heavyweight MLOps stack. |
| Reverse ETL | **A custom script** writing the churn score back into a mock "CRM" Postgres table | Reverse ETL tools (Census, Hightouch) are paid SaaS; replicating the pattern by hand is more honest for a portfolio project and shows you understand the *concept*, not just a vendor tool. |
| IaC | **Terraform** | Defines the Azure resources (storage account, ADF, Event Hubs, SQL DB) so the cloud deployment is reproducible and destroyable — critical for not burning your $100 credit by accident, and a hard requirement on most data engineer JDs now. |
| CI | **GitHub Actions** | Runs dbt tests and Python unit tests on every push — minimal but real software engineering rigor. |

---

## 4. Why a Medallion (Bronze/Silver/Gold) Architecture

This maps directly onto the "data temperature" and "schema handling"
considerations from the article:

- **Bronze**: raw, schema-on-read, append-only, immutable. This is your
  system of record for "what did the source actually send us" —
  essential for debugging and reprocessing. Stored cheap, accessed rarely
  (cold/lukewarm).
- **Silver**: cleaned, deduplicated, conformed types, enforced schema via
  Delta's schema enforcement. Accessed moderately (lukewarm) — feeds
  transformation jobs, not end users directly.
- **Gold**: business-level dimensional model (fact_orders,
  dim_customer with SCD Type 2, dim_product, monthly aggregates) plus ML
  feature tables. This is hot data — what dashboards and the churn model
  query directly, so it's small, pre-aggregated, and fast.

This also gives you a clean story for **schema evolution**: bronze tolerates
schema drift from the source, silver is where you decide how to handle a
new column or a type change, and you document that decision via dbt
model versioning + OpenMetadata's schema history tracking.

---

## 5. Why This Particular Mix of Batch + Streaming

Most portfolio projects are pure batch (easy) or pure streaming-toy
(flashy but shallow). Doing both, on the *same* underlying gold tables,
forces you to handle a real problem: reconciling a fast, approximate
streaming path with a slower, authoritative batch path. Concretely:

- The streaming path updates a `live_order_count` / `live_revenue_today`
  metric for an operational dashboard (operational analytics).
- The nightly batch path recomputes the authoritative daily aggregates
  from bronze, and you reconcile/correct the streaming numbers against it
  the next morning.

This is a real pattern (lambda-ish architecture) and gives you a genuine
answer when asked "how do you handle data that arrives via two paths."

---

## 6. Why a Churn Model + Reverse ETL, Not Just Dashboards

Dashboards alone only demonstrate the "Analytics" serving pattern from
the article. Adding a small churn-prediction model trained on gold-layer
features (recency, frequency, monetary value — classic RFM features,
which doubles as your "featurization" transformation example) and then
writing the prediction back into a mock CRM table demonstrates all three
serving patterns: Analytics, ML, and Reverse ETL, in one coherent
pipeline instead of three disconnected demos.

---

## 7. Governance, Quality, Lineage — How the Article's Requirements Map to Real Components

| Article requirement | How this project satisfies it |
|---|---|
| Master data / golden records | `dim_customer` as the single conformed customer record, built via SCD Type 2 merge logic in dbt/Delta |
| Data quality | Great Expectations suites gating bronze→silver and silver→gold promotion, results logged and visible |
| Data lineage | OpenMetadata auto-ingesting dbt + Airflow lineage, so you can show "this gold column traces back to this bronze field" |
| Schema evolution | Delta schema enforcement + dbt model versioning + OpenMetadata schema change history |
| Regulatory/sovereignty | Documented (not deeply implemented, given scope) — note in README where you'd apply column-level masking (e.g. customer PII) using Delta column ACLs or Purview classification if this were production |

Being honest about what you *documented vs. fully implemented* (like PII
masking) is itself a good interview signal — it shows you understand
scope tradeoffs rather than claiming to have built everything.

---

## 8. Project Structure (suggested repo layout)

```
retail-lakehouse/
├── infra/                  # Terraform for Azure resources
├── docker-compose.yml      # MinIO, Redpanda, Airflow, Postgres, Superset, OpenMetadata
├── ingestion/
│   ├── batch/               # Airflow DAGs, ADF pipeline JSON
│   └── streaming/            # event generator, Spark Structured Streaming job
├── transform/
│   └── dbt_project/          # bronze→silver→gold models, tests, docs
├── quality/                 # Great Expectations suites
├── ml/                      # feature engineering, churn model training, FastAPI serving
├── reverse_etl/             # script pushing churn scores to mock CRM
├── dashboards/               # Superset exports, Power BI .pbix
└── README.md                # architecture diagram, cost notes, local-vs-Azure tradeoffs
```

---

## 9. Suggested Build Order (roughly 6–8 weeks, part-time)

1. Local stack up via Docker Compose (MinIO, Redpanda, Airflow, Postgres) — get bronze ingestion of the historical CSV working.
2. dbt models for silver (cleaning, conforming, dedup) and gold (dimensional model + aggregates), with Great Expectations gates between layers.
3. Streaming path: event generator → Redpanda → Spark Structured Streaming → live gold metrics.
4. Superset dashboards on gold tables.
5. OpenMetadata ingestion from dbt + Airflow — lineage and catalog visible end to end.
6. Churn model (RFM features from gold) + FastAPI serving + reverse-ETL script.
7. Port storage/orchestration/serving to Azure (Terraform up ADLS Gen2, ADF, Event Hubs, Azure SQL, Synapse serverless), re-run the same dbt project against it.
8. Write the README: architecture diagram, the local-vs-Azure cost/operational comparison, what you'd add for production (PII masking, alerting, SLAs).

---

## 10. SLA / Retrieval Note

Define explicit, written SLAs even at toy scale — e.g. "gold tables
refreshed by 6am for the previous day," "streaming dashboard lag under 2
minutes." Then instrument Airflow/Spark to actually measure against
those numbers. An SLA you defined and measured, even informally, is a
much stronger talking point than an unstated assumption that the
pipeline "is fast."
