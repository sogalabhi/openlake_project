# Chapter 12: Architecture Decision - Option B, Protocols & dbt

*← [Back to Index](./index.md)*

---

## 12.1 The Databricks Quota Deadlock - The Full Story

This is one of the best interview stories in the whole project because it's not about things going right - it's about hitting a real infrastructure wall and engineering around it.

### What Databricks Actually Is (The Architecture First)

Databricks is not just a "managed Spark cluster." It operates on a **split-plane architecture**:

**Control Plane** - lives in Microsoft/Databricks' own Azure account. It hosts the web UI, notebook editor, job scheduler, and cluster management API. You interact with this but you don't own the VMs powering it.

**Data Plane** - lives in *your* Azure subscription. When you click "create cluster" in the Databricks UI, the Databricks control plane reaches into your subscription and automatically provisions a hidden **Managed Resource Group** (named `databricks-rg-<workspace-name>`) containing real Azure Virtual Machines to act as your Spark Driver and Spark Workers.

This is the critical detail: Databricks doesn't rent you VMs from their pool - it creates VMs *inside your subscription* using your quota.

### The 4 vCPU Wall

Azure Free Trial subscriptions have a hard, non-negotiable **Total Regional vCPU Quota of 4 cores per region**. This exists specifically to prevent abuse (free accounts spinning up GPU farms for crypto mining).

The math of why this creates a deadlock:

| Component | VM Family | Minimum Size | vCPUs Required |
|---|---|---|---|
| Spark Driver Node | Standard_DS3_v2 (minimum allowed family) | 4 cores | 4 |
| Spark Worker Node (at least 1) | Standard_DS3_v2 | 4 cores | 4 |
| **Total** | | | **8 vCPUs** |
| **Free Trial Quota** | | | **4 vCPUs** |

**8 required, 4 available** - Azure Resource Manager rejects the provisioning request instantly. There's no configuration that reduces this: `Standard_DS3_v2` is the smallest VM family Databricks permits in its managed resource group. Smaller VM families (like `Standard_B1s` with 1 vCPU) are available in Azure but not in the VM families Databricks supports for Spark workloads.

The Terraform `azurerm_databricks_workspace` resource in [`main.tf`](../infra/main.tf) provisioned the workspace shell fine - the workspace UI came up. The failure happened the moment you tried to create a *cluster* inside it: Databricks called Azure, Azure counted the required vCPUs against the quota, and returned a capacity error.

The only paths out:
1. Upgrade the Azure subscription to Pay-As-You-Go (removes the quota cap)
2. Request a quota increase (long process, often rejected for free trials)
3. **Don't use Databricks compute** - use something else

> [!TIP]
> **Interview Answer**: Databricks spins up VMs in your own Azure subscription (the data plane). The smallest viable Databricks cluster requires 8+ vCPUs - 4 for the driver, 4+ for at least one worker - using VM families like `Standard_DS3_v2`. Azure Free Trial enforces a hard 4 vCPU regional cap. This is a hard math problem: 8 > 4, no configuration resolves it. The workspace provisions fine, but cluster creation fails at the Azure quota check every time. This isn't a bug or misconfiguration - it's a deliberate platform constraint on free accounts.

---

## 12.2 Compute/Storage Decoupling - Why Option B Actually Works

The pivot to "run Spark locally, store data in Azure" only makes sense if you understand *why* modern data platforms are designed so that compute and storage don't need to be in the same place.

### The Old World: Coupled Architecture (Hadoop On-Premise)

In early Hadoop (pre-2015), compute and storage were **physically inseparable**. A Hadoop cluster was a rack of servers where each server had both hard drives (HDFS DataNode = storage) and CPUs (YARN NodeManager = compute). The data lived *on the same machines* doing the processing.

**The problem**: if you needed more storage (data grew), you had to buy more servers - which meant you also got more CPUs you didn't need (and paid for). If you needed more compute (heavier jobs), you had to buy more servers - and got more disk you didn't need. The two resources scaled together, even when only one was the bottleneck.

### The Modern World: Decoupled Architecture

Cloud providers broke this coupling in two steps:

**Step 1**: Object storage (S3, ADLS Gen2) became infinitely scalable, cheap, and accessible over HTTPS from anywhere. Data now lives in a central store that's not attached to any specific compute machine.

**Step 2**: Compute became ephemeral - Databricks, Snowflake, BigQuery, and EMR all provision compute clusters on-demand, pointed at the central object store, and tear them down when the job is done. You pay for compute only while it runs.

The consequence: **Spark doesn't care where it runs**, as long as it can reach the storage endpoint over the network. A Spark process running on your laptop, on an EC2 instance in AWS, or inside Databricks in Azure - all three can read from the same ADLS Gen2 account, because ADLS Gen2 is an HTTPS endpoint that accepts authenticated requests from anywhere on the internet.

### Option B: What Was Actually Done

```
Local Docker Spark Cluster (compute)
    ↓ spark-submit job
    ↓ reads/writes via abfss:// over HTTPS/WAN
Azure ADLS Gen2 (stopenlakeabhijith.dfs.core.windows.net) (storage)
```

The Spark executor running on your local machine processed the data. The results were written as Delta Lake Parquet files to ADLS Gen2 over your home internet connection. The Azure storage account didn't know or care that the compute was running in India - it just received authenticated HTTPS PUT requests with Parquet data.

### The Production Tradeoff You Must Name

This is an anti-pattern for production scale, and an interviewer who knows Spark will ask about it. The honest answer:

- Every Spark partition read/write requires a network round-trip over the public internet. Databricks in the same Azure region (`westus3`) would communicate with ADLS Gen2 over Azure's internal backbone network - orders of magnitude lower latency and no egress costs.
- For a small dataset (the online retail CSV is ~94MB), WAN latency is tolerable. For 10TB, local-to-cloud compute would take days and cost significantly in egress fees.
- This architecture is a **proof-of-concept / development pattern**, not production. In production, both compute and storage should be in the same Azure region on the same internal network.

> [!TIP]
> **Interview Answer**: Compute/storage decoupling is fundamental to modern cloud data architecture. Spark only needs network access to read/write storage - it doesn't need to be co-located with it. My local Spark cluster communicated with ADLS Gen2 over HTTPS/WAN exactly like a cloud-native cluster would, just with higher latency. The honest tradeoff: WAN round-trips add per-partition latency that's tolerable at dev scale but would be cost-prohibitive and slow at production scale. In production, you'd colocate compute (Databricks) and storage (ADLS) in the same Azure region to use the internal backbone network and avoid egress costs.

---

## 12.3 The Protocol Shift: `s3a://` → `abfss://`

This is the concrete technical change that made Option B work. It's worth understanding precisely.

### What These Prefixes Actually Are

These are not file paths - they're **URI schemes** that tell Spark's filesystem abstraction layer which underlying storage driver to use. Spark doesn't have native code to talk to S3 or ADLS - it delegates all storage I/O to the Hadoop FileSystem API, which is a pluggable interface.

```
s3a://lakehouse/silver/retail_transactions
↑                                         
URI scheme → tells Hadoop FileSystem to load the S3A connector
```

```
abfss://lakehouse@stopenlakeabhijith.dfs.core.windows.net/silver/retail_transactions
↑                                                                                    
URI scheme → tells Hadoop FileSystem to load the ABFS connector
Note: "abfss" = Azure Blob FileSystem Secure (uses HTTPS, not HTTP)
```

### The Two Connectors

**`hadoop-aws` / `s3a://`** - the S3A connector. Implements the Hadoop FileSystem API against Amazon S3's REST API (and any S3-compatible endpoint like MinIO). Authentication via AWS access key/secret key. Used for the local MinIO stack.

**`hadoop-azure` / `abfss://`** - the ABFS connector. Implements the Hadoop FileSystem API against Azure's Data Lake Storage REST API (a distinct API from the older Blob Storage REST API, specifically designed for HDFS-like filesystem semantics). Authentication via storage account access key or OAuth token.

The Spark packages declaration in [`dags/bronze_ingestion.py`](../dags/bronze_ingestion.py):
```python
packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4"
```
This tells `spark-submit` to download the `hadoop-azure` JAR from Maven at job launch time. Previously it was `hadoop-aws` for the MinIO setup.

### What Changed, What Didn't

**Changed**:
- Package: `hadoop-aws` → `hadoop-azure`
- Path prefix: `s3a://lakehouse/...` → `abfss://lakehouse@stopenlakeabhijith.dfs.core.windows.net/...`
- Auth config: `spark.hadoop.fs.s3a.access.key` → `fs.azure.account.key.stopenlakeabhijith.dfs.core.windows.net`

**Did NOT change**:
- All PySpark transformation logic
- Delta Lake write/read API calls
- Airflow DAG structure
- SparkSubmitOperator configuration (except `packages`)

This is the compute/storage separation paying off in code: the actual data processing code is storage-agnostic. The only thing that changes between MinIO and ADLS Gen2 is the connector configuration - not a single line of business logic.

```python
# push_churn_scores.py - the Azure config injection
spark = SparkSession.builder \
    .config(
        "fs.azure.account.key.stopenlakeabhijith.dfs.core.windows.net",
        os.environ.get("AZURE_STORAGE_KEY"),
    ) \
    .getOrCreate()

# The actual read - same Delta API, different path scheme
df = spark.read.format("delta").load(
    "abfss://lakehouse@stopenlakeabhijith.dfs.core.windows.net/silver/retail_transactions"
)
```

> [!TIP]
> **Interview Answer**: `s3a://` and `abfss://` are Hadoop FileSystem URI schemes - they tell Spark which storage connector to load, not just where the file is. `s3a` uses the S3 REST API (works against MinIO or AWS); `abfss` uses Azure's ADLS Gen2 REST API (the "secure" variant over HTTPS). Swapping connectors only changes the storage driver and auth config - the Spark transformation code, Delta Lake operations, and Airflow DAG are identical. This portability is the point: Spark processes data, storage is pluggable.

---

## 12.4 dbt - What It Actually Does

The Claude prep flagged this: "dbt fundamentals not yet covered in sessions." Here's the complete picture before the Azure SQL specifics.

### The Core Concept

dbt (Data Build Tool) is a **SQL-first transformation framework**. Its core insight: most analytical transformations can be expressed as SQL `SELECT` statements - but raw SQL scripts are hard to maintain, hard to test, and create no documentation or lineage.

dbt lets you write:
```sql
-- models/gold/customer_churn_scores.sql
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    churn_probability
FROM {{ ref('silver_retail_transactions') }}
WHERE customer_id IS NOT NULL
```

And dbt handles:
- **Dependency resolution**: `{{ ref('silver_retail_transactions') }}` tells dbt this model depends on another model - dbt builds the dependency graph and executes them in the correct order
- **Materialization**: dbt compiles this SELECT into a `CREATE TABLE AS SELECT` or `CREATE VIEW AS SELECT` statement and executes it on the target database
- **Testing**: define tests (`not_null`, `unique`, custom SQL checks) that run against the materialized output
- **Documentation**: auto-generated lineage graph and column-level documentation

### What dbt Is NOT

dbt does not move data. It doesn't ingest CSVs, run Spark jobs, or call APIs. It only transforms data that's already in a database (or data warehouse). In this project's architecture:

- **Spark** (Bronze → Silver): Ingests raw CSV, applies schemas, writes Delta Lake Parquet. Heavy compute, distributed.
- **dbt** (Silver data → Gold CRM schema in Azure SQL): Applies relational transformations, tests quality, materializes into Azure SQL tables. Lightweight, SQL-only.

### The dbt-sqlserver Adapter

dbt itself is database-agnostic - it's a Python tool that generates SQL. The adapter is what makes it generate the *correct SQL dialect* for a specific database:

- `dbt-postgres`: generates PostgreSQL-flavored SQL
- `dbt-bigquery`: generates BigQuery Standard SQL
- `dbt-sqlserver`: generates T-SQL (Transact-SQL, Microsoft's SQL Server dialect)

This is the same abstraction as Terraform providers: dbt is the engine, the adapter is the plugin that speaks a specific database's language.

### The Profiles File - How dbt Connects

```yaml
# transform/dbt_project/profiles.yml
dbt_project:
  target: azure
  outputs:
    azure:
      type: sqlserver
      driver: "ODBC Driver 18 for SQL Server"
      server: "{{ env_var('AZURE_SQL_SERVER') }}"
      port: 1433
      database: crm
      schema: dbo
      user: "{{ env_var('AZURE_SQL_USER') }}"
      password: "{{ env_var('AZURE_SQL_PASSWORD') }}"
```

`{{ env_var('AZURE_SQL_SERVER') }}` reads from environment variables - credentials never hardcoded in the profiles file (which is committed to git). This is the same pattern as `.env` injection in Docker Compose.

### The Columnstore Config

The issue from Chapter 9: `dbt-sqlserver` materializes tables with clustered columnstore indexes by default (optimized for analytical queries). Azure SQL Basic tier doesn't support this feature.

The fix in [`dbt_project.yml`](../transform/dbt_project/dbt_project.yml):
```yaml
models:
  dbt_project:
    +as_columnstore: false
```

What this does at the SQL compilation level: instead of generating:
```sql
CREATE TABLE customer_churn_scores (...) WITH (CLUSTERED COLUMNSTORE INDEX)
```

It generates:
```sql
CREATE TABLE customer_churn_scores (...)  -- standard row-store heap table
```

> [!TIP]
> **Interview Answer**: dbt is a SQL transformation framework - you write modular SELECT statements ("models"), and dbt compiles them into CREATE TABLE/VIEW statements, manages dependency order between models, runs data quality tests against the output, and generates lineage documentation. It doesn't move data - it transforms data already in a database. The adapter (dbt-sqlserver in this case) handles SQL dialect differences between databases, similar to how Terraform providers abstract different cloud APIs.

---

## 12.5 pymssql / MERGE vs Postgres `ON CONFLICT`

When porting the reverse ETL script from the local Postgres stack to Azure SQL, the upsert syntax had to change. This is a SQL dialect difference worth knowing.

### Postgres: `INSERT ... ON CONFLICT`

```sql
INSERT INTO customer_churn_scores (customer_id, churn_probability, ...)
VALUES (%s, %s, ...)
ON CONFLICT (customer_id)
DO UPDATE SET
    churn_probability = EXCLUDED.churn_probability,
    ...;
```

`ON CONFLICT` is PostgreSQL's native upsert syntax. It's concise, readable, and atomic. The `EXCLUDED` table refers to the row that *would have been inserted* - so `EXCLUDED.churn_probability` means "the value from the attempted insert."

### SQL Server (T-SQL): `MERGE`

SQL Server doesn't have `ON CONFLICT`. Its upsert mechanism is the `MERGE` statement:

```sql
MERGE customer_churn_scores AS target
USING (VALUES (%s, %s, ...)) AS source (customer_id, churn_probability, ...)
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET churn_probability = source.churn_probability, ...
WHEN NOT MATCHED THEN
    INSERT (customer_id, churn_probability, ...) VALUES (source.customer_id, ...);
```

`MERGE` is more verbose but also more expressive - you can add `WHEN NOT MATCHED BY SOURCE THEN DELETE` to delete rows from target that no longer exist in source. It's ANSI SQL standard (unlike `ON CONFLICT` which is Postgres-specific).

### Why pymssql and Not pyodbc?

`pymssql` is a pure-Python driver for SQL Server that doesn't require system-level ODBC drivers. `pyodbc` (what dbt uses via the adapter) requires the `unixodbc` system library and Microsoft's ODBC driver binary - the chain of dependencies documented in Chapter 9.

For the Airflow Python tasks running inside the Docker container, `pymssql` is simpler to install. For dbt running on the host machine, `pyodbc` was required because that's what `dbt-sqlserver` uses internally.

> [!TIP]
> **Interview Answer**: Postgres uses `INSERT ... ON CONFLICT DO UPDATE` for upserts - concise, Postgres-specific syntax. SQL Server uses `MERGE` - more verbose, but ANSI standard and more expressive (supports DELETE of unmatched target rows too). When porting to Azure SQL, every upsert had to be rewritten from `ON CONFLICT` to `MERGE`. The underlying logic is identical; it's purely a SQL dialect difference between the two databases.

---

## 12.6 The MERGE Optimization - Why 27 Minutes Became 10 Seconds

This is Block 4's core performance story. The code lives in [`reverse_etl/push_churn_scores.py`](../reverse_etl/push_churn_scores.py).

### The Original Problem

The first implementation looped over every record and executed a separate MERGE per row:

```python
# The slow version (conceptual - not the actual code)
for record in records:
    cursor.execute(merge_sql, record)
conn.commit()
```

5,042 records × 1 MERGE each = **5,042 individual database round-trips**.

The Azure SQL database is in `westus3` (USA). The compute runs in India. Each round-trip over the WAN costs approximately 200-250ms of network latency - just for the packet to travel and return, before any query execution happens.

**5,042 round-trips × ~200ms per round-trip = ~1,000 seconds = ~17 minutes.** Add query parsing, lock acquisition, and actual write I/O, and you get to 27 minutes.

There's also a secondary problem: with no `conn.commit()` between rows (committing only at the end), the database table was locked for the entire 27 minutes. Any concurrent read would see an empty table (or the previous run's data), not the in-progress write. This made the table invisible to dashboards and queries during the entire load.

### The Fix - Batched Multi-Row MERGE

The actual code in [`push_churn_scores.py`](../reverse_etl/push_churn_scores.py#L107-L160):

```python
batch_size = 100
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]

    # One MERGE statement for 100 rows simultaneously
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(batch))

    sql = f"""
    MERGE customer_churn_scores AS target
    USING (VALUES {placeholders}) AS source (customer_id, ...)
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE ...
    WHEN NOT MATCHED THEN INSERT ...;
    """

    params = [val for r in batch for val in r]
    cursor.execute(sql, params)
    conn.commit()  # commit after each batch - table visible incrementally
```

5,042 records ÷ 100 per batch = **51 MERGE statements** (round-trips).

**51 round-trips × ~200ms = ~10 seconds.** A 160x speedup.

### Why Batching Helps - The Deep Explanation

Three mechanisms work together:

**1. Network round-trip elimination**: Each round-trip costs 200ms regardless of how much data is in the query. Sending 100 rows in one query costs the same 200ms as sending 1 row - the bottleneck is latency, not bandwidth. Batching reduces round-trips from 5,042 to 51.

**2. Database execution plan reuse**: For every `cursor.execute()` call, the database server parses the SQL, generates an execution plan (decides whether to use an index, how to join the USING clause, etc.), and then executes. Parsing and planning 100-row batches 51 times is far less CPU work than parsing 1-row batches 5,042 times - the query structure is identical within a batch.

**3. Transaction granularity**: Committing every 100 rows (instead of every 1 row or all at once at the end) means:
   - The table is visible and partially updated throughout the load (not locked for 27 minutes)
   - If the job fails at batch 30, only batches 1-29 need to be rerun (not all 5,042 records)
   - Lock contention with concurrent readers is minimized

### Why 100, Not 1,000?

Honest answer from the project: empirical testing. Larger batches approach SQL Server's query parameter limits (there are limits on the number of parameters in a single statement). They also increase the risk of hitting lock contention under concurrent writes. 100 was a pragmatic choice that avoided these limits while achieving dramatic speedup.

> [!TIP]
> **Interview Answer**: The original loop executed one MERGE per record - 5,042 database round-trips over a WAN connection with 200ms latency. That's 1,000+ seconds of latency alone, plus query parsing overhead. Batching 100 records per MERGE reduces round-trips to 51. The speedup is almost entirely latency elimination: each round-trip costs 200ms regardless of payload size, so reducing trips from 5,042 to 51 cuts 99% of the wait time. Committing per-batch instead of at the end also means the table is readable throughout the load - no 27-minute lock.

---

## 12.7 Credentials via `.env` - Why It Works and Why It's Not Production

### How It Works in This Project

Credentials for Azure SQL, ADLS Gen2, and other services live in [`.env`](../.env) at the project root:

```
AZURE_STORAGE_KEY=<storage-account-access-key>
AZURE_SQL_SERVER=sql-openlake-abhijith.database.windows.net
AZURE_SQL_USER=sqladmin
AZURE_SQL_PASSWORD=SuperSecretPassword123!
```

The [`.gitignore`](../.gitignore) includes `.env`, so these values never appear in version control.

Docker Compose reads `.env` automatically and injects values as environment variables into containers. Python code reads them with `os.environ.get("AZURE_STORAGE_KEY")`.

The Spark session in [`push_churn_scores.py`](../reverse_etl/push_churn_scores.py#L23-L25):
```python
.config(
    "fs.azure.account.key.stopenlakeabhijith.dfs.core.windows.net",
    os.environ.get("AZURE_STORAGE_KEY"),
)
```

### Why It's Not Production-Ready

A `.env` file has several problems at scale:

1. **Plaintext on disk** - if the developer's machine is compromised, all credentials are immediately exposed.
2. **No access control** - any process running on the machine can read the file. There's no "this container is allowed to read the DB password but not the storage key."
3. **No rotation** - when credentials change (e.g., storage key rotation as a security practice), you manually update the file and restart containers.
4. **No audit trail** - you can't tell who accessed which credential, when.

### The Production Alternative: Azure Key Vault

Azure Key Vault is a managed secrets store:

```
1. Store secrets in Key Vault (done once by an admin):
   az keyvault secret set --vault-name kv-openlake --name AZURE-SQL-PASSWORD --value "..."

2. Grant the Airflow service identity read access to Key Vault (not the secret itself directly)

3. At runtime, the application fetches the secret via Key Vault API:
   credential = DefaultAzureCredential()
   client = SecretClient(vault_url="https://kv-openlake.vault.azure.net/", credential=credential)
   password = client.get_secret("AZURE-SQL-PASSWORD").value
```

Key Vault gives you:
- **Encryption at rest and in transit** - secrets never stored in plaintext
- **Access policies** - Airflow can read the DB password, but not the storage key; fine-grained
- **Audit logs** - every secret access is logged
- **Automatic rotation** - Key Vault can rotate storage account keys on a schedule

Airflow has a built-in secrets backend that integrates with Azure Key Vault - connection credentials are fetched at DAG runtime from Key Vault, no environment variable files involved.

> [!TIP]
> **Interview Answer**: `.env` is fine for local development - credentials stay off version control and the values are throwaway dev secrets. It breaks for production because credentials are plaintext on disk with no access control, rotation mechanism, or audit trail. The production pattern is Azure Key Vault: secrets are encrypted, access is per-identity (not per-machine), every access is logged, and rotation is automated. Airflow has a native Key Vault secrets backend so DAGs can fetch credentials at runtime without any plaintext files.

---

## 12.8 Block 5 - Track A vs. Track B: Defending the Comparison

When asked to defend the whole architecture, the key is explaining *why* you'd use each component - not just listing names.

### The Comparison Table

| Layer | Track A (Local) | Track B (Azure) | Why the Difference |
|---|---|---|---|
| Object Storage | MinIO | ADLS Gen2 | MinIO is S3-compatible for local dev; ADLS Gen2 adds HNS + native Azure integration |
| Batch Orchestration | Apache Airflow | Azure Data Factory | Airflow = code-first, self-managed; ADF = managed service, GUI-based |
| Streaming Broker | Redpanda | Azure Event Hubs | Redpanda = Kafka-API for local; Event Hubs = managed Kafka service at scale |
| Compute Engine | Apache Spark (local Docker) | Synapse Spark / Databricks | Local Spark = dev; Databricks = managed autoscaling production compute |
| Transformation | dbt-core (local Postgres target) | dbt-core (Azure SQL target) | Same dbt, different adapter/target |
| IaC | N/A (Docker Compose) | Terraform | Local stack via Compose; cloud stack via Terraform |
| CI | GitHub Actions (partial) | GitHub Actions | Same pipeline, not yet fully working |

### The Core Conceptual Defense

**Why not go straight to Track B?** Building Track A first let me iterate fast: spinning up MinIO is one `docker compose up` command versus provisioning cloud resources (takes minutes, costs money, requires auth). When the Silver transformation had a bug, I could debug it locally in seconds. Moving to cloud before the logic was proven would have made debugging 10x slower.

**Why Track B at all?** Track A can't scale past one machine. When data volume grows, you need elastic compute (Databricks autoscaling), managed storage with durability guarantees (ADLS Gen2 with LRS/GRS), and managed orchestration (ADF). Track B also mirrors real enterprise setups - cloud data stacks are the default in industry.

**Why Delta Lake on both tracks?** Table format portability - the exact same `_delta_log` structure, the same MERGE/time-travel semantics, on both MinIO and ADLS Gen2. The PySpark code barely changes (swap the path prefix and connector). This was deliberate: prove the logic locally, then port with minimal changes.

---

## 12.9 GitHub Actions CI - Talking About Unfinished Work

The [`ci.yml`](../.github/workflows/ci.yml) file exists and defines a pipeline, but it has a known issue: the `flake8` linting step fails because the codebase accumulated some style violations during rapid development.

### What It Does (and Would Do if Passing)

```yaml
on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

jobs:
  lint-and-test:
    steps:
      - uses: actions/checkout@v4
      - name: Code Formatting Check (Black)
        run: black --check . --exclude venv
      - name: Code Linting (Flake8)
        run: flake8 . --exclude venv --max-line-length=120
      - name: Run FastAPI Unit Tests
        run: pytest ml/
```

Every push to `main` triggers:
1. **Black formatting check** - ensures code follows a consistent style (Black is opinionated and non-negotiable; you either conform or you fail)
2. **Flake8 linting** - catches unused imports, undefined variables, PEP8 violations
3. **FastAPI unit tests** - runs `pytest` against the ML API layer

### How to Talk About It in an Interview

Never hide unfinished work - interviewers respect honesty and initiative far more than a polished lie.

**The honest framing**: "The CI pipeline is defined and structurally correct - it triggers on push, checks code style, and runs tests. The current blocker is that the existing codebase has some PEP8 violations from rapid iterative development that cause the flake8 step to fail. The fix is either reformatting the code with `black` to auto-correct, or selectively suppressing violations with `# noqa` annotations for lines where the violation is intentional (like long delta table paths). That's a cleanup task I haven't done yet, not a design gap."

**What you'd add if continuing**: 
- Terraform validation step (`terraform validate`) in CI to catch HCL syntax errors
- Integration tests that spin up a local Delta Lake fixture and run a sample transformation
- Deployment step: if tests pass on `main`, automatically apply `terraform apply` to a staging environment (with remote state + Service Principal auth)

> [!TIP]
> **Interview Answer on unfinished work**: "The CI file exists and the workflow structure is correct - it runs on push to main, checks formatting with Black, lints with flake8, and runs pytest on the ML layer. It doesn't pass yet because the codebase has accumulated some flake8 violations during rapid development. The fix is a code formatting pass, not an architectural change. The next additions I'd make are Terraform validation in CI and eventually automated apply to a staging environment using a Service Principal, which ties back to the IaC work."

---

## Summary: What Chapter 12 Answers

| Question | Short Answer |
|---|---|
| Why couldn't Databricks work on free tier? | Hard 4 vCPU quota; smallest Databricks cluster needs 8+ (driver + worker). Math is irreconcilable without upgrading to Pay-As-You-Go |
| Why is local-Spark-to-Azure-storage valid? | Compute/storage decoupling: Spark only needs HTTPS access to storage. Both local and cloud compute talk to ADLS Gen2 the same way |
| What's the production tradeoff of hybrid compute? | WAN latency per read/write; egress costs; not viable for large-scale production (colocate compute+storage in same region) |
| What is `abfss://`? | Azure Blob FileSystem Secure URI scheme - tells Spark to load the `hadoop-azure` connector and use Azure's ADLS Gen2 REST API |
| What changed between MinIO and ADLS Gen2? | Connector package, path prefix, auth config. Business logic: zero changes |
| What does dbt do? | Writes SQL SELECT statements as models, dbt compiles them into CREATE TABLE/VIEW on the target DB, manages dependencies, tests, docs |
| Why MERGE instead of ON CONFLICT? | Different SQL dialects: PostgreSQL uses ON CONFLICT; SQL Server (Azure SQL) uses MERGE. Same semantics, different syntax |
| Why was MERGE slow? | 5,042 round-trips × ~200ms WAN latency = ~17 minutes network overhead alone |
| Why does batching fix it? | Reduces 5,042 round-trips to 51. Latency is per-trip, not per-row - sending 100 rows costs the same network time as 1 |
| Why `.env` vs Key Vault? | `.env` = plaintext, dev-only, no audit trail. Key Vault = encrypted, audited, access-controlled, auto-rotating |
| GitHub Actions CI status? | Structurally correct, fails on flake8 violations from rapid development - fix is a formatting pass, not a redesign |

---

*Back: [Chapter 11 - Infrastructure as Code: Terraform & Azure](./ch11_terraform_iac.md)*
*Next: [Chapter 10 - Cloud Orchestration](./ch10_cloud_orchestration.md)*
