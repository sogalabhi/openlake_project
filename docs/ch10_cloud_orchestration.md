# Chapter 10: Cloud Orchestration, Hybrid Spark Compute & Reverse ETL Optimizations

In this final phase, we moved from validating independent cloud resources to orchestrating the entire 5-stage retail data pipeline. Running a hybrid compute pipeline (local Spark/Airflow compute talking to remote Azure storage and databases) exposed critical real-world bottlenecks: network latency over WAN, Docker-to-host permission layers, and sequential database writes. 

This chapter documents those challenges, how we diagnosed them, and the architectural optimizations we implemented to speed up execution by over 160x.

---

## 1. Cloud Ingestion Optimization: Local Uploads vs. Server-Side Copy

### The Problem: Network Write Timeouts & SDK Mismatch
Initially, we attempted to upload the raw 95MB `online_retail_II.csv` file from the local container's mounted data folder to Azure Blob Storage on every daily run. Over local/residential network connections, this frequently crashed with:
`azure.core.exceptions.ServiceResponseError: ('Connection aborted.', TimeoutError('The write operation timed out'))`

In attempting to log progress, we encountered a `TypeError` by passing `progress_callback=progress_callback` directly to the SDK's `upload_blob` method, which is unsupported in modern versions of the `azure-storage-blob` library. Furthermore, trying to override parameters on `BlobServiceClient.get_blob_client()` threw:
`TypeError: get_blob_client() got an unexpected keyword argument 'max_block_size'`

### The Diagnostics
*   **Default Block Size:** By default, the Azure Python SDK uploads large blobs in **4 MB chunks (blocks)**. On unstable or low-bandwidth connections, a 4MB PUT request takes too long, causing the Azure gateway to abort the socket.
*   **BlobClient Constructors:** Configuration arguments like `max_block_size` and `max_single_put_size` belong directly to the `BlobClient` class constructor, not the service client's factory method (`get_blob_client()`).

### The Solution: Server-Side Data Staging
Rather than uploading a 95MB file on every single DAG run over consumer internet, we staged the raw source CSV once in a permanent cloud landing folder:
*   **Staged Path:** `lakehouse/landing/online_retail_II.csv`

We then refactored the Airflow DAG to instruct Azure to copy the file internally from the landing folder to the target daily partition folder (e.g. `bronze/2026-07-13/`) using the **`start_copy_from_url`** API:

```python
# Create the client pointing to the daily partition target
blob_client = BlobClient(
    account_url="https://stopenlakeabhijith.blob.core.windows.net", 
    container_name="lakehouse", 
    blob_name=f"bronze/{execution_date}/online_retail_II.csv",
    credential=os.environ.get("AZURE_STORAGE_KEY")
)

source_url = "https://stopenlakeabhijith.blob.core.windows.net/lakehouse/landing/online_retail_II.csv"

# Trigger instant server-to-server copy
blob_client.start_copy_from_url(source_url)
```

> [!TIP]
> **Interview Answer**: In a production data pipeline, compute engines should never pull data from local developer laptops or rely on local internet uploads during scheduled execution. Raw data must land directly in cloud object storage (S3/ADLS). Pipelines should copy or register metadata references server-to-server. This took our copy step from **minutes to under 0.5 seconds** and eliminated connection timeouts entirely.

---

## 2. Hybrid Spark Compute: Resource Starvation

### The Problem: Stuck Jobs in Standalone Master
When triggering the pipeline, the second task `transform_bronze_to_silver` submitted its Spark job, but subsequent Spark tasks hung indefinitely with the following warnings:
`WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources`

### The Diagnostics
In `docker-compose.yml`, the local Spark worker is allocated **2 cores and 2 GB of RAM**.
When multiple DAG runs are triggered or run concurrently in Airflow:
1.  The first Spark application (`transform_bronze_to_silver`) registers with the Spark Master and claims the worker's entire capacity (2 cores and 2GB RAM).
2.  The next Spark job (`train_churn_model`) registers, but because the worker has zero available cores and memory, the Spark Master queues the job indefinitely.
3.  The task hangs until the Airflow task execution limit is breached.

### The Solution
We cleared the stale applications by restarting the Spark containers:
```bash
docker compose restart spark-master spark-worker
```
And to prevent local compute starvation, we limited Spark resource requests directly in the `SparkSubmitOperator` DAG configurations, allowing tasks to share the local compute resources:
```python
conf={
    "spark.master": "spark://spark-master:7077",
    "spark.executor.memory": "1g",   # Allocate only 1GB memory
    "spark.executor.cores": "1",    # Allocate only 1 core
    "spark.cores.max": "1"          # Cap total app allocation to 1 core
}
```

---

## 3. Local ML Model Training: Docker Host Permission Denied

### The Problem: Permission Denied on `model.pkl`
During the ML model training stage (`train_churn_model`), the task successfully pulled clean Silver data from Azure, split features, and trained a Random Forest model. However, it failed on the final write step:
`PermissionError: [Errno 13] Permission denied: '/opt/airflow/scripts/model.pkl'`

### The Diagnostics
*   The Airflow container is configured to run under a specific non-root user account (UID `50000` / GID `0`).
*   The host machine's `./scripts` folder was mapped into the container as a volume mount (`./scripts:/opt/airflow/scripts`).
*   Because the host folder or the existing `model.pkl` file was owned by the host developer account (or root), the container's user had no permissions to overwrite the file.

### The Solution
We changed the permissions of the local `scripts` folder on the host machine:
```bash
chmod -R 777 scripts/
# Or simply remove the old file so the container can instantiate a new one
rm scripts/model.pkl
```

---

## 4. Reverse ETL Optimization: 160x Speedup with Batched SQL Merges

### The Problem: The 27-Minute database load
Once the model finished training, the reverse ETL task (`push_churn_scores`) loaded the scored features and predictions into the Azure SQL CRM database. The process was painfully slow, taking **27 minutes and 15 seconds** to load 5,042 records.

### The Diagnostics
The original script looped over all rows and executed a single `MERGE` statement for each record sequentially:
```python
# Slow sequential loop
for record in records:
    cursor.execute(sql, record)
conn.commit()
```
Because the database is in Azure (USA) and the compute runs locally (India), each query suffered from WAN network round-trip latency (~200ms to 250ms). 5,000 round-trips resulted in 1,000+ seconds of wait time. Furthermore, because `conn.commit()` was called only at the end, the target table was locked, and the table read as empty (`0` rows) for the entire 27 minutes.

### The Solution: Multi-Row Batched Upserts
We optimized the loading script to group the records into **batches of 100** and construct a single SQL `MERGE` query with a multi-row `VALUES` constructor:

```python
batch_size = 100
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    
    # Create placeholders: (%s, %s, ...), (%s, %s, ...)
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(batch))
    
    sql = f"""
    MERGE customer_churn_scores AS target
    USING (VALUES {placeholders}) AS source (customer_id, churn_probability, churn_label, recency_days, frequency, monetary, scored_at)
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET churn_probability = source.churn_probability, ...
    WHEN NOT MATCHED THEN
        INSERT (...) VALUES (...);
    """
    params = [val for r in batch for val in r]
    cursor.execute(sql, params)
    conn.commit()
```
By reducing the database round-trips from **5,042 down to just 51**, the execution time was slashed from **27 minutes to under 10 seconds**!

---

## 5. Pipeline Idempotency on Reruns

| Stage | Action on Rerun | Idempotent? | Why? |
|---|---|---|---|
| **Bronze** | `overwrite=True` copy | Yes | Replaces the file in the specific daily partition path. |
| **Silver** | `.mode("append")` Delta write | No | Appends duplicate rows to the Delta table. In production, a Delta `MERGE` is required. |
| **ML Model** | `open(..., "wb")` overwrite | Yes | Replaces the existing `model.pkl` binary with the new classifier. |
| **Gold SQL** | Database `MERGE` statement | Yes | Matches on `customer_id` and updates existing rows or inserts new ones. |

---

## 6. Data Quality Guardrails: Great Expectations 1.x Fluent API

### The Problem: Legacy API Deprecation & Raw Data Anomalies
In early planning stages, we targetted the legacy `great_expectations.dataset.PandasDataset` wrapper. Running this inside modern Great Expectations 1.x environments threw a fatal initialization error:
`AttributeError: module 'great_expectations' has no attribute 'dataset'`

Furthermore, our initial price validation expectation (`expect_column_values_to_be_between(min_value=0.0)`) crashed when run on the raw UCI Online Retail II dataset, identifying negative prices.

### The Diagnostics
*   **Fluent API Shift:** Great Expectations 1.x completely deprecated the legacy Dataset wrappers, moving instead to an **ephemeral data context, fluent data sources, and Validation Definitions**.
*   **Retail Anomalies:** Real-world retail datasets contain refund, reversal, or debt adjustments that show up as transactions with negative unit prices. A hard pass/fail quality gate on `Price >= 0.0` would cause legitimate daily ingestion batches to fail.

### The Solution: Fluent API Migration & Tolerated Anomalies
We rewrote the quality pipeline ([validate_landing_data.py](file:///home/abhijith/coding/openlake_project/quality/validate_landing_data.py)) utilizing the GX 1.x Fluent context:

```python
# Modern GX 1.x Fluent Setup
context = gx.get_context(mode="ephemeral")
ds = context.data_sources.add_pandas("raw_retail_datasource")
asset = ds.add_dataframe_asset("retail_csv_asset")
batch_def = asset.add_batch_definition_whole_dataframe("raw_batch_definition")

# Register validations
suite = context.suites.add(gx.ExpectationSuite(name="retail_raw_expectations"))
suite.add_expectation(ExpectColumnValuesToNotBeNull(column="Invoice"))
suite.add_expectation(ExpectColumnValuesToBeBetween(column="Price", min_value=0.0, mostly=0.999))

# Execute validation definition
validation = context.validation_definitions.add(
    gx.ValidationDefinition(name="retail_landing_validation", data=batch_def, suite=suite)
)
result = validation.run(batch_parameters={"dataframe": df_raw})
```

To handle transaction adjustments, we set the Price expectation to allow up to `0.1%` negative exceptions using the `mostly=0.999` density threshold, while leaving our PySpark Silver pipeline to filter out or flag these anomalies cleanly.

> [!TIP]
> **Answer - How do you ensure data quality?**
> We enforce quality checks at two distinct boundaries: **Ingestion boundary** and **Conformed Analytical boundary**.
> 
> 1. **Ingestion Validation Gate (Great Expectations):** Before raw source files are copied into the daily Bronze directory, we run a data quality script ([validate_landing_data.py](file:///home/abhijith/coding/openlake_project/quality/validate_landing_data.py)) using Great Expectations. We assert that primary keys (`Invoice`, `StockCode`) are not null, enforce strict type checks, verify that `Price` is non-negative on at least `99.9%` of rows (using `mostly=0.999` to allow for transactional adjustments), and confirm customer presence rates. If validation fails, the ingestion pipeline aborts before corrupting Bronze.
> 2. **Conformed Analytical Validation (dbt Tests):** Once data is moved into our Gold dimension and fact schemas in Azure SQL Database, we execute `dbt test` to verify uniqueness of surrogate keys, check referential integrity (foreign keys properly link to parent dimensions), and ensure column boundaries (no nulls in fact tables).

---

## 7. Unit Testing FastAPI: Dynamic Open Mocks & Lifespan Contexts

### The Problem: Mocks Interfering with Internal Package Imports
During local unit testing of the FastAPI endpoints using `TestClient`, we encountered a `TypeError` and circular dependency crashes inside `pandas`:
`TypeError: _patch.__call__() takes 2 positional arguments but 3 were given`

Additionally, attempts to score customer churn on the `/predict` endpoint returned `503 Service Unavailable` with `Detail: Model is not loaded.`.

### The Diagnostics
*   **Global Mock Collisions:** The testing suite mocked `builtins.open` globally to redirect model file reads. However, when Python imported `pandas`, it invoked `builtins.open` to import the timezone database (`/usr/share/zoneinfo/UTC`). The global mock hijacked this call, resulting in a signature crash.
*   **Lifespan Hook Bypass:** Starlette/FastAPI loads models within the async `lifespan` hook. Instantiating a `TestClient(app)` directly bypasses this setup hook, meaning the model is never deserialized, causing it to return `503`.

### The Solution: Routing Mocks Dynamically
We updated [ml/test_serve.py](file:///home/abhijith/coding/openlake_project/ml/test_serve.py) to implement a dynamic open routing wrapper:

```python
original_open = builtins.open

def mock_open_fn(file, mode='r', *args, **kwargs):
    # Only redirect model file reads; let zoneinfo/tz files open normally
    if "model.pkl" in str(file):
        return MagicMock()
    return original_open(file, mode, *args, **kwargs)
```

And wrapped all endpoint test requests in `with` context blocks to trigger the FastAPI lifespan hook:
```python
def test_predict_endpoint_success(mock_app):
    with TestClient(mock_app) as client:
        # FastAPI lifespan completes successfully before running requests
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
```

---

## 8. CI/CD Pipeline: Auto-Formatting & Linting Enforcement

### The Problem: Pipeline Style Violations
When we pushed our initial CI workflow, GitHub Actions failed because the code accumulated formatting differences and style violations (unused variables, trailing whitespace, and line lengths exceeding PEP8 guidelines).

### The Solution: Lint Cleanup
1.  **Formatter:** Executed `python -m black . --exclude venv` to unify indentation and bracket styling.
2.  **Unused Variables:** Removed the unused `ti = context["ti"]` context mapping in `dags/bronze_ingestion.py`.
3.  **Line Breaks:** Split long package dependency list strings inside `dags/streaming_pipeline.py` and wrapped long SQL query paths inside `reverse_etl/push_churn_scores.py` and `scripts/transform_bronze_to_silver.py`.

---

## 9. Version Control: Git History Pruning & Large Binaries

### The Problem: GitHub 100MB Pre-Receive Reject
Pushing our pipeline modifications failed with a remote Git rejection:
`File terraform-provider-azurerm_v3.117.1_x5 is 222.83 MB; this exceeds GitHub's file size limit of 100.00 MB`

### The Diagnostics
Terraform download caches (located inside `infra/.terraform/`) were not originally ignored by `.gitignore`. Because they were committed in older local commits, simply running `git rm` or committing a deletion does not clean the history. Git still packs the binary to sync the older commits.

### The Solution: Pruning Git Commit History
We ran Git's history rewriter to surgically strip the `.terraform/` folder from all local commits:
```bash
git filter-branch --force --index-filter 'git rm -r --cached --ignore-unmatch infra/.terraform' --prune-empty --tag-name-filter cat -- origin/master..master
```
This cleaned the commit pack files without losing the commit messages and authors. We then updated `.gitignore` to permanently exclude `.terraform/`, `venv/`, and `*.pkl` binaries.

---

## 10. Repository Presentation: Mermaid Graph Quoting

### The Problem: Unable to Render Rich Display on GitHub Markdown
Our architecture diagram failed to render in the GitHub UI, displaying a parsing error:
`Expecting 'SEMI', 'NEWLINE', ... got 'PS'`

### The Diagnostics
Mermaid's syntax parser treats special characters (like `&` and parenthesis `()`) as reserved tokens. Subgraph labels like `Processing & Storage (Delta Medallion)` threw parsing exceptions.

### The Solution
We updated `Readme.md` to wrap all subgraph names containing special characters in double quotes:
`subgraph "Processing & Storage (Delta Medallion)"`

---

