# Troubleshooting Report: OpenLake Project Issues & Resolutions

This document provides a detailed log of the problems encountered and resolved during the setup of the OpenLake local machine learning/serving pipeline and the Azure Terraform infrastructure.

---

## Part 1: Local Machine Learning & Serving Pipeline

### 1. NumPy Version Conflict / Binary Incompatibility
* **Problem**: Rebuilding the Apache Airflow Docker image to install `scikit-learn` caused `pip` to install the latest NumPy (2.x) by default. However, the pre-installed versions of `pandas` and `pyarrow` in the base Airflow image were compiled against NumPy 1.x. This mismatch caused a runtime crash when starting the scoring script: `ValueError: numpy.dtype size changed`.
* **Root Cause**: NumPy 2.0 introduced breaking changes to the C-API, causing binary incompatibility with packages compiled against older versions.
* **Resolution**: Pinned `"numpy<2"` in the `pip install` step in the `Dockerfile`:
  ```dockerfile
  RUN pip install --no-cache-dir apache-airflow==2.9.0 apache-airflow-providers-apache-spark pyspark==3.5.0 scikit-learn "numpy<2"
  ```

### 2. Psycopg2 Type Adaptation Failures
* **Problem**: When constructing record tuples from the PySpark/Pandas scoring DataFrame, executing the upsert SQL query threw the exception: `can't adapt type 'numpy.int64'`.
* **Root Cause**: `psycopg2` cannot serialize NumPy-specific types (`numpy.int64`, `numpy.float64`) directly to Postgres; they must be standard Python native types.
* **Resolution**: When building the records list inside `push_churn_scores.py`, we explicitly cast NumPy scalars to standard Python primitives during row iteration:
  ```python
  records = [
      (row.customer_id, float(row.churn_probability), int(row.churn_label), 
       int(row.recency_days), int(row.frequency), float(row.monetary), row.scored_at)
      for row in df_pandas.itertuples()
  ]
  ```

### 3. Missing Volume Mount for Reverse ETL
* **Problem**: The `push_churn_scores.py` script lived in the host's `reverse_etl/` folder, which was not mounted in the Airflow docker containers, preventing Airflow from executing it.
* **Root Cause**: The default `docker-compose.yml` only mounted `./dags`, `./data`, and `./scripts` under the Airflow service template.
* **Resolution**: Added the `reverse_etl` volume mapping to the `x-airflow-common` volumes list:
  ```yaml
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - ./scripts:/opt/airflow/scripts
      - ./reverse_etl:/opt/airflow/reverse_etl
  ```

### 4. Accidental Deletion of DAG Task 3
* **Problem**: While editing `dags/bronze_ingestion.py` to add task 4 and task 5, the definition for `task_3` (`transform_bronze_to_silver`) was accidentally deleted, leading to a DAG parsing error because `task_3` was still used in the pipeline chain.
* **Root Cause**: Overwrote the definition block of `task_3` during string replacement.
* **Resolution**: Restored the `task_3` `SparkSubmitOperator` block and verified the sequential dependency string:
  ```python
  task_1 >> task_2 >> task_3 >> task_4 >> task_5
  ```

---

## Part 2: Azure Terraform Infrastructure

### 5. Azure SQL Server Provisioning Restrictions (`ProvisioningDisabled`)
* **Problem**: Running `terraform apply` to create an Azure SQL Server in `East US` / `East US 2` returned:
  ```
  Status: "ProvisioningDisabled"
  Message: "Provisioning is restricted in this region. Please choose a different region."
  ```
* **Root Cause**: Microsoft limits database provisioning for free-tier and student subscriptions in high-demand regions (such as the Eastern US) to save compute for paying enterprise customers.
* **Resolution**: Changed the default location variable to `westus3` in `variables.tf`, which has open capacity.

### 6. Terraform State Refresh Crash (404 ParentResourceNotFound)
* **Problem**: After changing regions or recreating resource groups, running `terraform plan` or `apply` crashed with:
  ```
  Error: reading SQL Server Blob Connection Policy Server ... unexpected status 404 (404 Not Found) with error: ParentResourceNotFound ...
  ```
* **Root Cause**: The Terraform state file held cache keys of database servers and storage accounts that were deleted on Azure when regions were switched. The Azure provider crashed trying to refresh connection policies for a server that no longer existed.
* **Resolution**: Purged the stale/deleted resources from the local Terraform state file:
  ```bash
  terraform state rm azurerm_mssql_database.sqldb azurerm_mssql_firewall_rule.allow_azure azurerm_mssql_server.sqlserver azurerm_storage_account.datalake
  ```

### 7. Resource Group Deletion Blocked by Unmanaged Resources
* **Problem**: Recreating the resource group threw an error stating that the resource group could not be deleted because it still contained unmanaged resources.
* **Root Cause**: Because we cleared the state of the SQL Server and Storage Account in Step 6, Terraform saw them as "unmanaged" and blocked deletion to protect against data loss.
* **Resolution**: Updated `providers.tf` to configure the `prevent_deletion_if_contains_resources` feature flag to `false`:
  ```hcl
  provider "azurerm" {
    features {
      resource_group {
        prevent_deletion_if_contains_resources = false
      }
    }
  }
  ```

### 8. Storage Account Name Collision
* **Problem**: Applying resources in `westus3` failed with a `ResourceNotFound` error when listing keys for the storage account `stopenlake12345`.
* **Root Cause**: Azure storage account names are globally unique. The placeholder `stopenlake12345` was already registered by someone else under a different subscription, meaning it could not be created under our resource group.
* **Resolution**: Renamed the resources in `main.tf` to use a unique suffix (`abhijith`):
  * Storage Account: `stopenlakeabhijith`
  * SQL Server: `sql-openlake-abhijith`

### 9. Database Tainted Resource Loop
* **Problem**: The creation of `azurerm_mssql_database.sqldb` finished successfully, but Terraform immediately marked the database resource as **tainted** because the subsequent metadata read for `backupLongTermRetentionPolicies` threw a 404.
* **Root Cause**: A known race condition in Azure where metadata endpoints take a few minutes to propagate after a database is created. The provider crashed during key queries and tainted the resource.
* **Resolution**: Untainted the database since it had successfully deployed on Azure:
  ```bash
  terraform untaint azurerm_mssql_database.sqldb
  ```

### 10. Connection Refused / Firewall Block
* **Problem**: Connecting from the local host machine using VS Code SQLTools failed with connection timeout / refused errors.
* **Root Cause**: The default firewall rules only whitelisted internal Azure services (`0.0.0.0`), blocking any outside traffic.
* **Resolution**: Queried the host's public IP address (`49.43.243.42`) and added an explicit firewall rule in `main.tf`:
  ```hcl
  resource "azurerm_mssql_firewall_rule" "allow_local" {
    name             = "AllowLocalIP"
    server_id        = azurerm_mssql_server.sqlserver.id
    start_ip_address = "49.43.243.42"
    end_ip_address   = "49.43.243.42"
  }
  ```

### 11. Orphaned / Unmanaged Azure Resources
* **Problem**: Changing the resource names in `main.tf` and purging their old mappings from the local state left the old placeholder resources (`stopenlake12345` and `sql-openlake-12345`) running active in Azure.
* **Root Cause**: When a resource is removed from the state via `terraform state rm`, Terraform stops tracking it. During subsequent applies with the new names, Terraform does not delete the old resources because it does not know they exist, leaving them orphaned in the resource group.
* **Resolution**: Cleared the stale unmanaged resources directly from Azure using the Azure CLI:
  ```bash
  az sql server delete --name sql-openlake-12345 --resource-group rg-openlake-dev --yes
  az storage account delete --name stopenlake12345 --resource-group rg-openlake-dev --yes
  ```

### 12. Azure Databricks Workspace SKU Availability
* **Problem**: Attempting to provision a Databricks Workspace in `westus3` failed with the error `DatabricksStandardSkuNotSupported`.
* **Root Cause**: Azure policy rules for newer subscription classes (like free trial accounts) restrict the deployment of the `"standard"` SKU for Databricks workspaces in certain regions, requiring either a different region or an upgrade to the `"premium"` SKU.
* **Resolution**: Updated the workspace configuration to use the `"premium"` SKU in `main.tf`:
  ```hcl
  sku = "premium"
  ```

### 13. Resource Group Region Mismatch Failure
* **Problem**: Changing the location region to `eastus` caused SQL Server creation to fail with `ProvisioningDisabled` again, and Databricks creation to fail with `ApplianceProvisioningFailed` stating: `"Invalid resource group location 'eastus'. The Resource group already exists in location 'westus3'."`
* **Root Cause**: Azure locks a resource group's location once created. When we changed the variable location, Terraform destroyed the resource group and recreated it in `eastus`. However, Azure's internal resource providers still had the old group location cached as `westus3`. Additionally, the SQL Server provisioning was restricted in the `eastus` region.
* **Resolution**: Reverted the region back to `westus3` (where SQL Server creation is allowed and the Databricks `"premium"` SKU is supported), and deleted the mismatched resource group entirely via the Azure CLI to clean up the backend cache:
  ```bash
  az group delete --name rg-openlake-dev --yes
  ```

### 14. Tainted Resources Refresh Loop
* **Problem**: Subsequent `terraform apply` runs crashed during state refresh with `ParentResourceNotFound` or `ResourceNotFound` on `blobServices` or `connectionPolicies` subresources.
* **Root Cause**: When previous applies failed or were interrupted (such as via `Ctrl+C`), the SQL Server and Storage Account were left in a "tainted" state in the Terraform state file. Because the resources actually existed on Azure but were tainted, the provider's refresh pass attempted to lookup incomplete metadata properties, causing a crash.
* **Resolution**: Untainted the SQL Server and Storage Account in the local state, allowing Terraform to recognize them as healthy and sync successfully:
  ```bash
  terraform untaint azurerm_mssql_server.sqlserver
  # AND
  terraform untaint azurerm_storage_account.datalake
  ```

### 15. Storage Account Data Plane 403 / 404 Errors
* **Problem**: Provisioning the ADLS Gen2 filesystem `lakehouse` failed with a `403 Server failed to authenticate the request` signature error.
* **Root Cause**: Two contributing factors:
  1. The active session in the Azure CLI was expired (`AADSTS50132`), causing the credential signatures to be rejected.
  2. Standard subscription roles (like Owner/Contributor) do not grant direct Data Plane read/write access (e.g. `dfs.core.windows.net`) to Storage accounts by default, requiring specific RBAC roles for secure client queries.
* **Resolution**: Re-authenticated the Azure session (`az login`) and explicitly assigned the **Storage Blob Data Contributor** role to the active user account principal (`4bb82cb3-8d33-4737-a2aa-bf5a3f1c8361`) at the resource group level:
  ```bash
  az role assignment create --assignee 4bb82cb3-8d33-4737-a2aa-bf5a3f1c8361 --role "Storage Blob Data Contributor" --scope /subscriptions/bea46914-4ae4-4614-9ed2-ed65026d0217/resourceGroups/rg-openlake-dev
  ```

### 16. Azure Databricks VM / Core Quota Deadlock
* **Problem**: Creating a compute cluster inside the Azure Databricks workspace failed with:
  > `This node type is not available on the current account subscription.`
  Or threw quota limits warnings for standard 4-core sizes (like `Standard_D4s_v3` or `Standard_DS3_v2`).
* **Root Cause**: A subscription-level constraint on Azure Free Trial/Student accounts:
  1. The subscription limits total regional vCPUs to **4**.
  2. Microsoft filters the Databricks service to disable lower-core VM families (like 2-core or 4-core Dv3/Dv4 types) and only permits a few legacy/large VM families (like `Standard_DS15_v2` requiring 20 cores).
  3. This creates a deadlock: the only VM sizes Databricks is allowed to deploy require far more cores than the subscription's total regional limit allows.
* **Resolution**: Pivoted from running compute inside Azure Databricks to running Spark compute locally inside the Apache Airflow Docker container stack, while keeping the data storage fully cloud-native. The local PySpark jobs were configured to read and write directly to the live Azure ADLS Gen2 storage account (`stopenlakeabhijith`) using the `abfss://` protocol and the `AZURE_STORAGE_KEY` access credential, and the Reverse ETL was updated to target the Azure SQL database using `pymssql` and the `MERGE` SQL syntax.

---

## Part 3: Medallion Optimizations, Test Automation & CI/CD Guardrails (Phase 5.4)

### 17. In-Cloud Storage Ingestion Timeout & SDK Mismatch
* **Problem**: Attempting to upload the raw 95MB transaction CSV from the local container to ADLS Gen2 failed with `ServiceResponseError: ('Connection aborted.', TimeoutError('The write operation timed out'))`. Additionally, passing `progress_callback` or overriding `max_block_size` in `get_blob_client()` threw `TypeError`.
* **Root Cause**: Residential internet upload bandwidth was too slow to push 4MB blocks, causing Azure's gateway to drop the connection. Furthermore, modern Azure Python SDK versions do not support `progress_callback` on `upload_blob`, and `max_block_size` belongs to the `BlobClient` constructor directly, not the client factory.
* **Resolution**: Staged the raw transaction CSV file once in a permanent landing zone (`lakehouse/landing/online_retail_II.csv`) and refactored the batch ingestion DAG to trigger Azure's server-side copy API (`start_copy_from_url`) to copy the file to the target daily partition folder (e.g. `bronze/2026-07-13/`). This resolved the transfer in **less than 0.5 seconds** and eliminated upload timeouts entirely.

### 18. Spark Operator Resource Starvation
* **Problem**: Pipeline runs stalled on the second Spark task (`transform_bronze_to_silver`), and subsequent Spark tasks hung with: `WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI...`.
* **Root Cause**: The local Spark standalone worker container is allocated 2 cores and 2GB RAM. Running concurrent Spark application instances (due to multiple manual DAG runs or consecutive tasks) causes the first job to lock all available executor cores, starving subsequent jobs.
* **Resolution**: Restarted Spark master/worker containers, failed duplicate Airflow runs, and capped application resource allocations (`spark.executor.memory="1g"`, `spark.executor.cores="1"`, `spark.cores.max="1"`) directly in the `SparkSubmitOperator` DAG configurations to allow tasks to share local compute resources.

### 19. Container Host Mount Permission Denials
* **Problem**: The `train_churn_model` Spark job failed to write the trained Random Forest classifier with `PermissionError: [Errno 13] Permission denied: '/opt/airflow/scripts/model.pkl'`.
* **Root Cause**: The Airflow container runs under a non-root user (UID `50000` / GID `0`). Since the host directory (`./scripts`) or the existing `model.pkl` file was created and owned by the host's `root` or main user profile, the container's user was denied write access.
* **Resolution**: Deleted the existing root-owned `model.pkl` file and ran `chmod -R 777 scripts/` on the host to grant global write access to the mount directory.

### 20. Reverse ETL Database Latency (WAN Network Round-Trips)
* **Problem**: Loading customer churn scores into Azure SQL Database (CRM) took **27 minutes and 15 seconds** for 5,042 rows.
* **Root Cause**: The script executed individual, sequential `MERGE` SQL queries for each customer record. Because the database was in `westus3` (USA) and the compute ran locally in India, each statement suffered from ~200ms network round-trip latency. In addition, the target table was locked for the entire duration, showing `0` rows to readers.
* **Resolution**: Grouped records into batches of 100 and rewrote the database write logic to construct a single `MERGE` statement utilizing a multi-row `VALUES` block. This reduced network round-trips from 5,042 down to 51, dropping database write times to **under 10 seconds (160x speedup)** and releasing table locks incrementally.

### 21. FastAPI Mock Open Hijacking / Pandas Import Collision
* **Problem**: Running the FastAPI unit test suite failed during Pandas import with `TypeError: _patch.__call__() takes 2 positional arguments but 3 were given` and circular import `AttributeError` on `_pandas_datetime_CAPI`.
* **Root Cause**: The `pytest` fixture mocked `builtins.open` globally to stub reading the ML model. When `pandas` initialized, it called `builtins.open` to read timezone database files (such as `/usr/share/zoneinfo/UTC`), which matched our mock structure and crashed.
* **Resolution**: Refactored `ml/test_serve.py` to route `open` calls dynamically using a side-effect wrapper function: if the requested filename contains `"model.pkl"`, it returns a mock object; otherwise, it delegates the call to Python's original `builtins.open` function.

### 22. FastAPI 503 Service Unavailable in Unit Tests
* **Problem**: The FastAPI `/predict` test failed with `503 Service Unavailable`.
* **Root Cause**: The ML model was loaded in the async contextmanager `lifespan(app)`. In Starlette/FastAPI, initializing `TestClient(app)` directly does not trigger the lifespan startup event unless the client is used inside a context manager.
* **Resolution**: Wrapped both tests inside the `with TestClient(mock_app) as client:` context block, which triggers the client startup events and successfully loads the model.

### 23. Git Push Blocked by Large Committed Binary (Pre-Receive Hook Decline)
* **Problem**: `git push` failed with a `pre-receive hook declined` error.
* **Root Cause**: A 222MB Terraform provider binary (`terraform-provider-azurerm`) under `infra/.terraform/` was accidentally committed in previous local commits. Even after deleting it, Git preserved the file in the commit history packfiles, which GitHub rejected.
* **Resolution**: Executed a history rewrite using `git filter-branch --force --index-filter 'git rm -r --cached --ignore-unmatch infra/.terraform' --prune-empty --tag-name-filter cat -- origin/master..master` to surgically prune `.terraform` from the local commit history, updated `.gitignore`, and successfully pushed.

### 24. CI Linting & Formatting Failures
* **Problem**: GitHub Actions CI workflow failed the `black --check` and `flake8` lint steps.
* **Root Cause**: Code formatting differences, unused variables (`ti` in `dags/bronze_ingestion.py`), trailing spaces, and too-long string literals in SQL/Airflow operators.
* **Resolution**: Formatted all Python files using `python -m black . --exclude venv` and split the long lines (such as packages lists and T-SQL queries) across lines.

### 25. Mermaid Flowchart Render Error on GitHub UI
* **Problem**: The README architecture diagram failed to render with parsing errors.
* **Root Cause**: Mermaid subgraphs with names containing special characters like `&` and `()` crashed the parser.
* **Resolution**: Wrapped the subgraph names in double quotes: `subgraph "Processing & Storage (Delta Medallion)"`.

### 26. Great Expectations 1.x Fluent API Migration
* **Problem**: Legacy `gx.dataset.PandasDataset` API threw `AttributeError: module 'great_expectations' has no attribute 'dataset'` in GX 1.x.
* **Root Cause**: Great Expectations 1.x deprecated the legacy Dataset API in favor of the Fluent Data Sources API.
* **Resolution**: Rewrote `quality/validate_landing_data.py` using the modern Fluent API: `context = gx.get_context(mode="ephemeral")`, `ds.add_pandas`, `asset.add_batch_definition_whole_dataframe`, `ValidationDefinition`, and `validation.run(batch_parameters={"dataframe": df})`. Added `mostly=0.999` to `Price` checks to tolerate negative transaction adjustments in the raw Kaggle dataset.

