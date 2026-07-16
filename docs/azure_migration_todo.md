# Phase 5: Azure Migration Roadmap & Progress

This checklist tracks progress as we port the OpenLake retail pipeline from local open-source containers (Track A) to native Azure services (Track B).

---

## 📦 Phase 5.1: Cloud Access & IaC Foundations
- [x] **Task 1: Install tools natively via package manager**
  *   *Details*: Installed the official `azure-cli` package and HashiCorp `terraform` package natively on the Arch Linux host using `pacman -S`. This allows native execution directly on the development machine.
- [x] **Task 2: Authenticate terminal via OAuth2 flow**
  *   *Details*: Ran `az login` to authenticate the terminal session with the active Azure subscription (`bea46914-4ae4-4614-9ed2-ed65026d0217`). We successfully resolved a session expiration issue (error `AADSTS50132`) by performing a re-login to renew the local AAD authentication token.
- [x] **Task 3: Establish IaC configuration scaffolding**
  *   *Details*: Created the `infra/` directory containing `providers.tf` and `variables.tf`. Configured the `azurerm` provider block, specifying the subscription ID and adjusting the `prevent_deletion_if_contains_resources = false` flag. Configured the default region to `"westus3"` to avoid subscription-specific SQL provisioning blocks.
- [x] **Task 4: Write foundational resource group configurations**
  *   *Details*: Declared `azurerm_resource_group.rg` named `rg-openlake-dev` in `infra/main.tf`. Resolved location mismatch issues (when the resource group was created in `eastus` but resources were in `westus3`) by cleanly deleting the mismatched group via CLI and letting Terraform recreate it in `westus3`.

---

## 💾 Phase 5.2: The Cloud Storage & Serving Layer
- [x] **Task 1: Provision ADLS Gen2 Storage Account & File System**
  *   *Details*: Defined the standard storage account `stopenlakeabhijith` with `is_hns_enabled = true` to activate the hierarchical namespace (making it a true ADLS Gen2 account rather than standard Blob storage). Created the root `lakehouse` filesystem container. 
  *   *RBAC Role Integration*: Assigned the **Storage Blob Data Contributor** role to your principal ID (`4bb82cb3-8d33-4737-a2aa-bf5a3f1c8361`) at the resource group scope to bypass data-plane authorization `403` signature errors.
  *   *Medallion Directory Structure*: Re-created the `bronze`, `silver`, and `gold` directories inside the `lakehouse` container using the Azure CLI.
- [x] **Task 2: Provision Azure SQL Server & CRM Database**
  *   *Details*: Standing up `sql-openlake-abhijith` (SQL Server) and its database `crm` (SQL Database) under the cost-efficient Basic tier. Added the `AllowAzureServices` firewall rule (`0.0.0.0`) and whitelisted your local developer IP `49.43.243.42` (`AllowLocalIP`) in `main.tf` to enable external connectivity.
- [x] **Task 3: Provision Databricks Workspace**
  *   *Details*: Added the `azurerm_databricks_workspace` resource (`dbw-openlake-abhijith`) to `main.tf`. Configured it with the `"premium"` SKU (required for Unity Catalog access and secure data governance) and `no_public_ip = true` under `custom_parameters`, successfully spinning it up in the `westus3` region.
- [x] **Task 4: Execute Infrastructure Provisioning**
  *   *Details*: Ran `terraform plan` and `terraform apply` to orchestrate and deploy all resources cleanly. Resolved transient Azure replication delays (resulting in 404 policy and key lookup crashes during server/database creation) by untainting resources (`terraform untaint`) to bypass destroy/recreate loops.
- [x] **Task 5: Verify Connectivity to Azure SQL**
  *   *Details*: Configured the VS Code **SQLTools** extension with server address, database (`crm`), credentials (`sqladmin`), and checked connection security options (Encrypt & Trust Server Certificate). Successfully connected and executed a test query `SELECT @@VERSION;` to verify version metadata from Microsoft SQL Azure.
- [x] **Task 6: Clean Up Stale/Orphaned Resources**
  *   *Details*: Ran Azure CLI deletion commands (`az sql server delete` and `az storage account delete`) to wipe out old placeholder resources (`stopenlake12345`, `sql-openlake-12345` and `sql-openlake-12345/crm`) that were left untracked in the resource group, saving subscription credits.

---

## ⚡ Phase 5.3: Processing & Transformation Porting
- [x] **Task 1: Rebuild the local Airflow Docker image to install necessary Python libraries (`azure-storage-blob`, `pymssql`) for Azure data plane communication**
  * *Details*: Successfully rebuilt the Airflow image using the modified Dockerfile. Bypassed permission issues accessing root-owned `/root/bin/pip` inside the container by executing the package lookup directly through `python -m pip show azure-storage-blob pymssql`.
- [x] **Task 2: Update PySpark ingestion and ML scripts to read and write using `abfss://` (Azure Blob File System) paths and inject the storage account access keys**
  * *Details*: Updated PySpark configurations and files (`transform_bronze_to_silver.py`, `train_churn_model.py`, `stream_live_orders.py`, `push_churn_scores.py`) to consume from ADLS Gen2. Fixed a duplicated Spark package dependency typo in the streaming DAG configuration (changed `hadoop-azure:3.3.4:3.3.4` to `hadoop-azure:3.3.4`).
- [x] **Task 3: Add a new target profile to your `transform/dbt_project/profiles.yml` file for Azure SQL (using the `dbt-sqlserver` adapter)**
  * *Details*: Initialized the dbt project under `transform/dbt_project/` and configured `profiles.yml` to target the Azure SQL database. Overcame missing ODBC driver compiler dependencies on Arch Linux by installing `unixodbc` via pacman and building/installing the `msodbcsql` driver from the AUR, validating the configuration via `dbt debug`.
- [x] **Task 4: Run `dbt run --target azure` to compile and build Silver and Gold schemas on the live Azure SQL database**
  * *Details*: Successfully compiled and executed the dbt run on the target Azure SQL Database. Overcame a database schema creation crash (`COLUMNSTORE is not supported in this service tier`) caused by Azure SQL Database's Basic Tier limitations by adding `+as_columnstore: false` globally under the models section of `dbt_project.yml`. Resolved a subsequent data validation test failure in the starter model `my_first_dbt_model.sql` by uncommenting the `where id is not null` filter.
- [x] **Task 5: Update connection strings and upsert queries in the FastAPI application and Reverse ETL scripts to target the live Azure SQL database using `pymssql` and the `MERGE` SQL syntax**
  * *Details*: Confirmed connection strings are updated for the Reverse ETL script `push_churn_scores.py` to connect via `pymssql` and execute database updates using the `MERGE` SQL statement. Verified that the FastAPI service in `ml/serve.py` serves the model strictly in-memory without direct database queries, leaving it clean of database configuration requirements.

---

## 🔄 Phase 5.4: Cloud Orchestration & CI/CD Guardrails
- [x] **Task 1: Securely mount Azure credentials (`AZURE_STORAGE_KEY`, `AZURE_SQL_PASSWORD`) into the Airflow container environment via `.env`**
  * *Details*: Populated local `.env` file with `AZURE_STORAGE_KEY`, `AZURE_SQL_SERVER`, `AZURE_SQL_USER`, and `AZURE_SQL_PASSWORD` credentials. Verified these environment variables are mounted and made available inside all Airflow containers (scheduler, webserver, init) via the `env_file` mapping in `docker-compose.yml`.
- [x] **Task 2: Execute and validate the 5-stage Airflow DAG in the local container stack, confirming data flow from raw CSV -> ADLS Gen2 bronze/silver -> local ML train -> Azure SQL table delivery**
  * *Details*: Orchestrated the full ingestion and ML pipeline. Bypassed local internet bandwidth bottlenecks by implementing server-side copy from `landing/` to partitioned `bronze/` container paths. Tuned Spark memory configurations and resolved a local write permission denial on the exported Random Forest classifier (`model.pkl`). Wrote a batched `MERGE` query constructor in `push_churn_scores.py` (processing chunks of 100 rows), reducing SQL Server insertion overhead from **27 minutes to ~10 seconds**.
- [x] **Task 3: Create your `.github/workflows/ci.yml` file in your repository**
  * *Details*: Created the `.github/workflows/ci.yml` file configuring the automated CI pipeline.
- [x] **Task 4: Configure the GitHub Actions workflow to pull down your code, install dependencies, run code linters (`black` or `flake8`), and validate FastAPI unit tests automatically on every push**
  * *Details*: Populated the workflow steps to checkout the repo, setup Python 3.12, install requirements, run formatting verification via `black`, static analysis via `flake8`, and run FastAPI endpoints unit tests with `pytest ml/`. Created `ml/test_serve.py` using `unittest.mock` to validate endpoints.
