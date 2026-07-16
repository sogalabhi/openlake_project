# Chapter 9: Porting Transformations to dbt & Azure SQL

> **Diary Entry: July 13, 2026**
> "Today, I set out to connect our local dbt development environment to our newly provisioned Azure SQL CRM database. What was supposed to be a simple 'dbt init' and a quick connection check turned into a journey through unixODBC drivers, Arch Linux system libraries, Docker user permissions, and environment variable loaders. Here is the first-person log of what broke, why it broke, and how I got everything talking to the cloud."

---

## 1. What is dbt and Why are we using it?

In our lakehouse architecture, we divide the data processing and transformation workload into two different engines:
1. **Apache Spark**: Runs heavy compute (raw CSV ingestion, schema projection, deduplication, and initial model feature assembly) directly over our raw storage container (`abfss://` ADLS Gen2).
2. **dbt (Data Build Tool)**: Takes over for modular SQL transformations on our relational engine. Instead of writing raw `CREATE TABLE AS SELECT` scripts, we write modular, testable SELECT queries in `.sql` files. dbt compiles these queries, builds dependencies, generates documentation, and runs the models directly on our cloud serving database (**Azure SQL Server**).

By utilizing **dbt-core** with the **`dbt-sqlserver`** adapter, we can:
*   Define target profiles using environment variables, ensuring no database passwords are hardcoded in the repository.
*   Establish data quality tests (e.g. checking that customer IDs in our models are unique and non-null) directly inside our SQL pipelines.
*   Run the compiled models on the live Azure SQL instance so that downstream applications (FastAPI ML endpoints, BI dashboards) query clean, structured schemas.

---

## 2. The Debugging Chronicles: What Broke & How We Fixed It

### Problem 1: Docker Exec / root `pip` Permission Denied
*   **The Symptom**: When I tried to verify that the updated Airflow containers had successfully installed the `azure-storage-blob` and `pymssql` libraries, I ran:
    ```bash
    docker exec -it airflow-webserver pip show azure-storage-blob pymssql
    ```
    This failed immediately with:
    ```bash
    bash: /root/bin/pip: Permission denied
    ```
*   **The Cause**: The Apache Airflow base image sets the default user to `airflow` (non-root) for security reasons. However, the system's `PATH` environment variable in the shell has `/root/bin` appended or prioritized. When bash searched the `PATH` for the `pip` executable, it tried to run root's binary and was blocked by Linux file permissions.
*   **The Fix**: To bypass the environment's `PATH` lookup issues, I ran the python package manager as a module directly through the active Python interpreter:
    ```bash
    docker exec -it airflow-webserver python -m pip show azure-storage-blob pymssql
    ```
    This resolved the correct user-level python library path (`/home/airflow/.local/lib/python...`) and successfully verified the installations.

---

### Problem 2: Spark Submit Package Version Typo
*   **The Symptom**: In the streaming DAG file, the Maven coordinates declared for the Hadoop Azure integration was structured incorrectly, causing package resolution failure.
*   **The Cause**: The Spark packages configuration in [streaming_pipeline.py](file:///home/abhijith/coding/openlake_project/dags/streaming_pipeline.py) had a duplicate version suffix:
    ```python
    packages="...,org.apache.hadoop:hadoop-azure:3.3.4:3.3.4,..."
    ```
*   **The Fix**: Cleaned up the Maven coordinates to point to the correct, single version coordinate:
    ```python
    packages="...,org.apache.hadoop:hadoop-azure:3.3.4,..."
    ```

---

### Problem 3: Missing `libodbc.so.2` System Library on Arch Linux
*   **The Symptom**: Upon executing `dbt init dbt_project` in the `transform/` folder, the initialization script crashed with:
    ```
    ImportError: libodbc.so.2: cannot open shared object file: No such file or directory
    ```
*   **The Cause**: The python `pyodbc` package (upon which `dbt-sqlserver` depends) is a wrapper around the system's ODBC Driver Manager. Because I am developing on Arch Linux, the native package `unixodbc` was not pre-installed on the host OS.
*   **The Fix**: Installed the driver manager directly via pacman:
    ```bash
    sudo pacman -S unixodbc
    ```

---

### Problem 4: Empty Git Repository for `msodbcsql18` on AUR
*   **The Symptom**: I tried to install the Microsoft ODBC Driver v18 from the Arch User Repository using:
    ```bash
    git clone https://aur.archlinux.org/msodbcsql18.git
    ```
    The clone succeeded, but git threw a warning: `You appear to have cloned an empty repository.` Running `makepkg` resulted in: `==> ERROR: PKGBUILD does not exist.`
*   **The Cause**: Microsoft's packaging on AUR is maintained under the package name `msodbcsql` (which tracks the latest version 18 under the hood), whereas `msodbcsql18` was an unmaintained or non-existent package alias.
*   **The Fix**: Cloned and built the package using the correct AUR name:
    ```bash
    git clone https://aur.archlinux.org/msodbcsql.git
    cd msodbcsql
    makepkg -si
    ```
    This built the official Microsoft ODBC driver library and registered it globally in `/etc/odbcinst.ini`.

---

### Problem 5: Environment Variable Parsing Error
*   **The Symptom**: Running `dbt debug --profiles-dir .` failed with:
    ```
    Parsing Error: Env var required but not provided: 'AZURE_SQL_SERVER'
    ```
*   **The Cause**: The database configurations were in our local `.env` file, but they had not been exported to my active terminal session. Furthermore, our `.env` file was formatted with spaces around the assignment operators (e.g. `AZURE_SQL_SERVER = sql-...`), which causes a standard `source .env` command to crash in bash.
*   **The Fix**: Formatted and exported the `.env` file variables on the fly using a sed-based parsing pipeline:
    ```bash
    export $(grep -v '^#' ../../.env | sed 's/ *= */=/g' | xargs)
    ```
    This stripped the extra whitespace, converted the lines into standard `VAR=VAL` format, and exported them to the shell session.

---

### Problem 6: Profile Name Mismatch
*   **The Symptom**: After fixing the environment variables, `dbt debug` returned:
    ```
    Profile loading failed for the following reason:
    Runtime Error
      Could not find profile named 'dbt_project'
    ```
*   **The Cause**: During `dbt init`, dbt generated a default `dbt_project.yml` referencing `profile: 'dbt_project'`. However, in my custom `profiles.yml` file, the top-level block was defined as `openlake_project:`.
*   **The Fix**: Edited `profiles.yml` and renamed the block from `openlake_project:` to `dbt_project:`, aligning it with the project definition. 

With this final adjustment, `dbt debug` succeeded:
```
15:18:08    Connection test: [OK connection ok]
15:18:08  All checks passed!
```

---

### Problem 7: Clustered Columnstore Indexes Not Supported in Azure SQL Basic Tier
*   **The Symptom**: When running `dbt run --target azure`, the compilation and execution of `my_first_dbt_model` crashed with the following error:
    ```
    Database Error in model my_first_dbt_model
      ('42000', "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]'COLUMNSTORE' is not supported in this service tier of the database...")
    ```
*   **The Cause**: By default, the `dbt-sqlserver` adapter materializes all tables with **clustered columnstore indexes** for high query speed on analytical tables. However, Azure SQL Database's Basic Tier (and standard tiers lower than S3) does not support Columnstore indexes.
*   **The Fix**: Added `+as_columnstore: false` to the `models:` configuration block in [dbt_project.yml](file:///home/abhijith/coding/openlake_project/transform/dbt_project/dbt_project.yml) to globally disable clustered columnstore indexing, reverting the materialization strategy to classic relational tables.

---

### Problem 8: Starter Model Validation Test Failure
*   **The Symptom**: After fixing the Columnstore index error, the project compiled and deployed successfully. However, running `dbt test --target azure` threw **1 test failure**:
    ```
    Failure in test not_null_my_first_dbt_model_id
      Got 1 result, configured to fail if != 0
    ```
*   **The Cause**: The starter template model `my_first_dbt_model.sql` explicitly includes a `null` row (`select null as id`) to demonstrate how data quality tests operate when they catch invalid records.
*   **The Fix**: Edited the file [my_first_dbt_model.sql](file:///home/abhijith/coding/openlake_project/transform/dbt_project/models/example/my_first_dbt_model.sql) to uncomment the `where id is not null` filter block. After saving, both `dbt run` and `dbt test` passed with 100% success.

---

## 3. Deep-Dive: Ingesting Large Files to ADLS Gen2 via HTTP (Block Blobs)

When uploading large datasets (such as our 94.8 MB retail transaction CSV file) over the internet, pushing the entire file in a single standard HTTP `PUT` request is highly unreliable. If the network drops at 99%, the entire upload fails and must start over. 

To solve this, Azure Blob Storage (and ADLS Gen2) implements the **Block Blob** protocol:
1.  **Splitting**: The client-side Azure Python SDK (`azure-storage-blob`) automatically splits the local file into small, fixed-size chunks (defaulting to 4 MB "blocks") in memory.
2.  **Staging**: The SDK uploads each block independently using standard HTTP `PUT` requests with a unique `blockid` query parameter (visible in the logs as `comp=block&blockid=...`). Azure stores these staged blocks temporarily but does not yet create the final file.
3.  **Committing**: Once all blocks are successfully uploaded and acknowledged (`Response status: 201`), the SDK sends a final, tiny HTTP request containing the ordered list of all block IDs (the `Put Block List` operation). Azure then stitches the staged blocks together on the server side, making the final file visible in the container.
4.  **Resilience**: If block 14 fails due to network jitter, the SDK only retries block 14 (4 MB) instead of starting the whole 94.8 MB file upload from scratch.

---

## 4. Interview Preparation: Key Concept Callouts

> [!TIP]
> **Interview Question**: How does Azure Blob Storage/ADLS Gen2 handle uploading extremely large files (e.g. 10GB+) over standard HTTP? Why is it better than a single PUT request?
> **Answer**: ADLS Gen2 uses Block Blobs for large files. The client SDK chunks the file into blocks (typically 4MB-100MB), uploads them sequentially or in parallel as staged blocks via individual HTTP `PUT` requests, and commits them with a final `Put Block List` request. This provides resilience (you only retry failed blocks, not the whole file), parallel transmission (utilizing multiple TCP streams for speed), and avoids high memory buffers on the client machine.

> [!TIP]
> **Interview Question**: If your Airflow upload task fails halfway through a 100MB file upload, do you have orphan blocks sitting in Azure? How do you prevent cost leakage from uncommitted blocks?
> **Answer**: Yes, if an upload fails before the final `Put Block List` commit is sent, the individual uploaded blocks remain in a "staged" (uncommitted) state on Azure. These blocks are invisible to standard list operations but still consume storage space and generate costs. To prevent cost leakage, you should configure an Azure Storage **Lifecycle Management Policy** to automatically delete uncommitted blobs after a set period (e.g., 7 days of inactivity).

> [!TIP]
> **Interview Question**: How would you monitor the progress of a file upload programmatically in your ingestion script to ensure it hasn't hung?
> **Answer**: The Azure Storage SDK's `upload_blob()` function supports a `progress_hook` callback parameter. You pass it a function that receives the `current` uploaded bytes and `total` file bytes. By calculating `current / total * 100`, you can output regular progress logs (e.g., every 5% uploaded). This can be fed into monitoring tools or used to trigger a timeout/heartbeat error if the bytes transferred stop incrementing.

> [!TIP]
> **Interview Question**: Why does `dbt-sqlserver` depend on `unixODBC` and system-level drivers on Linux?
> **Answer**: `dbt` acts as a compiler. It parses your SQL files and issues query instructions to the target database. To talk to SQL Server, Python uses the `pyodbc` database driver, which is a C-extension. This extension doesn't talk directly to the database; it communicates via `unixODBC` (the Linux ODBC Driver Manager), which in turn loads the database-specific driver (`msodbcsql`). If any of these links-the python library, the Linux manager, or the database-specific binary-are missing, the pipeline crashes.

> [!TIP]
> **Interview Question**: How does dbt compare to PySpark in your medallion architecture?
> **Answer**: We use **Spark** for our **ingestion and conform layers (Bronze & Silver)** because Spark is ideal for distributed compute, processing raw unstructured files, and doing complex programming tasks like deduplication and machine learning. We use **dbt** for our **serving layer (Gold)** because Gold data transformations are largely relational and analytical (aggregations, star-schema joins). dbt allows us to write these models in standard SQL, auto-generate data lineage diagrams, and easily implement data quality checks at the database layer.
