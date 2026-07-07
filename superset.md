## Complete Setup & Troubleshooting Guide

---

### Architecture
```
Redpanda (Kafka) → Spark Streaming → MinIO (Delta Lake) → Hive ThriftServer → Superset
```

---

## Part 1: Initial Setup

### 1. Start Spark ThriftServer with all required JARs

```bash
# Download required JARs
docker exec spark-master bash -c '
cd /opt/bitnami/spark/jars
wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar
wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
'

# Configure spark-defaults.conf
docker exec spark-master bash -c 'cat > /opt/bitnami/spark/conf/spark-defaults.conf << EOF
spark.jars /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/bitnami/spark/jars/delta-storage-3.1.0.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.access.key minioadmin
spark.hadoop.fs.s3a.secret.key miniopass
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
EOF'

# Kill any existing ThriftServer
docker exec spark-master pkill -f HiveThriftServer2
sleep 3

# Start ThriftServer
docker exec -d spark-master /opt/bitnami/spark/sbin/start-thriftserver.sh
sleep 25

# Fix port binding (ThriftServer binds to 127.0.0.1 by default)
docker exec -d spark-master socat TCP-LISTEN:10000,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:10000

# Verify
timeout 30 docker exec spark-master /opt/bitnami/spark/bin/beeline -u jdbc:hive2://127.0.0.1:10000 -e "SELECT 1;" 2>&1
```

### 2. Fix Superset Hive Driver

```bash
# Create the missing SQLAlchemy Hive dialect file
docker exec -u root superset bash -c 'cat > /app/.venv/lib/python3.10/site-packages/sqlalchemy/dialects/hive.py << EOF
from pyhive.sqlalchemy_hive import HiveDialect, HiveHTTPDialect, HiveHTTPSDialect
from sqlalchemy.dialects import registry

registry.register("hive", "pyhive.sqlalchemy_hive", "HiveDialect")
registry.register("hive.http", "pyhive.sqlalchemy_hive", "HiveHTTPDialect")
registry.register("hive.https", "pyhive.sqlalchemy_hive", "HiveHTTPSDialect")

dialect = HiveDialect
EOF'

# Install Hive driver in the correct Python environment
docker exec -u root superset bash -c "curl -sS https://bootstrap.pypa.io/get-pip.py | /app/.venv/bin/python"
docker exec -u root superset bash -c "/app/.venv/bin/python -m pip install pyhive pure-sasl thrift thrift-sasl"

# Run migrations and init
docker exec superset superset db upgrade
docker exec superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
docker exec superset superset init

# Restart
docker restart superset
```

### 3. Connect Superset to Hive

In Superset UI:
- **Settings → Database Connections → + Database**
- Select **Apache Hive**
- SQLAlchemy URI: `hive://spark-master:10000/default`
- **Test Connection → Connect**

---

## Part 2: Issues Faced & Solutions

| # | Issue | Root Cause | Solution |
|---|-------|-----------|----------|
| 1 | `Can't load plugin: sqlalchemy.dialects:hive` | Missing Hive dialect module in SQLAlchemy | Created `/app/.venv/.../sqlalchemy/dialects/hive.py` with dialect registration |
| 2 | `pip` not found in virtual env | pip was installed to user local, not .venv | Used `curl get-pip.py \| /app/.venv/bin/python` then `python -m pip install` |
| 3 | PyHive installed but SQLAlchemy can't import `hive` | PyHive 0.7.0 doesn't auto-register dialect in SQLAlchemy 1.4 | Manually created `hive.py` dialect file with `registry.register()` calls |
| 4 | `no such table: themes` | Superset DB migrations pending | Ran `superset db upgrade` |
| 5 | Derby metastore locked (`ERROR XSDB6`) | Previous Spark process didn't release lock | `pkill -f HiveThriftServer2`, deleted `metastore_db/`, used clean location `/tmp/metastore_db_clean` |
| 6 | ThriftServer binds to 127.0.0.1 only | Bitnami Spark image ignores `bind.host` config | Used `socat TCP-LISTEN:10000,bind=0.0.0.0,fork TCP:127.0.0.1:10000` |
| 7 | `NoClassDefFoundError: DeltaSourceUtils$` | Delta JARs not in ThriftServer classpath | Downloaded `delta-spark_2.12-3.1.0.jar` to Spark jars directory |
| 8 | `TABLE_OR_VIEW_NOT_FOUND: gold.live_order_metrics` | Table not registered in Hive metastore | Created VIEW or queried directly via `delta.\`s3a://...\`` |
| 9 | `CREATE TABLE ... USING DELTA` hangs/stuck | Missing `hadoop-aws` JAR for S3A connectivity | Downloaded `hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` |
| 10 | DDL/DML blocked in Superset | Superset security setting | Used Virtual Dataset with SQL query instead |
| 11 | `spark.sql.warehouse.dir` pointing to `/tmp` cleared on restart | Temporary directory lost | Used `spark-defaults.conf` for persistent config |
| 12 | `SASL authentication not complete` | ThriftServer crashed/restarted mid-query | Ensured ThriftServer fully started before queries |
| 13 | `netstat`/`ss` not available in container | Bitnami minimal image | Used `/proc/<pid>/net/tcp` or `bash -c 'echo >/dev/tcp/host/port'` |
| 14 | `curl` not installed in spark-master | Minimal image | Used `wget` instead, or `apt-get install -y wget` |
| 15 | `pkill -9 -f java` stopped the container | Killed Spark master process | Used `docker start spark-master` to restart |
| 16 | Spark streaming job not running | Script not on spark-master | Created `/tmp/stream_live_orders.py` with hardcoded credentials |

---

## Part 3: Key Commands Reference

### Restart ThriftServer after container restart
```bash
docker start spark-master
sleep 15
docker exec spark-master pkill -f HiveThriftServer2 2>/dev/null
sleep 3
docker exec -d spark-master /opt/bitnami/spark/sbin/start-thriftserver.sh
sleep 25
docker exec -d spark-master socat TCP-LISTEN:10000,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:10000
```

### Verify ThriftServer is working
```bash
docker exec spark-master /opt/bitnami/spark/bin/beeline -u jdbc:hive2://127.0.0.1:10000 -e "SELECT 1;" 2>&1
```

### Query Delta table directly
```sql
SELECT * FROM delta.`s3a://lakehouse/gold/live_order_metrics` LIMIT 3;
```

### Check MinIO for data
```bash
docker exec minio bash -c "mc alias set local http://localhost:9000 minioadmin miniopass 2>/dev/null; mc ls local/lakehouse/gold/ --recursive"
```

### Recreate Superset Hive dialect (after container rebuild)
```bash
docker exec -u root superset bash -c 'cat > /app/.venv/lib/python3.10/site-packages/sqlalchemy/dialects/hive.py << EOF
from pyhive.sqlalchemy_hive import HiveDialect, HiveHTTPDialect, HiveHTTPSDialect
from sqlalchemy.dialects import registry
registry.register("hive", "pyhive.sqlalchemy_hive", "HiveDialect")
registry.register("hive.http", "pyhive.sqlalchemy_hive", "HiveHTTPDialect")
registry.register("hive.https", "pyhive.sqlalchemy_hive", "HiveHTTPSDialect")
dialect = HiveDialect
EOF'
docker restart superset
```

---

## Part 4: Virtual Dataset in Superset

If DDL is blocked, create a Virtual Dataset:
1. **Datasets → + Dataset**
2. Select Hive database
3. **Virtual Dataset** SQL:
```sql
SELECT * FROM delta.`s3a://lakehouse/gold/live_order_metrics`
```
4. Name: `gold.live_order_metrics`
5. Save → Now usable in charts