# Azure Data Engineer Interview — Deep Dive Guide
### Level: Mid-Level | All Topics | All Rounds

---

## Table of Contents

1. [Deep Dive — SQL](#1-deep-dive--sql)
2. [Deep Dive — Python & Azure SDK](#2-deep-dive--python--azure-sdk)
3. [Deep Dive — Azure Data Factory](#3-deep-dive--azure-data-factory)
4. [Deep Dive — ADLS Gen2](#4-deep-dive--adls-gen2)
5. [Deep Dive — Azure Synapse Analytics](#5-deep-dive--azure-synapse-analytics)
6. [Deep Dive — Azure Databricks & Delta Lake](#6-deep-dive--azure-databricks--delta-lake)
7. [Deep Dive — Azure Event Hubs](#7-deep-dive--azure-event-hubs)
8. [Deep Dive — Azure Stream Analytics](#8-deep-dive--azure-stream-analytics)
9. [Deep Dive — Microsoft Purview](#9-deep-dive--microsoft-purview)
10. [Deep Dive — Apache Spark on Azure](#10-deep-dive--apache-spark-on-azure)
11. [Deep Dive — System Design](#11-deep-dive--system-design)
12. [Deep Dive — Behavioral](#12-deep-dive--behavioral)
13. [Top 100 Interview Questions & Answers](#13-top-100-interview-questions--answers)

---

## 1. Deep Dive — SQL

### Window Functions

```sql
-- ROW_NUMBER: unique rank per partition
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees;

-- RANK: same rank for ties, skips next
SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
FROM employees;

-- DENSE_RANK: same rank for ties, no skipping
SELECT *, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dense_rnk
FROM employees;

-- LAG: access previous row value
SELECT *, LAG(sales, 1, 0) OVER (PARTITION BY region ORDER BY sale_date) AS prev_sales
FROM sales;

-- LEAD: access next row value
SELECT *, LEAD(sales, 1, 0) OVER (PARTITION BY region ORDER BY sale_date) AS next_sales
FROM sales;

-- Running total
SELECT order_id, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;

-- Moving average (7 day)
SELECT date, value,
       AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
FROM metrics;
```

### Advanced SQL Problems

**Problem 1: Month-over-month revenue change**
```sql
WITH monthly AS (
    SELECT DATETRUNC('month', trip_date) AS month,
           SUM(fare_amount) AS revenue
    FROM trips
    GROUP BY 1
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) /
              LAG(revenue) OVER (ORDER BY month) * 100, 2) AS pct_change
FROM monthly;
```

**Problem 2: Top N per group**
```sql
SELECT category, product, revenue
FROM (
    SELECT category, product, revenue,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM product_sales
) t
WHERE rn <= 3;
```

**Problem 3: Gaps and Islands**
```sql
WITH numbered AS (
    SELECT date,
           ROW_NUMBER() OVER (ORDER BY date) AS rn,
           DATEADD(DAY, -ROW_NUMBER() OVER (ORDER BY date), date) AS grp
    FROM active_dates
)
SELECT MIN(date) AS start_date, MAX(date) AS end_date
FROM numbered
GROUP BY grp
ORDER BY start_date;
```

**Problem 4: Pivot table**
```sql
SELECT year,
       SUM(CASE WHEN trip_type = 'yellow' THEN revenue END) AS yellow,
       SUM(CASE WHEN trip_type = 'green' THEN revenue END) AS green,
       SUM(CASE WHEN trip_type = 'fhvhv' THEN revenue END) AS fhvhv
FROM trip_revenue
GROUP BY year;
```

**Problem 5: Customers with consecutive month purchases**
```sql
WITH monthly AS (
    SELECT customer_id,
           DATETRUNC('month', purchase_date) AS month,
           LAG(DATETRUNC('month', purchase_date))
               OVER (PARTITION BY customer_id ORDER BY purchase_date) AS prev_month
    FROM purchases
)
SELECT DISTINCT customer_id
FROM monthly
WHERE DATEDIFF(MONTH, prev_month, month) = 1;
```

### Synapse-specific SQL
```sql
-- CTAS (Create Table As Select) — fast way to create tables
CREATE TABLE silver.yellow_trips
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM bronze.yellow_trips_staging
WHERE fare_amount > 0;

-- External table on ADLS
CREATE EXTERNAL TABLE bronze.yellow_trips (
    trip_id NVARCHAR(50),
    fare_amount FLOAT,
    pickup_datetime DATETIME2
)
WITH (
    LOCATION = 'nyc-tlc/yellow/year=*/month=*/*.parquet',
    DATA_SOURCE = adls_bronze,
    FILE_FORMAT = parquet_format
);

-- Statistics for query optimization
CREATE STATISTICS stat_customer_id ON silver.yellow_trips(customer_id);
UPDATE STATISTICS silver.yellow_trips;
```

---

## 2. Deep Dive — Python & Azure SDK

### Azure Storage SDK
```python
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import pandas as pd
import io

# ── Authentication ─────────────────────────────────────────────
# Managed Identity (recommended in Azure)
credential = DefaultAzureCredential()

# Service Principal (CI/CD, local dev)
credential = ClientSecretCredential(
    tenant_id="<tenant-id>",
    client_id="<client-id>",
    client_secret="<client-secret>"
)

# ── ADLS Gen2 Operations ───────────────────────────────────────
adls_client = DataLakeServiceClient(
    account_url="https://<account>.dfs.core.windows.net",
    credential=credential
)

# Create directory
fs_client = adls_client.get_file_system_client("raw")
fs_client.create_directory("nyc-tlc/yellow/year=2024/month=01")

# Upload file
file_client = fs_client.get_file_client("nyc-tlc/yellow/year=2024/month=01/file.parquet")
with open("local.parquet", "rb") as f:
    file_client.upload_data(f, overwrite=True)

# List files
paths = fs_client.get_paths(path="nyc-tlc/yellow/year=2024/")
for path in paths:
    print(path.name, path.content_length)

# ── Blob Storage Operations ────────────────────────────────────
blob_service = BlobServiceClient(
    account_url="https://<account>.blob.core.windows.net",
    credential=credential
)

# Upload DataFrame as Parquet to ADLS/Blob
def upload_df_as_parquet(df, container, blob_path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(buffer, overwrite=True)

# Download Parquet from ADLS into DataFrame
def download_parquet_as_df(container, blob_path):
    blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
    data = blob_client.download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))
```

### Azure Data Factory SDK
```python
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from datetime import datetime, timezone

adf_client = DataFactoryManagementClient(credential, subscription_id)

# Trigger pipeline run
run_response = adf_client.pipelines.create_run(
    resource_group_name="my-rg",
    factory_name="my-adf",
    pipeline_name="nyc-tlc-ingestion",
    parameters={"jobtype": "historical"}
)
run_id = run_response.run_id

# Monitor pipeline run
pipeline_run = adf_client.pipeline_runs.get(
    resource_group_name="my-rg",
    factory_name="my-adf",
    run_id=run_id
)
print(f"Status: {pipeline_run.status}")
```

### Azure Key Vault for Secrets
```python
from azure.keyvault.secrets import SecretClient

vault_url = "https://<vault-name>.vault.azure.net"
secret_client = SecretClient(vault_url=vault_url, credential=credential)

# Get secret
db_password = secret_client.get_secret("db-password").value
storage_key = secret_client.get_secret("storage-account-key").value

# Set secret
secret_client.set_secret("new-secret", "secret-value")
```

### Error Handling & Retries
```python
import time
from functools import wraps
from azure.core.exceptions import AzureError, ServiceRequestError

def retry_azure(max_attempts=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except (AzureError, ServiceRequestError) as e:
                    if attempt == max_attempts:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay * attempt}s...")
                    time.sleep(delay * attempt)
        return wrapper
    return decorator

@retry_azure(max_attempts=3)
def upload_with_retry(container, blob_path, data):
    blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(data, overwrite=True)
```

---

## 3. Deep Dive — Azure Data Factory

### ADF Incremental Load Pattern
```
Pipeline: incremental_load
│
├── Activity 1: Lookup — get last watermark
│     SELECT MAX(updated_at) AS last_watermark FROM watermark_table
│
├── Activity 2: Copy Activity
│     Source: SQL Query
│       SELECT * FROM source_table
│       WHERE updated_at > @{activity('Lookup').output.firstRow.last_watermark}
│     Sink: ADLS Gen2 (Parquet)
│
└── Activity 3: Stored Procedure — update watermark
      EXEC update_watermark @new_watermark = @{activity('Copy').output.dataWritten}
```

### ADF Parameterized Pipeline
```json
{
  "name": "nyc_tlc_ingestion",
  "parameters": {
    "trip_type": { "type": "String", "defaultValue": "yellow" },
    "year": { "type": "Int", "defaultValue": 2024 },
    "month": { "type": "Int", "defaultValue": 1 }
  },
  "activities": [
    {
      "name": "CopyTripData",
      "type": "Copy",
      "inputs": [{
        "referenceName": "SourceDataset",
        "parameters": {
          "trip_type": "@pipeline().parameters.trip_type",
          "year": "@pipeline().parameters.year",
          "month": "@pipeline().parameters.month"
        }
      }]
    }
  ]
}
```

### ADF Trigger Types
| Trigger | Description | Use Case |
|---------|-------------|----------|
| Schedule | Fixed time/recurrence | Daily batch jobs |
| Tumbling Window | Non-overlapping time windows | Hourly processing with backfill |
| Event-based | On file arrival in ADLS/Blob | Event-driven pipelines |
| Manual | On-demand | Ad-hoc runs |

### ADF Data Flow (Visual ETL)
- Spark-backed visual transformations
- Source → Transform → Sink
- Transformations — filter, join, aggregate, derived column, sort
- Debug mode — test with sample data
- Auto-generates Spark code

### ADF Monitoring
```
ADF Monitor → Pipeline Runs
    ├── Status (Succeeded/Failed/In Progress)
    ├── Duration
    ├── Activity runs detail
    └── Re-run failed pipelines

Azure Monitor → Diagnostic Logs
    └── Send to Log Analytics for KQL queries

Alert Rules → Notify on failure
    └── Email / SMS / Webhook / Azure Function
```

---

## 4. Deep Dive — ADLS Gen2

### Hierarchical Namespace
```
Storage Account
    └── Container (File System)
            └── Directory
                    └── File

Example:
adls_account/
├── raw/
│   └── nyc-tlc/
│       └── yellow/
│           └── year=2024/
│               └── month=01/
│                   └── yellow_tripdata_2024-01.parquet
├── silver/
└── gold/
```

### Access Control
```
Two models:
1. RBAC (Role-Based Access Control) — coarse-grained, at container level
   - Storage Blob Data Reader
   - Storage Blob Data Contributor
   - Storage Blob Data Owner

2. ACLs (Access Control Lists) — fine-grained, at file/folder level
   - Read (r)
   - Write (w)
   - Execute (x) — required to traverse directories

Best practice: Use RBAC for broad access + ACLs for fine-grained control
```

### Authentication Methods
| Method | Use Case | Security |
|--------|----------|----------|
| Managed Identity | Azure services (ADF, Databricks, Functions) | Best |
| Service Principal | CI/CD, external apps | Good |
| SAS Token | Temporary, limited access | Medium |
| Access Key | Dev/test only | Low |
| Azure AD Pass-through | Interactive users | Good |

### ADLS Best Practices
- Use hierarchical namespace for ACL support
- Partition data by date/region for query performance
- Use Managed Identity instead of access keys
- Enable soft delete for data protection
- Use lifecycle management for cost optimization
- Use private endpoints for network security
- Register ADLS in Microsoft Purview for governance

---

## 5. Deep Dive — Azure Synapse Analytics

### Distribution Deep Dive
```sql
-- When to use HASH distribution
-- ✅ Large fact tables
-- ✅ Frequently joined on this column
-- ✅ High cardinality column
CREATE TABLE fact_trips (
    trip_id INT,
    customer_id INT,  -- join column → use as DISTKEY
    fare_amount DECIMAL(10,2)
)
WITH (DISTRIBUTION = HASH(customer_id));

-- When to use REPLICATE
-- ✅ Small dimension tables (< 2GB)
-- ✅ Frequently joined with fact tables
CREATE TABLE dim_zones (
    zone_id INT,
    zone_name NVARCHAR(100),
    borough NVARCHAR(50)
)
WITH (DISTRIBUTION = REPLICATE);

-- When to use ROUND_ROBIN
-- ✅ Staging/temporary tables
-- ✅ No clear join column
-- ✅ Before CTAS with HASH
CREATE TABLE staging_trips (
    trip_id INT,
    fare_amount DECIMAL
)
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
```

### Index Types
```sql
-- Clustered Columnstore Index (default, best for analytics)
CREATE TABLE silver.trips (
    trip_id INT,
    fare_amount DECIMAL
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- Heap (no index, best for staging)
CREATE TABLE staging.trips (
    trip_id INT
)
WITH (HEAP);

-- Clustered Rowstore (OLTP-style lookups)
CREATE TABLE dim.customers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100)
)
WITH (CLUSTERED INDEX (customer_id));
```

### Synapse UPSERT Pattern
```sql
-- Step 1: Create staging table
CREATE TABLE staging.trips_upsert
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
AS SELECT * FROM bronze.new_trips WHERE 1=0;

-- Step 2: Load new data into staging
COPY INTO staging.trips_upsert
FROM 'https://<account>.dfs.core.windows.net/raw/new_trips/*.parquet'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

-- Step 3: Delete matching rows
DELETE FROM silver.trips
WHERE trip_id IN (SELECT trip_id FROM staging.trips_upsert);

-- Step 4: Insert all from staging
INSERT INTO silver.trips
SELECT * FROM staging.trips_upsert;

-- Step 5: Drop staging
DROP TABLE staging.trips_upsert;
```

### Synapse Serverless SQL Pool
```sql
-- Ad-hoc query on ADLS Parquet
SELECT year, trip_type, COUNT(*) AS trips, SUM(fare_amount) AS revenue
FROM OPENROWSET(
    BULK 'https://<account>.dfs.core.windows.net/silver/nyc-tlc/**/*.parquet',
    FORMAT = 'PARQUET'
) WITH (
    year INT,
    trip_type VARCHAR(10),
    fare_amount FLOAT
) AS r
GROUP BY year, trip_type
ORDER BY year, revenue DESC;

-- Create view for Power BI
CREATE VIEW gold.v_trip_summary AS
SELECT year, month, trip_type,
       COUNT(*) AS trip_count,
       SUM(fare_amount) AS total_revenue,
       AVG(trip_distance) AS avg_distance
FROM OPENROWSET(
    BULK 'https://<account>.dfs.core.windows.net/silver/nyc-tlc/**/*.parquet',
    FORMAT = 'PARQUET'
) AS r
GROUP BY year, month, trip_type;
```

### Synapse vs Databricks vs Azure SQL
| Scenario | Use |
|----------|-----|
| BI dashboards, complex joins | Synapse Dedicated |
| Ad-hoc SQL on ADLS | Synapse Serverless |
| ML, advanced Spark, Delta Lake | Databricks |
| Transactional workloads | Azure SQL |
| Simple ETL with visual UI | Synapse Pipelines / ADF |

---

## 6. Deep Dive — Azure Databricks & Delta Lake

### Delta Lake Architecture
```
ADLS Gen2 (storage)
    └── Delta Table = Parquet files + _delta_log/
            └── _delta_log/
                    ├── 00000000000000000000.json  (initial commit)
                    ├── 00000000000000000001.json  (next commit)
                    └── checkpoint files
```

### Delta Lake Operations
```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ── WRITE ──────────────────────────────────────────────────────
# Overwrite
df.write.format("delta").mode("overwrite").save("abfss://silver@acct.dfs.core.windows.net/trips/")

# Append
df.write.format("delta").mode("append").save("abfss://silver@acct.dfs.core.windows.net/trips/")

# Partitioned write
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save("abfss://silver@acct.dfs.core.windows.net/trips/")

# ── MERGE (UPSERT) ─────────────────────────────────────────────
delta_table = DeltaTable.forPath(spark, "abfss://silver@acct.dfs.core.windows.net/trips/")

delta_table.alias("target").merge(
    source=new_df.alias("source"),
    condition="target.trip_id = source.trip_id"
).whenMatchedUpdate(set={
    "fare_amount": "source.fare_amount",
    "status": "source.status",
    "updated_at": "source.updated_at"
}).whenNotMatchedInsertAll() \
  .whenNotMatchedBySourceDelete() \
  .execute()

# ── TIME TRAVEL ────────────────────────────────────────────────
# By version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("abfss://...")
# By timestamp
df_ts = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("abfss://...")
# View history
delta_table.history().show()

# ── OPTIMIZE & Z-ORDER ─────────────────────────────────────────
# Compact small files
spark.sql("OPTIMIZE silver.trips")

# Z-order — co-locate related data for faster queries
spark.sql("OPTIMIZE silver.trips ZORDER BY (pickup_datetime, zone_id)")

# ── VACUUM ─────────────────────────────────────────────────────
# Remove old files (default 7 days retention)
spark.sql("VACUUM silver.trips RETAIN 168 HOURS")

# ── SCHEMA EVOLUTION ───────────────────────────────────────────
df_new_schema.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("abfss://...")
```

### Unity Catalog
```python
# Three-level namespace
# catalog.schema.table

# Create catalog
spark.sql("CREATE CATALOG prod_catalog")

# Create schema
spark.sql("CREATE SCHEMA prod_catalog.nyc_tlc")

# Create managed table
spark.sql("""
    CREATE TABLE prod_catalog.nyc_tlc.yellow_trips (
        trip_id STRING,
        fare_amount DOUBLE,
        pickup_datetime TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (year INT, month INT)
""")

# Grant permissions
spark.sql("GRANT SELECT ON TABLE prod_catalog.nyc_tlc.yellow_trips TO `analyst@company.com`")
spark.sql("GRANT SELECT ON TABLE prod_catalog.nyc_tlc.yellow_trips TO `data_analyst_group`")

# Column masking
spark.sql("""
    ALTER TABLE prod_catalog.nyc_tlc.yellow_trips
    ALTER COLUMN driver_license SET MASK mask_pii
""")

# Row filter
spark.sql("""
    ALTER TABLE prod_catalog.nyc_tlc.yellow_trips
    SET ROW FILTER filter_by_region ON (region)
""")
```

### Databricks Autoloader
```python
# Autoloader — incremental file ingestion from ADLS
df_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://checkpoints@acct.dfs.core.windows.net/schema/") \
    .load("abfss://raw@acct.dfs.core.windows.net/nyc-tlc/yellow/")

df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://checkpoints@acct.dfs.core.windows.net/yellow/") \
    .trigger(availableNow=True) \
    .start("abfss://silver@acct.dfs.core.windows.net/yellow/")
```

### Databricks Delta Live Tables (DLT)
```python
import dlt
from pyspark.sql.functions import *

# Bronze — raw ingestion
@dlt.table(name="bronze_yellow_trips")
def bronze_yellow_trips():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .load("abfss://raw@acct.dfs.core.windows.net/yellow/")

# Silver — cleaned
@dlt.table(name="silver_yellow_trips")
@dlt.expect("valid_fare", "fare_amount > 0")
@dlt.expect_or_drop("valid_trip", "trip_distance > 0")
def silver_yellow_trips():
    return dlt.read_stream("bronze_yellow_trips") \
        .filter(col("pickup_datetime").isNotNull()) \
        .withColumn("year", year("pickup_datetime")) \
        .withColumn("month", month("pickup_datetime"))

# Gold — aggregated
@dlt.table(name="gold_trip_summary")
def gold_trip_summary():
    return dlt.read("silver_yellow_trips") \
        .groupBy("year", "month") \
        .agg(count("*").alias("trip_count"),
             sum("fare_amount").alias("total_revenue"))
```

---

## 7. Deep Dive — Azure Event Hubs

### Event Hubs Architecture
```
Producers                    Event Hub Namespace
──────────                   ──────────────────
App Events  ──►  Event Hub (Topic)
IoT Devices ──►     Partition 0  ──► Consumer Group A (Databricks)
Clickstream ──►     Partition 1  ──► Consumer Group B (Stream Analytics)
                    Partition 2  ──► Consumer Group C (Azure Function)
                         │
                         ▼ Capture
                    ADLS Gen2 / Blob Storage
```

### Event Hubs Producer (Python)
```python
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
import json

credential = DefaultAzureCredential()
producer = EventHubProducerClient(
    fully_qualified_namespace="<namespace>.servicebus.windows.net",
    eventhub_name="nyc-tlc-trips",
    credential=credential
)

# Send single event
with producer:
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps({
        "trip_id": "123",
        "fare_amount": 15.5,
        "trip_type": "yellow"
    })))
    producer.send_batch(event_data_batch)

# Send batch of events
with producer:
    batch = producer.create_batch(partition_key="yellow")
    for trip in trips:
        batch.add(EventData(json.dumps(trip).encode('utf-8')))
    producer.send_batch(batch)
```

### Event Hubs Consumer (Python)
```python
from azure.eventhub import EventHubConsumerClient

def on_event(partition_context, event):
    print(f"Partition: {partition_context.partition_id}")
    print(f"Data: {event.body_as_str()}")
    partition_context.update_checkpoint(event)

consumer = EventHubConsumerClient(
    fully_qualified_namespace="<namespace>.servicebus.windows.net",
    eventhub_name="nyc-tlc-trips",
    consumer_group="$Default",
    credential=credential
)

with consumer:
    consumer.receive(
        on_event=on_event,
        starting_position="-1"  # from beginning
    )
```

### Event Hubs Capture
```json
{
  "captureDescription": {
    "enabled": true,
    "encoding": "Avro",
    "intervalInSeconds": 300,
    "sizeLimitInBytes": 314572800,
    "destination": {
      "name": "EventHubArchive.AzureBlockBlob",
      "properties": {
        "storageAccountResourceId": "/subscriptions/.../storageAccounts/myaccount",
        "blobContainer": "raw",
        "archiveNameFormat": "nyc-tlc/{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      }
    }
  }
}
```

---

## 8. Deep Dive — Azure Stream Analytics

### Windowing Functions
```sql
-- Tumbling Window: fixed, non-overlapping
SELECT TumblingWindow(minute, 5) AS window,
       trip_type,
       COUNT(*) AS trip_count,
       AVG(fare_amount) AS avg_fare
INTO [output-adls]
FROM [input-eventhubs] TIMESTAMP BY pickup_datetime
GROUP BY TumblingWindow(minute, 5), trip_type;

-- Hopping Window: fixed size, overlapping
SELECT HoppingWindow(minute, 10, 5) AS window,
       zone_id,
       COUNT(*) AS trip_count
INTO [output-powerbi]
FROM [input-eventhubs] TIMESTAMP BY pickup_datetime
GROUP BY HoppingWindow(minute, 10, 5), zone_id;

-- Sliding Window: trigger on every event
SELECT SlidingWindow(minute, 5) AS window,
       driver_id,
       COUNT(*) AS trip_count
INTO [output-cosmosdb]
FROM [input-eventhubs] TIMESTAMP BY pickup_datetime
GROUP BY SlidingWindow(minute, 5), driver_id
HAVING COUNT(*) > 10;

-- Session Window: dynamic, gap-based
SELECT SessionWindow(minute, 2, 10) AS window,
       user_id,
       COUNT(*) AS events
INTO [output-adls]
FROM [input-eventhubs] TIMESTAMP BY event_time
GROUP BY SessionWindow(minute, 2, 10), user_id;
```

### Stream Analytics — Late Arrival & Out-of-Order
```sql
-- Handle late arrivals up to 5 minutes
-- Configure in job settings:
-- Late arrival tolerance: 5 minutes
-- Out-of-order tolerance: 3 minutes

SELECT System.Timestamp() AS window_end,
       trip_type,
       COUNT(*) AS trips
INTO output
FROM input TIMESTAMP BY pickup_datetime
GROUP BY TumblingWindow(minute, 1), trip_type;
```

### Stream Analytics vs Databricks Streaming
| | Stream Analytics | Databricks Structured Streaming |
|--|-----------------|--------------------------------|
| Language | SQL | Python/Scala/SQL |
| Complexity | Simple | Complex |
| Serverless | Yes | No (cluster needed) |
| Delta Lake | No | Yes |
| ML integration | Limited | Full MLflow |
| Best for | Simple SQL aggregations | Complex transformations, ML |

---

## 9. Deep Dive — Microsoft Purview

### Purview Capabilities
```
Microsoft Purview
    ├── Data Map — scan and catalog data sources
    │     ├── Azure SQL, Synapse, ADLS, Databricks
    │     ├── On-prem SQL Server
    │     └── AWS S3, Redshift (multi-cloud)
    │
    ├── Data Catalog — business glossary, search
    │     ├── Search by name, column, classification
    │     └── Business terms and definitions
    │
    ├── Data Lineage — track data flow end-to-end
    │     ├── Source → ADF → ADLS → Synapse
    │     └── Column-level lineage
    │
    ├── Data Classification — auto-classify sensitive data
    │     ├── PII (Name, Email, Phone, SSN)
    │     ├── Financial data
    │     └── Custom classifiers
    │
    └── Access Policies — manage data access from Purview
```

### Purview Scanning
```python
from azure.purview.scanning import PurviewScanningClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = PurviewScanningClient(
    endpoint="https://<purview-account>.scan.purview.azure.com",
    credential=credential
)

# Trigger scan
scan_result = client.scans.run_scan(
    data_source_name="adls-bronze",
    scan_name="weekly-scan"
)
```

---

## 10. Deep Dive — Apache Spark on Azure

### PySpark on Databricks/Synapse
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# ── Read from ADLS Gen2 ────────────────────────────────────────
# Configure ADLS access (Databricks — use cluster config or Unity Catalog)
spark.conf.set(
    "fs.azure.account.auth.type.<account>.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.<account>.dfs.core.windows.net",
    "<client-id>"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.<account>.dfs.core.windows.net",
    "<client-secret>"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.<account>.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
)

# Read Parquet
df = spark.read.parquet("abfss://silver@<account>.dfs.core.windows.net/nyc-tlc/yellow/")

# Read Delta
df = spark.read.format("delta").load("abfss://silver@<account>.dfs.core.windows.net/nyc-tlc/yellow/")

# ── Transformations ────────────────────────────────────────────
df_clean = df \
    .filter(F.col("fare_amount") > 0) \
    .filter(F.col("trip_distance") > 0) \
    .dropna(subset=["pickup_datetime", "dropoff_datetime"]) \
    .withColumn("year", F.year("pickup_datetime")) \
    .withColumn("month", F.month("pickup_datetime")) \
    .withColumn("trip_duration_mins",
        (F.unix_timestamp("dropoff_datetime") -
         F.unix_timestamp("pickup_datetime")) / 60)

# ── Window Functions ───────────────────────────────────────────
window = Window.partitionBy("zone_id").orderBy(F.desc("pickup_datetime"))
df_ranked = df_clean.withColumn("rank", F.rank().over(window))

# ── Aggregations ───────────────────────────────────────────────
df_agg = df_clean.groupBy("year", "month", "trip_type").agg(
    F.count("*").alias("trip_count"),
    F.sum("fare_amount").alias("total_revenue"),
    F.avg("trip_distance").alias("avg_distance"),
    F.percentile_approx("fare_amount", 0.5).alias("median_fare")
)

# ── Write to Delta Lake ────────────────────────────────────────
df_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save("abfss://gold@<account>.dfs.core.windows.net/trip_summary/")
```

### Spark Optimization on Azure
```python
# ── Broadcast Join ─────────────────────────────────────────────
from pyspark.sql.functions import broadcast
df_result = df_trips.join(broadcast(df_zones), "zone_id")

# ── Handle Skew ────────────────────────────────────────────────
# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Manual salting
df_salted = df.withColumn(
    "salted_key",
    F.concat(F.col("zone_id"), F.lit("_"), (F.rand() * 10).cast("int").cast("string"))
)

# ── Caching ────────────────────────────────────────────────────
df_clean.cache()
df_clean.count()  # trigger cache

# ── Repartition ────────────────────────────────────────────────
df.repartition(200)                    # full shuffle, increase/decrease
df.repartition("year", "month")        # by column
df.coalesce(10)                        # no shuffle, only decrease

# ── Tuning ────────────────────────────────────────────────────
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

---

## 11. Deep Dive — System Design

### Design 1: Batch Lakehouse Pipeline on Azure

**Requirements:**
- Ingest data from Azure SQL + REST APIs daily
- Store in ADLS with Bronze/Silver/Gold layers
- Make queryable via Synapse and Power BI

**Architecture:**
```
Sources:
  Azure SQL (transactional DB)
  REST APIs (partner data)
        │
        ▼ (ADF Copy Activity + Web Activity)
ADLS Bronze Layer
  raw/nyc-tlc/year=YYYY/month=MM/*.parquet
        │
        ▼ (Databricks Autoloader)
ADLS Silver Layer (Delta Lake)
  silver/nyc-tlc/year=YYYY/month=MM/
        │
        ▼ (Databricks notebook job)
ADLS Gold Layer (Delta Lake)
  gold/trip_summary/
        │
        ├──► Synapse Serverless (ad-hoc SQL)
        ├──► Synapse Dedicated (BI dashboards)
        └──► Power BI (reports)

Governance:
  Microsoft Purview (catalog + lineage)
  Unity Catalog (Databricks access control)
  Azure Monitor + ADF Monitor (observability)
```

### Design 2: Real-time Trip Analytics

**Requirements:**
- Process live trip events from mobile app
- Show real-time KPIs on Power BI dashboard
- Store all events for historical analysis

**Architecture:**
```
Mobile App Events
        │
        ▼
Azure Event Hubs (10 partitions)
        │
        ├──► Stream Analytics
        │         │
        │         ├── Tumbling 1-min window → Power BI (live dashboard)
        │         └── Anomaly detection → Service Bus → Alert
        │
        ├──► Event Hubs Capture → ADLS Bronze (raw backup)
        │
        └──► Databricks Structured Streaming
                  │
                  ▼
            Delta Lake Silver (enriched events)
                  │
                  ▼
            Synapse Dedicated (historical analytics)
```

### Design 3: On-Prem to Azure Migration

**Requirements:**
- Migrate 10TB Oracle DB to Azure Synapse
- Ongoing CDC after initial migration
- Zero downtime

**Architecture:**
```
On-Prem Oracle
        │
        ▼ Full Load
Azure Database Migration Service (DMS)
        │
        ▼
Azure SQL MI (staging)
        │
        ▼ ADF Copy
ADLS Bronze (Parquet)
        │
        ▼ Databricks ETL
ADLS Silver + Gold (Delta Lake)
        │
        ▼ Synapse COPY
Synapse Dedicated Pool
        │
Ongoing CDC:
Oracle → Azure DMS (CDC) → ADLS → Databricks MERGE → Synapse
```

### Design 4: Multi-tenant Data Platform

**Requirements:**
- 10 teams, each with isolated data
- Shared compute, separate data access
- Fine-grained security

**Architecture:**
```
ADLS Gen2
  ├── /team-a/bronze/, /team-a/silver/, /team-a/gold/
  ├── /team-b/bronze/, /team-b/silver/, /team-b/gold/
  └── /shared/reference-data/

Security:
  ├── Azure AD Groups per team
  ├── ADLS ACLs at folder level
  ├── Unity Catalog — catalog per team
  └── Purview — classify and govern shared data

Compute:
  ├── Databricks SQL Warehouses (shared, access controlled by Unity Catalog)
  └── Synapse Serverless (RBAC per schema)
```

---

## 12. Deep Dive — Behavioral

### STAR Stories for Azure

**Story 1: ADF Pipeline built end-to-end**
- S: needed to ingest data from 5 on-prem SQL sources into Azure Data Lake daily
- T: design and build the full ADF pipeline with incremental loads
- A: built parameterized ADF pipelines with watermark-based incremental loads, ADLS Gen2 partitioned by date, Databricks for transformation, Unity Catalog for governance
- R: reduced data latency from T+1 day to 2 hours, zero manual intervention

**Story 2: Optimized slow Databricks job**
- S: Databricks job processing 300GB daily taking 3 hours
- T: reduce to under 1 hour
- A: profiled Spark UI, found data skew on zone_id, enabled AQE, applied Z-ordering on Delta table, switched to broadcast join for dimension tables
- R: reduced from 3 hours to 40 minutes, 60% cost saving on DBU

**Story 3: Production incident**
- S: ADF pipeline silently failed — Synapse table not updated for 8 hours
- T: diagnose and recover without data loss
- A: checked ADF Monitor, found linked service authentication failure (expired service principal secret), rotated secret in Key Vault, updated linked service, reran pipeline with backfill
- R: recovered all 8 hours of data, zero loss, added Azure Monitor alert for secret expiry

**Story 4: Learned new Azure service**
- S: project required Microsoft Purview — never used before
- T: implement data catalog and lineage for 20 data sources in 3 weeks
- A: studied Microsoft Learn docs, built POC scanning ADLS + Synapse, configured ADF lineage integration, presented to stakeholders
- R: delivered on time, team now has full data lineage visibility, reduced time to find data owners by 70%

---

## 13. Top 100 Interview Questions & Answers

### ADLS & Storage (10 Questions)

**Q1: What is the difference between ADLS Gen2 and Azure Blob Storage?**
- ADLS Gen2 = Blob Storage + hierarchical namespace + POSIX ACLs
- Hierarchical namespace enables true folder structure and ACL at file/folder level
- Better performance for analytics workloads

**Q2: What authentication methods are available for ADLS Gen2?**
- Managed Identity (best for Azure services)
- Service Principal (CI/CD, external apps)
- SAS Token (temporary, limited access)
- Access Key (dev/test only, avoid in production)

**Q3: What is the difference between RBAC and ACLs in ADLS?**
- RBAC — coarse-grained, applies at container level
- ACLs — fine-grained, applies at file/folder level
- Use both: RBAC for broad access + ACLs for row/column level control

### ADF (15 Questions)

**Q4: What is the difference between a Linked Service and a Dataset in ADF?**
- Linked Service — connection configuration (like a connection string)
- Dataset — pointer to specific data within a linked service (like a table or file path)

**Q5: What is the difference between Azure IR and Self-hosted IR?**
- Azure IR — cloud-to-cloud data movement, auto-managed
- Self-hosted IR — on-prem or private network sources, installed on your machine

**Q6: How do you implement incremental loads in ADF?**
- Watermark pattern — store last processed timestamp, query only new rows
- Tumbling window trigger — process each time window once
- File-based — process only new files in ADLS using event-based trigger

**Q7: What is a Tumbling Window trigger?**
- Non-overlapping, contiguous time windows
- Supports backfill — can rerun historical windows
- Best for processing data in fixed time intervals (e.g., hourly)

**Q8: How do you pass parameters between activities in ADF?**
- Use `@activity('ActivityName').output.value` expression
- Use pipeline parameters with `@pipeline().parameters.paramName`
- Use variables with `Set Variable` activity

**Q9: How do you handle errors and retries in ADF?**
- Activity retry settings — set retry count and interval
- `onFailure` path — connect activities to handle failure
- Failure alerts — Azure Monitor + email notification

**Q10: What is ADF Data Flow?**
- Visual Spark-backed ETL transformations
- No code — drag-and-drop transformations
- Runs on Spark clusters managed by ADF
- Supports filter, join, aggregate, derived column, sort, union

### Synapse Analytics (15 Questions)

**Q11: What is the difference between Synapse Dedicated and Serverless SQL Pool?**
- Dedicated — provisioned compute, MPP warehouse, best for BI and complex queries
- Serverless — on-demand, query ADLS directly, pay per TB scanned, best for ad-hoc

**Q12: When would you use HASH vs ROUND_ROBIN vs REPLICATE?**
- HASH — large fact tables, frequently joined column
- ROUND_ROBIN — staging tables, no clear join column
- REPLICATE — small dimension tables (< 2GB)

**Q13: What is a Clustered Columnstore Index?**
- Stores data by column instead of row
- Very high compression ratio
- Best for analytics/OLAP workloads
- Default index type in Synapse Dedicated Pool

**Q14: How do you load data efficiently into Synapse?**
- Use COPY INTO command (faster than PolyBase)
- Load from ADLS Parquet via Managed Identity
- Use staging table (HEAP + ROUND_ROBIN) then CTAS

**Q15: What is PolyBase?**
- Technology that allows Synapse to query external data in ADLS, Blob, or other sources
- Predecessor to COPY INTO command
- Still used for external tables

### Databricks & Delta Lake (20 Questions)

**Q16: What is Delta Lake and what problems does it solve?**
- Open-source storage layer on top of Parquet
- Adds ACID transactions, schema enforcement, time travel
- Solves small file problem, concurrent writes, schema drift

**Q17: What is the difference between OPTIMIZE and VACUUM?**
- OPTIMIZE — compacts small files into larger ones (improves read performance)
- VACUUM — removes old/deleted files beyond retention period (reduces storage cost)

**Q18: What is Z-ordering?**
- Multi-dimensional clustering of data within Delta files
- Co-locates related data so queries can skip irrelevant files
- Best for high-cardinality columns used in filters

**Q19: What is Delta Lake time travel?**
- Query previous versions of a Delta table
- By version number or timestamp
- Useful for auditing, rollback, reproducibility

**Q20: What is Unity Catalog?**
- Centralized governance layer for Databricks
- Three-level namespace: catalog.schema.table
- Fine-grained access control, data lineage, audit logs

**Q21: What is Databricks Autoloader?**
- Incrementally ingests new files from cloud storage
- Automatically detects schema changes
- Scales to millions of files
- Uses `cloudFiles` format in readStream

**Q22: What is Delta Live Tables (DLT)?**
- Declarative framework for building Delta Lake pipelines
- Defines pipeline as Python/SQL with @dlt.table decorators
- Handles dependencies, retries, data quality expectations automatically

**Q23: What is the difference between managed and external Delta tables?**
- Managed — Databricks manages both metadata and data files, dropping table deletes data
- External — Databricks manages metadata only, dropping table keeps data files

### Event Hubs & Streaming (15 Questions)

**Q24: What is the difference between Event Hubs and Service Bus?**
- Event Hubs — event streaming, high throughput, multiple consumers, retention
- Service Bus — enterprise messaging, FIFO, sessions, dead-letter queue, transactions

**Q25: How do you scale Event Hubs throughput?**
- Add more partitions (set at creation, can't change later for standard tier)
- Enable Auto-inflate to automatically scale throughput units
- Use Premium/Dedicated tier for higher limits

**Q26: What is Event Hubs Capture?**
- Automatically saves Event Hubs data to ADLS or Blob in Avro format
- No consumer code needed
- Configurable time and size thresholds
- Useful for raw data backup and replay

**Q27: What are the windowing types in Stream Analytics?**
- Tumbling — fixed, non-overlapping windows
- Hopping — fixed size, overlapping windows
- Sliding — triggers on every event within window
- Session — dynamic, gap-based windows

**Q28: How do you handle late-arriving events in Stream Analytics?**
- Configure late arrival tolerance (e.g., 5 minutes)
- Configure out-of-order tolerance
- Events outside tolerance are dropped or adjusted

### System Design (10 Questions)

**Q29: How would you design a real-time dashboard on Azure?**
- Event Hubs → Stream Analytics → Power BI streaming dataset
- Or Event Hubs → Databricks Structured Streaming → Synapse → Power BI

**Q30: How do you ensure exactly-once processing in Databricks Structured Streaming?**
- Use checkpointing to track processed offsets
- Write to Delta Lake (idempotent writes)
- Use `foreachBatch` with MERGE for upserts

**Q31: How do you implement data quality checks in a Databricks pipeline?**
- Delta Live Tables expectations (`@dlt.expect`, `@dlt.expect_or_drop`)
- Great Expectations library
- Custom validation functions with rejected record routing to quarantine table

**Q32: What is the medallion architecture?**
- Bronze — raw, unprocessed data, exact copy of source
- Silver — cleaned, validated, standardized
- Gold — business-ready aggregations and KPIs

**Q33: How would you implement CDC from Azure SQL to Synapse?**
- Enable CDC on Azure SQL source table
- ADF pipeline reads CDC change table
- Write changes to ADLS Bronze
- Databricks MERGE into Delta Silver
- Synapse CTAS for Gold aggregations

### Behavioral (5 Questions)

**Q34: Tell me about a complex Azure data pipeline you built.**
- Use STAR method
- Mention specific Azure services used
- Highlight technical challenges and solutions
- Quantify the outcome

**Q35: How do you approach cost optimization in Azure data pipelines?**
- Use Serverless SQL Pool for ad-hoc queries instead of Dedicated
- Use Spot instances for non-critical Databricks jobs
- Use ADLS lifecycle management (Hot → Cool → Archive)
- Optimize Delta tables with OPTIMIZE + Z-ORDER to reduce scan size
- Use Parquet + Snappy compression

---

## Quick Revision Flashcards

| Question | Answer |
|----------|--------|
| ADLS Gen2 vs Blob | ADLS Gen2 = Blob + hierarchical namespace + ACLs |
| ADF Linked Service vs Dataset | Linked Service = connection, Dataset = data pointer |
| Azure IR vs Self-hosted IR | Azure IR = cloud-to-cloud, Self-hosted = on-prem/private network |
| Synapse HASH vs REPLICATE | HASH = large fact tables, REPLICATE = small dim tables |
| CCI in Synapse | Clustered Columnstore Index — columnar, high compression, best for analytics |
| Delta Lake time travel | Query previous versions by version number or timestamp |
| OPTIMIZE vs VACUUM | OPTIMIZE = compact files, VACUUM = remove old files |
| Z-ordering | Multi-dim clustering for faster data skipping |
| Unity Catalog namespace | catalog.schema.table |
| Autoloader | Incremental file ingestion using cloudFiles format |
| DLT | Declarative Delta Lake pipeline framework |
| Event Hubs vs Service Bus | Event Hubs = streaming, Service Bus = enterprise messaging |
| Event Hubs Capture | Auto-save events to ADLS/Blob in Avro format |
| Tumbling vs Sliding window | Tumbling = non-overlapping, Sliding = overlapping |
| Medallion Architecture | Bronze (raw) → Silver (clean) → Gold (aggregated) |
| Managed Identity | Azure service auth without storing credentials |
| Microsoft Purview | Data catalog, lineage, classification, governance |
| ADF Tumbling Window | Non-overlapping time windows with backfill support |
| Synapse Serverless | Pay-per-query SQL on ADLS using OPENROWSET |
| Delta MERGE | UPSERT — update matched rows, insert new rows |
