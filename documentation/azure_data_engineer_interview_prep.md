# Azure Data Engineer Interview Preparation Guide
### Level: Mid-Level | Timeline: 7 Days

---

## Overview

| Day | Focus Area |
|-----|-----------|
| Day 1 | Core Data Engineering + Python + SQL Basics |
| Day 2 | Azure Data Ingestion & Pipelines (ADF, ADLS, Event Hubs) |
| Day 3 | Data Warehousing & Modelling (Synapse, Azure SQL) |
| Day 4 | Real-time Streaming (Event Hubs, Stream Analytics, Kafka on Azure) |
| Day 5 | Azure Databricks & Spark |
| Day 6 | System Design + Architecture |
| Day 7 | Mock Interview + Revision |

---

## Day 1 — Core Data Engineering + Python + SQL

### Core Concepts
- ETL vs ELT — differences, when to use each
- Batch vs Streaming — latency, use cases, tools
- Idempotency — running a pipeline multiple times gives same result
- Data partitioning — by date, region, category
- Slowly Changing Dimensions (SCD)
  - Type 1 — Overwrite
  - Type 2 — New row with effective dates
  - Type 3 — New column for old value

### File Formats
| Format | Type | Best For |
|--------|------|----------|
| Parquet | Columnar | Analytics, Synapse, Databricks |
| ORC | Columnar | Hive workloads |
| Avro | Row | Event Hubs, schema evolution |
| JSON | Row | APIs, semi-structured |
| Delta | Columnar + ACID | Databricks Lakehouse |

### Python (Must Know)
- List comprehensions, generators, decorators
- `azure-storage-blob` — ADLS/Blob operations
- `azure-identity` — authentication with DefaultAzureCredential
- File handling — reading/writing Parquet with `pandas` and `pyarrow`
- Error handling — try/except, retries

```python
# Common Azure SDK patterns
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Authenticate using managed identity / service principal
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(
    account_url="https://<account>.blob.core.windows.net",
    credential=credential
)

# Upload file
blob_client = blob_service.get_blob_client(container="raw", blob="nyc-tlc/file.parquet")
with open("local.parquet", "rb") as f:
    blob_client.upload_blob(f, overwrite=True)

# List blobs with prefix
container_client = blob_service.get_container_client("raw")
for blob in container_client.list_blobs(name_starts_with="nyc-tlc/"):
    print(blob.name, blob.size)
```

### SQL (Must Know)
- Window functions — `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`
- CTEs vs subqueries vs temp tables
- Query optimization — indexes, execution plans, partitioning
- Joins — INNER, LEFT, RIGHT, FULL OUTER, ANTI, CROSS
- Aggregations — `GROUP BY`, `HAVING`, `ROLLUP`, `CUBE`, `GROUPING SETS`

#### Practice Questions

**1. Find second highest salary per department:**
```sql
SELECT department, salary
FROM (
    SELECT department, salary,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM employees
) t
WHERE rnk = 2;
```

**2. Deduplicate keeping latest row:**
```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM records
) WHERE rn = 1;
```

**3. 7-day rolling average:**
```sql
SELECT date,
       AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM daily_metrics;
```

**4. Users who logged in on consecutive days:**
```sql
SELECT DISTINCT user_id FROM (
    SELECT user_id, login_date,
           LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS prev_date
    FROM logins
) WHERE DATEDIFF(DAY, prev_date, login_date) = 1;
```

**5. Running total:**
```sql
SELECT order_id, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM orders;
```

### Day 1 Checklist
- [ ] Revise ETL vs ELT, batch vs streaming
- [ ] Practice 5 SQL window function problems on LeetCode/StrataScratch
- [ ] Write an Azure SDK script to upload/download files from ADLS
- [ ] Understand SCD Type 1, 2, 3 with examples

---

## Day 2 — Azure Data Ingestion & Pipelines

### Azure Data Lake Storage Gen2 (ADLS Gen2)
- Built on Azure Blob Storage with hierarchical namespace
- Supports folders, fine-grained ACLs (POSIX-style)
- Access tiers — Hot, Cool, Archive
- Storage accounts → Containers → Directories → Files
- Authentication — Azure AD, SAS tokens, Access Keys, Managed Identity

### Azure Data Factory (ADF)
| Component | Description |
|-----------|-------------|
| Pipeline | Container of activities |
| Activity | Single step (Copy, Dataflow, Notebook) |
| Dataset | Pointer to data source/sink |
| Linked Service | Connection config (like a connection string) |
| Trigger | Schedule, tumbling window, event-based |
| Integration Runtime | Compute for ADF (Azure IR, Self-hosted IR) |

#### ADF Activity Types
| Activity | Use Case |
|----------|----------|
| Copy Activity | Move data between sources and sinks |
| Data Flow | Visual ETL transformations (Spark-backed) |
| Notebook Activity | Run Databricks/Synapse notebook |
| Stored Procedure | Execute SQL stored procedure |
| Web Activity | Call REST APIs |
| Until/ForEach | Control flow, iteration |
| If Condition | Conditional branching |

#### ADF Integration Runtimes
| Type | Use Case |
|------|----------|
| Azure IR | Cloud-to-cloud data movement |
| Self-hosted IR | On-prem or private network sources |
| Azure-SSIS IR | Run SSIS packages in Azure |

### ADF vs Glue vs Airflow
| | ADF | AWS Glue | Airflow |
|--|-----|----------|---------|
| Cloud | Azure | AWS | Any |
| Code | Low-code UI | PySpark/Python | Python DAGs |
| Orchestration | Yes | Limited | Yes |
| Transformations | Data Flows (Spark) | PySpark | External |
| Best for | Azure-native pipelines | AWS ETL | Complex workflows |

### Azure Event Hubs
- Fully managed real-time event ingestion
- Kafka-compatible API
- Partitions — unit of parallelism
- Consumer groups — independent readers
- Capture — auto-save to ADLS/Blob
- Retention — 1 to 90 days

### Azure Data Factory Incremental Pattern
```
Source (SQL/API)
      │
      ▼
ADF Pipeline (watermark-based)
      │
      ├── Lookup Activity (get last watermark)
      ├── Copy Activity (copy new rows)
      └── Stored Procedure (update watermark)
      │
      ▼
ADLS Gen2 (raw layer)
```

### Common Interview Questions — Day 2
1. What is the difference between Azure IR and Self-hosted IR in ADF?
2. How do you implement incremental loads in ADF?
3. What is a tumbling window trigger in ADF?
4. How do you handle pipeline failures and retries in ADF?
5. What is the difference between ADF Data Flow and Copy Activity?
6. How do you pass parameters between ADF activities?
7. How do you monitor ADF pipeline runs?

### Day 2 Checklist
- [ ] Understand ADF components — pipeline, activity, linked service, dataset, trigger
- [ ] Know integration runtime types and when to use each
- [ ] Understand ADLS Gen2 hierarchical namespace and access control
- [ ] Know Event Hubs architecture — partitions, consumer groups, capture
- [ ] Practice: design an incremental ingestion pipeline using ADF on paper

---

## Day 3 — Data Warehousing & Modelling

### Data Modelling Concepts
- **Star Schema** — fact table + dimension tables (simpler, faster queries)
- **Snowflake Schema** — normalized dimensions (less redundancy, more joins)
- **Data Vault** — hub, link, satellite (auditable, scalable)
- **Kimball** — bottom-up, star schema, business process focused
- **Inmon** — top-down, 3NF enterprise warehouse → data marts

### Azure Synapse Analytics
#### Pool Types
| Pool | Description | Use Case |
|------|-------------|----------|
| Dedicated SQL Pool | Provisioned MPP warehouse | BI dashboards, complex queries |
| Serverless SQL Pool | Query S3/ADLS on-demand | Ad-hoc queries, data exploration |
| Apache Spark Pool | Managed Spark clusters | Big data processing, ML |
| Data Explorer Pool | Time-series analytics | IoT, logs |

#### Dedicated SQL Pool Key Concepts
- **Distribution** — how data is spread across 60 distributions
  - ROUND_ROBIN — default, even spread
  - HASH — on a specific column, co-locate join data
  - REPLICATE — copy small tables to all nodes

- **Indexes**
  - Clustered Columnstore Index (CCI) — default, best for analytics
  - Heap — no index, best for staging tables
  - Clustered Rowstore — best for OLTP-style lookups

```sql
-- Hash distributed table
CREATE TABLE trips (
    trip_id INT,
    customer_id INT,
    fare_amount DECIMAL
)
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
);

-- Replicated table (small dimension)
CREATE TABLE zones (
    zone_id INT,
    zone_name VARCHAR(100)
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- Round robin staging table
CREATE TABLE staging_trips (
    trip_id INT,
    fare_amount DECIMAL
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
```

#### Synapse Serverless SQL Pool
```sql
-- Query Parquet files on ADLS directly
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<account>.dfs.core.windows.net/raw/nyc-tlc/year=2024/month=01/*.parquet',
    FORMAT = 'PARQUET'
) AS r;

-- Create external table
CREATE EXTERNAL TABLE nyc_tlc_yellow (
    vendor_id INT,
    fare_amount FLOAT,
    pickup_datetime DATETIME2
)
WITH (
    LOCATION = 'raw/nyc-tlc/yellow/year=*/month=*/*.parquet',
    DATA_SOURCE = adls_datasource,
    FILE_FORMAT = parquet_format
);
```

### Azure SQL Database / SQL Managed Instance
- Fully managed PaaS SQL Server
- DTU vs vCore purchasing model
- Elastic pools — share resources across multiple databases
- Read replicas for scaling reads
- Geo-replication for disaster recovery

### Delta Lake on Azure (Databricks)
```python
# Write Delta table
df.write.format("delta").mode("overwrite").save("abfss://raw@account.dfs.core.windows.net/nyc-tlc/")

# Read Delta table
df = spark.read.format("delta").load("abfss://raw@account.dfs.core.windows.net/nyc-tlc/")

# Time travel
df_old = spark.read.format("delta").option("versionAsOf", 5).load("abfss://...")

# MERGE (upsert)
from delta.tables import DeltaTable
target = DeltaTable.forPath(spark, "abfss://...")
target.alias("t").merge(
    source=df.alias("s"),
    condition="t.trip_id = s.trip_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Common Interview Questions — Day 3
1. What is the difference between Synapse Dedicated and Serverless SQL Pool?
2. When would you use HASH vs ROUND_ROBIN vs REPLICATE distribution?
3. What is a Clustered Columnstore Index and why is it default in Synapse?
4. How do you implement SCD Type 2 in Azure Synapse?
5. What is Delta Lake and how does it differ from regular Parquet?
6. How do you query ADLS data without loading it into Synapse?
7. What is PolyBase in Synapse?

### Day 3 Checklist
- [ ] Understand Synapse pool types and when to use each
- [ ] Know distribution types — HASH, ROUND_ROBIN, REPLICATE
- [ ] Understand Delta Lake — ACID, time travel, MERGE
- [ ] Know when to use Synapse vs Databricks vs Azure SQL
- [ ] Practice: write OPENROWSET queries against ADLS Parquet files

---

## Day 4 — Real-time Streaming

### Azure Event Hubs
| Concept | Description |
|---------|-------------|
| Namespace | Container for Event Hubs |
| Event Hub | Similar to Kafka topic |
| Partition | Unit of parallelism |
| Consumer Group | Independent reader |
| Capture | Auto-save to ADLS/Blob |
| Schema Registry | Centralized schema management |

### Event Hubs vs Service Bus vs Storage Queue
| | Event Hubs | Service Bus | Storage Queue |
|--|------------|-------------|---------------|
| Type | Event streaming | Message broker | Simple queue |
| Ordering | Per partition | FIFO (sessions) | No guarantee |
| Retention | Up to 90 days | Up to 14 days | Up to 7 days |
| Throughput | Very high | Medium | High |
| Use case | Telemetry, streaming | Enterprise messaging | Simple task queue |

### Azure Stream Analytics
- Serverless real-time SQL processing
- Input — Event Hubs, IoT Hub, Blob
- Output — ADLS, Synapse, Power BI, Event Hubs
- SQL-like query language with windowing

```sql
-- Tumbling window — count events per minute
SELECT
    TumblingWindow(minute, 1) AS window,
    trip_type,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
INTO output
FROM trips TIMESTAMP BY pickup_datetime
GROUP BY TumblingWindow(minute, 1), trip_type;

-- Sliding window — detect anomalies
SELECT *
INTO anomaly_output
FROM trips TIMESTAMP BY pickup_datetime
WHERE fare_amount > (
    SELECT AVG(fare_amount) * 3
    FROM trips TIMESTAMP BY pickup_datetime
    GROUP BY SlidingWindow(minute, 5)
);

-- Session window — user sessions
SELECT user_id, COUNT(*) AS events
INTO output
FROM events TIMESTAMP BY event_time
GROUP BY user_id, SessionWindow(minute, 5);
```

### Kafka on Azure (MSK equivalent)
- Event Hubs with Kafka API (recommended)
- HDInsight Kafka (managed Kafka cluster)
- Confluent Cloud on Azure (marketplace)

### Azure Databricks Structured Streaming
```python
# Read from Event Hubs
connection_str = "Endpoint=sb://..."
ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_str)
}

df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Parse JSON payload
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("fare_amount", DoubleType()),
    StructField("trip_type", StringType())
])

df_parsed = df_stream \
    .select(from_json(col("body").cast("string"), schema).alias("data")) \
    .select("data.*")

# Write to Delta Lake
df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://checkpoints@account.dfs.core.windows.net/trips/") \
    .start("abfss://silver@account.dfs.core.windows.net/trips/")
```

### Common Interview Questions — Day 4
1. What is the difference between Event Hubs and Service Bus?
2. How do you scale Event Hubs throughput?
3. What are the windowing functions in Stream Analytics?
4. How do you handle late-arriving events in Stream Analytics?
5. What is Event Hubs Capture and when would you use it?
6. How do you implement exactly-once processing in Databricks Structured Streaming?
7. When would you use Stream Analytics vs Databricks Structured Streaming?

### Day 4 Checklist
- [ ] Understand Event Hubs — partitions, consumer groups, capture
- [ ] Know Stream Analytics windowing — tumbling, sliding, session, hopping
- [ ] Understand Databricks Structured Streaming with Event Hubs
- [ ] Know Event Hubs vs Service Bus vs Storage Queue
- [ ] Practice: write Stream Analytics SQL for tumbling and sliding windows

---

## Day 5 — Azure Databricks & Spark

### Azure Databricks Architecture
```
Azure Databricks Workspace
        │
        ├── Clusters (Spark compute)
        │     ├── All-purpose (interactive)
        │     └── Job clusters (automated)
        │
        ├── Notebooks (Python/SQL/Scala/R)
        │
        ├── Jobs (scheduled pipelines)
        │
        ├── Delta Lake (storage layer)
        │
        └── Unity Catalog (governance)
```

### Cluster Types
| Type | Use Case | Cost |
|------|----------|------|
| All-purpose | Interactive development | Higher |
| Job cluster | Automated jobs | Lower |
| SQL Warehouse | Databricks SQL | Per DBU |

### Delta Lake Deep Dive
```python
# Create managed Delta table
spark.sql("""
    CREATE TABLE nyc_tlc_yellow (
        trip_id STRING,
        fare_amount DOUBLE,
        pickup_datetime TIMESTAMP,
        year INT,
        month INT
    )
    USING DELTA
    PARTITIONED BY (year, month)
    LOCATION 'abfss://silver@account.dfs.core.windows.net/yellow/'
""")

# MERGE (upsert pattern)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "abfss://silver@account.dfs.core.windows.net/yellow/")

delta_table.alias("target").merge(
    source=new_df.alias("source"),
    condition="target.trip_id = source.trip_id"
).whenMatchedUpdate(set={
    "fare_amount": "source.fare_amount",
    "pickup_datetime": "source.pickup_datetime"
}).whenNotMatchedInsertAll().execute()

# Time travel
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load("abfss://...")
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2024-01-14").load("abfss://...")

# Optimize (compaction)
spark.sql("OPTIMIZE silver.yellow_trips")

# Z-order (multi-dimensional clustering)
spark.sql("OPTIMIZE silver.yellow_trips ZORDER BY (pickup_datetime, zone_id)")

# Vacuum (remove old files)
spark.sql("VACUUM silver.yellow_trips RETAIN 168 HOURS")
```

### Unity Catalog
- Centralized governance for Databricks
- Three-level namespace — catalog.schema.table
- Fine-grained access control — table, column, row
- Data lineage tracking
- Audit logging

```python
# Unity Catalog three-level namespace
spark.sql("USE CATALOG prod_catalog")
spark.sql("USE SCHEMA nyc_tlc")
spark.sql("SELECT * FROM yellow_trips LIMIT 10")

# Full path
spark.sql("SELECT * FROM prod_catalog.nyc_tlc.yellow_trips")
```

### Databricks Workflows
```python
# Databricks job with multiple tasks
{
  "name": "nyc-tlc-pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": {"notebook_path": "/pipelines/01_bronze_ingestion"}
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": {"notebook_path": "/pipelines/02_silver_transform"}
    },
    {
      "task_key": "gold_aggregation",
      "depends_on": [{"task_key": "silver_transform"}],
      "notebook_task": {"notebook_path": "/pipelines/03_gold_aggregation"}
    }
  ]
}
```

### Databricks vs Synapse Spark
| | Databricks | Synapse Spark |
|--|-----------|---------------|
| Delta Lake | Native | Supported |
| Unity Catalog | Yes | No |
| MLflow | Native | Limited |
| Performance | Optimized Spark | Standard Spark |
| Cost | Higher | Lower |
| Best for | ML + ETL, Delta Lake | Azure-native, simpler ETL |

### Common Interview Questions — Day 5
1. What is Delta Lake and what problems does it solve?
2. What is the difference between OPTIMIZE and VACUUM in Delta Lake?
3. What is Z-ordering and when would you use it?
4. What is Unity Catalog and how does it differ from Hive Metastore?
5. When would you use Databricks over Synapse Spark?
6. How do you implement MERGE in Delta Lake?
7. What is Delta Lake time travel and how is it useful?

### Day 5 Checklist
- [ ] Understand Delta Lake — ACID, time travel, MERGE, OPTIMIZE, VACUUM
- [ ] Know Unity Catalog three-level namespace
- [ ] Understand Z-ordering vs partitioning
- [ ] Know Databricks cluster types
- [ ] Practice: write Delta Lake MERGE statements
- [ ] Know Databricks vs Synapse Spark tradeoffs

---

## Day 6 — System Design & Architecture

### Framework to Answer System Design Questions
1. **Clarify requirements** — scale, latency, SLA, data volume
2. **Identify source & target** — SQL DB, API, files → ADLS, Synapse
3. **Choose tools and justify** — always discuss trade-offs
4. **Handle failures** — retries, alerting, dead letter queues
5. **Monitoring** — Azure Monitor, Log Analytics, ADF monitoring
6. **Cost & scalability** — serverless vs provisioned

### Architecture Pattern 1 — Batch Ingestion Pipeline
```
Data Sources (Azure SQL / APIs / Files)
        │
        ▼
Azure Data Factory (Copy Activity)
        │
        ▼
ADLS Gen2 Bronze Layer
        │
        ▼
Azure Databricks / Synapse Spark (Transform)
        │
        ▼
ADLS Gen2 Silver Layer (Delta Lake)
        │
        ▼
Azure Databricks / Synapse Spark (Aggregate)
        │
        ▼
ADLS Gen2 Gold Layer + Synapse Dedicated Pool
        │
        ▼
Power BI / Synapse Serverless
```

### Architecture Pattern 2 — Real-time Streaming Pipeline
```
App / IoT Events
        │
        ▼
Azure Event Hubs (partitioned ingestion)
        │
        ├──► Stream Analytics (real-time SQL)
        │         │
        │         └──► Power BI (live dashboard)
        │
        ├──► Event Hubs Capture → ADLS (raw backup)
        │
        └──► Databricks Structured Streaming
                  │
                  ├──► Delta Lake Silver (cleaned)
                  └──► Synapse / Cosmos DB (serving)
```

### Architecture Pattern 3 — Modern Data Lakehouse
```
Sources
  ├── Azure SQL (CDC via ADF)
  ├── REST APIs (ADF Web Activity)
  ├── Files (SFTP / Blob)
  └── Event Hubs (streaming)
        │
        ▼
ADLS Gen2
  ├── Bronze — raw, immutable
  ├── Silver — cleaned, Delta Lake
  └── Gold — aggregated, business-ready
        │
        ├── Synapse Serverless (ad-hoc)
        ├── Synapse Dedicated (BI)
        ├── Databricks (ML/AI)
        └── Power BI (dashboards)

Governance:
  ├── Microsoft Purview (data catalog + lineage)
  ├── Unity Catalog (Databricks governance)
  └── Azure AD + RBAC + ADLS ACLs
```

### Architecture Pattern 4 — Lambda Architecture on Azure
```
Source Data
     │
     ├──► Batch Layer (ADF + Databricks + Synapse)
     │         └── Historical, accurate
     │
     ├──► Speed Layer (Event Hubs + Stream Analytics)
     │         └── Real-time, approximate
     │
     └──► Serving Layer (Synapse / Cosmos DB / Power BI)
               └── Merged batch + stream results
```

### Common System Design Questions
1. Design a batch pipeline to ingest on-prem SQL data into Azure Data Lakehouse
2. Design a real-time dashboard showing live event counts on Azure
3. Design a multi-source data platform using ADF + Databricks + Synapse
4. How would you migrate an on-prem data warehouse to Synapse?
5. Design a data platform that supports both batch and streaming on Azure
6. How would you implement data governance across a large Azure data lake?

### Monitoring & Observability on Azure
- **Azure Monitor** — metrics, logs, alerts for all Azure services
- **Log Analytics** — query logs with KQL (Kusto Query Language)
- **ADF Monitoring** — pipeline run history, activity logs
- **Databricks Ganglia UI** — Spark cluster metrics
- **Azure Alerts** — email/SMS/webhook on failures
- **Microsoft Purview** — data lineage and cataloging

### Cost Optimization on Azure
- Use ADLS Cool/Archive tier for infrequently accessed data
- Use Serverless SQL Pool for ad-hoc queries (pay per query)
- Use Job clusters in Databricks instead of all-purpose clusters
- Use Spot instances for Databricks non-critical jobs
- Use Synapse Serverless instead of Dedicated for exploration
- Use Parquet + Snappy to reduce storage and query costs
- Auto-pause Synapse Dedicated Pool when not in use

### Day 6 Checklist
- [ ] Practice drawing 2-3 Azure architecture diagrams from memory
- [ ] Be able to justify tool choices with trade-offs
- [ ] Know monitoring and alerting patterns on Azure
- [ ] Understand cost optimization strategies for Azure
- [ ] Practice answering system design questions out loud

---

## Day 7 — Mock Interview + Revision

### Morning — Revision Checklist

#### SQL
- [ ] Window functions — ROW_NUMBER, RANK, LAG, LEAD
- [ ] CTEs and subqueries
- [ ] Joins — INNER, LEFT, ANTI
- [ ] Aggregations with HAVING

#### Azure Services
- [ ] ADLS Gen2 — hierarchical namespace, access tiers, ACLs
- [ ] ADF — pipeline, activity, linked service, trigger, IR types
- [ ] Synapse — dedicated vs serverless, distribution types, CCI
- [ ] Databricks — Delta Lake, Unity Catalog, cluster types
- [ ] Event Hubs — partitions, consumer groups, capture
- [ ] Stream Analytics — windowing functions
- [ ] Azure SQL — DTU vs vCore, elastic pools
- [ ] Microsoft Purview — data governance, lineage

#### Spark & Delta Lake
- [ ] Transformations vs Actions
- [ ] Narrow vs Wide
- [ ] Repartition vs Coalesce
- [ ] Broadcast joins
- [ ] Delta MERGE, OPTIMIZE, VACUUM, Z-order
- [ ] Time travel

#### Streaming
- [ ] Event Hubs vs Service Bus vs Storage Queue
- [ ] Stream Analytics windowing
- [ ] Databricks Structured Streaming
- [ ] Delivery semantics

### Afternoon — Mock Interview Questions

#### Round 1 — SQL (30 mins)
1. Find the top 3 trip types by total revenue per year
2. Find months where revenue dropped more than 20% vs previous month
3. Calculate 3-month moving average of trip counts per trip type
4. Identify customers who made purchases in 3 or more consecutive months

#### Round 2 — Azure & Data Engineering (45 mins)
1. Walk me through a data pipeline you have built end to end
2. How would you design an incremental ADF pipeline that processes only new files?
3. What happens if your ADF pipeline fails halfway — how do you recover?
4. How do you handle schema changes in an ADLS-based data lake?
5. What is the difference between HASH and ROUND_ROBIN distribution in Synapse?
6. How do you optimize a Synapse query that's scanning too much data?

#### Round 3 — System Design (45 mins)
1. Design a pipeline to ingest on-prem SQL data into an Azure Data Lakehouse
2. Design a real-time dashboard showing live event counts using Azure services
3. How would you migrate a 10TB on-prem Oracle database to Azure Synapse?

#### Round 4 — Behavioral (30 mins)
Use the STAR method (Situation, Task, Action, Result):
1. Tell me about a data pipeline you built from scratch on Azure
2. Describe a time you optimized a slow Databricks/Synapse job
3. Tell me about a production incident you handled
4. Describe a time you had to learn a new Azure service quickly
5. How do you handle disagreements on technical decisions?

### Key Tips for Interview Day
- Always **clarify requirements** before answering system design questions
- Discuss **trade-offs** when choosing Azure tools
- Use **real examples** from your experience
- For SQL — think out loud, walk through your approach first
- For system design — start high-level, then go deep on each component

### Good Questions to Ask Interviewer
- What does the current Azure data stack look like?
- What are the biggest data engineering challenges the team faces?
- How is data quality measured and enforced on Azure?
- Is the team using Databricks, Synapse, or both?

---

## Quick Reference — Azure Services Cheat Sheet

| Service | Purpose | Key Feature |
|---------|---------|-------------|
| ADLS Gen2 | Data lake storage | Hierarchical namespace, ACLs |
| ADF | ETL orchestration | Low-code pipelines, 90+ connectors |
| Synapse Dedicated | Data warehouse | MPP, HASH/REPLICATE distribution |
| Synapse Serverless | Ad-hoc SQL on ADLS | Pay per query, OPENROWSET |
| Databricks | Spark + Delta Lake | Unity Catalog, optimized Spark |
| Event Hubs | Real-time ingestion | Kafka-compatible, partitions |
| Stream Analytics | Real-time SQL | Windowing, serverless |
| Azure SQL | Managed SQL Server | PaaS, elastic pools |
| Cosmos DB | NoSQL database | Multi-model, global distribution |
| Microsoft Purview | Data governance | Lineage, catalog, classification |
| Azure Monitor | Monitoring & logging | Metrics, alerts, Log Analytics |
| Key Vault | Secrets management | Manage credentials securely |

---

## Useful Resources

| Resource | Link | Purpose |
|----------|------|---------|
| LeetCode | https://leetcode.com | SQL and coding practice |
| StrataScratch | https://www.stratascratch.com | Data engineering SQL |
| Microsoft Learn | https://learn.microsoft.com | Official Azure documentation |
| Azure Architecture Center | https://learn.microsoft.com/azure/architecture | Architecture best practices |
| Databricks Docs | https://docs.databricks.com | Delta Lake, Unity Catalog |
| DP-203 Exam Guide | https://learn.microsoft.com/certifications/exams/dp-203 | Azure Data Engineer cert |
