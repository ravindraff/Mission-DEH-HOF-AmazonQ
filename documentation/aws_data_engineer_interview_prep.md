# AWS Data Engineer Interview Preparation Guide
### Level: Mid-Level | Timeline: 7 Days

---

## Overview

| Day | Focus Area |
|-----|-----------|
| Day 1 | Core Data Engineering + Python + SQL Basics |
| Day 2 | AWS Data Ingestion & Pipelines (S3, Glue, DMS) |
| Day 3 | Data Warehousing & Modelling (Redshift, Lake Formation) |
| Day 4 | Real-time Streaming (Kinesis, Kafka, MSK) |
| Day 5 | Apache Spark & EMR |
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
| Parquet | Columnar | Analytics, Athena, Glue |
| ORC | Columnar | Hive workloads |
| Avro | Row | Kafka, schema evolution |
| JSON | Row | APIs, semi-structured |
| Delta/Iceberg | Columnar + ACID | Lakehouse |

### Python (Must Know)
- List comprehensions, generators, decorators
- `boto3` — S3, Glue, Kinesis operations
- File handling — reading/writing Parquet with `pandas` and `pyarrow`
- Error handling — try/except, retries with `tenacity`
- `datetime` — date manipulation, timezone handling

```python
# Common boto3 patterns
import boto3

# S3 operations
s3 = boto3.client('s3')
s3.upload_file('local.parquet', 'my-bucket', 'prefix/file.parquet')
s3.download_file('my-bucket', 'prefix/file.parquet', 'local.parquet')

# List S3 objects with pagination
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='my-bucket', Prefix='raw/'):
    for obj in page.get('Contents', []):
        print(obj['Key'])
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
) WHERE DATEDIFF(login_date, prev_date) = 1;
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
- [ ] Write a boto3 script to upload/download files from S3
- [ ] Understand SCD Type 1, 2, 3 with examples

---

## Day 2 — AWS Data Ingestion & Pipelines

### Amazon S3
- Storage classes — Standard, IA, Glacier, Intelligent-Tiering
- Lifecycle policies — auto-transition objects between storage classes
- Partitioning strategy — `s3://bucket/raw/year=2024/month=01/`
- Event notifications — trigger Lambda/SQS/SNS on object upload
- S3 Select — query data inside objects without full download
- Versioning, Cross-Region Replication (CRR)

### AWS Glue
| Feature | Description |
|---------|-------------|
| Glue ETL Job | PySpark/Python Shell for data transformation |
| Glue Crawler | Auto-discovers schema and populates Data Catalog |
| Glue Data Catalog | Centralized metadata repository |
| Glue Job Bookmarks | Tracks processed data for incremental loads |
| Glue Triggers | Schedule or event-based job execution |
| Glue Workflows | Chain multiple jobs and crawlers |
| Glue DynamicFrame | Schema-flexible version of Spark DataFrame |

#### Glue Job Types
| Type | Use Case | Workers |
|------|----------|---------|
| Spark ETL | Large-scale transformations | G.1X, G.2X |
| Python Shell | Small scripts, boto3 tasks | 1 DPU or 0.0625 DPU |
| Streaming ETL | Real-time from Kinesis/Kafka | G.1X, G.2X |
| Ray | ML workloads | Z.2X |

#### Glue vs EMR
| | Glue | EMR |
|--|------|-----|
| Management | Serverless | Managed clusters |
| Startup | Slower (~2 min) | Faster (warm cluster) |
| Cost | Per DPU/hour | Per EC2 instance |
| Best for | Simple ETL, quick setup | Complex Spark, full control |

### AWS DMS (Database Migration Service)
- Full load — migrate entire table once
- CDC (Change Data Capture) — continuously replicate changes
- Source → Target: RDS, Aurora, Oracle → S3, Redshift, DynamoDB
- Replication instance — compute that runs the migration
- Endpoint — connection config for source/target

### AWS Lambda for Pipelines
- Event-driven — triggered by S3, Kinesis, DynamoDB Streams
- 15-minute timeout — not for long-running jobs
- Use for — file validation, metadata updates, triggering Glue jobs

### AWS Step Functions
- Orchestrate multi-step pipelines
- State machine — sequence of tasks with error handling
- Retry and catch — built-in retry logic
- Express vs Standard workflows

### Common Interview Questions — Day 2
1. How do you design an incremental data pipeline in AWS Glue?
2. What is a Glue job bookmark and how does it work?
3. How do you trigger a Glue job automatically when a file lands in S3?
4. What is the difference between Glue DynamicFrame and Spark DataFrame?
5. How would you handle schema evolution in a Glue pipeline?
6. When would you use DMS over Glue for data ingestion?
7. How do you monitor and alert on Glue job failures?

### Day 2 Checklist
- [ ] Understand Glue job types and when to use each
- [ ] Know S3 partitioning strategies and lifecycle policies
- [ ] Understand DMS full load vs CDC
- [ ] Know how Step Functions orchestrates pipelines
- [ ] Practice: design an end-to-end ingestion pipeline on paper

---

## Day 3 — Data Warehousing & Modelling

### Data Modelling Concepts
- **Star Schema** — fact table + dimension tables (simpler, faster queries)
- **Snowflake Schema** — normalized dimensions (less redundancy, more joins)
- **Data Vault** — hub, link, satellite (auditable, scalable)
- **Kimball** — bottom-up, star schema, business process focused
- **Inmon** — top-down, 3NF enterprise warehouse → data marts

### Amazon Redshift
#### Architecture
- Leader node — query planning, client connections
- Compute nodes — actual data storage and processing
- Node types — RA3 (managed storage), DC2 (compute-optimized)

#### Distribution Styles
| Style | When to Use |
|-------|-------------|
| EVEN | Default, no clear join key |
| KEY | Frequently joined columns |
| ALL | Small dimension tables |
| AUTO | Redshift chooses automatically |

#### Sort Keys
| Type | Description |
|------|-------------|
| Compound | Multiple columns in order, best for range filters |
| Interleaved | Equal weight to all columns, best for multi-column filters |

#### Performance Best Practices
- Choose correct `DISTKEY` on join/group columns
- Use `SORTKEY` on frequently filtered columns
- Run `VACUUM` after large deletes/updates (reclaims space)
- Run `ANALYZE` after large loads (updates statistics)
- Use `COPY` command for bulk loads (faster than INSERT)
- Use `Redshift Spectrum` to query S3 data directly

```sql
-- COPY command example
COPY my_table
FROM 's3://my-bucket/data/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- VACUUM and ANALYZE
VACUUM my_table;
ANALYZE my_table;
```

#### Redshift Serverless
- No cluster management
- Auto-scales compute
- Pay per query (RPU hours)
- Best for variable/unpredictable workloads

### AWS Lake Formation
- Centralized data lake governance
- Fine-grained access control (table, column, row level)
- Blueprints — pre-built workflows to ingest data
- Data lake permissions — on top of IAM
- Tag-based access control (LF-TBAC)

### Amazon Athena
- Serverless SQL on S3
- Supports Parquet, ORC, JSON, CSV
- Partition pruning — use `WHERE year=2024` to reduce scanned data
- Partition projection — no need to run MSCK REPAIR TABLE
- Pay per query — $5 per TB scanned
- Use columnar formats + compression to reduce cost

```sql
-- Athena partition projection example
CREATE EXTERNAL TABLE nyc_tlc (
    vendor_id INT,
    trip_distance DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://my-bucket/raw/nyc-tlc/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2019,2024',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12'
);
```

### Table Formats (Lakehouse)
| | Delta Lake | Apache Iceberg | Apache Hudi |
|--|-----------|---------------|-------------|
| ACID | Yes | Yes | Yes |
| Time Travel | Yes | Yes | Yes |
| Schema Evolution | Yes | Yes | Yes |
| Best With | Databricks | AWS (Athena, EMR) | AWS, Spark |
| AWS Native | No | Yes (Athena, Glue) | Yes (EMR) |

### Common Interview Questions — Day 3
1. How do you choose between DISTKEY styles in Redshift?
2. What is the difference between VACUUM and ANALYZE in Redshift?
3. How would you model a slowly changing dimension in Redshift?
4. When would you use Athena vs Redshift?
5. What is Lake Formation and how does it differ from IAM?
6. Explain the difference between Delta Lake, Iceberg, and Hudi.
7. How do you optimize Athena query costs?

### Day 3 Checklist
- [ ] Draw a star schema for a retail sales use case
- [ ] Understand Redshift distribution and sort keys
- [ ] Know when to use Athena vs Redshift vs Spectrum
- [ ] Understand Lake Formation permissions model
- [ ] Practice: write COPY command and VACUUM/ANALYZE statements

---

## Day 4 — Real-time Streaming

### Amazon Kinesis
| Service | Description | Use Case |
|---------|-------------|----------|
| Kinesis Data Streams | Real-time data ingestion | Custom consumers, low latency |
| Kinesis Data Firehose | Managed delivery to S3/Redshift | Simple delivery, no code |
| Kinesis Data Analytics | SQL/Flink on streams | Real-time aggregations |

#### Kinesis Data Streams Key Concepts
- **Shard** — unit of capacity (1 MB/s in, 2 MB/s out)
- **Partition key** — determines which shard a record goes to
- **Sequence number** — unique ID for each record
- **Retention** — 24 hours (default) to 365 days
- **Enhanced fan-out** — dedicated 2 MB/s per consumer

#### Kinesis vs SQS vs SNS
| | Kinesis | SQS | SNS |
|--|---------|-----|-----|
| Type | Streaming | Queue | Pub/Sub |
| Retention | Up to 365 days | Up to 14 days | No retention |
| Consumers | Multiple | Single | Multiple |
| Ordering | Per shard | Per FIFO queue | No ordering |
| Use case | Stream processing | Task queuing | Fan-out notifications |

### Amazon MSK (Managed Streaming for Kafka)
- Fully managed Apache Kafka on AWS
- Compatible with Kafka APIs
- MSK Serverless — no capacity planning
- MSK Connect — managed Kafka Connect connectors

#### Kafka Key Concepts
- **Broker** — server that stores messages
- **Topic** — category/feed of messages
- **Partition** — unit of parallelism within a topic
- **Consumer Group** — group sharing partition reads
- **Offset** — position of message in partition
- **Replication factor** — number of copies of each partition

#### Kafka vs Kinesis
| | Kafka/MSK | Kinesis |
|--|-----------|---------|
| Management | Self-managed or MSK | Fully managed |
| Retention | Configurable (forever) | Up to 365 days |
| Throughput | Very high | High |
| Cost | EC2 + storage | Per shard hour |
| Best for | Multi-cloud, high control | AWS-native streaming |

### Delivery Semantics
| Semantic | Description | Risk |
|----------|-------------|------|
| At-most-once | May lose messages | Data loss |
| At-least-once | May duplicate messages | Duplicates |
| Exactly-once | Each message processed once | Most complex |

### AWS Glue Streaming ETL
- Reads from Kinesis or Kafka
- Micro-batch processing
- Write to S3, Redshift
- Supports checkpointing

### Windowing Functions in Streaming
| Window | Description | Example |
|--------|-------------|---------|
| Tumbling | Fixed, non-overlapping | Count events per minute |
| Sliding | Fixed size, overlapping | 5-min window every 1 min |
| Session | Dynamic, gap-based | User session activity |

### Common Interview Questions — Day 4
1. What is a Kinesis shard and how do you scale it?
2. How do you handle duplicate records in a streaming pipeline?
3. When would you use Kinesis Firehose vs Kinesis Data Streams?
4. What is the difference between Kafka consumer groups and Kinesis enhanced fan-out?
5. How do you handle late-arriving data in a streaming pipeline?
6. What is exactly-once delivery and how is it achieved in Kafka?
7. How would you design a real-time fraud detection pipeline on AWS?

### Day 4 Checklist
- [ ] Understand Kinesis Streams vs Firehose vs Analytics
- [ ] Know Kafka architecture — brokers, topics, partitions, offsets
- [ ] Understand windowing — tumbling, sliding, session
- [ ] Know delivery semantics — at-most-once, at-least-once, exactly-once
- [ ] Practice: design a real-time clickstream pipeline on paper

---

## Day 5 — Apache Spark & EMR

### Apache Spark Core Concepts
- **RDD** — low-level, no optimization, fault-tolerant
- **DataFrame** — distributed table with schema, Catalyst optimized
- **Dataset** — type-safe + Catalyst (Scala/Java only)
- **Lazy Evaluation** — transformations execute only when action is called
- **DAG** — Directed Acyclic Graph of transformations

### Transformations vs Actions
| Transformations (Lazy) | Actions (Triggers execution) |
|----------------------|------------------------------|
| `map`, `filter` | `collect`, `count` |
| `groupBy`, `join` | `show`, `save` |
| `select`, `withColumn` | `write`, `foreach` |

### Narrow vs Wide Transformations
| | Narrow | Wide |
|--|--------|------|
| Shuffle | No | Yes |
| Examples | `map`, `filter`, `select` | `groupBy`, `join`, `distinct` |
| Performance | Fast | Slower (network I/O) |

### Spark Optimization
- **Repartition vs Coalesce**
  - `repartition(n)` — full shuffle, increase or decrease
  - `coalesce(n)` — no shuffle, only decrease

- **Broadcast Join** — for small tables (<= 10MB by default)
```python
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
```

- **Caching**
```python
df.cache()          # memory only
df.persist()        # configurable storage level
df.unpersist()      # release cache
```

- **Skew Handling**
  - Salting — add random prefix to skewed key
  - AQE (Adaptive Query Execution) — auto-handles skew in Spark 3.x
  - Broadcast join — if skewed table is small

- **Partitioning**
```python
df.repartition(100)                          # by count
df.repartition("year", "month")              # by column
df.write.partitionBy("year", "month").parquet("s3://bucket/path/")
```

### PySpark Common Operations
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Window function
window = Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rank", F.rank().over(window))

# Aggregations
df.groupBy("year", "month").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.count("*").alias("count")
)

# Handle nulls
df.fillna({"amount": 0, "name": "unknown"})
df.dropna(subset=["id", "date"])

# Date operations
df.withColumn("year", F.year("trip_date"))
df.withColumn("month", F.month("trip_date"))
```

### Amazon EMR
- Managed Hadoop/Spark clusters on EC2
- Supports — Spark, Hive, HBase, Presto, Flink
- EMR Serverless — no cluster management, auto-scales
- EMR on EKS — run Spark on Kubernetes

#### EMR vs Glue
| | EMR | Glue |
|--|-----|------|
| Control | Full control | Managed/Serverless |
| Startup | Faster (warm) | Slower (cold start) |
| Cost | EC2 pricing | DPU pricing |
| Best for | Complex Spark, custom libs | Simple ETL, quick jobs |

### Common Interview Questions — Day 5
1. What is the difference between `repartition` and `coalesce`?
2. How do you handle data skew in Spark?
3. Explain Catalyst optimizer in Spark.
4. When would you use EMR over Glue?
5. What happens when you call `.collect()` on a large dataset?
6. How do you tune a slow Spark job?
7. What is AQE and how does it help performance?

### Day 5 Checklist
- [ ] Understand RDD vs DataFrame vs Dataset
- [ ] Know narrow vs wide transformations
- [ ] Practice PySpark window functions and aggregations
- [ ] Understand repartition vs coalesce
- [ ] Know how to handle skewed data
- [ ] Understand EMR vs Glue tradeoffs

---

## Day 6 — System Design & Architecture

### Framework to Answer System Design Questions
1. **Clarify requirements** — scale, latency, SLA, data volume
2. **Identify source & target** — RDBMS, API, files → S3, Redshift, DynamoDB
3. **Choose tools and justify** — always discuss trade-offs
4. **Handle failures** — retries, DLQ, alerting
5. **Monitoring** — CloudWatch, CloudTrail, Glue job metrics
6. **Cost & scalability** — serverless vs provisioned

### Architecture Pattern 1 — Batch Ingestion Pipeline
```
Data Sources (RDS/APIs/Files)
        │
        ▼
   AWS Glue ETL / DMS
        │
        ▼
  S3 Raw (Bronze Layer)
        │
        ▼
   AWS Glue ETL (Transform)
        │
        ▼
  S3 Processed (Silver Layer)
        │
        ▼
  S3 Curated (Gold Layer)
        │
        ▼
Redshift / Athena / QuickSight
```

### Architecture Pattern 2 — Real-time Streaming Pipeline
```
App/IoT Events
      │
      ▼
Kinesis Data Streams / MSK
      │
      ▼
Glue Streaming / Lambda / KDA
      │
      ├──► S3 (cold path)
      │
      └──► Redshift / DynamoDB (hot path)
            │
            ▼
       QuickSight / Grafana
```

### Architecture Pattern 3 — Lambda Architecture
```
Source Data
     │
     ├──► Batch Layer (Glue + S3 + Redshift)
     │         │
     ├──► Speed Layer (Kinesis + Lambda)
     │         │
     └──► Serving Layer (Athena / Redshift / API)
```

### Architecture Pattern 4 — CDC Pipeline (RDS to Redshift)
```
RDS (source)
     │
     ▼
AWS DMS (CDC enabled)
     │
     ▼
S3 (staging)
     │
     ▼
AWS Glue (transform)
     │
     ▼
Redshift (target)
```

### Data Lakehouse on AWS
```
S3 (storage)
     │
     ├── Raw/Bronze — ingested as-is
     ├── Processed/Silver — cleaned, validated
     └── Curated/Gold — aggregated, business-ready
     │
     ▼
Glue Data Catalog (metadata)
     │
     ▼
Lake Formation (governance)
     │
     ├── Athena (ad-hoc SQL)
     ├── Redshift Spectrum (warehouse + lake)
     └── EMR/Glue (processing)
```

### Common System Design Questions
1. Design a batch pipeline to ingest NYC TLC data into a data lake
2. Design a real-time fraud detection system on AWS
3. Design an incremental data sync from RDS to Redshift
4. Design a data platform that supports both batch and streaming (Lambda architecture)
5. How would you design a multi-tenant data lake with fine-grained access control?
6. Design a pipeline that processes 1TB of data daily with < 1 hour SLA

### Monitoring & Observability
- **CloudWatch** — metrics, logs, alarms for Glue, Lambda, Kinesis
- **CloudTrail** — API call auditing
- **Glue Job Metrics** — bytes read/written, records processed
- **SNS + Lambda** — alerting on job failure
- **AWS Glue Data Quality** — built-in data quality checks

### Cost Optimization
- Use S3 Intelligent-Tiering for unknown access patterns
- Use Parquet + Snappy compression to reduce S3 storage and Athena scan costs
- Use Glue Python Shell (0.0625 DPU) for lightweight jobs
- Use Redshift Reserved Instances for predictable workloads
- Use Kinesis Firehose instead of Streams when real-time processing isn't needed

### Day 6 Checklist
- [ ] Practice drawing 2-3 architecture diagrams from memory
- [ ] Be able to justify tool choices with trade-offs
- [ ] Know monitoring and alerting patterns on AWS
- [ ] Understand cost optimization strategies
- [ ] Practice answering system design questions out loud

---

## Day 7 — Mock Interview + Revision

### Morning — Revision Checklist

#### SQL
- [ ] Window functions — ROW_NUMBER, RANK, LAG, LEAD
- [ ] CTEs and subqueries
- [ ] Joins — INNER, LEFT, ANTI
- [ ] Aggregations with HAVING

#### AWS Services
- [ ] S3 — partitioning, lifecycle, events
- [ ] Glue — job types, crawlers, bookmarks, DynamicFrame
- [ ] Redshift — DISTKEY, SORTKEY, VACUUM, ANALYZE, COPY
- [ ] Kinesis — Streams vs Firehose, shards, retention
- [ ] Athena — partition pruning, cost optimization
- [ ] DMS — full load vs CDC
- [ ] Lake Formation — governance, fine-grained access
- [ ] EMR — vs Glue, use cases
- [ ] Step Functions — orchestration, state machines

#### Spark
- [ ] Transformations vs Actions
- [ ] Narrow vs Wide
- [ ] Repartition vs Coalesce
- [ ] Broadcast joins
- [ ] Skew handling
- [ ] Caching

#### Streaming
- [ ] Kinesis Streams vs Firehose vs Analytics
- [ ] Kafka architecture
- [ ] Delivery semantics
- [ ] Windowing functions

### Afternoon — Mock Interview Questions

#### Round 1 — SQL (30 mins)
1. Find the top 3 trip types by total revenue per year from the NYC TLC dataset
2. Find months where revenue dropped more than 20% compared to the previous month
3. Identify drivers who completed trips in more than 3 different zones in a single day
4. Calculate the 3-month moving average of trip counts per trip type

#### Round 2 — AWS & Data Engineering (45 mins)
1. Walk me through a data pipeline you have built end to end
2. How would you design an incremental Glue job that processes only new files?
3. What happens if your Glue job fails halfway — how do you recover?
4. How do you handle schema changes in an S3-based data lake?
5. What is the difference between Redshift DISTKEY and SORTKEY?
6. How do you optimize an Athena query that's scanning too much data?

#### Round 3 — System Design (45 mins)
1. Design a pipeline to ingest NYC TLC data daily into a data lakehouse on AWS
2. Design a real-time dashboard showing live taxi trip counts by zone
3. How would you migrate a 10TB on-prem Oracle database to Redshift?

#### Round 4 — Behavioral (30 mins)
Use the STAR method (Situation, Task, Action, Result):

1. Tell me about a data pipeline you built from scratch
2. Describe a time you optimized a slow ETL job
3. Tell me about a production incident you handled
4. Describe a time you had to learn a new AWS service quickly
5. How do you handle disagreements on technical decisions?

### Key Tips for Interview Day
- Always **clarify requirements** before answering system design questions
- Discuss **trade-offs** when choosing tools — interviewers value this highly
- Use **real examples** from your experience (e.g., the NYC TLC pipeline)
- For SQL — think out loud, walk through your approach first
- For system design — start high-level, then go deep on components
- Ask **good questions** at the end — shows curiosity and engagement

### Good Questions to Ask Interviewer
- What does the current data stack look like?
- What are the biggest data engineering challenges the team faces?
- How is data quality measured and enforced?
- What does a typical sprint look like for the data engineering team?

---

## Quick Reference — AWS Services Cheat Sheet

| Service | Purpose | Key Feature |
|---------|---------|-------------|
| S3 | Object storage | Partitioning, lifecycle, events |
| Glue | ETL & catalog | Serverless Spark, crawlers |
| Redshift | Data warehouse | DISTKEY, SORTKEY, Spectrum |
| Athena | Serverless SQL | Query S3, pay per scan |
| Kinesis Streams | Real-time ingestion | Shards, 365-day retention |
| Kinesis Firehose | Managed delivery | S3/Redshift, no code |
| MSK | Managed Kafka | Kafka-compatible, serverless |
| DMS | Database migration | Full load + CDC |
| EMR | Managed Hadoop/Spark | Full control, custom libs |
| Lake Formation | Data lake governance | Fine-grained access control |
| Step Functions | Pipeline orchestration | State machines, retries |
| Lambda | Serverless compute | Event-driven, 15-min limit |
| CloudWatch | Monitoring & logging | Metrics, alarms, dashboards |

---

## Useful Resources

| Resource | Link | Purpose |
|----------|------|---------|
| LeetCode | https://leetcode.com | SQL and coding practice |
| StrataScratch | https://www.stratascratch.com | Data engineering SQL |
| AWS Docs | https://docs.aws.amazon.com | Official AWS documentation |
| AWS Well-Architected | https://aws.amazon.com/architecture/well-architected | Architecture best practices |
| Spark Docs | https://spark.apache.org/docs/latest | PySpark reference |
