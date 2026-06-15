# Data Engineer Interview Preparation Guide
### Level: Mid-Level (3 Years Experience)

---

## Table of Contents

1. [Core Data Engineering Concepts](#1-core-data-engineering-concepts)
2. [SQL](#2-sql)
3. [Apache Spark](#3-apache-spark)
4. [AWS Core Services](#4-aws-core-services)
5. [Azure Core Services](#5-azure-core-services)
6. [Data Warehousing](#6-data-warehousing)
7. [Orchestration & Workflow](#7-orchestration--workflow)
8. [Streaming](#8-streaming)
9. [System Design](#9-system-design)
10. [Behavioral](#10-behavioral)

---

## 1. Core Data Engineering Concepts

### ETL vs ELT
| | ETL | ELT |
|---|---|---|
| Transformation | Before loading | After loading |
| Best for | On-prem, limited storage | Cloud, scalable storage |
| Tools | Glue, Informatica | dbt, Spark, Snowflake |

### Batch vs Streaming
| | Batch | Streaming |
|---|---|---|
| Latency | High (minutes/hours) | Low (seconds/milliseconds) |
| Use case | Daily reports, historical loads | Real-time dashboards, fraud detection |
| Tools | Glue, Spark, ADF | Kafka, Kinesis, Spark Streaming |

### Key Concepts to Know
- **Idempotency** — running a pipeline multiple times produces the same result
- **Exactly-once semantics** — each record is processed exactly once
- **Data partitioning** — dividing data by a column (e.g., year, date) for faster queries
- **Slowly Changing Dimensions (SCD)**
  - Type 1 — Overwrite old value
  - Type 2 — Add new row with effective dates
  - Type 3 — Add new column for old value

### Data Modeling
- **Star Schema** — fact table surrounded by dimension tables (simpler, faster queries)
- **Snowflake Schema** — normalized dimensions (less redundancy, more joins)
- **Data Vault** — hub, link, satellite pattern for auditable, scalable warehouses
- **OLTP** — optimized for transactions (row-based)
- **OLAP** — optimized for analytics (columnar)

### File Formats
| Format | Type | Best For |
|---|---|---|
| Parquet | Columnar | Analytics, Spark, Athena |
| ORC | Columnar | Hive workloads |
| Avro | Row | Kafka, schema evolution |
| JSON | Row | APIs, semi-structured data |
| Delta/Iceberg | Columnar + ACID | Data lakehouse |

---

## 2. SQL

### Topics to Master
- Window functions — `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`
- CTEs vs subqueries vs temp tables
- Query optimization and execution plans
- Joins — INNER, LEFT, RIGHT, FULL, ANTI, SELF
- Aggregations — `GROUP BY`, `GROUPING SETS`, `ROLLUP`, `CUBE`
- Indexing strategies

### Practice Questions

**Find the second highest salary per department:**
```sql
SELECT department, salary
FROM (
    SELECT department, salary,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM employees
) t
WHERE rnk = 2;
```

**Deduplicate records keeping the latest row:**
```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM records
) t
WHERE rn = 1;
```

**7-day rolling average:**
```sql
SELECT date,
       AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM daily_metrics;
```

**Users who logged in on consecutive days:**
```sql
SELECT DISTINCT user_id
FROM (
    SELECT user_id, login_date,
           LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS prev_date
    FROM logins
) t
WHERE DATEDIFF(login_date, prev_date) = 1;
```

---

## 3. Apache Spark

### Core Concepts
- **RDD vs DataFrame vs Dataset**
  - RDD — low-level, no optimization, type-safe
  - DataFrame — distributed table with schema, Catalyst optimized
  - Dataset — type-safe + Catalyst optimized (Scala/Java only)

- **Lazy Evaluation** — transformations are not executed until an action is called
- **Narrow vs Wide Transformations**
  - Narrow — `map`, `filter` — no shuffle
  - Wide — `groupBy`, `join`, `distinct` — triggers shuffle

- **Repartition vs Coalesce**
  - `repartition(n)` — full shuffle, increases or decreases partitions
  - `coalesce(n)` — no shuffle, only decreases partitions

- **Caching** — `cache()` stores in memory, `persist()` allows storage level control

### Performance Tuning
- Avoid wide transformations where possible
- Use broadcast joins for small tables (`spark.sql.autoBroadcastJoinThreshold`)
- Handle data skew with salting or AQE (Adaptive Query Execution)
- Tune `spark.executor.memory`, `spark.executor.cores`, `spark.default.parallelism`
- Use Parquet + predicate pushdown for faster reads

### Common Interview Questions
- How do you handle skewed data in Spark?
  - Salting keys, AQE, broadcast joins
- What happens when you call `.collect()` on a large dataset?
  - Pulls all data to driver — risk of OOM
- Explain the Catalyst optimizer
  - Logical plan → Optimized logical plan → Physical plan → Code generation
- How do you tune a slow Spark job?
  - Check DAG, identify shuffle stages, tune parallelism, fix skew, use caching

---

## 4. AWS Core Services

| Service | What to Know |
|---|---|
| **S3** | Partitioning, lifecycle policies, storage classes, event notifications |
| **Glue** | ETL jobs, crawlers, Data Catalog, job bookmarks, Glue vs EMR |
| **Redshift** | Distribution keys (EVEN, KEY, ALL), sort keys, VACUUM, ANALYZE, Spectrum |
| **Athena** | Partitioning, partition projection, query optimization, supported formats |
| **Kinesis** | Streams vs Firehose vs Analytics, shards, retention |
| **Lambda** | Event-driven pipelines, 15-min timeout limitation, cold starts |
| **Step Functions** | Orchestrating pipelines, state machines, error handling |
| **EMR** | Spark/Hive on EMR, cluster vs serverless, EMR vs Glue tradeoffs |
| **Lake Formation** | Data lake governance, fine-grained access control, blueprints |

### Glue vs EMR
| | Glue | EMR |
|---|---|---|
| Management | Serverless | Managed clusters |
| Cost | Per DPU hour | Per EC2 instance |
| Best for | Simple ETL, quick setup | Complex Spark, full control |
| Startup time | Slower (cold start) | Faster (warm cluster) |

---

## 5. Azure Core Services

| Service | What to Know |
|---|---|
| **Azure Data Factory (ADF)** | Pipelines, triggers, linked services, integration runtimes |
| **Azure Databricks** | Spark clusters, Delta Lake, Unity Catalog, notebooks |
| **Azure Synapse Analytics** | Dedicated vs serverless SQL pools, Spark pools, pipelines |
| **ADLS Gen2** | Hierarchical namespace, RBAC, ACLs, access tiers |
| **Event Hubs** | Streaming ingestion, partitions, consumer groups, Kafka compatibility |
| **Stream Analytics** | Real-time processing, windowing functions (tumbling, hopping, sliding) |

### ADF vs Glue vs Airflow
| | ADF | Glue | Airflow |
|---|---|---|---|
| Cloud | Azure | AWS | Any |
| Code | Low-code UI | PySpark/Python | Python DAGs |
| Orchestration | Yes | Limited | Yes |
| Best for | Azure-native pipelines | AWS ETL jobs | Complex workflows |

---

## 6. Data Warehousing

### Methodologies
- **Kimball** — bottom-up, star schema, business process focused
- **Inmon** — top-down, 3NF enterprise warehouse, then data marts

### Key Concepts
- **Incremental load** — only load new/changed data (using watermarks or CDC)
- **Full load** — reload entire dataset every time
- **Late-arriving data** — handle with watermarks, reprocessing windows
- **CDC (Change Data Capture)** — capture row-level changes from source DB

### Table Formats (Lakehouse)
| | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| ACID | Yes | Yes | Yes |
| Time travel | Yes | Yes | Yes |
| Best with | Databricks | AWS (Athena, EMR) | AWS, Spark |
| Schema evolution | Yes | Yes | Yes |

### Redshift Best Practices
- Choose **DISTKEY** on join/group columns
- Use **SORTKEY** on filter columns
- Run `VACUUM` after large deletes/updates
- Run `ANALYZE` after large loads
- Use **Redshift Spectrum** to query S3 directly

---

## 7. Orchestration & Workflow

### Apache Airflow
- **DAG** — Directed Acyclic Graph, defines the pipeline workflow
- **Operators** — define a single task (PythonOperator, BashOperator, S3Operator)
- **Sensors** — wait for a condition (S3KeySensor, ExternalTaskSensor)
- **XComs** — pass small data between tasks
- **Backfill** — re-run historical DAG runs
- **Catchup** — automatically run missed DAG runs on startup

### Airflow vs Step Functions vs ADF
| | Airflow | Step Functions | ADF |
|---|---|---|---|
| Cloud | Any | AWS | Azure |
| Complexity | High flexibility | Medium | Low-code |
| Best for | Complex multi-system workflows | AWS service orchestration | Azure-native pipelines |

---

## 8. Streaming

### Apache Kafka
- **Broker** — server that stores and serves messages
- **Topic** — category of messages
- **Partition** — unit of parallelism within a topic
- **Consumer Group** — group of consumers sharing partition reads
- **Offset** — position of a message in a partition

### Kafka vs Kinesis
| | Kafka | Kinesis |
|---|---|---|
| Management | Self-managed or Confluent | Fully managed by AWS |
| Retention | Configurable (days to forever) | 1–365 days |
| Throughput | Very high | High |
| Best for | Multi-cloud, high control | AWS-native streaming |

### Delivery Semantics
| | Description | Risk |
|---|---|---|
| At-most-once | Message may be lost | Data loss |
| At-least-once | Message may be duplicated | Duplicates |
| Exactly-once | Message processed exactly once | Most complex |

### Spark Structured Streaming
- Watermarking — handles late-arriving data
- Output modes — `append`, `complete`, `update`
- Windowing — tumbling, sliding, session windows

---

## 9. System Design

### Framework to Answer
1. **Clarify requirements** — scale, latency, SLA, data volume
2. **Identify source & target** — RDBMS, API, files → data warehouse, S3
3. **Choose tools and justify** — always discuss trade-offs
4. **Handle failures** — retries, dead-letter queues, alerting
5. **Monitoring** — CloudWatch, Datadog, Grafana
6. **Cost & scalability** — serverless vs provisioned

### Common Design Questions

**Batch Ingestion Pipeline (e.g., NYC TLC project we built):**
```
NYC TLC HTTPS → AWS Glue ETL → S3 (raw) → Glue Crawler
→ Athena / Redshift Spectrum for querying
```

**Real-time Streaming Pipeline:**
```
App Events → Kafka / Kinesis → Spark Structured Streaming
→ S3 / Redshift → Dashboard
```

**Data Lake on AWS:**
```
Sources (RDS, APIs, Files)
→ S3 Raw (ingestion layer)
→ S3 Processed (Glue ETL, Delta/Iceberg)
→ S3 Curated (aggregated, business-ready)
→ Athena / Redshift Spectrum / Redshift
→ BI Tools (QuickSight, Tableau)
```

**Incremental Sync from RDBMS to Data Warehouse:**
```
RDS → AWS DMS (CDC) → S3 → Glue → Redshift
```

---

## 10. Behavioral (STAR Method)

**Format:** Situation → Task → Action → Result

### Prepare Stories For:

| Question | What to Highlight |
|---|---|
| Tell me about a pipeline you built end to end | Design decisions, tools chosen, outcome |
| A time you optimized a slow job | Root cause, fix, performance improvement % |
| A production incident you handled | How you diagnosed, fixed, and prevented recurrence |
| A time you disagreed with a technical decision | How you communicated, outcome, professionalism |
| Collaborating with data scientists or analysts | Communication, requirements gathering, delivery |

---

## Quick Tips

- Always discuss **trade-offs** when choosing tools — interviewers love this
- Relate answers to **real projects** (e.g., the NYC TLC ingestion pipeline)
- For system design — think out loud, clarify assumptions, draw components
- For SQL — practice on [LeetCode](https://leetcode.com) and [StrataScratch](https://www.stratascratch.com)
- For Spark — be ready to write and explain code
- Know your numbers — data volumes, job run times, optimization gains from your past work
