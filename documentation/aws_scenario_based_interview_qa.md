# AWS Data Engineer — Scenario-Based Interview Q&A
### Real-World Scenarios | All Rounds | Mid-Level

---

## Table of Contents

1. [AWS Glue Scenarios](#1-aws-glue-scenarios)
2. [Amazon S3 & Data Lake Scenarios](#2-amazon-s3--data-lake-scenarios)
3. [Amazon Redshift Scenarios](#3-amazon-redshift-scenarios)
4. [Streaming & Kinesis Scenarios](#4-streaming--kinesis-scenarios)
5. [Apache Spark & EMR Scenarios](#5-apache-spark--emr-scenarios)
6. [Pipeline Design & System Design Scenarios](#6-pipeline-design--system-design-scenarios)
7. [Data Quality & Governance Scenarios](#7-data-quality--governance-scenarios)
8. [Failure & Recovery Scenarios](#8-failure--recovery-scenarios)
9. [Cost Optimization Scenarios](#9-cost-optimization-scenarios)
10. [Security & Access Control Scenarios](#10-security--access-control-scenarios)

---

## 1. AWS Glue Scenarios

---

**Q1. Your Glue job runs daily and processes files from S3. One day a file arrives late — 2 days after the scheduled run. How do you ensure it gets processed?**

**Answer:**
The issue is that a standard scheduled job will not re-process data it has already seen. There are 2 approaches:

- **Glue Job Bookmarks** — enabled by default with `--job-bookmark-option job-bookmark-enable`. Glue tracks which S3 keys were already processed using file modification timestamps. When the late file arrives, the next scheduled run will pick it up automatically because its `LastModified` is newer than the last bookmark checkpoint.
- **Event-driven trigger** — configure an S3 Event Notification → EventBridge → Glue job trigger. This way the job fires immediately when any new file lands, regardless of schedule.

**Best approach:** Combine both — bookmarks for idempotency + S3 event trigger for low latency.

---

**Q2. Your Glue job is reading from a source that has inconsistent data types — sometimes `fare_amount` is a string, sometimes a double. How do you handle this?**

**Answer:**
Use Glue `DynamicFrame` with `resolveChoice`:

```python
from awsglue.transforms import ResolveChoice

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="nyc_tlc_db",
    table_name="yellow_trips"
)

# Cast ambiguous column to double
dyf_resolved = ResolveChoice.apply(
    dyf,
    specs=[("fare_amount", "cast:double")]
)

# Or drop rows with type conflicts
dyf_resolved = ResolveChoice.apply(
    dyf,
    choice="drop_null_fields"
)
```

`DynamicFrame` stores type conflicts in a `choice` type rather than failing. `resolveChoice` then handles them explicitly. A `DataFrame` would fail on schema mismatch at read time.

---

**Q3. A Glue job that used to run in 20 minutes now takes 2 hours. Nothing in the code changed. What do you investigate?**

**Answer:**
Step-by-step investigation:

1. **Check CloudWatch metrics** — look at `glue.driver.BlockManager.disk.diskSpaceUsed_MB` and `glue.ALL.jvm.heap.usage`. High heap = memory spill.
2. **Check Spark UI** — look for skewed stages (one task takes 10x longer than others).
3. **Check data volume** — has the source data grown significantly? More rows = more Spark partitions needed.
4. **Check S3 small file growth** — if source bucket accumulated thousands of small files, S3 LIST calls slow down job startup.
5. **Check shuffle partitions** — default `spark.sql.shuffle.partitions=200` may be too low or too high for the data size.
6. **Check Glue version** — if infrastructure was updated and Glue version changed.

**Fix options:**
- Increase worker count or switch from G.1X to G.2X workers
- Compact small S3 files upstream
- Add `repartition()` after reading to set optimal partition count
- Enable AQE: `spark.conf.set("spark.sql.adaptive.enabled", "true")`

---

**Q4. You need to run a Glue job that downloads files from an HTTPS URL (like NYC TLC CloudFront) and uploads to S3. Should you use a Spark ETL job or Python Shell? Why?**

**Answer:**
Use **Python Shell** (not Spark ETL).

Reasons:
- Downloading files from HTTP and uploading to S3 is I/O-bound, not compute-bound. Spark's distributed processing adds no benefit here.
- Python Shell costs 0.0625 DPU vs minimum 2 DPUs for Spark ETL — ~32x cheaper.
- Simpler code using `requests` + `boto3` with no Spark overhead.
- Faster startup — Python Shell starts in seconds, Spark ETL takes ~2 minutes.

```python
# Python Shell — simple and cost-effective
import requests
import boto3

s3 = boto3.client('s3')
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
response = requests.get(url, stream=True)
s3.upload_fileobj(response.raw, 'my-bucket', 'raw/yellow/yellow_tripdata_2024-01.parquet')
```

Only switch to Spark ETL when you need to transform/aggregate the data itself.

---

**Q5. Your Glue crawler runs after a new file lands in S3 but it's adding duplicate tables in the Glue Data Catalog. Why and how do you fix it?**

**Answer:**
Common causes:
- **Path mismatch** — crawler is configured to scan `s3://bucket/raw/` but files are in `s3://bucket/raw/yellow/` and `s3://bucket/raw/green/` — it creates separate tables for each subfolder instead of one.
- **No Hive-style partitioning** — files named `yellow_2024_01.parquet` instead of `year=2024/month=01/yellow.parquet` cause the crawler to see each folder as a new table.
- **Multiple crawlers overlapping** — two crawlers scanning the same path.

**Fix:**
- Restructure S3 paths to Hive-style: `raw/yellow/year=2024/month=01/`
- Set crawler's `TableLevelConfiguration` to the correct depth
- Use a single crawler with the correct root path

---

## 2. Amazon S3 & Data Lake Scenarios

---

**Q6. Athena queries on your NYC TLC data are slow and expensive. The table has 6 years of data. How do you optimize?**

**Answer:**
Three layers of optimization:

**1. Partitioning**
```sql
-- Bad: full table scan
SELECT * FROM nyc_tlc WHERE YEAR(pickup_datetime) = 2024;

-- Good: partition pruning (add year/month as partition columns)
SELECT * FROM nyc_tlc WHERE year = 2024 AND month = 1;
```
S3 path: `raw/yellow/year=2024/month=01/file.parquet`

**2. File format + compression**
- Convert CSV/JSON to Parquet (columnar) + Snappy compression
- Reduces data scanned by 70–90%
- Athena charges $5/TB scanned — Parquet directly reduces cost

**3. Partition Projection (avoid MSCK REPAIR TABLE)**
```sql
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2019,2024'
)
```

**4. Compact small files**
- Many small files = many S3 API calls
- Use Glue job to compact: `df.coalesce(1).write.parquet(...)`

---

**Q7. You have raw data in S3 that contains PII (passenger names, phone numbers). Different teams need access — some should see PII, others should not. How do you architect this?**

**Answer:**
Use a **multi-layer architecture** with Lake Formation column-level security:

```
S3 Raw (Bronze) — full PII — only data engineers
         │
         ▼ Glue ETL (mask PII)
S3 Silver — PII masked with SHA256 hash — analysts
         │
         ▼ Glue ETL
S3 Gold — aggregated, no PII at all — BI tools
```

**Lake Formation column permissions:**
```python
# Grant analysts access to Silver table but exclude PII columns
lf.grant_permissions(
    Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123:role/AnalystRole'},
    Resource={
        'TableWithColumns': {
            'DatabaseName': 'silver_db',
            'Name': 'trips',
            'ColumnNames': ['trip_id', 'fare_amount', 'pickup_zone']  # no PII
        }
    },
    Permissions=['SELECT']
)
```

**PII masking in Glue:**
```python
from pyspark.sql.functions import sha2, col
df = df.withColumn("passenger_name", sha2(col("passenger_name"), 256))
```

---

**Q8. Your S3 storage costs are growing 20% month-over-month even though data volume is flat. What could be causing this and how do you fix it?**

**Answer:**
**Investigate:**
1. **Versioning enabled with no lifecycle policy** — every overwrite creates a new version, all versions are billed
2. **Failed multipart uploads** — incomplete uploads accumulate and are billed
3. **Wrong storage class** — all data in Standard when cold data should be in Glacier
4. **Duplicate data** — pipeline writing same data twice due to no idempotency check

**Fix:**

```json
{
  "Rules": [
    {
      "ID": "expire-old-versions",
      "Status": "Enabled",
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30 }
    },
    {
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    },
    {
      "ID": "transition-to-glacier",
      "Status": "Enabled",
      "Transitions": [
        { "Days": 90, "StorageClass": "GLACIER" }
      ]
    }
  ]
}
```

---

**Q9. You need to share data from your S3 data lake with an external partner without giving them AWS credentials or access to your account. How do you do it?**

**Answer:**
Multiple options depending on requirements:

| Option | When to Use |
|--------|-------------|
| Pre-signed URLs | One-time or short-lived access to specific files |
| S3 Bucket Policy (cross-account) | Partner has their own AWS account |
| AWS Data Exchange | Formal data product sharing with billing |
| AWS Lake Formation cross-account | Share catalog tables, not raw S3 |

**Pre-signed URL (simplest):**
```python
s3 = boto3.client('s3')
url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'my-bucket', 'Key': 'raw/data.parquet'},
    ExpiresIn=3600  # 1 hour
)
```

**Cross-account bucket policy (partner has AWS account):**
```json
{
  "Effect": "Allow",
  "Principal": { "AWS": "arn:aws:iam::PARTNER_ACCOUNT:root" },
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::my-bucket/shared/*"
}
```

---

## 3. Amazon Redshift Scenarios

---

**Q10. A Redshift query joining `trips` (500M rows) and `zones` (300 rows) is taking 10 minutes. How do you fix it?**

**Answer:**
The `zones` table is tiny (300 rows). The query is slow because Redshift is distributing `zones` data across nodes for the join (network shuffle).

**Fix: Use `DISTYLE ALL` on the small dimension table**

```sql
-- Recreate zones with DISTSTYLE ALL
-- This copies the entire zones table to every node
-- Eliminates network shuffle for joins
CREATE TABLE zones_new (
    zone_id INT,
    zone_name VARCHAR(100),
    borough VARCHAR(50)
)
DISTSTYLE ALL;

INSERT INTO zones_new SELECT * FROM zones;
```

After this, the join happens locally on each node — no network I/O.

**Also check `trips` table:**
```sql
-- If trips is queried with WHERE zone_id = X, set SORTKEY
CREATE TABLE trips (
    trip_id BIGINT,
    zone_id INT,
    fare_amount DECIMAL(10,2)
)
DISTKEY(zone_id)           -- co-locate with zones join
SORTKEY(pickup_datetime);  -- fast range scans by date
```

---

**Q11. You run a daily COPY into Redshift. After 6 months the table is getting slow to query. What maintenance do you perform?**

**Answer:**
Daily loads with no maintenance cause:
- **Unsorted blocks** — new data appended without respecting SORTKEY order
- **Deleted/updated rows** — rows marked for deletion but space not reclaimed

**Maintenance steps:**
```sql
-- 1. Check table health
SELECT "table", unsorted, stats_off, tbl_rows, skew_rows
FROM svv_table_info
WHERE "table" = 'trips';

-- 2. VACUUM to reclaim space and re-sort
VACUUM FULL trips;
-- For large tables, do it in stages:
VACUUM SORT ONLY trips;   -- re-sort first
VACUUM DELETE ONLY trips; -- reclaim space second

-- 3. ANALYZE to update statistics (query planner uses these)
ANALYZE trips;

-- 4. Check for skew after vacuum
SELECT slice, count(*) FROM stv_blocklist
WHERE tbl = (SELECT id FROM stv_tbl_perm WHERE name = 'trips')
GROUP BY slice;
```

For large tables, schedule VACUUM during off-peak hours. Redshift also does auto-vacuum in the background but manual vacuum is faster.

---

**Q12. A data analyst reports that their Redshift query is fast in the morning but very slow in the afternoon. What is causing this and how do you fix it?**

**Answer:**
This is a **WLM (Workload Management) queue** problem. In the afternoon, more concurrent users are running queries, exhausting the queue slots.

**Diagnose:**
```sql
-- Check query queue wait times
SELECT query, queue_start_time, exec_start_time,
       DATEDIFF(seconds, queue_start_time, exec_start_time) AS wait_seconds
FROM stl_wlm_query
ORDER BY queue_start_time DESC
LIMIT 20;
```

**Fix options:**

1. **Configure WLM queues** — separate queues for interactive vs batch
```json
{
  "query_groups": ["interactive"],
  "query_concurrency": 10,
  "memory_percent_to_use": 40
}
```

2. **Use Redshift Serverless** — auto-scales compute based on load

3. **Query result caching** — enable for repeated identical queries
```sql
SET enable_result_cache_for_session = ON;
```

4. **Use Concurrency Scaling** — automatically adds cluster capacity during peak times

---

## 4. Streaming & Kinesis Scenarios

---

**Q13. Your Kinesis stream consumer (Lambda) is getting `ProvisionedThroughputExceededException`. What does this mean and how do you fix it?**

**Answer:**
The consumer is reading faster than the shard allows (2 MB/s per shard read limit).

**Causes:**
- Too many Lambda invocations polling the same shard
- One shard receiving all traffic due to bad partition key design (hot shard)

**Fix options:**

1. **Add more shards** — split hot shards
```bash
aws kinesis split-shard \
  --stream-name nyc-tlc-stream \
  --shard-to-split shardId-000000000000 \
  --new-starting-hash-key 170141183460469231731687303715884105728
```

2. **Use Enhanced Fan-Out** — gives each consumer 2 MB/s dedicated throughput
```python
kinesis.subscribe_to_shard(
    ConsumerARN='arn:aws:kinesis:...:consumer/my-consumer',
    ShardId='shardId-000000000000',
    StartingPosition={'Type': 'LATEST'}
)
```

3. **Fix partition keys** — distribute records evenly
```python
# Bad: all records go to same shard
partition_key = "yellow_taxi"

# Good: distribute based on trip_id
partition_key = str(trip_id % 100)  # spreads across 100 possible shards
```

---

**Q14. You are building a real-time dashboard showing NYC taxi trips per zone every minute. Walk through the architecture.**

**Answer:**

```
Taxi App (events)
      │
      ▼
Kinesis Data Streams
(partition key = zone_id, 10 shards)
      │
      ├──► Kinesis Data Analytics (Apache Flink)
      │    -- Tumbling 1-min window
      │    -- Count trips per zone
      │    -- Output to DynamoDB
      │
      └──► Kinesis Firehose → S3
           (raw backup for replay)

DynamoDB → API Gateway → React Dashboard
                              ↑
                    WebSocket (real-time push)
```

**Flink SQL for 1-min tumbling window:**
```sql
SELECT
    TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS window_start,
    zone_id,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
FROM trips
GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), zone_id;
```

**Key design decisions:**
- DynamoDB for hot path (millisecond reads)
- S3 + Firehose for cold path (replay, historical analysis)
- Partition Kinesis by zone_id for locality

---

**Q15. A Glue Streaming job consuming from Kinesis is producing duplicate records in S3. How do you fix this?**

**Answer:**
Duplicates in Glue Streaming typically come from:
- **At-least-once delivery** — Kinesis retries cause the same record to be processed twice
- **Checkpoint failure** — job crashed before committing checkpoint, replays from last checkpoint

**Fix options:**

1. **Idempotent writes using deduplication key**
```python
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Deduplicate before writing
w = Window.partitionBy("trip_id").orderBy("event_time")
df_deduped = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
```

2. **Use Apache Iceberg with MERGE (exactly-once upsert)**
```python
# Iceberg supports ACID MERGE — safe to rerun
spark.sql("""
    MERGE INTO silver.trips t
    USING new_trips s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

3. **Use Glue checkpointing** — ensures job replays from last committed position
```python
glueContext.forEachBatch(
    frame=kinesis_df,
    batch_function=process_batch,
    options={"windowSize": "60 seconds", "checkpointLocation": "s3://bucket/checkpoints/"}
)
```

---

## 5. Apache Spark & EMR Scenarios

---

**Q16. Your PySpark job is reading 1TB of Parquet files from S3 but only 5 of the 200 tasks are actually doing work. The rest finish immediately. What is the problem?**

**Answer:**
This is the **small file problem** combined with **insufficient parallelism**.

**Diagnosis:**
- 200 tasks created but data spread unevenly
- Most tasks read tiny files (< 1 MB) and finish instantly
- 5 tasks read the large files and become bottleneck

**Fix:**
```python
# Option 1: Repartition after read to distribute work evenly
df = spark.read.parquet("s3://bucket/raw/")
df = df.repartition(200)  # forces even distribution

# Option 2: Control input partition size
spark.conf.set("spark.sql.files.maxPartitionBytes", "256mb")
spark.conf.set("spark.sql.files.openCostInBytes", "8mb")

# Option 3: Compact small files first (upstream fix)
df.coalesce(1).write.mode("overwrite").parquet("s3://bucket/compacted/")
```

**Root cause fix:** Upstream pipeline should write files of 128–256 MB to match HDFS block size.

---

**Q17. A Spark join between two large DataFrames takes 45 minutes. The Spark UI shows one stage taking 40 minutes while others take 1–2 minutes. What is the issue and how do you fix it?**

**Answer:**
This is **data skew** — one partition has far more records than others (e.g., a single `vendor_id` has 80% of all trips).

**Diagnose in Spark UI:**
- Stage timeline shows one task taking 40 min, others taking < 1 min
- Task metrics show one task processing 500M rows, others processing 1M

**Fix options:**

**Option 1: Salting (for skewed join key)**
```python
import random
from pyspark.sql.functions import col, concat, lit, floor, rand

NUM_SALTS = 10

# Add salt to the large (skewed) table
df_large = df_large.withColumn(
    "salted_key",
    concat(col("vendor_id"), lit("_"), (rand() * NUM_SALTS).cast("int").cast("string"))
)

# Explode the small table to match all salts
from pyspark.sql.functions import array, explode
df_small = df_small.withColumn("salt_array", array([lit(i) for i in range(NUM_SALTS)]))
df_small = df_small.withColumn("salt", explode("salt_array"))
df_small = df_small.withColumn(
    "salted_key",
    concat(col("vendor_id"), lit("_"), col("salt").cast("string"))
)

df_result = df_large.join(df_small, "salted_key").drop("salted_key", "salt", "salt_array")
```

**Option 2: Enable AQE skew join (Spark 3.x)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Option 3: Broadcast join (if one side is small enough)**
```python
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "vendor_id")
```

---

**Q18. You need to process 6 years of NYC TLC data (about 500GB) as a one-time historical backfill. Should you use Glue or EMR?**

**Answer:**
For a **one-time 500GB historical job**, use **EMR** — here is why:

| Factor | Glue | EMR |
|--------|------|-----|
| Startup | ~2 min cold start per job | Fast with pre-warmed cluster |
| Control | Limited Spark config | Full spark-defaults.conf control |
| Libraries | Limited custom JARs | Install anything via bootstrap |
| Cost | DPU-hour billing | EC2 Spot pricing (up to 90% cheaper) |
| Best for | Recurring small/medium ETL | One-time large batch processing |

**EMR Spot cluster for cost savings:**
```json
{
  "InstanceFleets": [{
    "InstanceFleetType": "CORE",
    "TargetSpotCapacityUnits": 20,
    "InstanceTypeConfigs": [
      {"InstanceType": "m5.2xlarge"},
      {"InstanceType": "m5.4xlarge"}
    ],
    "LaunchSpecifications": {
      "SpotSpecification": {
        "TimeoutDurationMinutes": 60,
        "TimeoutAction": "SWITCH_TO_ON_DEMAND"
      }
    }
  }]
}
```

For ongoing daily runs after the backfill, switch to Glue (serverless, no cluster management).

---

## 6. Pipeline Design & System Design Scenarios

---

**Q19. Design an incremental pipeline that syncs only new/changed records from an RDS MySQL database to S3 daily.**

**Answer:**

**Option A: DMS with CDC (recommended)**
```
RDS MySQL (binary log enabled)
       │
       ▼ CDC
AWS DMS (replication instance)
       │
       ▼
S3 (raw/cdc/table=trips/YYYY/MM/DD/HH/)
       │
       ▼ Glue ETL (daily)
       │   - Read CDC files
       │   - Apply INSERT/UPDATE/DELETE
       │   - Write UPSERT to S3 Silver
       ▼
S3 Silver (full current state)
```

**Option B: Glue with watermark column (if no CDC)**
```python
# Store last processed timestamp in DynamoDB or S3
last_run = get_last_run_time()  # e.g., 2024-01-14 00:00:00

df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://rds-endpoint/mydb") \
    .option("dbtable", f"(SELECT * FROM trips WHERE updated_at > '{last_run}') t") \
    .option("user", "<user>") \
    .option("password", "<password>") \
    .load()

# Save data then update watermark
df.write.mode("append").parquet("s3://bucket/silver/trips/")
save_last_run_time(current_run_time)
```

**Key design points:**
- DMS CDC captures deletes — watermark approach cannot
- Always write idempotently (upsert, not append)
- Validate row counts after each sync

---

**Q20. A business stakeholder says "the data in the dashboard is wrong." You check S3 and Redshift. How do you systematically debug this?**

**Answer:**
Follow the data lineage backwards:

```
Dashboard (wrong data)
        │
        ▼ Step 1: Check Redshift/Athena query
Is the SQL query correct? Check aggregations, joins, filters.

        │
        ▼ Step 2: Check Gold/Curated layer (S3)
SELECT COUNT(*), SUM(fare_amount) FROM gold.trips WHERE year=2024;
Does it match what dashboard shows?

        │
        ▼ Step 3: Check Silver layer
Do row counts match between Silver and Gold?
Did a Glue transformation introduce errors?

        │
        ▼ Step 4: Check Bronze/Raw layer
Do raw files from source match Silver counts?

        │
        ▼ Step 5: Check source system
Does source DB/API match what landed in Bronze?
```

**Common root causes:**
- Timezone issue — events in UTC stored as local time, daily aggregation shifts by hours
- Null handling — `SUM(fare_amount)` ignores NULLs, `COUNT(*)` does not
- Partition pruning issue — Athena query missing partition filter, showing partial data
- Glue job ran twice — duplicate records in Silver layer
- Schema change — a new column added upstream changed data meaning

---

**Q21. Your team is onboarding 5 new data sources into the data lake per month. How do you design a scalable, self-service ingestion framework?**

**Answer:**
Build a **metadata-driven pipeline framework**:

```
Config Store (DynamoDB or JSON in S3)
{
  "source_name": "nyc_tlc",
  "source_type": "http",
  "source_url": "https://...",
  "target_bucket": "my-bucket",
  "target_prefix": "raw/nyc-tlc/",
  "file_format": "parquet",
  "schedule": "monthly",
  "partition_cols": ["year", "month"]
}

         │
         ▼
Generic Glue ETL Job
(reads config, executes ingestion)

         │
         ▼
Glue Crawler (auto-discover schema)

         │
         ▼
Glue Data Catalog (auto-registered table)
```

**Benefits:**
- Adding a new source = adding a config entry, no code change
- Same job handles all sources
- All sources follow same S3 partitioning convention
- Crawler auto-registers new tables

**Step Functions for orchestration:**
```
EventBridge (schedule)
      │
      ▼
Step Functions
      ├── Start Glue Ingestion Job
      ├── Wait for Completion
      ├── Run Crawler
      ├── Run DQ Checks
      └── Notify on Success/Failure (SNS)
```

---

## 7. Data Quality & Governance Scenarios

---

**Q22. After a pipeline run, you discover 15% of `fare_amount` values are negative. How do you handle this?**

**Answer:**
This requires both **immediate fix** and **prevention**:

**Immediate: Quarantine bad records**
```python
df_valid   = df.filter(col("fare_amount") >= 0)
df_invalid = df.filter(col("fare_amount") < 0)

# Write valid to Silver
df_valid.write.mode("append").parquet("s3://bucket/silver/trips/")

# Write invalid to quarantine for investigation
df_invalid.write.mode("append").parquet("s3://bucket/quarantine/trips/")
```

**Prevention: Add DQ checks in pipeline**
```python
# Glue Data Quality ruleset
ruleset = """
Rules = [
    ColumnValues "fare_amount" >= 0,
    ColumnValues "trip_distance" >= 0,
    IsComplete "pickup_datetime",
    ColumnLength "vendor_id" between 1 and 10
]
"""
```

**Root cause investigation:**
- Refunds coded as negative fares in source system
- Data type overflow (DECIMAL precision issue)
- ETL bug in sign conversion

**Notify stakeholders** via SNS when quarantine threshold exceeds 1%.

---

**Q23. How do you implement data lineage in an AWS data lake so you can answer "where did this data come from and how was it transformed?"**

**Answer:**
Use **AWS Glue Data Catalog + CloudTrail + custom tagging**:

**Option 1: AWS Glue lineage (built-in)**
- Glue automatically records job run metadata in Data Catalog
- `GetJobRun` API shows which tables were read/written

**Option 2: OpenLineage with Marquez (open source)**
```python
# Emit lineage events from Glue job
from openlineage.client import OpenLineageClient
client = OpenLineageClient(url="http://marquez-server:5000")

# Records: source table → transformation → target table
```

**Option 3: Custom tagging approach (lightweight)**
```python
# Tag every S3 object with its source
s3.put_object_tagging(
    Bucket='my-bucket',
    Key='silver/trips/2024/01/data.parquet',
    Tagging={'TagSet': [
        {'Key': 'source', 'Value': 'nyc_tlc_cloudfront'},
        {'Key': 'pipeline', 'Value': 'nyc-tlc-ingestion-job'},
        {'Key': 'job_run_id', 'Value': job_run_id},
        {'Key': 'processed_at', 'Value': '2024-01-15T10:30:00Z'}
    ]}
)
```

**Option 4: Amazon DataZone** — AWS-native data governance with lineage, catalog, and access management.

---

## 8. Failure & Recovery Scenarios

---

**Q24. Your Glue job writes to S3 and then inserts into Redshift. The Glue job succeeds but the Redshift COPY fails. How do you handle partial failure?**

**Answer:**
This is a **two-phase commit** problem. Use this pattern:

```python
import boto3

def run_pipeline():
    s3_key = f"staging/trips/{job_run_id}/data.parquet"

    try:
        # Phase 1: Write to S3 staging (not the final path yet)
        df.write.parquet(f"s3://bucket/{s3_key}")

        # Phase 2: COPY to Redshift
        redshift_copy(s3_key)

        # Phase 3: Move from staging to final S3 path (atomic)
        s3.copy_object(
            Bucket='bucket',
            CopySource={'Bucket': 'bucket', 'Key': s3_key},
            Key=f"silver/trips/{date}/data.parquet"
        )
        s3.delete_object(Bucket='bucket', Key=s3_key)

        mark_success(job_run_id)

    except Exception as e:
        # Rollback: delete staging file, Redshift data NOT committed
        s3.delete_object(Bucket='bucket', Key=s3_key)
        raise
```

**Alternative: Use Step Functions with error handling**
```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::glue:startJobRun.sync",
  "Catch": [{
    "ErrorEquals": ["States.ALL"],
    "Next": "RollbackState"
  }]
}
```

**Best practice:** Always write to a staging location first, validate, then atomically move to production path.

---

**Q25. A Glue job that processes NYC TLC data has been failing silently for 3 days — no data in S3, no alerts fired. How do you prevent this in future?**

**Answer:**
Silent failures happen when:
- Job completes with exit code 0 but processed 0 records
- Job is scheduled but not triggered due to trigger misconfiguration
- DQ checks pass because empty DataFrame has no rule violations

**Prevention checklist:**

1. **CloudWatch alarm on job state**
```python
# Alarm if job not in SUCCEEDED state
cloudwatch.put_metric_alarm(
    AlarmName='GlueJobFailed-nyc-tlc',
    MetricName='glue.driver.aggregate.numFailedTasks',
    Namespace='Glue',
    Threshold=1,
    AlarmActions=['arn:aws:sns:us-east-1:123:alerts']
)
```

2. **Record count validation**
```python
output_count = df_output.count()
if output_count == 0:
    raise Exception(f"Pipeline produced 0 records — aborting job")
```

3. **Reconciliation check**
```python
source_count = get_source_record_count()
target_count = spark.read.parquet("s3://bucket/silver/").count()
if abs(source_count - target_count) / source_count > 0.05:
    raise Exception(f"Count mismatch: source={source_count}, target={target_count}")
```

4. **Heartbeat pattern** — write a `.done` file after successful job and monitor its absence
```python
s3.put_object(
    Bucket='bucket',
    Key=f'_SUCCESS/{date}/nyc_tlc.done',
    Body=json.dumps({'count': output_count, 'run_id': job_run_id})
)
```

5. **EventBridge rule** — alert if no `.done` file by 6 AM

---

## 9. Cost Optimization Scenarios

---

**Q26. Your monthly AWS bill for the data platform is $50,000. Leadership asks you to cut it by 30%. Where do you look first?**

**Answer:**
Investigate in order of typical cost impact:

**1. Redshift (usually highest cost)**
- Are clusters running 24/7 when used 8 hours/day? → Enable auto-pause for dev/test clusters
- Switch to Redshift Serverless for variable workloads
- Use Reserved Instances for stable production clusters (up to 75% savings)

**2. EMR Spot Instances**
```bash
# Replace On-Demand with Spot (up to 90% cheaper)
# Add fallback to On-Demand if Spot not available
```

**3. Glue job optimization**
- Replace Spark ETL jobs with Python Shell where Spark is not needed (32x cheaper)
- Right-size workers — G.2X for memory-intensive, G.1X for standard
- Set `--enable-continuous-cloudwatch-log` only when debugging (reduces CloudWatch cost)

**4. S3 storage**
- Enable Intelligent-Tiering for data with unknown access patterns
- Add lifecycle policies to move old data to Glacier
- Delete duplicate data from failed pipeline runs
- Abort incomplete multipart uploads

**5. Athena query cost**
- Enforce Parquet + partitioning for all tables
- Set workgroup data scanned limits to prevent expensive runaway queries
```sql
-- Workgroup limit: max 10GB per query
aws athena create-work-group --name cost-controlled \
  --configuration "BytesScannedCutoffPerQuery=10737418240"
```

---

**Q27. You have a Glue job running every 5 minutes to check if new files arrived in S3. It runs 288 times per day and usually finds nothing. How do you optimize?**

**Answer:**
This is a **polling anti-pattern**. Replace polling with **event-driven triggering**:

```
Before: Glue job → polls S3 every 5 min → 288 runs/day (mostly empty)

After:  S3 Object Created Event
             │
             ▼
        EventBridge Rule
             │
             ▼
        Glue Job Trigger (only fires when file arrives)
```

**Setup:**
```python
# S3 Event Notification → EventBridge
# EventBridge rule → Glue job

events = boto3.client('events')
events.put_rule(
    Name='nyc-tlc-file-arrival',
    EventPattern=json.dumps({
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {"name": ["my-bucket"]},
            "object": {"key": [{"prefix": "raw/nyc-tlc/"}]}
        }
    }),
    State='ENABLED'
)
```

**Cost savings:** From 288 Glue runs/day (most empty) → 1 run per actual file arrival. If 10 files arrive/day, that is 97% fewer job runs.

---

## 10. Security & Access Control Scenarios

---

**Q28. A data engineer accidentally deleted production data from S3. How do you recover it and prevent it from happening again?**

**Answer:**

**Immediate recovery:**
```bash
# If versioning was enabled, restore previous version
aws s3api list-object-versions \
  --bucket my-bucket \
  --prefix raw/nyc-tlc/ \
  --query 'DeleteMarkers[?IsLatest==`true`]'

# Remove delete markers to restore files
aws s3api delete-object \
  --bucket my-bucket \
  --key raw/nyc-tlc/yellow/2024/data.parquet \
  --version-id <delete-marker-version-id>
```

**Prevention checklist:**

1. **Enable S3 Versioning** on all production buckets (never disable for production)

2. **Enable S3 Object Lock (Compliance mode)** for critical raw data
```bash
aws s3api put-object-lock-configuration \
  --bucket my-bucket \
  --object-lock-configuration "ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=365}}"
```

3. **IAM least privilege** — data engineers should not have `s3:DeleteObject` on production buckets
```json
{
  "Effect": "Deny",
  "Action": ["s3:DeleteObject", "s3:DeleteBucket"],
  "Resource": "arn:aws:s3:::production-bucket/*"
}
```

4. **MFA Delete** — require MFA to delete S3 objects or disable versioning

5. **S3 Replication** — replicate production bucket to a backup bucket in another region

---

**Q29. A team member committed AWS access keys to a public GitHub repository. What do you do immediately?**

**Answer:**
Act within minutes — bots scan GitHub continuously and will use exposed keys within seconds.

**Immediate actions (in order):**

1. **Deactivate the key immediately**
```bash
aws iam update-access-key \
  --access-key-id AKIA... \
  --status Inactive \
  --user-name <username>
```

2. **Check CloudTrail for unauthorized usage**
```bash
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=AccessKeyId,AttributeValue=AKIA... \
  --start-time 2024-01-15T00:00:00Z
```

3. **Check for new IAM users, roles, or policies created**
```bash
aws iam list-users
aws iam list-roles
```

4. **Delete the access key** (after verifying no legitimate services use it)
```bash
aws iam delete-access-key --access-key-id AKIA... --user-name <username>
```

5. **Rotate all secrets** potentially exposed (DB passwords, API keys stored near the key)

6. **Report to security team** for incident response

**Prevention:**
- Use `git-secrets` or `trufflehog` pre-commit hooks to block key commits
- Never use long-lived access keys — use IAM roles instead
- Enable AWS GuardDuty — detects anomalous API calls from compromised keys

---

**Q30. How do you give a third-party analytics vendor (running on their own AWS account) read-only access to specific tables in your Glue Data Catalog without exposing raw S3 paths?**

**Answer:**
Use **AWS Lake Formation cross-account sharing**:

**Step 1: Share the Glue database via Lake Formation**
```python
lf = boto3.client('lakeformation')

# Grant SELECT on specific table to external account
lf.grant_permissions(
    Principal={
        'DataLakePrincipalIdentifier': 'arn:aws:iam::VENDOR_ACCOUNT_ID:root'
    },
    Resource={
        'Table': {
            'CatalogId': 'YOUR_ACCOUNT_ID',
            'DatabaseName': 'gold_db',
            'Name': 'trip_summary'
        }
    },
    Permissions=['SELECT'],
    PermissionsWithGrantOption=[]
)
```

**Step 2: Vendor accepts the RAM share**
- Lake Formation uses AWS Resource Access Manager (RAM) under the hood
- Vendor sees a shared database in their own Glue Data Catalog
- They can query via Athena — they never see the S3 bucket name or structure

**Benefits over direct S3 access:**
- No S3 bucket policy needed
- Column-level and row-level filtering applied transparently
- CloudTrail logs show what queries the vendor ran
- Revoke access instantly by removing Lake Formation permission

---

## Quick Scenario Answer Framework

Use this structure for any scenario question:

```
1. CLARIFY   — What is the exact problem? What are the constraints?
2. DIAGNOSE  — How do you identify the root cause?
3. SOLVE     — What is the immediate fix?
4. PREVENT   — How do you stop it from happening again?
5. TRADE-OFF — What are the pros/cons of your approach?
```

---

## Cheat Sheet — Scenario → Right Answer

| Scenario | Key Service/Solution |
|----------|---------------------|
| Process only new S3 files | Glue Job Bookmarks + S3 Event Trigger |
| Inconsistent schema in source | Glue DynamicFrame + resolveChoice |
| Slow Athena queries on 6 years data | Partition pruning + Parquet + Partition Projection |
| Hot shard in Kinesis | Better partition key distribution + shard split |
| Data skew in Spark join | Salting or AQE or Broadcast join |
| Small files in S3 | coalesce() / compaction Glue job |
| Duplicate records in streaming | Deduplication key + Iceberg MERGE |
| Redshift slow joins | DISTSTYLE ALL for small tables, DISTKEY for large |
| PII in data lake | Multi-layer with Lake Formation column masking |
| Cross-account data sharing | Lake Formation cross-account + RAM |
| Silent Glue job failure | Record count check + CloudWatch alarm + .done file |
| S3 data accidentally deleted | S3 Versioning + Object Lock + IAM Deny Delete |
| Polling anti-pattern | S3 Events → EventBridge → Glue trigger |
| Redshift slow during peak | WLM queue tuning + Concurrency Scaling |
| Cost too high | Spot EMR + Redshift Reserved + Glue Python Shell |
