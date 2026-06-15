# AWS Data Engineer Interview — Deep Dive Guide
### Level: Mid-Level | All Topics | All Rounds

---

## Table of Contents

1. [Deep Dive — SQL](#1-deep-dive--sql)
2. [Deep Dive — Python & boto3](#2-deep-dive--python--boto3)
3. [Deep Dive — AWS Glue](#3-deep-dive--aws-glue)
4. [Deep Dive — Amazon S3](#4-deep-dive--amazon-s3)
5. [Deep Dive — AWS DMS](#5-deep-dive--aws-dms)
6. [Deep Dive — Amazon Redshift](#6-deep-dive--amazon-redshift)
7. [Deep Dive — Amazon Athena](#7-deep-dive--amazon-athena)
8. [Deep Dive — AWS Lake Formation](#8-deep-dive--aws-lake-formation)
9. [Deep Dive — Amazon Kinesis](#9-deep-dive--amazon-kinesis)
10. [Deep Dive — Amazon MSK & Kafka](#10-deep-dive--amazon-msk--kafka)
11. [Deep Dive — Apache Spark & EMR](#11-deep-dive--apache-spark--emr)
12. [Deep Dive — System Design](#12-deep-dive--system-design)
13. [Deep Dive — Behavioral](#13-deep-dive--behavioral)
14. [Top 100 Interview Questions & Answers](#14-top-100-interview-questions--answers)

---

## 1. Deep Dive — SQL

### Window Functions

```sql
-- ROW_NUMBER: unique rank per partition
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees;

-- RANK: same rank for ties, skips next rank
SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
FROM employees;

-- DENSE_RANK: same rank for ties, no skipping
SELECT *, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dense_rnk
FROM employees;

-- LAG: access previous row value
SELECT *, LAG(sales, 1, 0) OVER (PARTITION BY region ORDER BY date) AS prev_sales
FROM sales;

-- LEAD: access next row value
SELECT *, LEAD(sales, 1, 0) OVER (PARTITION BY region ORDER BY date) AS next_sales
FROM sales;

-- NTILE: divide rows into N buckets
SELECT *, NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM employees;

-- FIRST_VALUE / LAST_VALUE
SELECT *, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS highest_salary
FROM employees;

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
    SELECT DATE_TRUNC('month', trip_date) AS month,
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

**Problem 2: Find customers who purchased in consecutive months**
```sql
WITH monthly_purchases AS (
    SELECT customer_id,
           DATE_TRUNC('month', purchase_date) AS month,
           LAG(DATE_TRUNC('month', purchase_date)) 
               OVER (PARTITION BY customer_id ORDER BY purchase_date) AS prev_month
    FROM purchases
)
SELECT DISTINCT customer_id
FROM monthly_purchases
WHERE DATEDIFF('month', prev_month, month) = 1;
```

**Problem 3: Top N per group**
```sql
-- Top 3 products per category by revenue
SELECT category, product, revenue
FROM (
    SELECT category, product, revenue,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM product_sales
) t
WHERE rn <= 3;
```

**Problem 4: Gaps and Islands (find consecutive date ranges)**
```sql
WITH numbered AS (
    SELECT date,
           ROW_NUMBER() OVER (ORDER BY date) AS rn,
           date - INTERVAL (ROW_NUMBER() OVER (ORDER BY date)) DAY AS grp
    FROM active_dates
)
SELECT MIN(date) AS start_date, MAX(date) AS end_date
FROM numbered
GROUP BY grp
ORDER BY start_date;
```

**Problem 5: Median calculation**
```sql
-- Redshift / standard SQL
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) AS median_fare
FROM trips;
```

**Problem 6: Pivot table**
```sql
SELECT year,
       SUM(CASE WHEN trip_type = 'yellow' THEN revenue END) AS yellow,
       SUM(CASE WHEN trip_type = 'green' THEN revenue END) AS green,
       SUM(CASE WHEN trip_type = 'fhvhv' THEN revenue END) AS fhvhv
FROM trip_revenue
GROUP BY year;
```

**Problem 7: Self join to find pairs**
```sql
-- Find employees in same department earning within $1000 of each other
SELECT a.name, b.name, a.dept, a.salary, b.salary
FROM employees a
JOIN employees b ON a.dept = b.dept
                 AND a.id < b.id
                 AND ABS(a.salary - b.salary) <= 1000;
```

### Query Optimization Tips
- Use `EXPLAIN` / `EXPLAIN ANALYZE` to view execution plan
- Avoid `SELECT *` — select only needed columns
- Filter early — push `WHERE` clauses as close to source as possible
- Use `EXISTS` instead of `IN` for subqueries with large datasets
- Avoid functions on indexed/sort key columns in `WHERE` clause
- Partition pruning — always filter on partition columns
- Use CTEs for readability but be aware some engines materialize them

---

## 2. Deep Dive — Python & boto3

### boto3 Patterns

```python
import boto3
from botocore.exceptions import ClientError

# ── S3 Operations ──────────────────────────────────────────────
s3 = boto3.client('s3')

# Upload file
s3.upload_file('local.parquet', 'my-bucket', 'prefix/file.parquet')

# Download file
s3.download_file('my-bucket', 'prefix/file.parquet', 'local.parquet')

# List objects with pagination
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='my-bucket', Prefix='raw/'):
    for obj in page.get('Contents', []):
        print(obj['Key'], obj['Size'])

# Check if object exists
def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

# Read Parquet from S3 into pandas
import pandas as pd
df = pd.read_parquet('s3://my-bucket/raw/data.parquet')

# Write Parquet to S3
df.to_parquet('s3://my-bucket/processed/data.parquet', index=False)

# ── Glue Operations ────────────────────────────────────────────
glue = boto3.client('glue', region_name='us-east-1')

# Start Glue job
response = glue.start_job_run(
    JobName='my-glue-job',
    Arguments={'--jobtype': 'historical'}
)
job_run_id = response['JobRunId']

# Check job status
status = glue.get_job_run(JobName='my-glue-job', RunId=job_run_id)
print(status['JobRun']['JobRunState'])

# ── Kinesis Operations ─────────────────────────────────────────
kinesis = boto3.client('kinesis', region_name='us-east-1')

# Put record
kinesis.put_record(
    StreamName='my-stream',
    Data=b'{"event": "trip_started", "driver_id": "123"}',
    PartitionKey='driver_123'
)

# Get records
shard_iterator = kinesis.get_shard_iterator(
    StreamName='my-stream',
    ShardId='shardId-000000000000',
    ShardIteratorType='TRIM_HORIZON'
)['ShardIterator']

records = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)

# ── SSM Parameter Store ────────────────────────────────────────
ssm = boto3.client('ssm')

# Get secret/config
param = ssm.get_parameter(Name='/myapp/db_password', WithDecryption=True)
password = param['Parameter']['Value']
```

### Error Handling & Retries
```python
import time
from functools import wraps

def retry(max_attempts=3, delay=2, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay * attempt)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=2, exceptions=(ClientError,))
def upload_with_retry(bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, Body=data)
```

### Working with Parquet & Pandas
```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read multiple Parquet files
import glob
files = glob.glob('data/year=2024/**/*.parquet', recursive=True)
df = pd.concat([pd.read_parquet(f) for f in files])

# Write partitioned Parquet
table = pa.Table.from_pandas(df)
pq.write_to_dataset(
    table,
    root_path='s3://my-bucket/processed/',
    partition_cols=['year', 'month']
)

# Data type conversions
df['trip_date'] = pd.to_datetime(df['trip_date'])
df['year'] = df['trip_date'].dt.year
df['month'] = df['trip_date'].dt.month

# Handle nulls
df['fare_amount'] = df['fare_amount'].fillna(0)
df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime'])
```

---

## 3. Deep Dive — AWS Glue

### Glue DynamicFrame vs Spark DataFrame

```python
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Glue Data Catalog into DynamicFrame
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="nyc_tlc_db",
    table_name="yellow_trips",
    transformation_ctx="dyf"
)

# DynamicFrame → DataFrame
df = dyf.toDF()

# DataFrame → DynamicFrame
from awsglue.dynamicframe import DynamicFrame
dyf_back = DynamicFrame.fromDF(df, glueContext, "dyf_back")

# Resolve choice — handle ambiguous types
dyf_resolved = dyf.resolveChoice(
    specs=[("fare_amount", "cast:double")]
)

# Apply mapping — rename and recast columns
dyf_mapped = ApplyMapping.apply(
    frame=dyf,
    mappings=[
        ("vendorid", "long", "vendor_id", "int"),
        ("fare_amount", "double", "fare_amount", "double"),
        ("tpep_pickup_datetime", "string", "pickup_datetime", "timestamp")
    ]
)

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=dyf_mapped,
    connection_type="s3",
    connection_options={
        "path": "s3://my-bucket/processed/yellow/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)
```

### Glue Job Bookmarks
```python
# Enable bookmark in job arguments
# --job-bookmark-option: job-bookmark-enable

# Use transformation_ctx for bookmark tracking
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="nyc_tlc_db",
    table_name="yellow_trips",
    transformation_ctx="datasource"  # bookmark tracks this context
)

# Commit job — required to save bookmark state
job.commit()
```

### Glue Incremental Processing Pattern
```python
import boto3
from datetime import datetime, timedelta

def get_new_s3_files(bucket, prefix, last_processed_time):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    new_files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['LastModified'].replace(tzinfo=None) > last_processed_time:
                new_files.append(f"s3://{bucket}/{obj['Key']}")
    
    return new_files
```

### Glue Data Quality
```python
# Built-in Glue Data Quality rules
from awsglue.transforms import *

ruleset = """
Rules = [
    IsComplete "trip_id",
    IsUnique "trip_id",
    ColumnValues "fare_amount" > 0,
    ColumnValues "pickup_datetime" <= now(),
    ColumnCount = 20
]
"""
```

### Glue Workflow Orchestration
- Chain Glue jobs and crawlers
- Trigger on schedule or event
- Monitor entire pipeline in one view

```
S3 File Arrives
      │
      ▼
EventBridge Rule
      │
      ▼
Glue Workflow Start
      │
      ├──► Job: Bronze Ingestion
      │         │
      ├──► Crawler: Update Catalog
      │         │
      ├──► Job: Silver Transform
      │         │
      └──► Job: Gold Aggregation
```

---

## 4. Deep Dive — Amazon S3

### S3 Storage Classes
| Class | Use Case | Retrieval | Cost |
|-------|----------|-----------|------|
| Standard | Frequent access | Instant | Highest |
| Standard-IA | Infrequent access | Instant | Lower |
| One Zone-IA | Non-critical, infrequent | Instant | Lower |
| Intelligent-Tiering | Unknown access patterns | Instant | Auto-optimized |
| Glacier Instant | Archive, rare access | Instant | Low |
| Glacier Flexible | Archive | 1–12 hours | Very low |
| Glacier Deep Archive | Long-term archive | Up to 48 hours | Lowest |

### S3 Lifecycle Policy Example
```json
{
  "Rules": [
    {
      "ID": "move-to-ia-after-30-days",
      "Status": "Enabled",
      "Transitions": [
        { "Days": 30, "StorageClass": "STANDARD_IA" },
        { "Days": 90, "StorageClass": "GLACIER" },
        { "Days": 365, "StorageClass": "DEEP_ARCHIVE" }
      ],
      "Expiration": { "Days": 2555 }
    }
  ]
}
```

### S3 Partitioning Strategy
```
# Hive-style partitioning (recommended for Athena/Glue)
s3://bucket/raw/nyc-tlc/year=2024/month=01/file.parquet

# Date-based partitioning
s3://bucket/raw/events/2024/01/15/events.parquet

# Region-based partitioning
s3://bucket/raw/orders/region=us-east/year=2024/orders.parquet
```

### S3 Event Notifications
```json
{
  "LambdaFunctionConfigurations": [
    {
      "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:trigger-glue",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            { "Name": "prefix", "Value": "raw/nyc-tlc/" },
            { "Name": "suffix", "Value": ".parquet" }
          ]
        }
      }
    }
  ]
}
```

### S3 Best Practices
- Use Hive-style partitioning for Athena/Glue compatibility
- Use Parquet + Snappy for analytics workloads
- Enable versioning for critical data buckets
- Use bucket policies + IAM for access control
- Enable S3 access logging for audit trails
- Use Transfer Acceleration for cross-region uploads
- Avoid too many small files — compact them (small file problem)

---

## 5. Deep Dive — AWS DMS

### DMS Architecture
```
Source DB (RDS/Oracle/MySQL)
         │
         ▼
  Replication Instance (EC2)
         │
         ▼
  Target (S3/Redshift/DynamoDB)
```

### Full Load vs CDC
| | Full Load | CDC |
|--|-----------|-----|
| What | Entire table | Only changes (INSERT/UPDATE/DELETE) |
| When | Initial migration | Ongoing replication |
| Impact | High source load | Low source load |
| Speed | Fast (bulk) | Near real-time |

### CDC Change Types
```json
// DMS CDC record format
{
  "metadata": {
    "operation": "update",
    "table-name": "trips",
    "timestamp": "2024-01-15T10:30:00Z"
  },
  "before-image": { "trip_id": 1, "status": "active" },
  "after-image":  { "trip_id": 1, "status": "completed" }
}
```

### DMS to S3 + Glue Pattern
```
RDS (source)
     │
     ▼ CDC
AWS DMS ──► S3 (raw/cdc/table=trips/YYYY/MM/DD/)
                    │
                    ▼
               Glue ETL (process CDC records)
                    │
                    ▼ UPSERT
               Redshift / Delta Lake
```

---

## 6. Deep Dive — Amazon Redshift

### Distribution Keys Deep Dive
```sql
-- EVEN distribution (default)
CREATE TABLE orders (order_id INT, amount DECIMAL)
DISTSTYLE EVEN;

-- KEY distribution (best for joins on this column)
CREATE TABLE orders (order_id INT, customer_id INT, amount DECIMAL)
DISTKEY(customer_id);

-- ALL distribution (small dimension tables)
CREATE TABLE customers (customer_id INT, name VARCHAR)
DISTSTYLE ALL;

-- AUTO (Redshift decides)
CREATE TABLE orders (order_id INT, amount DECIMAL)
DISTSTYLE AUTO;
```

### Sort Keys Deep Dive
```sql
-- Compound sort key (multi-column, order matters)
CREATE TABLE trips (
    trip_id INT,
    pickup_date DATE,
    trip_type VARCHAR
)
COMPOUND SORTKEY(pickup_date, trip_type);

-- Interleaved sort key (equal weight to all columns)
CREATE TABLE trips (
    trip_id INT,
    pickup_date DATE,
    zone_id INT
)
INTERLEAVED SORTKEY(pickup_date, zone_id);
```

### VACUUM Types
```sql
VACUUM FULL my_table;        -- reclaim space + resort rows
VACUUM SORT ONLY my_table;   -- resort rows only
VACUUM DELETE ONLY my_table; -- reclaim space only
VACUUM REINDEX my_table;     -- rebuild interleaved sort key index
```

### COPY Command Patterns
```sql
-- From S3 Parquet
COPY trips
FROM 's3://my-bucket/raw/yellow/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- From S3 CSV
COPY trips
FROM 's3://my-bucket/raw/trips.csv'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
CSV IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto';

-- From S3 JSON
COPY trips
FROM 's3://my-bucket/raw/trips.json'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS JSON 'auto';
```

### UPSERT Pattern in Redshift
```sql
-- Step 1: Load new data into staging table
CREATE TEMP TABLE staging (LIKE trips);

COPY staging
FROM 's3://my-bucket/raw/new_trips/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- Step 2: Delete matching rows from target
DELETE FROM trips
USING staging
WHERE trips.trip_id = staging.trip_id;

-- Step 3: Insert all rows from staging
INSERT INTO trips SELECT * FROM staging;

-- Step 4: Drop staging
DROP TABLE staging;
```

### Redshift Spectrum
```sql
-- Create external schema pointing to S3 via Glue Catalog
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'nyc_tlc_db'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Query S3 data directly from Redshift
SELECT t.trip_type, SUM(t.fare_amount) AS total_fare
FROM spectrum_schema.yellow_trips t  -- S3 data
JOIN customers c ON t.customer_id = c.id  -- Redshift table
GROUP BY 1;
```

### Redshift Performance Checklist
- [ ] Choose correct DISTKEY — high cardinality join columns
- [ ] Choose correct SORTKEY — date/range filter columns
- [ ] Run VACUUM regularly after bulk operations
- [ ] Run ANALYZE after large data loads
- [ ] Use COPY for bulk loads (not INSERT row by row)
- [ ] Compress columns — use ENCODE AUTO or specify encodings
- [ ] Use WLM (Workload Management) to prioritize queries
- [ ] Monitor with `SVL_QUERY_REPORT` and `STL_SCAN`

---

## 7. Deep Dive — Amazon Athena

### Partitioning & Partition Projection
```sql
-- Standard partitioned table
CREATE EXTERNAL TABLE nyc_tlc_yellow (
    vendor_id INT,
    fare_amount DOUBLE,
    pickup_datetime TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://my-bucket/raw/yellow/';

-- Add partitions manually
ALTER TABLE nyc_tlc_yellow ADD PARTITION (year=2024, month=1)
LOCATION 's3://my-bucket/raw/yellow/year=2024/month=1/';

-- Auto-discover partitions
MSCK REPAIR TABLE nyc_tlc_yellow;

-- Partition projection (no MSCK needed)
CREATE EXTERNAL TABLE nyc_tlc_yellow (
    vendor_id INT,
    fare_amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://my-bucket/raw/yellow/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2019,2024',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'storage.location.template' = 
        's3://my-bucket/raw/yellow/year=${year}/month=${month}/'
);
```

### Athena Cost Optimization
```sql
-- Bad: scans entire table
SELECT * FROM nyc_tlc_yellow WHERE YEAR(pickup_datetime) = 2024;

-- Good: uses partition pruning
SELECT * FROM nyc_tlc_yellow WHERE year = 2024;

-- Use columnar format + compression
-- Parquet + Snappy can reduce scan size by 80-90%
```

### Athena vs Redshift Decision
| Scenario | Use |
|----------|-----|
| Ad-hoc queries on S3 | Athena |
| Complex joins, BI dashboards | Redshift |
| Large concurrent users | Redshift |
| No infrastructure management | Athena |
| Query S3 + Redshift together | Redshift Spectrum |
| Cost-sensitive, low frequency | Athena |

---

## 8. Deep Dive — AWS Lake Formation

### Permission Model
```
IAM (coarse-grained)
        +
Lake Formation (fine-grained)
        │
        ├── Database permissions
        ├── Table permissions
        ├── Column permissions
        └── Row-level security (via row filters)
```

### Granting Permissions
```python
import boto3

lf = boto3.client('lakeformation')

# Grant table access
lf.grant_permissions(
    Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123:role/DataAnalystRole'},
    Resource={
        'Table': {
            'DatabaseName': 'nyc_tlc_db',
            'Name': 'yellow_trips'
        }
    },
    Permissions=['SELECT']
)

# Grant column-level access
lf.grant_permissions(
    Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123:role/AnalystRole'},
    Resource={
        'TableWithColumns': {
            'DatabaseName': 'nyc_tlc_db',
            'Name': 'yellow_trips',
            'ColumnNames': ['trip_id', 'fare_amount', 'pickup_datetime']
        }
    },
    Permissions=['SELECT']
)
```

### Data Lake Zones with Lake Formation
```
Bronze (Raw)        — ingestion team only
Silver (Processed)  — data engineers
Gold (Curated)      — analysts, BI tools
```

---

## 9. Deep Dive — Amazon Kinesis

### Kinesis Data Streams Architecture
```
Producers                    Kinesis Stream               Consumers
──────────                   ─────────────                ─────────
App Events  ──►  Shard 1  ──►  Lambda
IoT Devices ──►  Shard 2  ──►  Kinesis Analytics
Clickstream ──►  Shard 3  ──►  Glue Streaming
                              ──►  Custom Consumer (KCL)
```

### Shard Capacity
- 1 shard = 1 MB/s ingestion, 2 MB/s consumption
- Scale up — split shards
- Scale down — merge shards
- Enhanced fan-out — 2 MB/s per consumer per shard

### Producer Code (boto3)
```python
import boto3
import json

kinesis = boto3.client('kinesis', region_name='us-east-1')

def send_event(stream_name, data, partition_key):
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data).encode('utf-8'),
        PartitionKey=partition_key
    )
    return response['SequenceNumber']

# Batch put (up to 500 records)
records = [
    {
        'Data': json.dumps({'trip_id': i, 'fare': 10.5}).encode(),
        'PartitionKey': f'trip_{i}'
    }
    for i in range(100)
]
kinesis.put_records(StreamName='nyc-tlc-stream', Records=records)
```

### Kinesis Firehose Delivery Patterns
```
Kinesis Data Streams
        │
        ▼
Kinesis Firehose
        │
        ├── S3 (raw backup)
        ├── Redshift (via S3 COPY)
        ├── OpenSearch
        └── HTTP Endpoint
```

### Kinesis Data Analytics (Flink)
```sql
-- Tumbling window — count trips per minute
SELECT
    TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS window_start,
    trip_type,
    COUNT(*) AS trip_count
FROM trips
GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), trip_type;

-- Sliding window — 5-min window every 1 min
SELECT
    HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
    AVG(fare_amount) AS avg_fare
FROM trips
GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);
```

---

## 10. Deep Dive — Amazon MSK & Kafka

### Kafka Architecture Deep Dive
```
Producer → Topic (Partition 0) → Consumer Group A
                (Partition 1) → Consumer Group B
                (Partition 2) → Consumer Group C

Each partition is replicated across brokers:
  Partition 0: Leader (Broker 1), Replica (Broker 2), Replica (Broker 3)
```

### Kafka Producer (Python)
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['broker1:9092', 'broker2:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',              # wait for all replicas
    retries=3,
    compression_type='snappy'
)

producer.send('nyc-tlc-trips', {
    'trip_id': '123',
    'fare_amount': 15.5,
    'trip_type': 'yellow'
})
producer.flush()
```

### Kafka Consumer (Python)
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'nyc-tlc-trips',
    bootstrap_servers=['broker1:9092'],
    group_id='etl-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    trip = message.value
    print(f"Trip {trip['trip_id']}: ${trip['fare_amount']}")
```

### MSK vs Self-managed Kafka vs Kinesis
| Feature | MSK | Self-managed Kafka | Kinesis |
|---------|-----|--------------------|---------|
| Management | AWS managed | You manage | Fully managed |
| Kafka API | Yes | Yes | No |
| Cost | EC2 + storage | EC2 + ops | Per shard hour |
| Scaling | Manual | Manual | On-demand |
| Retention | Configurable | Configurable | Up to 365 days |
| Best for | Kafka migration to AWS | Full control | AWS-native apps |

---

## 11. Deep Dive — Apache Spark & EMR

### Spark Internals

```
User Code
    │
    ▼
Catalyst Optimizer
    │
    ├── Logical Plan (what to do)
    ├── Optimized Logical Plan (push filters down, etc.)
    ├── Physical Plan (how to do it)
    └── Code Generation (Tungsten)
    │
    ▼
Spark Execution
    │
    ├── Job → Stages → Tasks
    └── Each Stage separated by shuffle boundary
```

### PySpark Deep Dive
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("NYC TLC Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Read Parquet from S3
df = spark.read.parquet("s3://my-bucket/raw/yellow/")

# Schema inspection
df.printSchema()
df.describe().show()

# Filtering and selection
df_filtered = df.filter(
    (F.col("fare_amount") > 0) &
    (F.col("trip_distance") > 0) &
    F.col("pickup_datetime").isNotNull()
)

# Add derived columns
df_enriched = df_filtered \
    .withColumn("year", F.year("pickup_datetime")) \
    .withColumn("month", F.month("pickup_datetime")) \
    .withColumn("hour", F.hour("pickup_datetime")) \
    .withColumn("trip_duration_mins",
        (F.unix_timestamp("dropoff_datetime") - 
         F.unix_timestamp("pickup_datetime")) / 60)

# Window functions
window = Window.partitionBy("zone_id").orderBy(F.desc("pickup_datetime"))
df_ranked = df_enriched.withColumn("rank", F.rank().over(window))

# Aggregations
df_agg = df_enriched.groupBy("year", "month", "trip_type").agg(
    F.count("*").alias("trip_count"),
    F.sum("fare_amount").alias("total_fare"),
    F.avg("trip_distance").alias("avg_distance"),
    F.percentile_approx("fare_amount", 0.5).alias("median_fare")
)

# Write partitioned Parquet
df_agg.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://my-bucket/processed/yellow/")
```

### Handling Skewed Data
```python
# Method 1: Salting
import random

def add_salt(df, skew_col, num_salts=10):
    return df.withColumn(
        "salted_key",
        F.concat(F.col(skew_col), F.lit("_"), 
                 (F.rand() * num_salts).cast("int").cast("string"))
    )

# Method 2: Broadcast join for small tables
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "zone_id")

# Method 3: Enable AQE (Spark 3.x)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Spark Tuning Parameters
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "10") \
    .config("spark.driver.memory", "4g") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()
```

### EMR Cluster Configuration
```json
{
  "Name": "nyc-tlc-cluster",
  "ReleaseLabel": "emr-6.15.0",
  "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
  "Instances": {
    "MasterInstanceType": "m5.xlarge",
    "SlaveInstanceType": "m5.2xlarge",
    "InstanceCount": 5
  },
  "Configurations": [
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.sql.adaptive.enabled": "true"
      }
    }
  ]
}
```

---

## 12. Deep Dive — System Design

### Design 1: NYC TLC Batch Pipeline (What We Built)

**Requirements:**
- Ingest all NYC TLC trip data (4 types, 2019–2024)
- Store in S3 data lake
- Make queryable via Athena and Redshift
- Run monthly for new data

**Architecture:**
```
NYC TLC CloudFront (HTTPS)
        │
        ▼
AWS Glue Python Shell Job
(download & upload to S3)
        │
        ▼
S3 Bronze Layer
raw/hvfhv/year=YYYY/month=MM/
        │
        ▼
AWS Glue Crawler
(update Data Catalog)
        │
        ▼
Glue ETL Job (transform + validate)
        │
        ▼
S3 Silver Layer (cleaned Parquet)
        │
        ▼
Glue ETL Job (aggregate)
        │
        ▼
S3 Gold Layer + Redshift
        │
        ▼
Athena / QuickSight
```

**Key Design Decisions:**
- Python Shell for download (no Spark needed for HTTP → S3)
- Hive-style partitioning for Athena partition pruning
- Job bookmarks to avoid reprocessing
- CloudWatch alarms on job failure
- SNS notification on completion (.done file pattern)

---

### Design 2: Real-time Fraud Detection

**Requirements:**
- Detect fraudulent transactions in < 5 seconds
- Process 100K events/second
- Store all events for audit
- Alert on fraud

**Architecture:**
```
Transaction Events
        │
        ▼
Kinesis Data Streams (10 shards)
        │
        ├──► Lambda (real-time scoring)
        │         │
        │         ├── Fraud → SNS Alert → SQS → Block Transaction
        │         └── Clean → pass through
        │
        ├──► Kinesis Firehose → S3 (raw audit log)
        │
        └──► Kinesis Analytics (Flink)
                  │
                  ▼
            Tumbling window aggregations
                  │
                  ▼
            DynamoDB (real-time counters)
```

---

### Design 3: Data Lakehouse on AWS

**Layers:**
```
Sources
  ├── RDS (transactional)
  ├── APIs (3rd party)
  └── Files (partner feeds)
        │
        ▼ (DMS / Glue / Lambda)
Bronze Layer (S3 raw)
  └── Exact copy of source, immutable
        │
        ▼ (Glue ETL)
Silver Layer (S3 processed)
  └── Cleaned, validated, standardized
        │
        ▼ (Glue ETL)
Gold Layer (S3 curated)
  └── Business aggregations, KPIs
        │
        ├──► Athena (ad-hoc SQL)
        ├──► Redshift Spectrum (dashboards)
        └──► SageMaker (ML)

Governance:
  └── Lake Formation (access control)
  └── Glue Data Catalog (metadata)
  └── CloudTrail (audit)
```

---

### Design 4: Multi-Region Data Pipeline

**Requirements:**
- Ingest data from 3 regions (US, EU, APAC)
- Centralize in US for analytics
- Comply with data residency laws

**Architecture:**
```
US Region          EU Region          APAC Region
    │                  │                   │
  Glue Job          Glue Job           Glue Job
    │                  │                   │
  S3 (us)           S3 (eu)            S3 (apac)
    │                  │                   │
    └──────────────────┼───────────────────┘
                       │
                  S3 CRR (Cross-Region Replication)
                  (EU/APAC → US central bucket)
                       │
                       ▼
                  Glue ETL (centralize)
                       │
                       ▼
                  Redshift (us-east-1)
```

---

## 13. Deep Dive — Behavioral

### STAR Method Framework
- **S**ituation — set the context
- **T**ask — what was your responsibility
- **A**ction — what steps did YOU take
- **R**esult — measurable outcome

### Prepared Stories

**Story 1: End-to-end pipeline (use NYC TLC project)**
- S: needed to ingest 5 years of NYC taxi data into a data lake
- T: design and implement the full ingestion pipeline
- A: built Glue Python Shell job, CloudFormation for infra, S3 partitioning strategy, job bookmarks
- R: automated monthly ingestion, 0 manual intervention, 80% cost reduction vs EC2-based approach

**Story 2: Optimizing a slow job**
- S: Spark job processing 500GB daily was taking 4 hours
- T: reduce runtime to under 1 hour
- A: profiled DAG, found data skew on zone_id, applied salting + broadcast join, tuned executor config, switched to Parquet
- R: reduced runtime from 4 hours to 45 minutes, 70% cost saving

**Story 3: Production incident**
- S: Glue job silently failed — no data landed in Redshift for 6 hours
- T: diagnose and fix without data loss
- A: checked CloudWatch logs, found AccessDenied on S3, traced to IAM role policy change, updated policy, added CloudWatch alarm for future failures
- R: backfilled 6 hours of data, zero data loss, implemented monitoring to catch within 5 minutes

**Story 4: Learning new service quickly**
- S: project required Lake Formation — never used before
- T: implement fine-grained access control for 10 teams within 2 weeks
- A: read AWS docs, built a POC, implemented tag-based access control (TBAC)
- R: delivered on time, reduced IAM policy management overhead by 60%

---

## 14. Top 100 Interview Questions & Answers

### SQL (20 Questions)

**Q1: What is the difference between RANK and DENSE_RANK?**
- RANK skips subsequent ranks after a tie (1,1,3)
- DENSE_RANK does not skip (1,1,2)

**Q2: When would you use a CTE vs a subquery?**
- CTE — when you need to reference the same subquery multiple times, or for readability
- Subquery — for simple one-off filters

**Q3: What is the difference between WHERE and HAVING?**
- WHERE filters rows before aggregation
- HAVING filters groups after aggregation

**Q4: How do you find duplicate records?**
```sql
SELECT id, COUNT(*) FROM table GROUP BY id HAVING COUNT(*) > 1;
```

**Q5: What is a self join?**
- Joining a table to itself, useful for hierarchical or comparison queries

### AWS Glue (20 Questions)

**Q6: What is a Glue DynamicFrame?**
- A distributed data abstraction similar to Spark DataFrame but with schema flexibility — can handle inconsistent data types across records

**Q7: What is a Glue job bookmark?**
- Tracks which data has already been processed so re-runs only pick up new data

**Q8: What is the difference between Glue ETL and Python Shell?**
- ETL — runs PySpark on multiple workers, for large-scale transformations
- Python Shell — runs plain Python, for small scripts and boto3 operations

**Q9: How do you handle schema evolution in Glue?**
- Use DynamicFrame with `resolveChoice` for type conflicts
- Enable schema evolution in Glue Catalog table settings
- Use Iceberg/Delta table formats that natively support schema evolution

**Q10: How does Glue Crawler work?**
- Scans S3/databases, infers schema, creates/updates tables in Glue Data Catalog

### Amazon S3 (10 Questions)

**Q11: What is S3 Intelligent-Tiering?**
- Automatically moves objects between access tiers based on access patterns at no retrieval fee

**Q12: What is the S3 small file problem?**
- Many small files (< 128MB) slow down Spark/Athena because of too many S3 API calls
- Fix: compact small files into larger ones using Glue or Spark

**Q13: How do you secure S3 data?**
- Bucket policies, IAM roles, SSE-S3/SSE-KMS encryption, VPC endpoints, access logging

### Amazon Redshift (15 Questions)

**Q14: What is the difference between DISTKEY and SORTKEY?**
- DISTKEY — controls how data is distributed across nodes (affects join performance)
- SORTKEY — controls how data is sorted on disk (affects range scan performance)

**Q15: When would you use INTERLEAVED vs COMPOUND sort key?**
- Compound — queries filter on first sort key column most of the time
- Interleaved — queries filter on any combination of sort key columns equally

**Q16: What does VACUUM do in Redshift?**
- Reclaims disk space from deleted/updated rows and re-sorts unsorted rows

**Q17: What is Redshift Spectrum?**
- Allows Redshift to query data directly in S3 without loading it, using external tables

**Q18: How do you load data efficiently into Redshift?**
- Use COPY command with Parquet format from S3 — fastest bulk load method

### Kinesis (10 Questions)

**Q19: What is a Kinesis shard?**
- Unit of throughput: 1 MB/s write, 2 MB/s read per shard

**Q20: When would you use Kinesis Firehose over Kinesis Streams?**
- Firehose — when you just need to deliver data to S3/Redshift without custom processing
- Streams — when you need custom consumers, real-time processing, or multiple consumers

**Q21: How do you handle hot shards in Kinesis?**
- Distribute partition keys evenly
- Use random suffix on partition key to spread load

### Apache Spark (20 Questions)

**Q22: What is lazy evaluation in Spark?**
- Transformations are not executed until an action is called, allowing Spark to optimize the execution plan

**Q23: What is the difference between cache() and persist()?**
- cache() = persist with MEMORY_AND_DISK storage level
- persist() allows custom storage level (memory only, disk only, etc.)

**Q24: How do you fix a slow Spark job?**
1. Check Spark UI for slow stages
2. Identify shuffle operations
3. Check for data skew
4. Tune partitions (`spark.sql.shuffle.partitions`)
5. Enable AQE
6. Use broadcast joins for small tables
7. Cache frequently reused DataFrames

**Q25: What is AQE (Adaptive Query Execution)?**
- Spark 3.x feature that optimizes query plan at runtime based on actual data statistics
- Auto-coalesces partitions, handles skew, switches join strategies

### Streaming (10 Questions)

**Q26: What is exactly-once semantics in Kafka?**
- Each message is processed exactly once even in case of failures
- Achieved via idempotent producers + transactional consumers

**Q27: What is a consumer group in Kafka?**
- A group of consumers sharing the work of consuming a topic
- Each partition is consumed by exactly one consumer in the group

**Q28: How do you handle late-arriving data in streaming?**
- Watermarking — define how late data is allowed
- Reprocessing windows — keep state for a defined period
- Dead letter queue — route late records for offline processing

**Q29: What is the difference between tumbling and sliding windows?**
- Tumbling — fixed, non-overlapping (e.g., every 5 mins)
- Sliding — fixed size, overlapping (e.g., 5-min window every 1 min)

### System Design (5 Questions)

**Q30: How would you design a pipeline for 1TB daily with < 1 hour SLA?**
- Kinesis or MSK for ingestion
- Glue ETL with G.2X workers (10+)
- Parquet + Snappy format
- Partition by date for parallel processing
- Monitor with CloudWatch, alert on 45-min mark

**Q31: How do you ensure data quality in a pipeline?**
- Schema validation on ingestion
- Glue Data Quality rules
- Great Expectations or Deequ
- Dead letter queues for bad records
- Data reconciliation checks (source count vs target count)

**Q32: How do you handle pipeline failures and recovery?**
- Idempotent writes — safe to re-run
- Job bookmarks — track processed data
- Dead letter queues — capture failed records
- CloudWatch alarms + SNS notifications
- Step Functions for retry logic

**Q33: What is the medallion architecture?**
- Bronze — raw, unprocessed data
- Silver — cleaned, validated data
- Gold — business-ready aggregations and KPIs

**Q34: How would you migrate a 10TB Oracle database to Redshift?**
1. Set up DMS replication instance
2. Full load to S3 staging
3. Transform with Glue ETL
4. COPY to Redshift
5. Enable CDC for ongoing sync
6. Validate row counts and checksums
7. Cut over when validated

---

## Quick Revision Flashcards

| Question | Answer |
|----------|--------|
| Glue DynamicFrame vs DataFrame | DynamicFrame handles schema inconsistency, DataFrame requires fixed schema |
| DISTKEY purpose | Distribute data across nodes to co-locate join data |
| SORTKEY purpose | Sort data on disk to speed up range scans |
| Kinesis shard capacity | 1 MB/s in, 2 MB/s out |
| Exactly-once in Kafka | Idempotent producer + transactional API |
| Lazy evaluation | Transformations execute only when action called |
| Repartition vs Coalesce | Repartition = full shuffle, Coalesce = no shuffle (only reduce) |
| Broadcast join | Send small table to all executors to avoid shuffle |
| AQE | Runtime query optimization in Spark 3.x |
| Lake Formation | Fine-grained access control on top of IAM for data lakes |
| Watermark | Max lateness allowed for streaming late data |
| CDC | Capture row-level changes (INSERT/UPDATE/DELETE) from source |
| Job Bookmark | Tracks processed data in Glue for incremental loads |
| Medallion Architecture | Bronze → Silver → Gold data quality layers |
| VACUUM Redshift | Reclaim space + re-sort rows after deletes/updates |
