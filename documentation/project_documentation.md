# NYC TLC Data Ingestion - Project Documentation

## Overview

This document captures the end-to-end steps taken to build and deploy the NYC TLC Data Ingestion pipeline on AWS using S3, AWS Glue, IAM, and CloudFormation.

---

## Table of Contents

1. [Project Setup](#1-project-setup)
2. [S3 Bucket Creation](#2-s3-bucket-creation)
3. [Glue ETL Script](#3-glue-etl-script)
4. [CloudFormation Infrastructure](#4-cloudformation-infrastructure)
5. [Deployment Script](#5-deployment-script)
6. [Deployment & Execution](#6-deployment--execution)
7. [Issue & Fix](#7-issue--fix)
8. [Final Project Structure](#8-final-project-structure)

---

## 1. Project Setup

Created the following folder structure in the workspace:

```
Mission-DEH-HOF-AmazonQ/
├── documentation/
├── infrastructure/
└── src/
    └── glue/
```

- `infrastructure/` — holds the CloudFormation template
- `src/glue/` — holds the Glue PySpark ETL script
- `documentation/` — holds project documentation

---

## 2. S3 Bucket Creation

Retrieved the AWS account number using the AWS CLI:

```bash
aws sts get-caller-identity --query Account --output text
# Output: 315527911454
```

Created the S3 bucket in `us-east-1`:

```bash
aws s3api create-bucket \
  --bucket mission-deh-hof-amazon-q-315527911454 \
  --region us-east-1
```

| Property | Value                                       |
|----------|---------------------------------------------|
| Name     | `mission-deh-hof-amazon-q-315527911454`     |
| Region   | `us-east-1`                                 |

---

## 3. Glue ETL Script

**File:** `src/glue/nyc_tlc_ingestion.py`

### Initial Approach (Attempt 1)
The first version of the script attempted to read NYC TLC Parquet files from the publicly listed `s3://nyc-tlc/` AWS Open Data bucket using a signed boto3 client and PySpark.

**Problem:** The `s3://nyc-tlc/` public bucket is no longer publicly accessible — it returns `AccessDenied` on `ListObjectsV2`.

### Fix (Attempt 2 - Anonymous S3 Access)
Updated the script to configure Spark's `AnonymousAWSCredentialsProvider` and use an unsigned boto3 client to access the public bucket.

**Problem:** Still returned `AccessDenied` — the bucket is fully retired and does not allow anonymous access either.

### Final Approach (Attempt 3 - HTTPS CloudFront)
Rewrote the script to download files directly from the official NYC TLC CloudFront HTTPS endpoint:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/<trip_type>_tripdata_<YYYY-MM>.parquet
```

**How it works:**
- Iterates over all trip types: `yellow`, `green`, `fhv`, `fhvhv`
- Iterates over years 2019–2024, all 12 months
- Streams each file via `urllib.request.urlopen` directly into S3 using `boto3.upload_fileobj`
- Skips files that return HTTP 404 (some year/month combinations don't exist)

**Output S3 path pattern:**
```
s3://mission-deh-hof-amazon-q-315527911454/raw/nyc-tlc/<trip_type>/<year>/<file>.parquet
```

---

## 4. CloudFormation Infrastructure

**File:** `infrastructure/glue_job.yaml`

Provisions the following AWS resources:

### IAM Role — `nyc-tlc-glue-job-role`
- Trust policy allows `glue.amazonaws.com` to assume the role
- Attached managed policy: `AWSGlueServiceRole`
- Custom inline policy `nyc-tlc-s3-access` grants:
  - `s3:GetObject`, `s3:ListBucket` on `s3://nyc-tlc/*` (public source bucket)
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on the target bucket

### Glue Job — `nyc-tlc-ingestion-job`

| Property          | Value                                                               |
|-------------------|---------------------------------------------------------------------|
| Glue Version      | `4.0`                                                               |
| Worker Type       | `G.2X`                                                              |
| Number of Workers | `10`                                                                |
| Timeout           | `2880 mins (48 hrs)`                                                |
| Script Location   | `s3://mission-deh-hof-amazon-q-315527911454/scripts/nyc_tlc_ingestion.py` |
| Python Version    | `3`                                                                 |
| Spark UI Logs     | `s3://mission-deh-hof-amazon-q-315527911454/spark-logs/`           |
| CloudWatch Logs   | Enabled                                                             |

---

## 5. Deployment Script

**File:** `deploy.sh`

Automates the full deployment in 3 steps:

| Step | Action                                      |
|------|---------------------------------------------|
| 1    | Uploads Glue script to S3                   |
| 2    | Deploys/updates the CloudFormation stack    |
| 3    | Triggers the Glue job and prints the Run ID |

**Usage:**
```bash
bash deploy.sh
```

---

## 6. Deployment & Execution

### Upload Glue Script
```bash
aws s3 cp src/glue/nyc_tlc_ingestion.py \
  s3://mission-deh-hof-amazon-q-315527911454/scripts/nyc_tlc_ingestion.py
```

### Deploy CloudFormation Stack
```bash
aws cloudformation deploy \
  --template-file infrastructure/glue_job.yaml \
  --stack-name nyc-tlc-ingestion-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

Stack resources created:

| Resource       | Type             | Status            |
|----------------|------------------|-------------------|
| `GlueJobRole`  | `AWS::IAM::Role` | `CREATE_COMPLETE` |
| `NYCTLCGlueJob`| `AWS::Glue::Job` | `CREATE_COMPLETE` |

### Trigger Glue Job
```bash
aws glue start-job-run \
  --job-name nyc-tlc-ingestion-job \
  --region us-east-1
```

### Monitor Job Status
```bash
aws glue get-job-run \
  --job-name nyc-tlc-ingestion-job \
  --run-id <job_run_id> \
  --region us-east-1
```

---

## 7. Issue & Fix

### Problem
| Attempt | Source                    | Error                                              |
|---------|---------------------------|----------------------------------------------------|
| 1       | `s3://nyc-tlc/` (signed)  | `AccessDenied` on `ListObjectsV2`                  |
| 2       | `s3://nyc-tlc/` (unsigned)| `AccessDenied` — bucket fully retired by AWS       |
| 3       | HTTPS CloudFront URL      | ✅ Success — files streamed directly into S3       |

### Root Cause
The `s3://nyc-tlc/` AWS Open Data bucket was retired and is no longer publicly accessible. The official data source moved to the NYC TLC CloudFront endpoint.

### Fix
Switched to streaming files directly from:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/<trip_type>_tripdata_<YYYY-MM>.parquet
```

---

## 8. Final Project Structure

```
Mission-DEH-HOF-AmazonQ/
├── README.md
├── deploy.sh
├── documentation/
│   └── project_documentation.md
├── infrastructure/
│   └── glue_job.yaml
└── src/
    └── glue/
        └── nyc_tlc_ingestion.py
```
