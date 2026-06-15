# Mission DEH HOF - NYC TLC Data Ingestion

This project ingests NYC Taxi & Limousine Commission (TLC) trip data into an S3 bucket using AWS Glue.

## Project Structure

```
Mission-DEH-HOF-AmazonQ/
├── deploy.sh                        # Deployment script
├── infrastructure/
│   └── glue_job.yaml                # CloudFormation template (IAM Role + Glue Job)
└── src/
    └── glue/
        └── nyc_tlc_ingestion.py     # Glue PySpark ETL script
```

## Architecture

```
NYC TLC CloudFront (HTTPS)
        │
        ▼
  AWS Glue ETL Job
        │
        ▼
  S3 Bucket (raw/nyc-tlc/<trip_type>/<year>/<file>.parquet)
```

## Prerequisites

- AWS CLI configured with sufficient permissions
- Python 3.x
- AWS account with access to:
  - S3
  - AWS Glue
  - IAM
  - CloudFormation

## S3 Bucket

| Property  | Value                                         |
|-----------|-----------------------------------------------|
| Name      | `mission-deh-hof-amazon-q-<account_number>`   |
| Region    | `us-east-1`                                   |

## Glue Job Details

| Property        | Value                    |
|-----------------|--------------------------|
| Job Name        | `nyc-tlc-ingestion-job`  |
| Glue Version    | `4.0`                    |
| Worker Type     | `G.2X`                   |
| Number of Workers | `10`                   |
| Timeout         | `2880 mins (48 hrs)`     |
| Python Version  | `3`                      |

## Data Source

NYC TLC trip data is sourced from the official NYC TLC CloudFront endpoint:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/<trip_type>_tripdata_<YYYY-MM>.parquet
```

### Trip Types
| Type    | Description                        |
|---------|------------------------------------|
| yellow  | Yellow Taxi Trip Records           |
| green   | Green Taxi Trip Records            |
| fhv     | For-Hire Vehicle Trip Records      |
| fhvhv   | High Volume For-Hire Vehicle Records |

### Year Range
- 2019 to 2024 (all months)

## Data Layout in S3

```
s3://mission-deh-hof-amazon-q-<account_number>/
├── scripts/
│   └── nyc_tlc_ingestion.py
├── raw/
│   └── nyc-tlc/
│       ├── yellow/
│       │   └── <year>/
│       │       └── yellow_tripdata_<YYYY-MM>.parquet
│       ├── green/
│       │   └── <year>/
│       │       └── green_tripdata_<YYYY-MM>.parquet
│       ├── fhv/
│       │   └── <year>/
│       │       └── fhv_tripdata_<YYYY-MM>.parquet
│       └── fhvhv/
│           └── <year>/
│               └── fhvhv_tripdata_<YYYY-MM>.parquet
└── spark-logs/
```

## Deployment

Run the deployment script from the project root:

```bash
bash deploy.sh
```

This will:
1. Upload the Glue script to S3
2. Deploy (or update) the CloudFormation stack
3. Trigger the Glue job and print the Run ID

## Monitor Job Status

```bash
aws glue get-job-run \
  --job-name nyc-tlc-ingestion-job \
  --run-id <job_run_id> \
  --region us-east-1
```

## CloudFormation Stack

| Property   | Value                      |
|------------|----------------------------|
| Stack Name | `nyc-tlc-ingestion-stack`  |
| Region     | `us-east-1`                |

### Deploy manually

```bash
aws cloudformation deploy \
  --template-file infrastructure/glue_job.yaml \
  --stack-name nyc-tlc-ingestion-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

### Tear down stack

```bash
aws cloudformation delete-stack \
  --stack-name nyc-tlc-ingestion-stack \
  --region us-east-1
```
