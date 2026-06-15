import sys
import boto3
import urllib.request
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

TARGET_BUCKET = "mission-deh-hof-amazon-q-315527911454"
TARGET_PREFIX = "raw/nyc-tlc"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TRIP_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# NYC TLC data available from 2019 to 2024
YEARS = range(2019, 2025)
MONTHS = range(1, 13)

s3 = boto3.client("s3")

def upload_to_s3(url, bucket, key):
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            s3.upload_fileobj(response, bucket, key)
        print(f"Uploaded: s3://{bucket}/{key}")
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"Not found (skipping): {url}")
        else:
            print(f"HTTP error {e.code} for {url}: {e.reason}")
        return False
    except Exception as e:
        print(f"Error uploading {url}: {str(e)}")
        return False

for trip_type in TRIP_TYPES:
    for year in YEARS:
        for month in MONTHS:
            month_str = str(month).zfill(2)
            file_name = f"{trip_type}_tripdata_{year}-{month_str}.parquet"
            url = f"{BASE_URL}/{file_name}"
            s3_key = f"{TARGET_PREFIX}/{trip_type}/{year}/{file_name}"
            upload_to_s3(url, TARGET_BUCKET, s3_key)

job.commit()
