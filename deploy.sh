#!/bin/bash
set -e

# ── Configuration ────────────────────────────────────────────────────────────
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="mission-deh-hof-amazon-q-${ACCOUNT_ID}"
STACK_NAME="nyc-tlc-ingestion-stack"
SCRIPT_LOCAL="src/glue/nyc_tlc_ingestion.py"
SCRIPT_S3_KEY="scripts/nyc_tlc_ingestion.py"
CFN_TEMPLATE="infrastructure/glue_job.yaml"
GLUE_JOB_NAME="nyc-tlc-ingestion-job"

echo "==========================================="
echo " NYC TLC Ingestion - Deployment"
echo "==========================================="
echo " Account  : ${ACCOUNT_ID}"
echo " Region   : ${REGION}"
echo " Bucket   : ${BUCKET}"
echo " Stack    : ${STACK_NAME}"
echo "==========================================="

# ── Step 1: Upload Glue Script to S3 ────────────────────────────────────────
echo ""
echo "[1/3] Uploading Glue script to S3..."
aws s3 cp ${SCRIPT_LOCAL} s3://${BUCKET}/${SCRIPT_S3_KEY} --region ${REGION}
echo "      Done: s3://${BUCKET}/${SCRIPT_S3_KEY}"

# ── Step 2: Deploy CloudFormation Stack ─────────────────────────────────────
echo ""
echo "[2/3] Deploying CloudFormation stack: ${STACK_NAME}..."
aws cloudformation deploy \
  --template-file ${CFN_TEMPLATE} \
  --stack-name ${STACK_NAME} \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --parameter-overrides TargetBucket=${BUCKET}
echo "      Done: Stack deployed successfully"

# ── Step 3: Trigger Glue Job ─────────────────────────────────────────────────
echo ""
echo "[3/3] Triggering Glue job: ${GLUE_JOB_NAME}..."
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name ${GLUE_JOB_NAME} \
  --region ${REGION} \
  --query JobRunId \
  --output text)
echo "      Done: Job triggered with Run ID: ${JOB_RUN_ID}"

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "==========================================="
echo " Deployment Complete!"
echo "==========================================="
echo " Glue Job Run ID : ${JOB_RUN_ID}"
echo " Monitor job status with:"
echo "   aws glue get-job-run \\"
echo "     --job-name ${GLUE_JOB_NAME} \\"
echo "     --run-id ${JOB_RUN_ID} \\"
echo "     --region ${REGION}"
echo "==========================================="
