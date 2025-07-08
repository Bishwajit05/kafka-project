#!/bin/bash

ENV=${1:-dev}
BUCKET="${ENV}-tweet-analysis-code"

# Create S3 bucket if it doesn't exist
aws s3 mb "s3://${BUCKET}" --region us-east-1 || true

# Upload mapper and reducer
aws s3 cp mapper.py "s3://${BUCKET}/mapper.py"
aws s3 cp reducer.py "s3://${BUCKET}/reducer.py"

echo "Code uploaded to s3://${BUCKET}/"
