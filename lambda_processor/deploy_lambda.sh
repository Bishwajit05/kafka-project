#!/bin/bash

# Exit on error
set -e

# Configuration
FUNCTION_NAME="tweet-processor"
ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-kinesis-role"  # Replace with your IAM role ARN
HANDLER="lambda_function.lambda_handler"
RUNTIME="python3.9"
TIMEOUT="60"
MEMORY_SIZE="256"
ZIP_FILE="deployment-package.zip"

# Create a virtual environment and install dependencies
echo "Setting up virtual environment..."
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create deployment package
echo "Creating deployment package..."
cd venv/lib/python*/site-packages
zip -r9 ../../../../$ZIP_FILE .
cd ../../../..
zip -g $ZIP_FILE lambda_function.py

# Deploy to AWS Lambda
echo "Deploying to AWS Lambda..."
aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://$ZIP_FILE \
    --handler $HANDLER \
    --runtime $RUNTIME \
    --timeout $TIMEOUT \
    --memory-size $MEMORY_SIZE \
    --role $ROLE_ARN \
    --environment "Variables={OPENSEARCH_HOST=your-opensearch-domain.region.es.amazonaws.com,OPENSEARCH_INDEX=tweets,S3_RAW_BUCKET=your-raw-bucket,S3_RESULTS_BUCKET=your-results-bucket}" \
    || aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://$ZIP_FILE

echo "Deployment complete!"

# Clean up
rm $ZIP_FILE
deactivate
rm -rf venv
