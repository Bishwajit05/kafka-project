#!/bin/bash

# Exit on error
set -e

# Configuration
STACK_NAME="tweet-processing-stack"
ENVIRONMENT="dev"
REGION="us-east-1"  # Change to your preferred region

# Package and upload Lambda code
echo "Packaging Lambda function..."
cd ../lambda_processor
zip -r lambda_function.zip lambda_function.py
aws s3 cp lambda_function.zip s3://${ENVIRONMENT}-tweet-processor-code-$(aws sts get-caller-identity --query 'Account' --output text)/${ENVIRONMENT}/tweet-processor.zip
cd ../cloudformation

# Deploy CloudFormation stack
echo "Deploying CloudFormation stack..."
aws cloudformation deploy \
    --stack-name $STACK_NAME \
    --template-file template.yaml \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        EnvironmentName=$ENVIRONMENT \
        KinesisStreamName="${ENVIRONMENT}-tweet-stream" \
        S3RawBucketName="${ENVIRONMENT}-tweet-raw-data-$(aws sts get-caller-identity --query 'Account' --output text)" \
        S3ProcessedBucketName="${ENVIRONMENT}-tweet-processed-data-$(aws sts get-caller-identity --query 'Account' --output text)" \
        OpenSearchDomainName="${ENVIRONMENT}-tweet-analysis" \
    --region $REGION

# Get stack outputs
echo "Stack deployment complete!"
echo "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs' \
    --output table \
    --region $REGION

echo "\nTo test the pipeline, you can send a test record to the Kinesis stream:"
echo "aws kinesis put-record \\"
echo "    --stream-name ${ENVIRONMENT}-tweet-stream \\"
echo "    --partition-key 1 \\"
echo "    --data '{\"text\":\"This is a test tweet! #test\",\"id_str\":\"test-$(date +%s)\\''"

echo "\nTo view the OpenSearch dashboard:"
echo "https://${ENVIRONMENT}-tweet-analysis.${REGION}.es.amazonaws.com/_dashboards/"

echo "\nTo clean up resources, run:"
echo "aws cloudformation delete-stack --stack-name $STACK_NAME --region $REGION"
