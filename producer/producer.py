import pandas as pd
import time
import json
import kagglehub
import logging
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kinesis_client():
    """Creates a Kinesis client.
    Assumes credentials and region are configured via environment variables or IAM roles.
    """
    aws_region = os.environ.get('AWS_REGION', 'us-east-1') # Default region if not set
    logger.info(f"Creating Kinesis client in region: {aws_region}")
    try:
        client = boto3.client('kinesis', region_name=aws_region)
        print("client", client)
        
        # Quick check to see if client can describe streams (verifies credentials and region vaguely)
        # client.list_streams(Limit=1)
        logger.info("Kinesis client created successfully.")
        return client
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"AWS credentials not found or incomplete: {e}")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'UnrecognizedClientException':
            logger.error(f"AWS Region '{aws_region}' might be incorrect or Kinesis service is not available there: {e}")
        else:
            logger.error(f"Failed to create Kinesis client due to ClientError: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating Kinesis client: {e}")
        return None

def _send_batch(kinesis_client, stream_name, batch_records):
    """Sends a batch of records to Kinesis using PutRecords and handles retries for failures."""
    if not batch_records:
        return 0

    failed_record_count = 0
    try:
        response = kinesis_client.put_records(
            Records=batch_records,
            StreamName=stream_name
        )

        if response.get('FailedRecordCount', 0) > 0:
            logger.warning(f"Batch send to Kinesis had {response['FailedRecordCount']} failed records.")
            # Basic retry for failed records (could be more sophisticated with backoff)
            # For simplicity, we just log them here and count them as failed.
            # A production system might implement more robust retry or dead-letter queue.
            failed_records_details = []
            for i, record_response in enumerate(response.get('Records', [])):
                if 'ErrorCode' in record_response: # Indicates failure for this specific record
                    failed_records_details.append({
                        "original_record_index": i, # Index in the batch_records list
                        "ErrorCode": record_response.get('ErrorCode'),
                        "ErrorMessage": record_response.get('ErrorMessage')
                    })
            logger.warning(f"Details of failed records: {json.dumps(failed_records_details)}")
            failed_record_count = response['FailedRecordCount']
        else:
            logger.info(f"Successfully sent batch of {len(batch_records)} records to Kinesis.")

    except ClientError as e:
        logger.error(f"ClientError sending batch to Kinesis: {e}. All {len(batch_records)} records in this batch considered failed.")
        return len(batch_records) # All records in batch failed
    except Exception as e:
        logger.error(f"Unexpected error sending batch to Kinesis: {e}. All {len(batch_records)} records in this batch considered failed.")
        return len(batch_records) # All records in batch failed

    return failed_record_count


def stream_data(kinesis_client, stream_name, csv_file):
    if not kinesis_client:
        logger.error("Kinesis client is not available. Cannot stream data.")
        return

    MAX_RECORDS_PER_BATCH = 500  # Kinesis PutRecords limit
    # Max payload size for PutRecords is 5MB, each record up to 1MB.
    # We'll primarily limit by record count here for simplicity.
    MAX_BATCH_BYTES = 4 * 1024 * 1024 # Target 4MB to stay well under 5MB Kinesis limit

    try:
        df = pd.read_csv(csv_file)
        
        if 'clean_text' not in df.columns:
            logger.error("Error: 'clean_text' column not found in the CSV.")
            return
        
        df = df.dropna(subset=['clean_text'])
        # df = df.tail(1000) # We'll stream the whole CSV or a larger part for batch testing

        total_records_to_stream = len(df)
        logger.info(f"Starting to stream approximately {total_records_to_stream} records in batches to Kinesis stream: {stream_name}")

        records_batch = []
        current_batch_bytes = 0
        records_sent_count = 0
        total_failed_records = 0

        for index, row in df.iterrows():
            message = {
                "id": index, # Using DataFrame index as part of ID
                "text": row['clean_text'],
                "produced_at": time.time(),
            }
            partition_key = str(message["id"]) # Simple partition key
            payload = json.dumps(message).encode('utf-8')

            # Check if adding this record exceeds batch size limits
            if len(records_batch) >= MAX_RECORDS_PER_BATCH or (current_batch_bytes + len(payload)) >= MAX_BATCH_BYTES:
                if records_batch:
                    failed_count = _send_batch(kinesis_client, stream_name, records_batch)
                    total_failed_records += failed_count
                    records_sent_count += (len(records_batch) - failed_count)
                    records_batch = []
                    current_batch_bytes = 0
                    time.sleep(0.05) # Small delay between batches to manage API call rate

            records_batch.append({'Data': payload, 'PartitionKey': partition_key})
            current_batch_bytes += len(payload)

        # Send any remaining records in the last batch
        if records_batch:
            failed_count = _send_batch(kinesis_client, stream_name, records_batch)
            total_failed_records += failed_count
            records_sent_count += (len(records_batch) - failed_count)

        logger.info(f"Finished streaming. Total records sent successfully: {records_sent_count}. Total failed records: {total_failed_records}.")
            
    except FileNotFoundError:
        logger.error(f"Error: The file {csv_file} was not found.")
    except Exception as e:
        logger.error(f"An error occurred during data streaming: {e}", exc_info=True)

if __name__ == "__main__":
    kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME')
    if not kinesis_stream_name:
        logger.error("KINESIS_STREAM_NAME environment variable not set. Exiting.")
    else:
        kinesis_client = create_kinesis_client()
        if kinesis_client:
            try:
                # Path to the Kaggle dataset CSV
                # In a Fargate/container environment, this dataset might need to be baked into the image
                # or downloaded from S3 at startup. For simplicity, we keep the Kaggle download here.
                # Ensure the container has internet access and kagglehub is configured if using this directly.
                logger.info("Downloading dataset from Kaggle...")
                path = kagglehub.dataset_download("saurabhshahane/twitter-sentiment-dataset")
                csv_path = os.path.join(path, "Twitter_Data.csv")
                logger.info(f"Dataset downloaded to: {csv_path}")

                stream_data(kinesis_client, kinesis_stream_name, csv_path)
            except Exception as e:
                logger.error(f"Failed to download or process dataset: {e}", exc_info=True)
            finally:
                logger.info("Producer finished.")
                # Kinesis client doesn't have a 'close()' method like KafkaProducer