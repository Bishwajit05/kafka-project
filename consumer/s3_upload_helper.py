import boto3


def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    """Upload a file to an S3 bucket."""
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_file_path} to S3: {e}")
