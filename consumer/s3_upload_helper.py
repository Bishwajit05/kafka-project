import boto3
import os
import io
import pickle
import pandas as pd
from botocore.exceptions import ClientError

class S3Uploader:
    def __init__(self):
        # Get AWS credentials from environment variables
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_session_token = os.getenv('AWS_SESSION_TOKEN')
        self.aws_region = "us-east-1"
        
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token]):
            print("⚠️ Warning: Some AWS credentials are missing from environment variables")
        
        # Initialize session and client
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.aws_region
        )
        self.s3_client = self.session.client('s3')
        
        # Track uploads
        self.upload_history = {
            'files': [],
            'dataframes': [],
            'plots': [],
            'models': []
        }

    def ensure_bucket_exists(self, bucket):
        """Check if a bucket exists, create it if it doesn't
        
        Args:
            bucket (str): Name of the S3 bucket
            
        Returns:
            bool: True if bucket exists or was created successfully, False otherwise
        """
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=bucket)
            print(f"✅ Bucket exists: {bucket}")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            # If bucket doesn't exist
            if error_code == '404' or error_code == 'NoSuchBucket':
                try:
                    # Create bucket in the same region as our client
                    self.s3_client.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                    )
                    print(f"✅ Created new bucket: {bucket}")
                    return True
                except Exception as create_error:
                    print(f"❌ Failed to create bucket {bucket}: {create_error}")
                    return False
            # If we don't have permission to check bucket
            elif error_code == '403':
                print(f"❌ No permission to access bucket: {bucket}")
                return False
            else:
                print(f"❌ Error checking bucket {bucket}: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error checking bucket {bucket}: {e}")
            return False

    def upload_file(self, local_path, bucket, s3_key=None):
        """Upload a file to S3 bucket
        
        Args:
            local_path (str): Local file path
            bucket (str): S3 bucket name
            s3_key (str, optional): S3 object key. If None, uses basename of local_path
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure bucket exists
            if not self.ensure_bucket_exists(bucket):
                return False
                
            if s3_key is None:
                s3_key = os.path.basename(local_path)
                
            self.s3_client.upload_file(local_path, bucket, s3_key)
            print(f"✅ Uploaded to S3: s3://{bucket}/{s3_key}")
            self.upload_history['files'].append(f"s3://{bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"❌ S3 upload failed for {s3_key}: {e}")
            return False

    def upload_dataframe(self, df, bucket, s3_key):
        """Upload pandas DataFrame to S3 as CSV
        
        Args:
            df (pandas.DataFrame): DataFrame to upload
            bucket (str): S3 bucket name
            s3_key (str): S3 object key (should end with .csv)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure bucket exists
            if not self.ensure_bucket_exists(bucket):
                return False
                
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            print(f"✅ Uploaded DataFrame to S3: s3://{bucket}/{s3_key}")
            self.upload_history['dataframes'].append(f"s3://{bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"❌ S3 DataFrame upload failed for {s3_key}: {e}")
            return False

    def upload_plot(self, fig, filename, bucket, s3_key=None):
        """Save matplotlib figure and upload to S3
        
        Args:
            fig (matplotlib.figure.Figure): Figure to save and upload
            filename (str): Local filename to save as
            bucket (str): S3 bucket name
            s3_key (str, optional): S3 object key. If None, uses filename
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure bucket exists
            if not self.ensure_bucket_exists(bucket):
                return False
                
            # Save locally
            fig.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"📁 Plot saved locally: {filename}")
            
            # Upload to S3
            success = self.upload_file(filename, bucket, s3_key)
            if success:
                self.upload_history['plots'].append(f"s3://{bucket}/{s3_key or filename}")
            return success
            
        except Exception as e:
            print(f"❌ Plot save/upload failed: {e}")
            return False

    def upload_model(self, model, model_name, bucket, s3_key):
        """Serialize model and upload to S3
        
        Args:
            model: Model object to serialize
            model_name (str): Name of the model
            bucket (str): S3 bucket name
            s3_key (str): S3 object key
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure bucket exists
            if not self.ensure_bucket_exists(bucket):
                return False
                
            # Create local filename
            local_filename = f"{model_name}_model.pkl"
            
            # Serialize model
            with open(local_filename, 'wb') as f:
                pickle.dump(model, f)
            print(f"📦 Model serialized: {local_filename}")
            
            # Upload to S3
            success = self.upload_file(local_filename, bucket, s3_key)
            if success:
                self.upload_history['models'].append(f"s3://{bucket}/{s3_key}")
            
            # Clean up local file
            if os.path.exists(local_filename):
                os.remove(local_filename)
                
            return success
            
        except Exception as e:
            print(f"❌ Model serialization/upload failed: {e}")
            return False

# Create a global instance
s3_uploader = S3Uploader()
