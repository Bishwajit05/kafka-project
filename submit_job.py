#!/usr/bin/env python3
"""
EMR Job Submission Script

This script submits a MapReduce job to an EMR cluster for processing tweet data.
It can be used to analyze trending words from tweet data stored in S3.
"""

import boto3
import time
import argparse
from datetime import datetime

def submit_emr_job(environment='dev', input_path=None, output_path=None):
    """
    Submit a MapReduce job to EMR for processing tweet data.
    
    Args:
        environment (str): Deployment environment (dev/stage/prod)
        input_path (str): S3 path to input data (e.g., 's3://bucket/input/')
        output_path (str): S3 path for output data (will have timestamp appended)
    """
    # Default S3 paths if not provided
    if not input_path:
        input_path = f's3://{environment}-tweet-data/input/'
    if not output_path:
        output_path = f's3://{environment}-tweet-data/output/trending-{int(time.time())}'
    
    # EMR client
    emr = boto3.client('emr', region_name='us-east-1')
    
    # Job configuration
    job_name = f'{environment}-trending-words-{datetime.now().strftime("%Y%m%d%H%M")}'
    
    try:
        response = emr.run_job_flow(
            Name=job_name,
            LogUri=f's3://{environment}-emr-logs/',
            ReleaseLabel='emr-6.8.0',
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'}
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': 'your-key-pair',  # Replace with your key pair
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            Steps=[{
                'Name': 'Trending Words Analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hadoop-streaming',
                        '-files', f's3://{environment}-tweet-analysis-code/mapper.py,s3://{environment}-tweet-analysis-code/reducer.py',
                        '-mapper', 'mapper.py',
                        '-reducer', 'reducer.py',
                        '-input', input_path,
                        '-output', output_path
                    ]
                }
            }],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            Tags=[
                {'Key': 'Environment', 'Value': environment},
                {'Key': 'Project', 'Value': 'TweetAnalysis'}
            ]
        )
        
        job_id = response['JobFlowId']
        print(f"✅ Job submitted successfully!")
        print(f"Job ID: {job_id}")
        print(f"Cluster status: https://console.aws.amazon.com/emr/home?region=us-east-1#/cluster-details/{job_id}")
        print(f"S3 Output: {output_path}")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Error submitting job: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Submit EMR job for tweet analysis')
    parser.add_argument('--env', default='dev', help='Environment (dev/stage/prod)')
    parser.add_argument('--input', help='S3 input path (e.g., s3://bucket/input/)')
    parser.add_argument('--output', help='S3 output path (will have timestamp appended)')
    
    args = parser.parse_args()
    
    print(f"🚀 Submitting EMR job with the following configuration:")
    print(f"Environment: {args.env}")
    print(f"Input path: {args.input or 'default'}")
    print(f"Output path: {args.output or 'default'}")
    
    submit_emr_job(
        environment=args.env,
        input_path=args.input,
        output_path=args.output
    )

if __name__ == '__main__':
    main()
