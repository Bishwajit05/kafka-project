# Real-time Tweet Analysis with AWS

This project implements a serverless real-time tweet analysis pipeline using AWS services. It processes streaming tweet data, performs sentiment analysis, and stores the results in Amazon S3 and Amazon OpenSearch Service.

## Architecture

```
+----------------+     +------------------+     +------------------+     +---------------+
|                |     |                  |     |                  |     |               |
|  Tweet Source  +---->+  Kinesis Stream  +---->+  AWS Lambda     +---->+  S3 (Raw)    |
|  (CSV/API)     |     |                  |     |  (Processing)   |     |               |
+----------------+     +------------------+     +--------+-------+     +---------------+
                                                         |                     
                                                         |                     
                                                         v                     
                                                   +-----+-----+               
                                                   |           |               
                                                   |  S3       |               
                                                   | (Results) |               
                                                   |           |               
                                                   +-----+-----+               
                                                         |                     
                                                         v                     
                                                   +-----+-----+               
                                                   |           |               
                                                   | OpenSearch|               
                                                   |  Service  |               
                                                   |           |               
                                                   +-----------+               
```

## Features

- Real-time data ingestion using Amazon Kinesis Data Streams
- Serverless processing with AWS Lambda
- Sentiment analysis using NLTK
- Data storage in Amazon S3
- Search and analytics with Amazon OpenSearch Service
- Monitoring with Amazon CloudWatch

## Prerequisites

- Python 3.8+
- AWS Account with appropriate permissions
- AWS CLI configured with credentials
- Required AWS resources:
  - Kinesis Data Stream
  - S3 Buckets (for raw data and results)
  - OpenSearch Domain
  - IAM roles with appropriate permissions

## Setup Instructions

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-project
```

2. **Create and activate virtual environment**:

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
.\venv\Scripts\activate  # On Windows
```

3. **Install Python dependencies**:

```bash
pip install -r requirements.txt
```

4. **Set up environment variables**:
   Create a `.env` file in the project root with the following variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region

# Kinesis
KINESIS_STREAM_NAME=your-kinesis-stream

# S3
S3_RAW_BUCKET=your-raw-bucket
S3_RESULTS_BUCKET=your-results-bucket

# OpenSearch
OPENSEARCH_DOMAIN=your-opensearch-domain
OPENSEARCH_INDEX=tweets
```

5. **Download NLTK Data**:
   Run the following command to download required NLTK data:

```bash
python -c "import nltk; nltk.download('vader_lexicon'); nltk.download('stopwords')"

   ```bash
   python setup_nltk.py
   ```

5. **Start Kafka using Docker**:

```bash
docker-compose up -d
```

6. **Run the setup test to verify everything is working**:

```bash
python test_setup.py
```

### Option 2: Docker Compose Setup (Recommended)

This project includes Docker configuration to run both the producer and consumer services along with Kafka and Zookeeper in containers.

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-project
```

2. **Build the Docker images**:

```bash
docker-compose build
```

3. **Start all services**:

```bash
docker-compose up -d
```

4. **View logs from the services**:

```bash
# View all logs
docker-compose logs -f

# View logs from a specific service
docker-compose logs -f producer
docker-compose logs -f consumer
```

5. **Stop all services**:

```bash
docker-compose down
```

## Project Structure

```
kafka-project/
├── producer/
│   └── producer.py       # Kafka producer for data ingestion
├── consumer/
│   └── consumer.py       # Spark Streaming consumer for processing
├── setup_nltk.py        # NLTK data setup script
├── test_setup.py        # System setup verification
├── docker-compose.yml    # Docker configuration for all services
├── Dockerfile           # Single Dockerfile for both producer and consumer
└── requirements.txt      # Python dependencies
```

## Running the Application

### AWS Kinesis/EMR/S3 Deployment (Scalable)

This section describes how to deploy the system to AWS using managed services for scalability. The producer will run on AWS Fargate, the consumer (Spark Streaming) on Amazon EMR, data will be streamed via Amazon Kinesis Data Streams, and results/NLTK data will use Amazon S3.

**Prerequisites on AWS:**

1.  **Amazon Kinesis Data Stream**: Create a Kinesis Data Stream. Note its name and AWS region.
2.  **Amazon S3 Buckets**:
    *   One bucket for storing NLTK data (e.g., `s3://your-nltk-data-bucket/nltk_data/`).
    *   One bucket for the Spark consumer to write performance metrics and potentially other outputs (e.g., `s3://your-output-bucket/consumer_results/`).
    *   One bucket for EMR checkpointing (e.g., `s3://your-emr-checkpoint-bucket/checkpoints/`).
3.  **IAM Roles**:
    *   **Fargate Task Role**: Needs permissions to `kinesis:PutRecord` to the created Kinesis stream.
    *   **EMR EC2 Instance Profile Role**: Needs permissions to:
        *   Read from the Kinesis stream (`kinesis:DescribeStream`, `kinesis:GetRecords`, `kinesis:GetShardIterator`).
        *   Read NLTK data from its S3 bucket (`s3:GetObject`).
        *   Write results and checkpoint data to their respective S3 buckets (`s3:PutObject`, `s3:ListBucket`, etc.).
        *   Write logs to CloudWatch Logs.
    *   **EMR Service Role**: Standard EMR service role permissions.
4.  **VPC and Networking**: Ensure Fargate tasks and EMR clusters can access Kinesis endpoints and S3 (e.g., via VPC endpoints or NAT Gateway).

**Deployment Steps:**

1.  **Prepare NLTK Data for S3**:
    *   Run `python setup_nltk.py` locally to download NLTK data (especially `vader_lexicon` and `stopwords`).
    *   Upload the required NLTK resources (e.g., `nltk_data/sentiment/vader_lexicon.zip`, `nltk_data/corpora/stopwords.zip`) from your local `nltk_data` directory to your designated S3 bucket for NLTK data (e.g., `s3://your-nltk-data-bucket/nltk_data/sentiment/vader_lexicon.zip`).

2.  **Build and Push Docker Image for Producer**:
    *   The provided `Dockerfile` can be used to build an image for the producer.
    *   Ensure `requirements.txt` includes `boto3`.
    *   Build the image: `docker build -t your-producer-image .`
    *   Push it to a container registry like Amazon ECR:
        ```bash
        aws ecr get-login-password --region <your-region> | docker login --username AWS --password-stdin <your-aws-account-id>.dkr.ecr.<your-region>.amazonaws.com
        docker tag your-producer-image:latest <your-aws-account-id>.dkr.ecr.<your-region>.amazonaws.com/your-producer-repo:latest
        docker push <your-aws-account-id>.dkr.ecr.<your-region>.amazonaws.com/your-producer-repo:latest
        ```

3.  **Deploy Producer to AWS Fargate**:
    *   Create an ECS Task Definition for the producer:
        *   Use the ECR image URI from the previous step.
        *   Assign the Fargate Task Role.
        *   Set necessary environment variables:
            *   `KINESIS_STREAM_NAME`: Name of your Kinesis stream.
            *   `AWS_REGION`: The AWS region of your Kinesis stream.
            *   (Optional) `KAGGLE_USERNAME`, `KAGGLE_KEY` if `kagglehub` needs authentication in Fargate for dataset download. Consider pre-packaging the data or downloading from S3 in a production setup.
    *   Create an ECS Service or run a Task using this Task Definition.

4.  **Deploy Consumer to Amazon EMR**:
    *   Upload `consumer/consumer.py` and `requirements.txt` to an S3 bucket accessible by EMR.
    *   Create an EMR cluster with Spark installed.
    *   **Bootstrap Actions**: Configure a bootstrap action to:
        *   Install Python dependencies: `sudo python3 -m pip install -r /path/to/requirements.txt_on_emr_master_or_s3` (ensure `boto3` and `pyspark` are correctly handled, often PySpark is part of EMR).
        *   Download NLTK data from S3 to a standard NLTK path on all nodes (e.g., `/usr/share/nltk_data` or `/home/hadoop/nltk_data`):
            ```bash
            #!/bin/bash
            sudo aws s3 cp s3://your-nltk-data-bucket/nltk_data/sentiment/vader_lexicon.zip /tmp/vader_lexicon.zip --region <your-region>
            sudo aws s3 cp s3://your-nltk-data-bucket/nltk_data/corpora/stopwords.zip /tmp/stopwords.zip --region <your-region>
            sudo mkdir -p /usr/share/nltk_data/sentiment
            sudo mkdir -p /usr/share/nltk_data/corpora
            sudo unzip -o /tmp/vader_lexicon.zip -d /usr/share/nltk_data/sentiment/
            sudo unzip -o /tmp/stopwords.zip -d /usr/share/nltk_data/corpora/
            # Add other NLTK resources as needed
            # Set NLTK_DATA environment variable if using a non-standard path, though standard paths are preferred.
            # export NLTK_DATA=/usr/share/nltk_data
            ```
    *   **Submit Spark Application as a Step**:
        *   Application: `s3://path-to-your/consumer.py`
        *   Spark-submit options: `--deploy-mode cluster` (recommended for EMR)
        *   Ensure the EMR cluster's EC2 instances have the correct IAM Instance Profile Role.
        *   Pass necessary configurations to the Spark application, typically via environment variables set in the EMR step configuration (Spark environment tab) or directly in `spark-submit` arguments if preferred. Key environment variables for `consumer.py`:
            *   `KINESIS_STREAM_NAME`: Name of your Kinesis stream.
            *   `AWS_REGION`: AWS region of Kinesis and S3.
            *   `KINESIS_ENDPOINT_URL`: e.g., `https://kinesis.<your-region>.amazonaws.com`
            *   `S3_OUTPUT_PATH`: e.g., `s3a://your-output-bucket/consumer_results/` (Note: `s3a://` prefix for Spark S3 access)
            *   `CHECKPOINT_LOCATION`: e.g., `s3a://your-emr-checkpoint-bucket/checkpoints/`
            *   `KINESIS_STARTING_POSITION`: (Optional) `latest` or `trim_horizon`. Defaults to `latest`.
            *   `TRIGGER_PROCESSING_TIME`: (Optional) e.g., `60 seconds`. Defaults to `60 seconds`.
            *   `MAX_OFFSETS_PER_TRIGGER`: (Optional) Defaults to `1000`.
        *   The `spark-streaming-kinesis-asl` JAR is usually available on EMR and does not need to be specified in `--packages` if EMR is configured for Kinesis. If not, you might need: `--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:YOUR_SPARK_VERSION`

**Monitoring**:
*   Use Amazon CloudWatch for logs from Fargate and EMR.
*   Monitor Kinesis stream metrics in the Kinesis console.
*   Check S3 buckets for output data.

### Local Execution (Original Kafka-based)

**IMPORTANT**: The consumer application relies on the NLP data downloaded in the setup steps. Ensure you have run `python setup_nltk.py` successfully before starting the consumer.

1. **Start the Consumer**:
   Open a terminal and run the consumer application. It will wait for data from Kafka.

   ```bash
   python consumer/consumer.py
   ```

2. **Start the Producer**:
   In a separate terminal, run the producer to start streaming data.
   ```bash
   python producer/producer.py
   ```

### Docker Execution

With Docker Compose, both the producer and consumer services are started automatically along with Kafka and Zookeeper:

```bash
# Build and start all services
docker-compose build
docker-compose up -d

# Monitor the application logs
docker-compose logs -f

# When finished, stop all services
docker-compose down
```

## Components

1. **Data Producer**

   - Streams text data from a dataset to Kafka
   - Configurable data source and streaming rate

2. **Data Consumer**
   - Real-time processing using Spark Streaming
   - Word count analysis
   - Sentiment analysis
   - Trending topics detection

## Monitoring

The application outputs processing results to the console in real-time, showing:

- Word frequencies
- Sentiment scores
- Current trending topics

## Stopping the Application

### Local Execution
1. Stop the producer and consumer applications (Ctrl+C)
2. Stop Kafka:

```bash
docker-compose down
```

### Docker Execution
Stop all containers with a single command:

```bash
docker-compose down
```

## Troubleshooting

### Local Setup Issues

1. Make sure all dependencies are installed:

```bash
pip install -r requirements.txt
```

2. Verify NLTK data is properly downloaded:

```bash
python setup_nltk.py
```

3. Check if Kafka is running:

```bash
docker ps
```

4. Run the setup test:

```bash
python test_setup.py
```

### Docker Setup Issues

1. Check container logs for errors:

```bash
docker-compose logs -f
# Or for a specific service
docker-compose logs -f consumer
```

2. Verify all containers are running:

```bash
docker-compose ps
```

3. Restart the services if needed:

```bash
docker-compose restart
```

4. If problems persist, rebuild the images:

```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```
