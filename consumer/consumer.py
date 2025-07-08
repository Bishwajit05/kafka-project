import os
import sys
import csv
import time

# Point Spark to the Python executable in the current virtual environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Note: For EMR, PYSPARK_SUBMIT_ARGS might be configured differently,
# often through EMR step configuration rather than environment variables.
# The `spark-streaming-kinesis-asl` JAR is typically provided by EMR for Kinesis integration.
# If running locally with Spark and Kinesis, you'd need to ensure this package is available.
# Example for local spark-submit:
# --packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.2.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4
# For now, we'll assume the Kinesis JAR is available in the EMR environment or Spark setup.
# We remove the explicit kafka package if not using Kafka directly for primary source.
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.2.4 pyspark-shell'


from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, split, explode, length, avg, count, desc, udf, current_timestamp, window, lower, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, BooleanType, DoubleType
from pyspark import StorageLevel
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging
from nltk.corpus import stopwords
import pandas as pd # For creating DataFrame for S3 write

stopword_set = set(stopwords.words('english'))

def is_not_stopword(word):
    return word not in stopword_set

is_not_stopword_udf = udf(is_not_stopword, BooleanType())

# Configure logging
logging.basicConfig(level=logging.INFO) # Changed to INFO for more visibility during AWS transition
logger = logging.getLogger(__name__)

# NLTK data (e.g., vader_lexicon) is expected to be pre-loaded in the Spark environment,
# typically via EMR bootstrap actions that copy it from S3 to a path NLTK can find.

@udf(FloatType())
def get_sentiment(text):
    if not hasattr(get_sentiment, "sia"):
        try:
            # This will look in standard NLTK data paths.
            # Ensure vader_lexicon is available there.
            get_sentiment.sia = SentimentIntensityAnalyzer()
            logger.info("SentimentIntensityAnalyzer initialized successfully.")
        except LookupError as e:
            logger.error(f"NLTK LookupError: {e}. Make sure 'vader_lexicon' is downloaded and available in NLTK_DATA paths.")
            # Raise the exception or return a specific error code if you want to handle it downstream
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing SentimentIntensityAnalyzer: {e}")
            raise

    try:
        if not isinstance(text, str) or len(text.strip()) == 0:
            return 0.0
        return float(get_sentiment.sia.polarity_scores(str(text))['compound'])
    except Exception as e:
        logger.error(f"Error in sentiment UDF processing text '{text[:50]}...': {e}")
        return 0.0


def process_unified_batch(df, epoch_id):
    s3_output_path = os.environ.get('S3_OUTPUT_PATH') # e.g., "s3a://your-bucket/performance-data/"
    if not s3_output_path:
        logger.error("S3_OUTPUT_PATH environment variable not set. Cannot save performance results.")
        # Decide if you want to proceed without saving or stop. For now, we'll log and continue.

    start_time_batch_processing = time.time()
    logger.info(f"\n--- Processing Batch {epoch_id} ---")

    df.persist() # Persist the raw batch DataFrame
    record_count = df.count()

    if record_count == 0:
        logger.info("Status: No new data in this batch. Waiting for next trigger...")
        df.unpersist()
        return

    logger.info(f"Status: Received {record_count} new records. Starting analysis...")

    avg_latency = None
    if "produced_at" in df.columns and record_count > 0 : # Ensure column exists and there are records
        # Calculate current time once for consistent latency calculation within the batch
        current_processing_time = time.time()
        # Latency: current processing time - time message was produced
        # Assuming 'produced_at' is a Unix timestamp (seconds since epoch)
        df_with_latency_col = df.withColumn("latency", lit(current_processing_time) - col("produced_at"))

        latency_result = df_with_latency_col.agg(avg("latency")).collect()
        if latency_result and latency_result[0] and latency_result[0][0] is not None:
            avg_latency = latency_result[0][0]
            logger.info(f"Average latency for batch: {avg_latency:.2f} seconds")
        else:
            logger.warning("Could not calculate average latency for the batch.")
        # The original df is used for further processing to avoid carrying the latency column if not needed everywhere
    else:
        logger.warning("'produced_at' column not found or empty batch, skipping latency calculation.")
        df_with_latency_col = df # Use original df if latency cannot be computed

    # --- Sentiment Analysis ---
    logger.info("\n=== Sentiment Analysis Summary ===")
    # Use the df_with_latency_col if you want latency in the sentiment_df, otherwise use original df
    sentiment_df = df.withColumn("sentiment", get_sentiment(col("text")))
    sentiment_summary = sentiment_df.agg(
        avg("sentiment").alias("average_sentiment"),
        count("*").alias("tweet_count")
    )
    sentiment_summary.show()

    # --- Word Count & Hashtag Trends ---
    # Use original df for word counts to avoid processing potentially modified df_with_latency_col structure
    words_df_exploded = df.select(explode(split(col("text"), " ")).alias("word")) \
                       .filter(length(col("word")) > 1)
    words_df_exploded.persist() # Persist this intermediate result as it's used multiple times

    logger.info("\n=== Top 5 Trending Words ===")
    word_counts = words_df_exploded.groupBy("word").count().orderBy(desc("count"))
    word_counts.show(5, truncate=False)

    logger.info("\n=== Top 5 Trending Hashtags ===")
    hashtag_counts = words_df_exploded.filter(col("word").startswith("#")) \
                                  .groupBy("word").count().orderBy(desc("count"))
    hashtag_counts.show(5, truncate=False)

    # --- Windowed Word Counts (Excluding Stopwords) ---
    # Add processing time column to the original df for windowing
    df_with_processing_time = df.withColumn("processing_time_ts", current_timestamp())

    words_for_windowing = df_with_processing_time.select(
        explode(split(lower(col("text")), " ")).alias("word"),
        col("processing_time_ts") # Use the timestamp column for windowing
    ).filter(length(col("word")) > 1)

    words_for_windowing = words_for_windowing.filter(is_not_stopword_udf(col("word")))

    windowed_word_counts = words_for_windowing.groupBy(
        window(col("processing_time_ts"), "5 minutes", "5 minutes"), # Tumbling window
        col("word")
    ).count().orderBy(desc("count"))

    logger.info("\n=== Top 5 Words in Last 5 Minutes (Excluding Stopwords) ===")
    windowed_word_counts.show(5, truncate=False)

    words_df_exploded.unpersist() # Unpersist intermediate df
    df.unpersist() # Unpersist the raw batch DataFrame

    batch_duration_secs = time.time() - start_time_batch_processing
    throughput = record_count / batch_duration_secs if batch_duration_secs > 0 else 0
    logger.info(f"Throughput: {throughput:.2f} messages/second")

    # Save performance results to S3
    if s3_output_path:
        try:
            # Create a Spark DataFrame from the single row of metrics
            spark = SparkSession.getActiveSession()
            if spark:
                metrics_data = [(epoch_id, record_count, batch_duration_secs, throughput, avg_latency if avg_latency is not None else -1.0)]
                metrics_schema = StructType([
                    StructField("epoch_id", IntegerType(), True),
                    StructField("record_count", IntegerType(), True),
                    StructField("batch_duration_sec", DoubleType(), True),
                    StructField("throughput_msg_per_sec", DoubleType(), True),
                    StructField("avg_latency_sec", DoubleType(), True)
                ])
                metrics_df = spark.createDataFrame(data=metrics_data, schema=metrics_schema)

                # Define the output file path. Appending epoch_id to make filenames unique per batch.
                # For appending to a single CSV, different strategies are needed (e.g. collect to driver, use Pandas).
                # Writing individual files per batch is simpler for distributed writes.
                # Or, use mode("append") if the schema is consistent and file system supports it well for CSV.
                # For S3, it's often better to write partitioned data (e.g., by date/hour/epoch_id)
                # results_file_path = os.path.join(s3_output_path, f"performance_epoch_{epoch_id}.csv")

                # Appending to a single CSV on S3 can be tricky with Spark's distributed nature.
                # It's often more robust to write data to a directory, potentially partitioned.
                # For this example, we'll try to append to a single CSV.
                # This might result in multiple files in a _temporary directory before being moved if not careful.
                # A common pattern is to write to a new directory each time and process them later,
                # or use a table format like Delta Lake or Hudi for atomic appends.

                # Simple approach: write as a single CSV part file to a directory.
                # Spark will create a directory with part-00000... files.
                # To get a single CSV, you might need a post-processing step or .coalesce(1).
                results_dir_path = os.path.join(s3_output_path, "performance_metrics")

                logger.info(f"Writing performance metrics for batch {epoch_id} to S3 path: {results_dir_path}")
                metrics_df.coalesce(1).write.mode("append").option("header", "true").csv(results_dir_path)
                logger.info(f"Successfully wrote performance metrics to S3 for batch {epoch_id}.")

            else:
                logger.error("Could not get active Spark session to save performance metrics.")
        except Exception as e:
            logger.error(f"Error saving performance results to S3 path {s3_output_path}: {e}", exc_info=True)

    logger.info(f"--- Batch {epoch_id} processing finished in {batch_duration_secs:.2f} seconds ---")


from pyspark.sql.functions import lit # Import lit function

def create_spark_session():
    """Create a Spark session."""
    # When running on EMR, Spark session is often managed by EMR.
    # This configuration is more for local testing or specific Spark submit scenarios.
    # EMR usually provides necessary Hadoop S3 connectors. If running locally, ensure Hadoop libs for S3 (s3a) are on classpath.
    app_name = "KinesisSparkConsumer"
    builder = SparkSession.builder.appName(app_name)

    # For local development with S3, you might need to configure AWS credentials for Spark's Hadoop S3A connector
    # For EMR, IAM roles (EC2 instance profile) typically handle S3 access.
    # Example (local, not recommended for EMR):
    # aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    # aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    # if aws_access_key_id and aws_secret_access_key:
    #     builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    #                      .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    #                      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # General configurations (can be overridden by spark-submit or EMR settings)
    builder = builder.master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") # Slightly increased for potential S3 writes

    logger.info(f"Creating Spark session for {app_name}")
    return builder.getOrCreate()


def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN") # Keep WARN or ERROR for less verbose logs in production
        
        kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME')
        aws_region = os.environ.get('AWS_REGION', 'us-east-1')
        kinesis_endpoint_url = os.environ.get('KINESIS_ENDPOINT_URL', f'https://kinesis.{aws_region}.amazonaws.com')
        kinesis_starting_position = os.environ.get('KINESIS_STARTING_POSITION', 'latest')

        if not kinesis_stream_name:
            logger.error("KINESIS_STREAM_NAME environment variable not set. Exiting.")
            return

        logger.info(f"Connecting to Kinesis stream: {kinesis_stream_name} in region {aws_region} at endpoint {kinesis_endpoint_url}")
        logger.info(f"Starting position: {kinesis_starting_position}")

        # --- DStream Kinesis Integration ---
        from pyspark import SparkContext
        from pyspark.streaming import StreamingContext
        from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

        sc = spark.sparkContext
        ssc = StreamingContext(sc, 10)  # 10 second batch interval

        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        # Remove protocol from endpoint for DStream API
        endpoint_url = kinesis_endpoint_url.replace("https://", "").replace("http://", "")

        kinesisStream = KinesisUtils.createStream(
            ssc,
            "KinesisDStreamApp",  # app name
            kinesis_stream_name,
            endpoint_url,
            aws_region,
            InitialPositionInStream.LATEST if kinesis_starting_position == "latest" else InitialPositionInStream.TRIM_HORIZON,
            2,  # checkpoint interval (seconds)
            aws_access_key,
            aws_secret_key,
            StorageLevel.DISK_ONLY_2  
        )
        checkpoint_location = os.environ.get('CHECKPOINT_LOCATION', "kinesis_checkpoint_dir_local")
        if checkpoint_location.startswith("s3"):
             logger.info(f"Using S3 checkpoint location: {checkpoint_location}")
        else:
             logger.warning(f"Using local checkpoint location: {checkpoint_location}. This is not suitable for production EMR.")


        query = parsed_df.writeStream \
            .foreachBatch(process_unified_batch) \
            .trigger(processingTime=os.environ.get('TRIGGER_PROCESSING_TIME', '60 seconds')) \
            .option("checkpointLocation", checkpoint_location) \
            .start()

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    except Exception as e:
        logger.error(f"An error occurred in the main Kinesis consumer application: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("\nShutting down Spark session...")
            spark.stop()
            logger.info("Spark session stopped successfully.")

if __name__ == "__main__":
    main()