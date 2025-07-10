#!/usr/bin/env python3
import argparse
import time
import logging
import os
import random
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from itertools import chain
import kagglehub

# Import s3_uploader for uploading results to S3
try:
    from consumer.s3_upload_helper import s3_uploader

    s3_available = True
except ImportError:
    s3_available = False
    logging.warning(
        "S3 upload helper not available. Results will be saved locally only."
    )

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Try to import Kafka libraries but don't fail if they're not available
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    kafka_available = True
except ImportError:
    kafka_available = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("load_testing.log")],
)
logger = logging.getLogger(__name__)

# Initialize NLTK
try:
    nltk.data.find("vader_lexicon")
except LookupError:
    nltk.download("vader_lexicon")

# Constants
KAFKA_TOPIC = "airline_review_load_test"
DEFAULT_KAFKA_BROKER = "localhost:9092"


def download_kaggle_dataset(size=1000):
    """Download and load the Indian Airlines Customer Reviews dataset from Kaggle"""
    logger.info(f"Loading Indian Airlines Customer Reviews dataset (limiting to {size} records)")
    kaggle_dataset = "jagathratchakan/indian-airlines-customer-reviews"
    kaggle_csv = "Indian_Domestic_Airline.csv"
    
    try:
        # Download the dataset
        local_dir = kagglehub.dataset_download(kaggle_dataset)
        csv_path = os.path.join(local_dir, kaggle_csv)
        
        # Load and prepare the dataset
        df = pd.read_csv(csv_path)
        
        # Check for required columns
        required_cols = [
            "AirLine_Name", "Rating - 10", "Title", "Name", "Date", "Review", "Recommond"
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Dataset missing required columns: {missing_cols}")
            # Fall back to synthetic data if columns are missing
            return generate_synthetic_data_fallback(size)
        
        # Clean the data
        df = df.dropna(subset=["Review"])
        
        # Limit to the specified size
        if len(df) > size:
            df = df.sample(size)
        elif len(df) < size:
            logger.warning(f"Dataset only contains {len(df)} records, less than requested {size}")
        
        logger.info(f"Loaded {len(df)} records from Kaggle dataset")
        return df
    
    except Exception as e:
        logger.error(f"Failed to load Kaggle dataset: {e}")
        # Fall back to synthetic data if download fails
        return generate_synthetic_data_fallback(size)


def generate_synthetic_data_fallback(size=1000):
    """Generate synthetic airline review data as fallback if Kaggle dataset is unavailable"""
    logger.info(f"Falling back to synthetic data generation for {size} records")
    airlines = ["IndiGo", "Air India", "SpiceJet", "Vistara", "GoAir"]
    titles = [
        "Good Experience",
        "Bad Experience",
        "Average Flight",
        "Delayed Flight",
        "Excellent Service",
    ]
    data = []

    for i in range(size):
        airline = random.choice(airlines)
        rating = random.randint(1, 10)

        # Create more realistic reviews based on rating
        if rating >= 7:
            review = f"I had a great experience with {airline}. The flight was on time and the staff was friendly."
        elif rating >= 4:
            review = f"My flight with {airline} was okay. Some delays but overall acceptable service."
        else:
            review = f"I had a terrible experience with {airline}. The flight was delayed and staff was unhelpful."

        data.append(
            {
                "AirLine_Name": airline,
                "Rating - 10": rating,
                "Title": random.choice(titles),
                "Review": review,
                "Name": f"User_{i}",
                "Date": "2023-01-01",
                "Recommond": "YES" if rating > 5 else "NO",
            }
        )

    return pd.DataFrame(data)


def setup_kafka():
    """Set up Kafka topic for streaming tests if it doesn't exist"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot run streaming tests.")
        return False

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=DEFAULT_KAFKA_BROKER, client_id="load_test_admin"
        )

        # Check if topic exists, create if it doesn't
        try:
            topics = admin_client.list_topics()
            if KAFKA_TOPIC not in topics:
                logger.info(f"Creating Kafka topic: {KAFKA_TOPIC}")
                topic_list = [
                    NewTopic(name=KAFKA_TOPIC, num_partitions=4, replication_factor=1)
                ]
                admin_client.create_topics(new_topics=topic_list)
        except Exception as e:
            logger.warning(f"Error checking/creating topics: {e}")

        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

        return producer
    except Exception as e:
        logger.error(f"Failed to setup Kafka: {e}")
        return None


def send_to_kafka(producer, data, batch_size=100, delay=0.001):
    """Send data to Kafka in batches"""
    if producer is None or not kafka_available:
        return False

    total_sent = 0
    start_time = time.time()

    try:
        for i, row in enumerate(data.to_dict("records")):
            producer.send(KAFKA_TOPIC, value=row)
            total_sent += 1

            # Send in batches and add small delay to prevent overwhelming
            if i % batch_size == 0:
                producer.flush()
                time.sleep(delay)

        # Final flush
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(f"Sent {total_sent} records to Kafka in {elapsed:.2f} seconds")
        return True
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")
        return False


def process_with_threads(worker_count, data):
    """Process data using Python ThreadPoolExecutor"""
    logger.info(f"Processing with {worker_count} threads")

    # Initialize sentiment analyzer
    sia = SentimentIntensityAnalyzer()

    # Function to process a single record
    def process_record(record):
        review = str(record["Review"])
        result = {
            "airline": record["AirLine_Name"],
            "rating": record["Rating - 10"],
            "word_count": len(review.split()),
            "sentiment": sia.polarity_scores(review)["compound"],
            "text_length": len(review),
        }
        return result

    # Split data into chunks for each worker
    records = data.to_dict("records")
    chunk_size = max(1, len(records) // worker_count)
    chunks = [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]

    # Process data with ThreadPoolExecutor
    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        # Map each chunk to a worker
        futures = [
            executor.submit(lambda chunk: [process_record(r) for r in chunk], chunk)
            for chunk in chunks
        ]

        # Collect results
        for future in futures:
            results.extend(future.result())

    elapsed = time.time() - start_time

    return pd.DataFrame(results), elapsed


# MapReduce helper functions
def map_function(record, sia):
    """Map function for processing a single record"""
    review = str(record["Review"])
    result = {
        "airline": record["AirLine_Name"],
        "rating": record["Rating - 10"],
        "word_count": len(review.split()),
        "sentiment": sia.polarity_scores(review)["compound"],
        "text_length": len(review),
    }
    return result


def reduce_function(results):
    """Reduce function to combine all results"""
    return pd.DataFrame(results)


def process_batch_chunk(chunk, sia):
    """Process a chunk of records using map function for batch processing"""
    return [map_function(record, sia) for record in chunk]


def process_with_mapreduce(worker_count, data, sequential=False):
    """Process data using MapReduce pattern with multiprocessing"""
    mode = "sequential" if sequential else "parallel"
    logger.info(f"Processing with MapReduce ({mode}) using {worker_count} workers")

    # Initialize sentiment analyzer
    sia = SentimentIntensityAnalyzer()

    # Split data into chunks for each worker
    records = data.to_dict("records")
    chunk_size = max(1, len(records) // worker_count)
    chunks = [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]

    # Process data
    start_time = time.time()

    if sequential:
        # Sequential processing
        results = []
        for chunk in chunks:
            results.extend(process_chunk(chunk, sia))
    else:
        # Parallel processing with multiprocessing
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            # Use partial to set the sia parameter
            process_func = partial(process_chunk, sia=sia)
            # Execute the map phase in parallel
            map_results = executor.map(process_func, chunks)
            # Combine results (reduce phase)
            results = list(chain.from_iterable(map_results))

    elapsed = time.time() - start_time

    # Final reduce phase to create DataFrame
    result_df = reduce_function(results)

    return result_df, elapsed


def consume_from_kafka(worker_count, data_size, timeout=60, sequential=False):
    """Consume and process messages from Kafka using MapReduce pattern"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot consume from Kafka.")
        return None, 0

    try:
        # Create consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"load_test_group_{int(time.time())}",  # Unique group ID
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        # We'll use the global process_kafka_message function instead of defining one locally

        # Process messages
        start_time = time.time()
        message_count = 0
        all_messages = []

        # Fetch all messages first (within timeout)
        logger.info(f"Starting to consume messages, expecting {data_size} records")

        while message_count < data_size and (time.time() - start_time) < timeout:
            # Poll for messages with a short timeout
            messages = consumer.poll(
                timeout_ms=1000, max_records=min(500, data_size - message_count)
            )

            if not messages:
                continue

            # Collect messages
            for tp, msgs in messages.items():
                for message in msgs:
                    all_messages.append(message)
                    message_count += 1

                    # Break if we have enough messages
                    if message_count >= data_size:
                        break
                if message_count >= data_size:
                    break

            logger.info(f"Consumed {message_count}/{data_size} messages...")

        # Now process the messages using MapReduce pattern
        if all_messages:
            processing_start_time = time.time()

            # Split messages into chunks for parallel processing
            chunk_size = max(1, len(all_messages) // worker_count)
            chunks = [
                all_messages[i : i + chunk_size]
                for i in range(0, len(all_messages), chunk_size)
            ]

            results = []
            if sequential or worker_count == 1:
                # Sequential processing (Map phase)
                for message in all_messages:
                    results.append(process_kafka_message(message))
            else:
                # Parallel processing with multiprocessing (Map phase)
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    # Process chunks in parallel using our picklable function
                    map_results = executor.map(process_kafka_chunk, chunks)

                    # Combine results (Reduce phase)
                    results = list(chain.from_iterable(map_results))

            # Final timing includes only processing time, not message fetching
            processing_elapsed = time.time() - processing_start_time
            total_elapsed = time.time() - start_time

            logger.info(f"Processed {message_count} messages:")
            logger.info(f"  Total time: {total_elapsed:.2f} seconds")
            logger.info(f"  Processing time: {processing_elapsed:.2f} seconds")

            # Close consumer
            consumer.close()

            # Reduce phase - create DataFrame
            return pd.DataFrame(results), processing_elapsed
        else:
            logger.warning("No messages were consumed within the timeout period")
            consumer.close()
            return None, 0

    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}")
        return None, 0


def process_message(msg):
    """Process a single message for batch mode
    
    Args:
        msg: Message data with 'review_id', 'airlines_name', and 'review' fields
        
    Returns:
        dict: Processed result with review_id, airline, sentiment score and polarity
    """
    try:
        text = msg.get("review", "")
        if not text:
            return None

        # Analyze sentiment
        sid = SentimentIntensityAnalyzer()
        sentiment = sid.polarity_scores(text)
        polarity = "positive" if sentiment["compound"] > 0 else "negative"

        # Return processed result
        return {
            "review_id": msg.get("review_id", "unknown"),
            "airline": msg.get("airlines_name", "unknown"),
            "sentiment_score": sentiment["compound"],
            "polarity": polarity,
        }
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


def process_chunk(chunk):
    """Process a chunk of messages in parallel for batch mode
    
    Args:
        chunk: List of messages to process
        
    Returns:
        list: List of processed results
    """
    return [process_message(msg) for msg in chunk]


def process_kafka_message(message):
    """Process a single Kafka message for streaming mode
    
    Args:
        message: Kafka message object
        
    Returns:
        dict: Processed result with airline, rating, sentiment, etc.
    """
    try:
        record = message.value
        review = str(record.get("Review", ""))
        
        # Initialize sentiment analyzer
        sid = SentimentIntensityAnalyzer()
        sentiment = sid.polarity_scores(review)
        
        return {
            "airline": record.get("AirLine_Name", ""),
            "rating": record.get("Rating - 10", 0),
            "word_count": len(review.split()),
            "sentiment": sentiment["compound"],
            "text_length": len(review),
        }
    except Exception as e:
        logger.error(f"Error processing Kafka message: {e}")
        return None


def process_kafka_chunk(chunk):
    """Process a chunk of Kafka messages in parallel for stream mode
    
    Args:
        chunk: List of Kafka messages to process
        
    Returns:
        list: List of processed results
    """
    return [process_kafka_message(msg) for msg in chunk]


def run_stream_test(data_sizes, worker_counts):
    """Run streaming mode tests"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot run streaming tests.")
        return None

    results = []

    # Setup Kafka
    producer = setup_kafka()
    if producer is None:
        return None

    for data_size in data_sizes:
        # Generate test data
        data = download_kaggle_dataset(data_size)

        # Send data to Kafka
        logger.info(f"Sending {data_size} records to Kafka")
        if not send_to_kafka(producer, data):
            continue

        # Sequential MapReduce test
        seq_result, seq_elapsed = consume_from_kafka(1, data_size, sequential=True)

        if seq_result is not None:
            seq_throughput = data_size / seq_elapsed if seq_elapsed > 0 else 0
            results.append(
                {
                    "mode": "stream-sequential",
                    "data_size": data_size,
                    "worker_count": 1,
                    "elapsed_time": seq_elapsed,
                    "throughput": seq_throughput,
                }
            )
            logger.info(
                f"Stream-sequential test: {data_size} records, 1 worker, {seq_elapsed:.2f} seconds, {seq_throughput:.2f} records/sec"
            )

        # Parallel tests with different worker counts
        for worker_count in worker_counts:
            if worker_count == 1:
                # Skip if already tested with sequential
                continue

            logger.info(f"Testing with {worker_count} workers on {data_size} records")

            # Consume and process from Kafka using MapReduce
            result_df, elapsed = consume_from_kafka(
                worker_count, data_size, sequential=False
            )

            if result_df is not None:
                throughput = data_size / elapsed if elapsed > 0 else 0

                results.append(
                    {
                        "mode": "stream-mapreduce",
                        "data_size": data_size,
                        "worker_count": worker_count,
                        "elapsed_time": elapsed,
                        "throughput": throughput,
                    }
                )

                logger.info(
                    f"Stream-mapreduce test: {data_size} records, {worker_count} workers, {elapsed:.2f} seconds, {throughput:.2f} records/sec"
                )

    # Close producer
    if producer:
        producer.close()

    return pd.DataFrame(results) if results else None


def run_batch_test(data_sizes, worker_counts):
    """Run batch mode tests"""
    results = []

    for data_size in data_sizes:
        # Generate test data
        data = download_kaggle_dataset(data_size)

        # Sequential MapReduce test
        seq_result, seq_elapsed = process_with_mapreduce(1, data, sequential=True)
        seq_throughput = data_size / seq_elapsed if seq_elapsed > 0 else 0

        results.append(
            {
                "mode": "batch-sequential",
                "data_size": data_size,
                "worker_count": 1,
                "elapsed_time": seq_elapsed,
                "throughput": seq_throughput,
            }
        )

        logger.info(
            f"Batch-sequential test: {data_size} records, 1 worker, {seq_elapsed:.2f} seconds, {seq_throughput:.2f} records/sec"
        )

        # Parallel tests with different worker counts
        for worker_count in worker_counts:
            if worker_count == 1:
                # Skip if already tested with sequential
                continue

            logger.info(f"Testing with {worker_count} workers on {data_size} records")

            # Process with parallel MapReduce
            mr_result, mr_elapsed = process_with_mapreduce(
                worker_count, data, sequential=False
            )
            mr_throughput = data_size / mr_elapsed if mr_elapsed > 0 else 0

            results.append(
                {
                    "mode": "batch-mapreduce",
                    "data_size": data_size,
                    "worker_count": worker_count,
                    "elapsed_time": mr_elapsed,
                    "throughput": mr_throughput,
                }
            )

            logger.info(
                f"Batch-mapreduce test: {data_size} records, {worker_count} workers, {mr_elapsed:.2f} seconds, {mr_throughput:.2f} records/sec"
            )

            # Process with threads (keep this for comparison)
            thread_result, thread_elapsed = process_with_threads(worker_count, data)
            thread_throughput = data_size / thread_elapsed if thread_elapsed > 0 else 0

            results.append(
                {
                    "mode": "batch-threads",
                    "data_size": data_size,
                    "worker_count": worker_count,
                    "elapsed_time": thread_elapsed,
                    "throughput": thread_throughput,
                }
            )

            logger.info(
                f"Batch-threads test: {data_size} records, {worker_count} workers, {thread_elapsed:.2f} seconds, {thread_throughput:.2f} records/sec"
            )

    return pd.DataFrame(results)


def visualize_results(results):
    """Create visualizations from test results and upload to S3 if available"""
    if results is None or len(results) == 0:
        logger.error("No results to visualize")
        return

    # Create directory for results
    results_dir = "load_test_results"
    os.makedirs(results_dir, exist_ok=True)

    # Save raw results
    csv_file = f"{results_dir}/load_test_results.csv"
    results.to_csv(csv_file, index=False)

    # Upload CSV to S3 if available
    if s3_available:
        bucket_name = os.getenv("S3_BUCKET_NAME", "kafka-load-test-results")
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        s3_key = f"load_test_results/{timestamp}/load_test_results.csv"
        logger.info(f"Uploading results CSV to S3 bucket: {bucket_name}")
        s3_uploader.upload_file(csv_file, bucket_name, s3_key)

    # Create plot figures
    plt.figure(figsize=(15, 10))

    # 1. Execution time by worker count
    plt.subplot(2, 2, 1)
    for mode in results["mode"].unique():
        for size in results["data_size"].unique():
            df_subset = results[
                (results["mode"] == mode) & (results["data_size"] == size)
            ]
            if not df_subset.empty:
                plt.plot(
                    df_subset["worker_count"],
                    df_subset["elapsed_time"],
                    marker="o",
                    label=f"{mode} - {size} records",
                )

    plt.xlabel("Worker Count")
    plt.ylabel("Execution Time (s)")
    plt.title("Execution Time vs Worker Count")
    plt.grid(True)
    plt.legend()

    # 2. Throughput by worker count
    plt.subplot(2, 2, 2)
    for mode in results["mode"].unique():
        for size in results["data_size"].unique():
            df_subset = results[
                (results["mode"] == mode) & (results["data_size"] == size)
            ]
            if not df_subset.empty:
                plt.plot(
                    df_subset["worker_count"],
                    df_subset["throughput"],
                    marker="o",
                    label=f"{mode} - {size} records",
                )

    plt.xlabel("Worker Count")
    plt.ylabel("Throughput (records/sec)")
    plt.title("Throughput vs Worker Count")
    plt.grid(True)
    plt.legend()

    # 3. Scalability (speedup)
    plt.subplot(2, 2, 3)
    for mode in results["mode"].unique():
        for size in results["data_size"].unique():
            df_subset = results[
                (results["mode"] == mode) & (results["data_size"] == size)
            ]
            if not df_subset.empty and len(df_subset) > 1:
                # Calculate speedup relative to single worker
                baseline = df_subset[
                    df_subset["worker_count"] == min(df_subset["worker_count"])
                ]["elapsed_time"].values[0]
                speedups = [baseline / t for t in df_subset["elapsed_time"]]
                plt.plot(
                    df_subset["worker_count"],
                    speedups,
                    marker="o",
                    label=f"{mode} - {size} records",
                )

    # Add ideal speedup line
    max_workers = results["worker_count"].max()
    plt.plot([1, max_workers], [1, max_workers], "k--", label="Ideal Speedup")

    plt.xlabel("Worker Count")
    plt.ylabel("Speedup")
    plt.title("Scalability: Speedup vs Worker Count")
    plt.grid(True)
    plt.legend()

    # 4. Efficiency
    plt.subplot(2, 2, 4)
    for mode in results["mode"].unique():
        for size in results["data_size"].unique():
            df_subset = results[
                (results["mode"] == mode) & (results["data_size"] == size)
            ]
            if not df_subset.empty and len(df_subset) > 1:
                # Calculate efficiency (speedup / worker count)
                baseline = df_subset[
                    df_subset["worker_count"] == min(df_subset["worker_count"])
                ]["elapsed_time"].values[0]
                efficiencies = [
                    (baseline / t) / w * 100
                    for t, w in zip(
                        df_subset["elapsed_time"], df_subset["worker_count"]
                    )
                ]
                plt.plot(
                    df_subset["worker_count"],
                    efficiencies,
                    marker="o",
                    label=f"{mode} - {size} records",
                )

    plt.axhline(y=100, color="k", linestyle="--", label="Ideal Efficiency (100%)")
    plt.xlabel("Worker Count")
    plt.ylabel("Efficiency (%)")
    plt.title("Parallel Efficiency vs Worker Count")
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    summary_plot_file = "load_test_results/load_test_summary.png"
    plt.savefig(summary_plot_file, dpi=300, bbox_inches="tight")

    # Upload summary plot to S3 if available
    if s3_available:
        bucket_name = os.getenv("S3_BUCKET_NAME", "kafka-load-test-results")
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        s3_key = f"load_test_results/{timestamp}/load_test_summary.png"
        logger.info(f"Uploading summary plot to S3 bucket: {bucket_name}")
        s3_uploader.upload_file(summary_plot_file, bucket_name, s3_key)

    # Only show plot in interactive mode
    if "ipykernel" in sys.modules:
        plt.show()
    else:
        plt.close()

    # Create separate plots for each mode and data size
    for mode in results["mode"].unique():
        plt.figure(figsize=(12, 6))

        for size in results["data_size"].unique():
            df_subset = results[
                (results["mode"] == mode) & (results["data_size"] == size)
            ]
            if not df_subset.empty:
                plt.plot(
                    df_subset["worker_count"],
                    df_subset["throughput"],
                    marker="o",
                    linewidth=2,
                    label=f"{size} records",
                )

        plt.xlabel("Worker Count")
        plt.ylabel("Throughput (records/sec)")
        plt.title(f"Throughput vs Worker Count for {mode}")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        mode_plot_file = f"load_test_results/throughput_{mode}.png"
        plt.savefig(mode_plot_file, dpi=300, bbox_inches="tight")

        # Upload mode-specific plot to S3 if available
        if s3_available:
            bucket_name = os.getenv("S3_BUCKET_NAME", "kafka-load-test-results")
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            s3_key = f"load_test_results/{timestamp}/throughput_{mode}.png"
            logger.info(f"Uploading {mode} plot to S3 bucket: {bucket_name}")
            s3_uploader.upload_file(mode_plot_file, bucket_name, s3_key)

        plt.close()

    if s3_available:
        logger.info(
            "Visualizations saved to load_test_results directory and uploaded to S3"
        )
        logger.info(
            "S3 Upload paths: s3://{}/{}/".format(bucket_name, f"load_test_results/{timestamp}")
        )
    else:
        logger.info("Visualizations saved to load_test_results directory")


# Variable for tracking upload paths
upload_paths = []


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Load testing script for data processing"
    )
    parser.add_argument(
        "--mode",
        choices=["stream", "batch", "both"],
        default="both",
        help="Processing mode: stream, batch, or both",
    )
    parser.add_argument(
        "--data-sizes",
        type=str,
        default="1000,5000,10000",
        help="Comma-separated list of data sizes to test",
    )
    parser.add_argument(
        "--worker-counts",
        type=str,
        default="1,2,4,8",
        help="Comma-separated list of worker counts to test",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Parse arguments
    data_sizes = [int(x) for x in args.data_sizes.split(",")]
    worker_counts = [int(x) for x in args.worker_counts.split(",")]

    logger.info(f"Running load tests with:")
    logger.info(f"  Mode: {args.mode}")
    logger.info(f"  Data sizes: {data_sizes}")
    logger.info(f"  Worker counts: {worker_counts}")

    all_results = []

    # Run tests based on mode
    if args.mode in ["batch", "both"]:
        logger.info("Running batch mode tests")
        batch_results = run_batch_test(data_sizes, worker_counts)
        if batch_results is not None:
            all_results.append(batch_results)

    if args.mode in ["stream", "both"]:
        if kafka_available:
            logger.info("Running stream mode tests")
            stream_results = run_stream_test(data_sizes, worker_counts)
            if stream_results is not None:
                all_results.append(stream_results)
        else:
            logger.error("Kafka libraries not available. Skipping stream tests.")

    # Combine results and visualize
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        visualize_results(combined_results)

        # Print summary
        print("\nLoad Testing Results Summary:")
        print(combined_results.to_string(index=False))
    else:
        logger.error("No test results were generated")


if __name__ == "__main__":
    main()
