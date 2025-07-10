"""
Load Testing for MapReduce Implementation
- Tests sequential vs parallel processing
- Tests batch vs streaming processing
- Tests different data load sizes
"""

import time
import logging
import os
import json
import random
import argparse
from typing import List, Dict, Any
import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import S3 upload helper
from consumer.s3_upload_helper import s3_uploader

# Import our MapReduce implementation
from mapreduce_implementation import (
    TextAnalyzer,
    create_kafka_consumer,
    consume_messages,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("load_test.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def load_kaggle_dataset():
    """Load the Kaggle dataset used in the producer"""
    try:
        logger.info("Downloading Kaggle dataset...")
        import kagglehub

        path = kagglehub.dataset_download("saurabhshahane/twitter-sentiment-dataset")
        csv_path = os.path.join(path, "Twitter_Data.csv")

        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Clean the data - drop rows with missing text
        df = df.dropna(subset=["clean_text"])

        # Convert to list of texts
        texts = df["clean_text"].astype(str).tolist()

        logger.info(f"Loaded {len(texts)} texts from Kaggle dataset")
        return texts
    except Exception as e:
        logger.error(f"Error loading Kaggle dataset: {str(e)}")
        raise


def get_sample_data(size=1000):
    """Get sample text data from the Kaggle dataset with optimized loading"""
    try:
        # Load the full dataset
        all_texts = load_kaggle_dataset()

        # If requested size is larger than available, use all and log a warning
        if size > len(all_texts):
            logger.warning(
                f"Requested size {size} is larger than dataset size {len(all_texts)}. Using all available texts."
            )
            return all_texts

        # For very large samples, use a more memory-efficient approach
        if size > 10000:
            logger.info(
                f"Sampling {size} texts (large sample, this might take a moment)..."
            )
            # Use numpy for faster random sampling with large datasets
            import numpy as np

            indices = np.random.choice(len(all_texts), size=size, replace=False)
            return [all_texts[i] for i in indices]
        else:
            # For smaller samples, use random.sample
            return random.sample(all_texts, size)
    except Exception as e:
        logger.error(f"Error getting sample data: {str(e)}")
        # Fallback to simple random text generation if there's an error
        logger.error(
            "Failed to load Kaggle dataset. Please ensure kagglehub is installed and configured."
        )
        raise


def run_kafka_producer(
    texts: List[str], topic: str = "text_data", batch_size: int = 100
):
    """Run a Kafka producer to send messages."""
    kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(
        f"Starting Kafka producer to {kafka_server}, sending {len(texts)} messages to topic {topic}"
    )

    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_server.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Send messages in batches
        sent_count = 0
        for i, text in enumerate(texts):
            producer.send(topic, {"text": text})
            sent_count += 1

            # Log progress
            if (i + 1) % batch_size == 0:
                logger.info(f"Sent {i + 1} messages...")
                producer.flush()  # Ensure messages are sent before continuing

        # Final flush
        producer.flush()
        logger.info(f"Successfully sent {sent_count} messages to Kafka topic: {topic}")

    except KafkaError as e:
        logger.error(f"Error sending messages to Kafka: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in Kafka producer: {str(e)}")
        return False

    return True


def run_batch_test(data_sizes: List[int], worker_counts: List[int]) -> Dict[str, Any]:
    """Run batch processing tests with different data sizes."""
    logger.info("\n" + "=" * 50)
    logger.info("BATCH PROCESSING TESTS")
    logger.info("=" * 50)

    all_results = []

    for size in data_sizes:
        logger.info(f"\nTesting with batch size: {size}")

        # Get test data from Kaggle dataset
        texts = get_sample_data(size)

        # Test sequential processing
        logger.info("Running sequential analysis...")
        seq_analyzer = TextAnalyzer(sequential=True)
        start_time = time.time()
        seq_results = seq_analyzer.run_analysis(texts)
        seq_time = time.time() - start_time

        seq_results["execution_time"] = seq_time
        seq_results["analyzer_type"] = "sequential"
        seq_results["batch_size"] = size
        seq_results["num_workers"] = 1
        seq_results["processing_type"] = "batch"

        # Calculate throughput (docs/second)
        seq_results["throughput"] = size / seq_time if seq_time > 0 else 0

        all_results.append(seq_results)
        logger.info(f"Sequential execution time: {seq_time:.2f} seconds")

        # Test parallel processing with different worker counts
        for worker_count in worker_counts:
            logger.info(f"Running parallel analysis with {worker_count} workers...")
            par_analyzer = TextAnalyzer(sequential=False, num_workers=worker_count)

            start_time = time.time()
            par_results = par_analyzer.run_analysis(texts)
            par_time = time.time() - start_time

            par_results["execution_time"] = par_time
            par_results["analyzer_type"] = "parallel"
            par_results["batch_size"] = size
            par_results["num_workers"] = worker_count
            par_results["processing_type"] = "batch"

            # Calculate metrics
            par_results["throughput"] = size / par_time if par_time > 0 else 0
            par_results["speedup"] = seq_time / par_time if par_time > 0 else 0
            if worker_count > 1:
                par_results["efficiency"] = (
                    par_results["speedup"] / worker_count
                ) * 100

            all_results.append(par_results)
            logger.info(
                f"Parallel ({worker_count} workers) execution time: {par_time:.2f} seconds"
            )
            if "speedup" in par_results:
                logger.info(f"Speedup: {par_results['speedup']:.2f}x")
                if "efficiency" in par_results:
                    logger.info(f"Efficiency: {par_results['efficiency']:.1f}%")

    return all_results


def run_stream_test(
    data_sizes: List[int], worker_counts: List[int], topic: str = "text_data"
) -> Dict[str, Any]:
    """Run stream processing tests with different data sizes."""
    logger.info("\n" + "=" * 50)
    logger.info("STREAM PROCESSING TESTS")
    logger.info("=" * 50)

    all_results = []

    for size in data_sizes:
        logger.info(f"\nTesting with stream size: {size}")

        # Get test data from Kaggle dataset
        texts = get_sample_data(size)

        # Send data to Kafka
        if not run_kafka_producer(texts, topic):
            logger.error(
                f"Failed to send messages to Kafka. Skipping stream test with size {size}."
            )
            continue

        # Test sequential streaming
        logger.info("Running sequential stream analysis...")

        # Create Kafka consumer
        consumer = create_kafka_consumer(topic, group_id=f"mapreduce_sequential_{size}")
        if not consumer:
            logger.error(
                "Failed to create Kafka consumer. Skipping sequential stream test."
            )
            continue

        # Consume and process messages sequentially
        start_time = time.time()
        stream_texts = consume_messages(consumer, max_messages=size)
        consumer.close()

        if len(stream_texts) == 0:
            logger.warning(
                f"No messages received from Kafka for size {size}. Check Kafka setup."
            )
            continue

        seq_analyzer = TextAnalyzer(sequential=True)
        seq_results = seq_analyzer.run_analysis(stream_texts)
        seq_time = time.time() - start_time

        seq_results["execution_time"] = seq_time
        seq_results["analyzer_type"] = "sequential"
        seq_results["stream_size"] = size
        seq_results["actual_size"] = len(stream_texts)
        seq_results["num_workers"] = 1
        seq_results["processing_type"] = "stream"

        # Calculate throughput (docs/second)
        seq_results["throughput"] = len(stream_texts) / seq_time if seq_time > 0 else 0

        all_results.append(seq_results)
        logger.info(f"Sequential stream processing time: {seq_time:.2f} seconds")

        # Test parallel streaming with different worker counts
        for worker_count in worker_counts:
            logger.info(
                f"Running parallel stream analysis with {worker_count} workers..."
            )

            # Create Kafka consumer
            consumer = create_kafka_consumer(
                topic, group_id=f"mapreduce_parallel_{size}_{worker_count}"
            )
            if not consumer:
                logger.error(
                    f"Failed to create Kafka consumer. Skipping parallel stream test with {worker_count} workers."
                )
                continue

            # Consume and process messages in parallel
            start_time = time.time()
            stream_texts = consume_messages(consumer, max_messages=size)
            consumer.close()

            if len(stream_texts) == 0:
                logger.warning(
                    f"No messages received from Kafka for size {size} with {worker_count} workers. Check Kafka setup."
                )
                continue

            par_analyzer = TextAnalyzer(sequential=False, num_workers=worker_count)
            par_results = par_analyzer.run_analysis(stream_texts)
            par_time = time.time() - start_time

            par_results["execution_time"] = par_time
            par_results["analyzer_type"] = "parallel"
            par_results["stream_size"] = size
            par_results["actual_size"] = len(stream_texts)
            par_results["num_workers"] = worker_count
            par_results["processing_type"] = "stream"

            # Calculate metrics
            par_results["throughput"] = (
                len(stream_texts) / par_time if par_time > 0 else 0
            )
            par_results["speedup"] = seq_time / par_time if par_time > 0 else 0
            if worker_count > 1:
                par_results["efficiency"] = (
                    par_results["speedup"] / worker_count
                ) * 100

            all_results.append(par_results)
            logger.info(
                f"Parallel ({worker_count} workers) stream processing time: {par_time:.2f} seconds"
            )
            if "speedup" in par_results:
                logger.info(f"Speedup: {par_results['speedup']:.2f}x")
                if "efficiency" in par_results:
                    logger.info(f"Efficiency: {par_results['efficiency']:.1f}%")

    return all_results


def plot_results(batch_results, stream_results, output_prefix="load_test"):
    """Plot comparison results."""
    # Convert to DataFrames
    batch_df = pd.DataFrame(batch_results)
    stream_df = pd.DataFrame(stream_results) if stream_results else None

    # Plot batch results
    if not batch_df.empty:
        plot_processing_type_results(
            batch_df, f"{output_prefix}_batch.png", "Batch Processing"
        )

    # Plot stream results
    if stream_df is not None and not stream_df.empty:
        plot_processing_type_results(
            stream_df, f"{output_prefix}_stream.png", "Stream Processing"
        )

    # Plot comparison between batch and stream if both available
    if stream_df is not None and not stream_df.empty and not batch_df.empty:
        plot_batch_vs_stream(batch_df, stream_df, f"{output_prefix}_comparison.png")


def plot_processing_type_results(df, output_file, title_prefix):
    """Plot results for a specific processing type (batch or stream)."""
    plt.figure(figsize=(16, 12))

    # Extract unique data sizes and worker counts
    data_sizes = (
        sorted(df["batch_size"].unique())
        if "batch_size" in df.columns
        else sorted(df["stream_size"].unique())
    )
    worker_counts = sorted(df["num_workers"].unique())

    # 1. Execution Time vs Data Size
    plt.subplot(2, 2, 1)
    for analyzer_type in ["sequential", "parallel"]:
        for worker_count in worker_counts:
            if analyzer_type == "sequential" and worker_count > 1:
                continue

            subset = df[
                (df["analyzer_type"] == analyzer_type)
                & (df["num_workers"] == worker_count)
            ]
            if len(subset) > 0:
                size_col = (
                    "batch_size" if "batch_size" in subset.columns else "stream_size"
                )
                label = f"{analyzer_type.title()} ({worker_count} workers)"
                plt.plot(subset[size_col], subset["execution_time"], "o-", label=label)

    plt.title(f"{title_prefix}: Execution Time vs Data Size")
    plt.xlabel("Data Size (number of messages)")
    plt.ylabel("Execution Time (seconds)")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend()

    # 2. Throughput vs Data Size
    plt.subplot(2, 2, 2)
    for analyzer_type in ["sequential", "parallel"]:
        for worker_count in worker_counts:
            if analyzer_type == "sequential" and worker_count > 1:
                continue

            subset = df[
                (df["analyzer_type"] == analyzer_type)
                & (df["num_workers"] == worker_count)
            ]
            if len(subset) > 0:
                size_col = (
                    "batch_size" if "batch_size" in subset.columns else "stream_size"
                )
                label = f"{analyzer_type.title()} ({worker_count} workers)"
                plt.plot(subset[size_col], subset["throughput"], "o-", label=label)

    plt.title(f"{title_prefix}: Throughput vs Data Size")
    plt.xlabel("Data Size (number of messages)")
    plt.ylabel("Throughput (messages/second)")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend()

    # 3. Speedup vs Data Size
    plt.subplot(2, 2, 3)
    speedup_available = False
    for worker_count in worker_counts:
        if worker_count == 1:  # Skip single worker (sequential)
            continue

        subset = df[
            (df["analyzer_type"] == "parallel") & (df["num_workers"] == worker_count)
        ]
        if len(subset) > 0 and "speedup" in subset.columns:
            speedup_available = True
            size_col = "batch_size" if "batch_size" in subset.columns else "stream_size"
            label = f"{worker_count} workers"
            plt.plot(subset[size_col], subset["speedup"], "o-", label=label)

    if speedup_available:
        plt.axhline(y=1, color="r", linestyle="--", label="Baseline (1x)")
        plt.title(f"{title_prefix}: Speedup vs Data Size")
        plt.xlabel("Data Size (number of messages)")
        plt.ylabel("Speedup (Sequential / Parallel)")
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.legend()
    else:
        plt.text(
            0.5,
            0.5,
            "Speedup metrics not available",
            ha="center",
            va="center",
            fontsize=12,
        )
        plt.title(f"{title_prefix}: Speedup vs Data Size")
        plt.xticks([])
        plt.yticks([])

    # 4. Efficiency vs Data Size
    plt.subplot(2, 2, 4)
    efficiency_available = False
    for worker_count in worker_counts:
        if worker_count == 1:  # Skip single worker
            continue

        subset = df[
            (df["analyzer_type"] == "parallel") & (df["num_workers"] == worker_count)
        ]
        if len(subset) > 0 and "efficiency" in subset.columns:
            efficiency_available = True
            size_col = "batch_size" if "batch_size" in subset.columns else "stream_size"
            label = f"{worker_count} workers"
            plt.plot(subset[size_col], subset["efficiency"], "o-", label=label)

    if efficiency_available:
        plt.axhline(y=100, color="r", linestyle="--", label="Ideal (100%)")
        plt.title(f"{title_prefix}: Parallel Efficiency vs Data Size")
        plt.xlabel("Data Size (number of messages)")
        plt.ylabel("Efficiency (%)")
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.legend()
    else:
        plt.text(
            0.5,
            0.5,
            "Efficiency metrics not available",
            ha="center",
            va="center",
            fontsize=12,
        )
        plt.title(f"{title_prefix}: Parallel Efficiency vs Data Size")
        plt.xticks([])
        plt.yticks([])

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Saved {title_prefix} results plot to {output_file}")

    # Save detailed results to CSV
    csv_file = output_file.replace(".png", ".csv")
    df.to_csv(csv_file, index=False)
    logger.info(f"Saved {title_prefix} detailed results to {csv_file}")

    # Upload plot and CSV to S3
    bucket_name = os.environ.get("S3_BUCKET_NAME", "kafka-project-results")
    
    try:
        # Create matplotlib figure for upload_plot method
        fig = plt.gcf()  # Get current figure
        
        # Upload plot image
        s3_plot_key = f"load_testing/{os.path.basename(output_file)}"
        s3_uploader.upload_plot(fig, output_file, bucket_name, s3_plot_key)
        logger.info(f"Uploaded plot to S3: s3://{bucket_name}/{s3_plot_key}")
        
        # Upload CSV file
        s3_csv_key = f"load_testing/{os.path.basename(csv_file)}"
        s3_uploader.upload_file(csv_file, bucket_name, s3_csv_key)
        logger.info(f"Uploaded CSV to S3: s3://{bucket_name}/{s3_csv_key}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        logger.info("Continuing without S3 upload. Set AWS credentials to enable uploads.")


def plot_batch_vs_stream(batch_df, stream_df, output_file):
    """Plot comparison between batch and stream processing."""
    plt.figure(figsize=(16, 12))

    # Normalize sizes for comparison
    batch_sizes = sorted(batch_df["batch_size"].unique())
    stream_sizes = sorted(stream_df["stream_size"].unique())

    # Find common sizes or closest matches
    common_sizes = []
    for b_size in batch_sizes:
        if b_size in stream_sizes:
            common_sizes.append((b_size, b_size))
        else:
            # Find closest stream size
            closest = min(stream_sizes, key=lambda x: abs(x - b_size))
            if abs(b_size - closest) / b_size < 0.2:  # Within 20%
                common_sizes.append((b_size, closest))

    if not common_sizes:
        logger.warning("No comparable sizes between batch and stream data")
        return

    # 1. Sequential Processing Time Comparison
    plt.subplot(2, 2, 1)
    batch_times = []
    stream_times = []
    x_labels = []

    for b_size, s_size in common_sizes:
        b_subset = batch_df[
            (batch_df["analyzer_type"] == "sequential")
            & (batch_df["batch_size"] == b_size)
        ]
        s_subset = stream_df[
            (stream_df["analyzer_type"] == "sequential")
            & (stream_df["stream_size"] == s_size)
        ]

        if not b_subset.empty and not s_subset.empty:
            batch_times.append(b_subset["execution_time"].iloc[0])
            stream_times.append(s_subset["execution_time"].iloc[0])
            x_labels.append(f"{b_size}")

    if batch_times and stream_times:
        x = range(len(batch_times))
        plt.bar(
            [i - 0.2 for i in x], batch_times, width=0.4, label="Batch", color="blue"
        )
        plt.bar(
            [i + 0.2 for i in x], stream_times, width=0.4, label="Stream", color="green"
        )
        plt.xticks(x, x_labels)
        plt.title("Sequential Processing: Batch vs Stream")
        plt.xlabel("Data Size")
        plt.ylabel("Execution Time (seconds)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)

    # 2. Parallel Processing Time Comparison (using 4 workers or closest available)
    plt.subplot(2, 2, 2)
    target_workers = 4  # Target number of workers for comparison

    batch_times = []
    stream_times = []
    x_labels = []
    batch_workers = []
    stream_workers = []

    for b_size, s_size in common_sizes:
        # Find batch entry with closest worker count to target
        b_worker_counts = batch_df[
            (batch_df["analyzer_type"] == "parallel")
            & (batch_df["batch_size"] == b_size)
        ]["num_workers"].unique()
        b_worker = (
            min(b_worker_counts, key=lambda x: abs(x - target_workers))
            if b_worker_counts.size > 0
            else None
        )

        # Find stream entry with closest worker count to target
        s_worker_counts = stream_df[
            (stream_df["analyzer_type"] == "parallel")
            & (stream_df["stream_size"] == s_size)
        ]["num_workers"].unique()
        s_worker = (
            min(s_worker_counts, key=lambda x: abs(x - target_workers))
            if s_worker_counts.size > 0
            else None
        )

        if b_worker and s_worker:
            b_subset = batch_df[
                (batch_df["analyzer_type"] == "parallel")
                & (batch_df["batch_size"] == b_size)
                & (batch_df["num_workers"] == b_worker)
            ]
            s_subset = stream_df[
                (stream_df["analyzer_type"] == "parallel")
                & (stream_df["stream_size"] == s_size)
                & (stream_df["num_workers"] == s_worker)
            ]

            if not b_subset.empty and not s_subset.empty:
                batch_times.append(b_subset["execution_time"].iloc[0])
                stream_times.append(s_subset["execution_time"].iloc[0])
                batch_workers.append(b_worker)
                stream_workers.append(s_worker)
                x_labels.append(f"{b_size}")

    if batch_times and stream_times:
        x = range(len(batch_times))
        plt.bar(
            [i - 0.2 for i in x],
            batch_times,
            width=0.4,
            label=f"Batch ({batch_workers[0]} workers)",
            color="blue",
        )
        plt.bar(
            [i + 0.2 for i in x],
            stream_times,
            width=0.4,
            label=f"Stream ({stream_workers[0]} workers)",
            color="green",
        )
        plt.xticks(x, x_labels)
        plt.title("Parallel Processing: Batch vs Stream")
        plt.xlabel("Data Size")
        plt.ylabel("Execution Time (seconds)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)

    # 3. Throughput Comparison (Sequential)
    plt.subplot(2, 2, 3)
    batch_throughput = []
    stream_throughput = []
    x_labels = []

    for b_size, s_size in common_sizes:
        b_subset = batch_df[
            (batch_df["analyzer_type"] == "sequential")
            & (batch_df["batch_size"] == b_size)
        ]
        s_subset = stream_df[
            (stream_df["analyzer_type"] == "sequential")
            & (stream_df["stream_size"] == s_size)
        ]

        if not b_subset.empty and not s_subset.empty:
            batch_throughput.append(b_subset["throughput"].iloc[0])
            stream_throughput.append(s_subset["throughput"].iloc[0])
            x_labels.append(f"{b_size}")

    if batch_throughput and stream_throughput:
        x = range(len(batch_throughput))
        plt.bar(
            [i - 0.2 for i in x],
            batch_throughput,
            width=0.4,
            label="Batch",
            color="blue",
        )
        plt.bar(
            [i + 0.2 for i in x],
            stream_throughput,
            width=0.4,
            label="Stream",
            color="green",
        )
        plt.xticks(x, x_labels)
        plt.title("Sequential Throughput: Batch vs Stream")
        plt.xlabel("Data Size")
        plt.ylabel("Throughput (messages/second)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)

    # 4. Throughput Comparison (Parallel)
    plt.subplot(2, 2, 4)
    batch_throughput = []
    stream_throughput = []
    x_labels = []

    for b_size, s_size in common_sizes:
        # Find batch entry with closest worker count to target
        b_worker_counts = batch_df[
            (batch_df["analyzer_type"] == "parallel")
            & (batch_df["batch_size"] == b_size)
        ]["num_workers"].unique()
        b_worker = (
            min(b_worker_counts, key=lambda x: abs(x - target_workers))
            if b_worker_counts.size > 0
            else None
        )

        # Find stream entry with closest worker count to target
        s_worker_counts = stream_df[
            (stream_df["analyzer_type"] == "parallel")
            & (stream_df["stream_size"] == s_size)
        ]["num_workers"].unique()
        s_worker = (
            min(s_worker_counts, key=lambda x: abs(x - target_workers))
            if s_worker_counts.size > 0
            else None
        )

        if b_worker and s_worker:
            b_subset = batch_df[
                (batch_df["analyzer_type"] == "parallel")
                & (batch_df["batch_size"] == b_size)
                & (batch_df["num_workers"] == b_worker)
            ]
            s_subset = stream_df[
                (stream_df["analyzer_type"] == "parallel")
                & (stream_df["stream_size"] == s_size)
                & (stream_df["num_workers"] == s_worker)
            ]

            if not b_subset.empty and not s_subset.empty:
                batch_throughput.append(b_subset["throughput"].iloc[0])
                stream_throughput.append(s_subset["throughput"].iloc[0])
                x_labels.append(f"{b_size}")

    if batch_throughput and stream_throughput:
        x = range(len(batch_throughput))
        plt.bar(
            [i - 0.2 for i in x],
            batch_throughput,
            width=0.4,
            label=f"Batch ({batch_workers[0]} workers)",
            color="blue",
        )
        plt.bar(
            [i + 0.2 for i in x],
            stream_throughput,
            width=0.4,
            label=f"Stream ({stream_workers[0]} workers)",
            color="green",
        )
        plt.xticks(x, x_labels)
        plt.title("Parallel Throughput: Batch vs Stream")
        plt.xlabel("Data Size")
        plt.ylabel("Throughput (messages/second)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Saved Batch vs Stream comparison plot to {output_file}")

    # Upload comparison plot to S3
    bucket_name = os.environ.get("S3_BUCKET_NAME", "kafka-project-results")
    
    try:
        # Create matplotlib figure for upload_plot method
        fig = plt.gcf()  # Get current figure
        
        # Upload plot image
        s3_plot_key = f"load_testing/{os.path.basename(output_file)}"
        s3_uploader.upload_plot(fig, output_file, bucket_name, s3_plot_key)
        logger.info(f"Uploaded comparison plot to S3: s3://{bucket_name}/{s3_plot_key}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        logger.info("Continuing without S3 upload. Set AWS credentials to enable uploads.")


def main():
    parser = argparse.ArgumentParser(
        description="Run load testing for MapReduce implementation"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "stream", "both"],
        default="both",
        help="Testing mode (batch, stream, or both)",
    )
    parser.add_argument(
        "--data-sizes",
        type=str,
        default="10,50,100,500",
        help="Comma-separated list of data sizes to test",
    )
    parser.add_argument(
        "--worker-counts",
        type=str,
        default="1,2,4,8",
        help="Comma-separated list of worker counts for parallel processing",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="text_data",
        help="Kafka topic for stream processing tests",
    )
    parser.add_argument(
        "--output-prefix", type=str, default="load_test", help="Prefix for output files"
    )

    args = parser.parse_args()

    # Parse arguments
    data_sizes = [int(x) for x in args.data_sizes.split(",")]
    worker_counts = [int(x) for x in args.worker_counts.split(",")]

    # Run tests
    batch_results = []
    stream_results = []

    if args.mode in ["batch", "both"]:
        batch_results = run_batch_test(data_sizes, worker_counts)

    if args.mode in ["stream", "both"]:
        stream_results = run_stream_test(data_sizes, worker_counts, args.topic)

    # Plot results
    plot_results(batch_results, stream_results, args.output_prefix)

    # Save all results to CSV
    if batch_results and stream_results:
        all_results = batch_results + stream_results
        all_df = pd.DataFrame(all_results)
        all_csv_file = f"{args.output_prefix}_all_results.csv"
        all_df.to_csv(all_csv_file, index=False)
        logger.info(f"Saved all results to {all_csv_file}")

        # Upload combined results to S3
        bucket_name = os.environ.get("S3_BUCKET_NAME", "kafka-project-results")
        
        try:
            # Upload CSV file
            s3_csv_key = f"load_testing/{os.path.basename(all_csv_file)}"
            s3_uploader.upload_file(all_csv_file, bucket_name, s3_csv_key)
            logger.info(f"Uploaded all results to S3: s3://{bucket_name}/{s3_csv_key}")
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            logger.info("Continuing without S3 upload. Set AWS credentials to enable uploads.")


if __name__ == "__main__":
    main()
