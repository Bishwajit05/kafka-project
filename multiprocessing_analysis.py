#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import multiprocessing as mp
import os
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import Dict, List, Tuple, Any

import matplotlib.pyplot as plt
import nltk
import pandas as pd
import kagglehub
from kafka import KafkaConsumer
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from consumer.s3_upload_helper import s3_uploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("mapreduce_analysis.log")],
)
logger = logging.getLogger(__name__)

# Initialize NLTK components
try:
    nltk.data.find("vader_lexicon")
    nltk.data.find("stopwords")
except LookupError:
    nltk.download("vader_lexicon")
    nltk.download("stopwords")

# Constants
KAFKA_TOPIC = "airline_customer_review_stream"
DEFAULT_KAFKA_BROKER = "localhost:9092"
KAGGLE_DATASET = "jagathratchakan/indian-airlines-customer-reviews"
KAGGLE_CSV_FILE = "Indian_Domestic_Airline.csv"
STOPWORDS = set(stopwords.words("english"))

# Initialize sentiment analyzer (shared resource)
sentiment_analyzer = SentimentIntensityAnalyzer()


# MapReduce functions for word count analysis
def map_words(text: str) -> List[Tuple[str, int]]:
    """Map function: Split text into words and emit (word, 1) pairs."""
    if not isinstance(text, str) or not text.strip():
        return []

    # Convert to lowercase and split by non-alphanumeric characters
    words = re.findall(r"\b[a-z]+\b", text.lower())

    # Filter out stopwords and very short words
    filtered_words = [
        (word, 1) for word in words if word not in STOPWORDS and len(word) > 2
    ]
    return filtered_words


def reduce_words(mapped_data: List[Tuple[str, int]]) -> Dict[str, int]:
    """Reduce function: Count occurrences of each word."""
    word_counts = {}
    for word, count in mapped_data:
        if word in word_counts:
            word_counts[word] += count
        else:
            word_counts[word] = count
    return word_counts


def map_sentiment(text: str) -> float:
    """Map function: Calculate sentiment score for text."""
    if not isinstance(text, str) or not text.strip():
        return 0.0

    try:
        return sentiment_analyzer.polarity_scores(text)["compound"]
    except Exception as e:
        logger.warning(f"Sentiment analysis error: {e}")
        return 0.0


# Time window tracking for stream processing
class TimeWindowTracker:
    """Tracks data within time windows for streaming analysis."""

    def __init__(self, window_size_minutes: int = 5):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.data_points = []  # List of (timestamp, data) tuples

    def add_data(self, data, timestamp=None):
        """Add a data point with its timestamp."""
        if timestamp is None:
            timestamp = datetime.now()
        self.data_points.append((timestamp, data))
        self._clean_old_data()

    def _clean_old_data(self):
        """Remove data older than the window size."""
        cutoff = datetime.now() - self.window_size
        self.data_points = [(ts, data) for ts, data in self.data_points if ts >= cutoff]

    def get_window_data(self):
        """Get all data points in the current window."""
        self._clean_old_data()
        return [data for _, data in self.data_points]


# Batch processing functions
def process_batch_sequential(
    data: List[Dict[str, Any]],
) -> Tuple[Dict[str, int], float]:
    """Process a batch of data sequentially."""
    start_time = time.time()

    # Extract review texts
    reviews = [item.get("Review", "") for item in data if item.get("Review")]

    # Word count MapReduce
    mapped_words = []
    for review in reviews:
        mapped_words.extend(map_words(review))

    word_counts = reduce_words(mapped_words)

    # Sentiment analysis
    sentiments = [map_sentiment(review) for review in reviews]
    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0

    elapsed = time.time() - start_time
    logger.info(f"Sequential batch processing completed in {elapsed:.2f}s")

    return word_counts, avg_sentiment


def process_batch_parallel(
    data: List[Dict[str, Any]], n_workers: int
) -> Tuple[Dict[str, int], float]:
    """Process a batch of data in parallel using MapReduce."""
    start_time = time.time()

    # Extract review texts
    reviews = [item.get("Review", "") for item in data if item.get("Review")]

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # Map phase: Process reviews in parallel
        mapped_words_lists = list(executor.map(map_words, reviews))
        sentiments = list(executor.map(map_sentiment, reviews))

    # Flatten mapped words
    mapped_words = [item for sublist in mapped_words_lists for item in sublist]

    # Reduce phase: Count words
    word_counts = reduce_words(mapped_words)

    # Calculate average sentiment
    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0

    elapsed = time.time() - start_time
    logger.info(
        f"Parallel batch processing with {n_workers} workers completed in {elapsed:.2f}s"
    )

    return word_counts, avg_sentiment


# Stream processing functions
def process_message(message, window_tracker):
    """Process a single message from Kafka stream."""
    try:
        # Parse JSON message
        data = json.loads(message.value.decode("utf-8"))

        # Extract review text
        review_text = data.get("Review", "")
        if not review_text:
            return None

        # Process message
        word_pairs = map_words(review_text)
        sentiment = map_sentiment(review_text)

        # Add to time window tracker
        timestamp = datetime.fromtimestamp(data.get("timestamp", time.time()))
        window_tracker.add_data((word_pairs, sentiment), timestamp)

        return data
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


def analyze_window_data(window_tracker):
    """Analyze data in the current time window."""
    window_data = window_tracker.get_window_data()
    if not window_data:
        return {}, 0.0

    # Unpack data
    all_word_pairs = []
    all_sentiments = []
    for word_pairs, sentiment in window_data:
        all_word_pairs.extend(word_pairs)
        all_sentiments.append(sentiment)

    # Reduce word counts
    word_counts = reduce_words(all_word_pairs)

    # Calculate average sentiment
    avg_sentiment = sum(all_sentiments) / len(all_sentiments) if all_sentiments else 0.0

    return word_counts, avg_sentiment


def process_stream_sequential(timeout=300):
    """Process Kafka stream sequentially."""
    logger.info("Starting sequential stream processing...")

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=DEFAULT_KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="mapreduce_sequential_group",
    )

    # Set up time window tracker
    window_tracker = TimeWindowTracker(window_size_minutes=5)

    # Track metrics
    start_time = time.time()
    message_count = 0
    metrics = []
    last_analysis_time = time.time()

    try:
        # Process messages for the specified timeout
        while time.time() - start_time < timeout:
            # Poll for messages with a timeout
            messages = consumer.poll(timeout_ms=1000, max_records=10)

            # Process each message
            for _, msg_list in messages.items():
                for message in msg_list:
                    data = process_message(message, window_tracker)
                    if data:
                        message_count += 1

            # Analyze window data every 5 seconds
            if time.time() - last_analysis_time >= 5:
                analysis_start = time.time()

                # Get top words and sentiment for the current window
                word_counts, avg_sentiment = analyze_window_data(window_tracker)

                # Get top 5 words
                top_words = sorted(
                    word_counts.items(), key=lambda x: x[1], reverse=True
                )[:5]

                # Calculate elapsed time for this analysis
                analysis_elapsed = time.time() - analysis_start

                # Log results
                logger.info(f"Last 5 min - Top 5 words: {top_words}")
                logger.info(f"Last 5 min - Avg sentiment: {avg_sentiment:.3f}")

                # Store metrics
                metrics.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "processing_mode": "sequential",
                        "messages_processed": message_count,
                        "analysis_time": analysis_elapsed,
                        "avg_sentiment": avg_sentiment,
                    }
                )

                last_analysis_time = time.time()
    except KeyboardInterrupt:
        logger.info("Sequential stream processing interrupted")
    finally:
        consumer.close()

    # Return metrics
    return metrics


def process_stream_parallel(n_workers=4, timeout=300):
    """Process Kafka stream in parallel."""
    logger.info(f"Starting parallel stream processing with {n_workers} workers...")

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=DEFAULT_KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="mapreduce_parallel_group",
    )

    # Set up time window tracker
    window_tracker = TimeWindowTracker(window_size_minutes=5)

    # Create a process pool
    pool = ProcessPoolExecutor(max_workers=n_workers)

    # Track metrics
    start_time = time.time()
    message_count = 0
    metrics = []
    last_analysis_time = time.time()

    try:
        # Process messages for the specified timeout
        while time.time() - start_time < timeout:
            # Poll for messages with a timeout
            messages = consumer.poll(timeout_ms=1000, max_records=50)

            # Flatten messages
            all_messages = []
            for _, msg_list in messages.items():
                all_messages.extend(msg_list)

            if all_messages:
                # Process messages in parallel
                with ProcessPoolExecutor(max_workers=n_workers) as executor:
                    # Use partial to pass window_tracker to each worker
                    # Note: In a real implementation, you'd need a more sophisticated
                    # approach for sharing window_tracker across processes
                    process_func = partial(
                        process_message, window_tracker=window_tracker
                    )
                    results = list(executor.map(process_func, all_messages))

                    # Count processed messages
                    message_count += sum(1 for r in results if r is not None)

            # Analyze window data every 5 seconds
            if time.time() - last_analysis_time >= 5:
                analysis_start = time.time()

                # Get top words and sentiment for the current window
                word_counts, avg_sentiment = analyze_window_data(window_tracker)

                # Get top 5 words
                top_words = sorted(
                    word_counts.items(), key=lambda x: x[1], reverse=True
                )[:5]

                # Calculate elapsed time for this analysis
                analysis_elapsed = time.time() - analysis_start

                # Log results
                logger.info(f"Last 5 min - Top 5 words (parallel): {top_words}")
                logger.info(
                    f"Last 5 min - Avg sentiment (parallel): {avg_sentiment:.3f}"
                )

                # Store metrics
                metrics.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "processing_mode": "parallel",
                        "worker_count": n_workers,
                        "messages_processed": message_count,
                        "analysis_time": analysis_elapsed,
                        "avg_sentiment": avg_sentiment,
                    }
                )

                last_analysis_time = time.time()
    except KeyboardInterrupt:
        logger.info("Parallel stream processing interrupted")
    finally:
        consumer.close()
        pool.shutdown()

    # Return metrics
    return metrics


# Visualization functions
def plot_metrics(all_metrics):
    """Plot performance metrics."""
    df = pd.DataFrame(all_metrics)

    if df.empty:
        logger.warning("No metrics to plot")
        return

    # Create figure with subplots
    fig, axs = plt.subplots(2, 2, figsize=(15, 10))

    # Plot 1: Processing time comparison (batch mode)
    batch_df = df[df["mode"] == "batch"]
    if not batch_df.empty:
        axs[0, 0].set_title("Batch Processing Time Comparison")
        for mode in ["sequential", "parallel"]:
            mode_df = batch_df[batch_df["processing_mode"] == mode]
            if not mode_df.empty:
                worker_groups = mode_df.groupby("worker_count")
                for workers, group in worker_groups:
                    label = (
                        f"{mode} ({workers} workers)"
                        if mode == "parallel"
                        else "sequential"
                    )
                    axs[0, 0].plot(
                        group["data_size"],
                        group["processing_time"],
                        marker="o",
                        label=label,
                    )

        axs[0, 0].set_xlabel("Data Size")
        axs[0, 0].set_ylabel("Processing Time (s)")
        axs[0, 0].legend()
        axs[0, 0].grid(True)

    # Plot 2: Throughput comparison (messages/second)
    stream_df = df[df["mode"] == "stream"]
    if not stream_df.empty:
        axs[0, 1].set_title("Stream Processing Throughput")
        for mode in ["sequential", "parallel"]:
            mode_df = stream_df[stream_df["processing_mode"] == mode]
            if not mode_df.empty:
                worker_groups = mode_df.groupby("worker_count")
                for workers, group in worker_groups:
                    label = (
                        f"{mode} ({workers} workers)"
                        if mode == "parallel"
                        else "sequential"
                    )
                    # Calculate throughput (messages per analysis period)
                    throughput = group["messages_processed"] / group["analysis_time"]
                    axs[0, 1].plot(
                        group["timestamp"], throughput, marker=".", label=label
                    )

        axs[0, 1].set_xlabel("Time")
        axs[0, 1].set_ylabel("Messages/Second")
        axs[0, 1].legend()
        axs[0, 1].grid(True)
        plt.setp(axs[0, 1].xaxis.get_majorticklabels(), rotation=45)

    # Plot 3: Sentiment over time
    if not stream_df.empty:
        axs[1, 0].set_title("Average Sentiment Over Time")
        for mode in ["sequential", "parallel"]:
            mode_df = stream_df[stream_df["processing_mode"] == mode]
            if not mode_df.empty:
                worker_groups = mode_df.groupby("worker_count")
                for workers, group in worker_groups:
                    label = (
                        f"{mode} ({workers} workers)"
                        if mode == "parallel"
                        else "sequential"
                    )
                    axs[1, 0].plot(
                        group["timestamp"],
                        group["avg_sentiment"],
                        marker=".",
                        label=label,
                    )

        axs[1, 0].set_xlabel("Time")
        axs[1, 0].set_ylabel("Average Sentiment")
        axs[1, 0].legend()
        axs[1, 0].grid(True)
        plt.setp(axs[1, 0].xaxis.get_majorticklabels(), rotation=45)

    # Plot 4: Analysis time comparison
    if not stream_df.empty:
        axs[1, 1].set_title("Analysis Time Comparison")
        for mode in ["sequential", "parallel"]:
            mode_df = stream_df[stream_df["processing_mode"] == mode]
            if not mode_df.empty:
                worker_groups = mode_df.groupby("worker_count")
                for workers, group in worker_groups:
                    label = (
                        f"{mode} ({workers} workers)"
                        if mode == "parallel"
                        else "sequential"
                    )
                    axs[1, 1].plot(
                        group["timestamp"],
                        group["analysis_time"],
                        marker=".",
                        label=label,
                    )

        axs[1, 1].set_xlabel("Time")
        axs[1, 1].set_ylabel("Analysis Time (s)")
        axs[1, 1].legend()
        axs[1, 1].grid(True)
        plt.setp(axs[1, 1].xaxis.get_majorticklabels(), rotation=45)

    # Adjust layout and save
    plt.tight_layout()
    plt.savefig("mapreduce_performance.png")

    # Upload to S3
    try:
        s3_uploader.upload_plot(
            "mapreduce_performance.png",
            os.environ.get("S3_BUCKET", "performance-metrics"),
            "mapreduce_performance.png",
        )
        logger.info("Uploaded performance metrics plot to S3")
    except Exception as e:
        logger.error(f"Failed to upload plot to S3: {e}")

    # Save metrics to CSV
    df.to_csv("mapreduce_performance_metrics.csv", index=False)

    # Upload to S3
    try:
        s3_uploader.upload_file(
            "mapreduce_performance_metrics.csv",
            os.environ.get("S3_BUCKET", "performance-metrics"),
            "mapreduce_performance_metrics.csv",
        )
        logger.info("Uploaded performance metrics CSV to S3")
    except Exception as e:
        logger.error(f"Failed to upload metrics CSV to S3: {e}")


# Main execution functions
def download_kaggle_dataset(use_local=False):
    """
    Download the Indian Airlines customer review dataset from Kaggle and return the path to the CSV file.

    Args:
        use_local: If True, try to use a locally cached version first
    """
    # Check for cached dataset
    cache_dir = os.path.expanduser("~/.cache/kaggle_datasets")
    cached_path = os.path.join(
        cache_dir, KAGGLE_DATASET.replace("/", "_"), KAGGLE_CSV_FILE
    )

    if use_local and os.path.exists(cached_path):
        logger.info(f"Using locally cached dataset: {cached_path}")
        return cached_path

    logger.info(f"Downloading Kaggle dataset: {KAGGLE_DATASET}")
    try:
        local_dir = kagglehub.dataset_download(KAGGLE_DATASET)
        csv_path = os.path.join(local_dir, KAGGLE_CSV_FILE)
        if os.path.exists(csv_path):
            logger.info(f"Dataset downloaded successfully to {csv_path}")
            return csv_path
        else:
            logger.error(f"CSV file not found at {csv_path}")
            return None
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None


def load_airline_data(csv_path, limit=None):
    """
    Load airline review data from the CSV file.

    Args:
        csv_path: Path to the CSV file
        limit: Maximum number of records to load (None for all)

    Returns:
        List of dictionaries containing the review data
    """
    logger.info(f"Loading airline review data from {csv_path}")
    try:
        df = pd.read_csv(csv_path)
        # Clean the data
        df = df.dropna(subset=["Review"])

        # Limit the data if needed
        if limit is not None and limit > 0:
            df = df.head(limit)

        # Convert to list of dictionaries
        data = df.to_dict("records")

        # Add timestamp if not present
        for item in data:
            if "timestamp" not in item:
                item["timestamp"] = time.time()

        logger.info(f"Loaded {len(data)} records from the dataset")
        return data
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return []


def run_batch_test(data_sizes, worker_counts, use_local_data=False):
    """Run batch processing tests with different data sizes and worker counts."""
    logger.info("Starting batch processing tests...")

    # Download the Kaggle dataset
    csv_path = download_kaggle_dataset(use_local=use_local_data)
    if not csv_path:
        logger.error("Failed to download the dataset. Cannot run batch tests.")
        return []

    # Load the full dataset
    full_data = load_airline_data(csv_path)
    if not full_data:
        logger.error("Failed to load data from the CSV file. Cannot run batch tests.")
        return []

    metrics = []

    # Run tests for different data sizes
    for data_size in data_sizes:
        # Ensure data_size doesn't exceed the available data
        actual_size = min(data_size, len(full_data))
        data = full_data[:actual_size]

        # Sequential processing
        logger.info(
            f"Running sequential batch processing with {actual_size} records..."
        )
        start_time = time.time()
        word_counts, avg_sentiment = process_batch_sequential(data)
        seq_elapsed = time.time() - start_time

        # Log top 5 words
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        logger.info(f"Sequential - Top 5 words: {top_words}")
        logger.info(f"Sequential - Avg sentiment: {avg_sentiment:.3f}")

        # Store metrics
        metrics.append(
            {
                "mode": "batch",
                "processing_mode": "sequential",
                "worker_count": 1,
                "data_size": actual_size,
                "processing_time": seq_elapsed,
                "avg_sentiment": avg_sentiment,
            }
        )

        # Parallel processing with different worker counts
        for n_workers in worker_counts:
            logger.info(
                f"Running parallel batch processing with {actual_size} records and {n_workers} workers..."
            )
            start_time = time.time()
            word_counts, avg_sentiment = process_batch_parallel(data, n_workers)
            par_elapsed = time.time() - start_time

            # Log top 5 words
            top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[
                :5
            ]
            logger.info(f"Parallel ({n_workers} workers) - Top 5 words: {top_words}")
            logger.info(
                f"Parallel ({n_workers} workers) - Avg sentiment: {avg_sentiment:.3f}"
            )

            # Store metrics
            metrics.append(
                {
                    "mode": "batch",
                    "processing_mode": "parallel",
                    "worker_count": n_workers,
                    "data_size": actual_size,
                    "processing_time": par_elapsed,
                    "avg_sentiment": avg_sentiment,
                }
            )

    return metrics


def run_stream_test(worker_counts, timeout=60):
    """Run stream processing tests with different worker counts."""
    logger.info("Starting stream processing tests...")

    metrics = []

    # Sequential stream processing
    logger.info("Running sequential stream processing...")
    seq_metrics = process_stream_sequential(timeout=timeout)
    for metric in seq_metrics:
        metric["mode"] = "stream"
        metric["worker_count"] = 1
        metrics.append(metric)

    # Parallel stream processing with different worker counts
    for n_workers in worker_counts:
        logger.info(f"Running parallel stream processing with {n_workers} workers...")
        par_metrics = process_stream_parallel(n_workers=n_workers, timeout=timeout)
        for metric in par_metrics:
            metric["mode"] = "stream"
            metric["worker_count"] = n_workers
            metrics.append(metric)

    return metrics


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="MapReduce text analysis without Spark"
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "stream", "both"],
        default="both",
        help="Processing mode: batch, stream, or both",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout for stream processing in seconds",
    )
    parser.add_argument(
        "--data-sizes",
        type=int,
        nargs="+",
        default=[100, 500, 1000],
        help="Data sizes for batch processing",
    )
    parser.add_argument(
        "--worker-counts",
        type=int,
        nargs="+",
        default=[2, 4, 8],
        help="Number of workers for parallel processing",
    )
    parser.add_argument(
        "--local-data",
        action="store_true",
        help="Use locally cached dataset if available",
    )
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()

    all_metrics = []

    # Run batch tests
    if args.mode in ["batch", "both"]:
        batch_metrics = run_batch_test(
            args.data_sizes, args.worker_counts, args.local_data
        )
        all_metrics.extend(batch_metrics)

    # Run stream tests
    if args.mode in ["stream", "both"]:
        stream_metrics = run_stream_test(args.worker_counts, args.timeout)
        all_metrics.extend(stream_metrics)

    # Plot and save metrics
    if all_metrics:
        plot_metrics(all_metrics)

        # Log a summary of the analysis
        logger.info("==== MapReduce Analysis Summary ====")
        batch_metrics_df = pd.DataFrame(
            [m for m in all_metrics if m["mode"] == "batch"]
        )
        if not batch_metrics_df.empty:
            seq_metrics = batch_metrics_df[
                batch_metrics_df["processing_mode"] == "sequential"
            ]
            par_metrics = batch_metrics_df[
                batch_metrics_df["processing_mode"] == "parallel"
            ]

            if not seq_metrics.empty and not par_metrics.empty:
                # Calculate average speedup
                seq_times = seq_metrics.groupby("data_size")["processing_time"].mean()
                par_times = par_metrics.groupby(["data_size", "worker_count"])[
                    "processing_time"
                ].mean()

                logger.info("Average processing times (seconds):")
                for data_size in seq_times.index:
                    logger.info(f"  Data size {data_size}:")
                    logger.info(f"    Sequential: {seq_times[data_size]:.2f}s")
                    for worker_count in set(par_metrics["worker_count"]):
                        if (data_size, worker_count) in par_times.index:
                            par_time = par_times[(data_size, worker_count)]
                            speedup = seq_times[data_size] / par_time
                            logger.info(
                                f"    Parallel ({worker_count} workers): {par_time:.2f}s (Speedup: {speedup:.2f}x)"
                            )

        logger.info(
            "Analysis complete. Results saved to mapreduce_performance_metrics.csv and mapreduce_performance.png"
        )


if __name__ == "__main__":
    main()
