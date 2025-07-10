"""
MapReduce Implementation for Text Processing
- Word Count
- Sentiment Analysis
- Hashtag Trends
with performance comparison between sequential and parallel processing.
"""

import time
import re
import logging
from collections import defaultdict
from typing import List, Dict, Tuple, Iterable, Any, Callable
from multiprocessing import Pool, cpu_count
from textblob import TextBlob
import pandas as pd
import matplotlib.pyplot as plt
import nltk
import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from consumer.s3_upload_helper import upload_file_to_s3
from nltk.corpus import stopwords
from typing import List, Dict, Any, Optional

# Download required NLTK data
nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("mapreduce_analysis.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class MapReduce:
    """Generic MapReduce implementation using multiprocessing."""

    def __init__(self, num_workers: int = None):
        self.num_workers = num_workers or (cpu_count() - 1) or 1

    def map_reduce(
        self,
        data: Iterable[Any],
        mapper: Callable,
        reducer: Callable,
        chunksize: int = 1000,
    ) -> List[Tuple[Any, Any]]:
        """
        Run MapReduce on the input data.

        Args:
            data: Input data to process
            mapper: Function that takes an item and yields (key, value) pairs
            reducer: Function that takes (key, values) and yields results
            chunksize: Number of items to process in each worker process

        Returns:
            List of (key, value) pairs from the reduce step
        """
        # Map phase
        with Pool(processes=self.num_workers) as pool:
            mapped = pool.imap_unordered(mapper, data, chunksize=chunksize)

            # Shuffle phase
            shuffled = defaultdict(list)
            for key_values in mapped:
                for key, value in key_values:
                    shuffled[key].append(value)

            # Reduce phase
            results = []
            for key, values in shuffled.items():
                for result in reducer(key, values):
                    results.append(result)

        return results


class TextAnalyzer:
    """Text analysis using MapReduce with sequential and parallel processing."""

    def __init__(self, num_workers: int = None, sequential: bool = False):
        self.sequential = sequential
        if not sequential:
            self.map_reduce = MapReduce(num_workers)
        self.stop_words = set(stopwords.words("english"))

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and preprocess text."""
        if not isinstance(text, str):
            return ""
        # Remove URLs, mentions, and special characters
        text = re.sub(r"http\S+|www\.\S+", "", text, flags=re.MULTILINE)
        text = re.sub(r"@\w+", "", text)
        text = re.sub(r"[^\w\s]", "", text.lower())
        return text.strip()

    # ===== Word Count =====
    def word_count_mapper(self, text: str) -> List[Tuple[str, int]]:
        """Mapper function for word count."""
        words = self.clean_text(text).split()
        # Filter out stopwords and short words
        words = [w for w in words if w not in self.stop_words and len(w) > 2]
        return [(word, 1) for word in words]

    @staticmethod
    def word_count_reducer(key: str, values: List[int]) -> List[Tuple[str, int]]:
        """Reducer function for word count."""
        return [(key, sum(values))]

    def sequential_word_count(self, texts: List[str]) -> Dict[str, int]:
        """Sequential word count implementation."""
        word_counts = defaultdict(int)
        for text in texts:
            words = self.clean_text(text).split()
            for word in words:
                if word not in self.stop_words and len(word) > 2:
                    word_counts[word] += 1
        return dict(sorted(word_counts.items(), key=lambda x: x[1], reverse=True))

    # ===== Sentiment Analysis =====
    def sentiment_mapper(self, text: str) -> List[Tuple[str, int]]:
        """Mapper function for sentiment analysis."""
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 to 1
        # Categorize sentiment
        if sentiment > 0.1:
            return [("positive", 1)]
        elif sentiment < -0.1:
            return [("negative", 1)]
        else:
            return [("neutral", 1)]

    @staticmethod
    def sentiment_reducer(key: str, values: List[int]) -> List[Tuple[str, int]]:
        """Reducer function for sentiment analysis."""
        return [(key, sum(values))]

    def sequential_sentiment(self, texts: List[str]) -> Dict[str, int]:
        """Sequential sentiment analysis implementation."""
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        for text in texts:
            if not isinstance(text, str):
                sentiment = 0
            else:
                blob = TextBlob(text)
                sentiment = blob.sentiment.polarity

            if sentiment > 0.1:
                sentiment_counts["positive"] += 1
            elif sentiment < -0.1:
                sentiment_counts["negative"] += 1
            else:
                sentiment_counts["neutral"] += 1
        return sentiment_counts

    # ===== Hashtag Analysis =====
    def hashtag_mapper(self, text: str) -> List[Tuple[str, int]]:
        """Mapper function for hashtag trends."""
        hashtags = re.findall(r"#(\w+)", text.lower())
        return [(tag, 1) for tag in hashtags]

    @staticmethod
    def hashtag_reducer(key: str, values: List[int]) -> List[Tuple[str, int]]:
        """Reducer function for hashtag trends."""
        return [(key, sum(values))]

    def sequential_hashtags(self, texts: List[str]) -> Dict[str, int]:
        """Sequential hashtag analysis implementation."""
        hashtag_counts = defaultdict(int)
        for text in texts:
            if isinstance(text, str):
                hashtags = re.findall(r"#(\w+)", text.lower())
                for tag in hashtags:
                    hashtag_counts[tag] += 1
        return dict(sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True))

    # ===== Analysis Runner =====
    def run_analysis(self, texts: List[str]) -> Dict[str, Any]:
        """Run all analyses on the input texts."""
        start_time = time.time()
        results = {}

        try:
            # Word Count Analysis
            word_count_start = time.time()
            if self.sequential:
                word_counts = self.sequential_word_count(texts)
            else:
                word_counts = dict(
                    self.map_reduce.map_reduce(
                        texts, self.word_count_mapper, self.word_count_reducer
                    )
                )
            results["word_count_time"] = time.time() - word_count_start
            results["top_words"] = dict(
                sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            )

            # Sentiment Analysis
            sentiment_start = time.time()
            if self.sequential:
                sentiment_counts = self.sequential_sentiment(texts)
            else:
                sentiment_counts = dict(
                    self.map_reduce.map_reduce(
                        texts, self.sentiment_mapper, self.sentiment_reducer
                    )
                )
            results["sentiment_time"] = time.time() - sentiment_start
            results["sentiment"] = sentiment_counts

            # Hashtag Analysis
            hashtag_start = time.time()
            if self.sequential:
                hashtag_counts = self.sequential_hashtags(texts)
            else:
                hashtag_counts = dict(
                    self.map_reduce.map_reduce(
                        texts, self.hashtag_mapper, self.hashtag_reducer
                    )
                )
            results["hashtag_time"] = time.time() - hashtag_start
            results["top_hashtags"] = dict(
                sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            )

            # Overall metrics
            results["total_time"] = time.time() - start_time
            results["num_documents"] = len(texts)
            results["success"] = True

        except Exception as e:
            logger.error(f"Error in analysis: {str(e)}")
            results["success"] = False
            results["error"] = str(e)

        return results


def create_kafka_consumer(topic: str, group_id: str = 'mapreduce_consumer') -> Optional[KafkaConsumer]:
    """
    Create a Kafka consumer with the project's standard configuration.
    
    Args:
        topic: Kafka topic to consume from
        group_id: Consumer group ID
        
    Returns:
        KafkaConsumer instance or None if creation fails
    """
    kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    logger.info(f"Connecting to Kafka at {kafka_server}")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server.split(','),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=10000  # 10 seconds timeout
        )
        logger.info(f"Successfully connected to Kafka topic: {topic}")
        return consumer
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available. Please check your Kafka setup.")
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
    return None

def consume_messages(consumer: KafkaConsumer, max_messages: int = 1000) -> List[str]:
    """
    Consume messages from a Kafka consumer.
    
    Args:
        consumer: Configured KafkaConsumer instance
        max_messages: Maximum number of messages to consume
        
    Returns:
        List of text messages
    """
    messages = []
    try:
        for i, message in enumerate(consumer):
            if i >= max_messages:
                break
                
            try:
                # Extract text from message (matches producer's format)
                if isinstance(message.value, dict):
                    msg_text = message.value.get('text') or message.value.get('clean_text')
                    if msg_text:
                        messages.append(msg_text)
                elif isinstance(message.value, str):
                    messages.append(message.value)
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Consumed {i + 1} messages...")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    
    logger.info(f"Successfully consumed {len(messages)} messages from Kafka")
    return messages


def run_performance_test(
    text_analyzer: TextAnalyzer, 
    texts: List[str],
    upload_to_s3: bool = True,
    s3_bucket: str = None,
    s3_prefix: str = "mapreduce_analysis"
) -> Dict[str, Any]:
    """Run performance test with the given text analyzer."""
    start_time = time.time()
    results = text_analyzer.run_analysis(texts)
    results["execution_time"] = time.time() - start_time
    results["analyzer_type"] = "sequential" if text_analyzer.sequential else "parallel"
    results["num_workers"] = (
        1 if text_analyzer.sequential else text_analyzer.map_reduce.num_workers
    )
    
    # Save results to CSV and upload to S3 if enabled
    if upload_to_s3 and s3_bucket:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Save metrics to CSV
            metrics_df = pd.DataFrame([results])
            metrics_file = f"metrics_{timestamp}.csv"
            metrics_df.to_csv(metrics_file, index=False)
            
            # Upload metrics to S3
            s3_metrics_key = f"{s3_prefix}/metrics/{timestamp}_metrics.csv"
            upload_file_to_s3(metrics_file, s3_bucket, s3_metrics_key)
            logger.info(f"Uploaded metrics to s3://{s3_bucket}/{s3_metrics_key}")
            
            # Clean up local file
            if os.path.exists(metrics_file):
                os.remove(metrics_file)
                
        except Exception as e:
            logger.error(f"Failed to upload metrics to S3: {e}")
    
    return results


def plot_comparison(
    results: List[Dict[str, Any]], 
    output_file: str = "performance_comparison.png",
    upload_to_s3: bool = True,
    s3_bucket: str = None,
    s3_prefix: str = "mapreduce_analysis"
):
    """Plot comparison between sequential and parallel execution."""
    df = pd.DataFrame(results)

    # Filter out failed runs
    df = df[df["success"]]

    if len(df) == 0:
        logger.warning("No successful runs to plot")
        return

    # Plot 1: Execution time comparison
    plt.figure(figsize=(12, 5))

    plt.subplot(1, 2, 1)
    for _, row in df.iterrows():
        label = f"{row['analyzer_type'].title()} ({row['num_workers']} workers)"
        times = {
            "Word Count": row.get("word_count_time", 0),
            "Sentiment": row.get("sentiment_time", 0),
            "Hashtags": row.get("hashtag_time", 0),
        }
        plt.bar(times.keys(), times.values(), alpha=0.6, label=label)

    plt.title("Processing Time by Task")
    plt.ylabel("Time (seconds)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    # Plot 2: Total execution time
    plt.subplot(1, 2, 2)
    for _, row in df.iterrows():
        label = f"{row['analyzer_type'].title()} ({row['num_workers']} workers)"
        plt.bar(label, row["execution_time"], alpha=0.6)

    plt.title("Total Execution Time")
    plt.ylabel("Time (seconds)")
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(output_file)
    plt.close()
    logger.info(f"Saved comparison plot to {output_file}")
    
    if upload_to_s3 and s3_bucket:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{s3_prefix}/performance/{timestamp}_{os.path.basename(output_file)}"
        upload_file_to_s3(output_file, s3_bucket, s3_key)
        
    # Save results to CSV
    base_name = output_file.replace(".png", "")
    df.to_csv(f"{base_name}_results.csv", index=False)
    return df


def main():
    # Configuration
    s3_bucket = os.getenv("S3_BUCKET_NAME")  # Set your S3 bucket name in environment variables
    kafka_topic = os.getenv("KAFKA_TOPIC", "text_data")  # Match producer's default topic
    max_messages = int(os.getenv("MAX_MESSAGES", "1000"))
    s3_prefix = "mapreduce_analysis"
    upload_to_s3 = bool(s3_bucket)
    
    # Log configuration
    if upload_to_s3:
        logger.info(f"S3 uploads are ENABLED. Files will be uploaded to s3://{s3_bucket}/{s3_prefix}")
    else:
        logger.warning("S3_BUCKET_NAME environment variable not set. S3 uploads are DISABLED.")
    
    # Create Kafka consumer
    consumer = create_kafka_consumer(kafka_topic)
    if not consumer:
        logger.error("Failed to create Kafka consumer. Exiting.")
        return
    
    try:
        # Consume messages
        logger.info(f"Consuming up to {max_messages} messages from Kafka topic '{kafka_topic}'...")
        texts = consume_messages(consumer, max_messages)
        
        if not texts:
            logger.warning("No messages received from Kafka. Using sample data instead.")
            texts = [
                "This is a sample tweet about #BigData and #Analytics. I love data science!",
                "Feeling excited about my new project! #coding #python",
                "The weather is nice today. #sunny #summer",
                "Just finished reading a great book about machine learning. #AI #ML",
                "Working on a new data pipeline with Kafka and Spark. #BigData #Streaming"
            ]
        
        logger.info(f"Processing {len(texts)} messages")
        
        # Process the consumed messages
        logger.info(f"\n{'='*50}")
        logger.info(f"PROCESSING MESSAGES")
        logger.info(f"{'='*50}")
        
        # Test parameters
        worker_counts = [2, 4, 8]  # Number of workers to test
        all_results = []
        
        # Run sequential analysis
        logger.info("Running sequential analysis...")
        sequential_analyzer = TextAnalyzer(sequential=True)
        sequential_results = run_performance_test(
            sequential_analyzer, 
            texts,
            upload_to_s3=upload_to_s3,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix
        )
        all_results.append(sequential_results)
        
        # Run parallel analysis with different worker counts
        for num_workers in worker_counts:
            logger.info(f"\nRunning parallel analysis with {num_workers} workers...")
            parallel_analyzer = TextAnalyzer(num_workers=num_workers, sequential=False)
            parallel_results = run_performance_test(
                parallel_analyzer,
                texts,
                upload_to_s3=upload_to_s3,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix
            )
            all_results.append(parallel_results)
        
        # Generate and save comparison plot
        plot_comparison(
            all_results,
            upload_to_s3=upload_to_s3,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix
        )
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed")

if __name__ == "__main__":
    main()
