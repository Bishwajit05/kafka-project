#!/usr/bin/env python3
"""
MapReduce implementation for Twitter data analysis using multiprocessing.
Consumes data from Kafka and implements Word Count, Sentiment Analysis, and Hashtag Trends.
"""

import re
import json
import time
import logging
import os
import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from typing import List, Dict, Tuple, Iterable, Any, Callable
from textblob import TextBlob
import matplotlib.pyplot as plt
import nltk
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from kafka.admin import NewTopic
from nltk.corpus import stopwords

# Download required NLTK data
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mapreduce_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MapReduce:
    """Generic MapReduce implementation using multiprocessing."""
    
    def __init__(self, num_workers: int = None):
        self.num_workers = num_workers or (cpu_count() - 1) or 1
    
    def map_reduce(self, 
                  data: Iterable[Any],
                  mapper: Callable,
                  reducer: Callable,
                  chunksize: int = 1000) -> List[Tuple[Any, Any]]:
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

class TwitterAnalyzer:
    """Analyze Twitter data using MapReduce."""
    
    def __init__(self, num_workers: int = None):
        self.map_reduce = MapReduce(num_workers)
        self.stop_words = set(stopwords.words('english'))
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and preprocess text."""
        if not isinstance(text, str):
            return ""
        # Remove URLs, mentions, and special characters
        text = re.sub(r'http\S+|www\.\S+', '', text, flags=re.MULTILINE)
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'[^\w\s]', '', text.lower())
        return text.strip()
    
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
    
    def sentiment_mapper(self, text: str) -> List[Tuple[str, float]]:
        """Mapper function for sentiment analysis."""
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 to 1
        # Categorize sentiment
        if sentiment > 0.1:
            return [('positive', 1)]
        elif sentiment < -0.1:
            return [('negative', 1)]
        else:
            return [('neutral', 1)]
    
    @staticmethod
    def sentiment_reducer(key: str, values: List[int]) -> List[Tuple[str, int]]:
        """Reducer function for sentiment analysis."""
        return [(key, sum(values))]
    
    def hashtag_mapper(self, text: str) -> List[Tuple[str, int]]:
        """Mapper function for hashtag trends."""
        hashtags = re.findall(r'#(\w+)', text.lower())
        return [(tag, 1) for tag in hashtags]
    
    @staticmethod
    def hashtag_reducer(key: str, values: List[int]) -> List[Tuple[str, int]]:
        """Reducer function for hashtag trends."""
        return [(key, sum(values))]
    
    def run_analysis(self, texts: List[str]) -> dict:
        """Run all analyses on the input texts."""
        logger.info(f"Starting analysis on {len(texts)} tweets")
        start_time = time.time()
        
        # Word Count Analysis
        logger.info("Running word count analysis...")
        word_counts = self.map_reduce.map_reduce(
            texts,
            self.word_count_mapper,
            self.word_count_reducer
        )
        top_words = sorted(word_counts, key=lambda x: x[1], reverse=True)[:20]
        
        # Sentiment Analysis
        logger.info("Running sentiment analysis...")
        sentiment_counts = dict(self.map_reduce.map_reduce(
            texts,
            self.sentiment_mapper,
            self.sentiment_reducer
        ))
        
        # Hashtag Analysis
        logger.info("Running hashtag analysis...")
        hashtag_counts = self.map_reduce.map_reduce(
            texts,
            self.hashtag_mapper,
            self.hashtag_reducer
        )
        top_hashtags = sorted(hashtag_counts, key=lambda x: x[1], reverse=True)[:20]
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Analysis completed in {execution_time:.2f} seconds")
        
        # Prepare results
        results = {
            'word_counts': dict(top_words),
            'sentiment': sentiment_counts,
            'top_hashtags': dict(top_hashtags),
            'execution_time': execution_time,
            'num_workers': self.map_reduce.num_workers,
            'num_documents': len(texts)
        }
        
        return results

def plot_results(results: dict, output_file: str = 'mapreduce_results.png'):
    """Plot the analysis results."""
    plt.figure(figsize=(15, 10))
    
    # Word Count Plot
    plt.subplot(2, 2, 1)
    words = list(results['word_counts'].keys())
    counts = list(results['word_counts'].values())
    plt.bar(words, counts)
    plt.title('Top 20 Words')
    plt.xticks(rotation=45, ha='right')
    
    # Sentiment Plot - Handle cases where some sentiments might be missing
    plt.subplot(2, 2, 2)
    sentiment = results['sentiment']
    # Ensure all sentiment categories exist with at least 0 count
    sentiment_categories = ['positive', 'neutral', 'negative']
    values = [sentiment.get(k, 0) for k in sentiment_categories]
    labels = [f"{k} ({v})" for k, v in zip(sentiment_categories, values) if v > 0]
    values = [v for v in values if v > 0]  # Only include non-zero values
    
    if values:  # Only plot if we have data
        plt.pie(values, labels=labels, autopct='%1.1f%%' if len(values) > 1 else None)
        plt.title('Sentiment Distribution')
    else:
        plt.text(0.5, 0.5, 'No sentiment data', 
                horizontalalignment='center',
                verticalalignment='center',
                transform=plt.gca().transAxes)
        plt.title('No Sentiment Data')
    
    # Hashtag Plot
    plt.subplot(2, 1, 2)
    hashtags = list(results['top_hashtags'].keys())
    counts = list(results['top_hashtags'].values())
    plt.bar(hashtags, counts)
    plt.title('Top 20 Hashtags')
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"Results plotted and saved to {output_file}")

def create_kafka_consumer(topic: str, group_id: str = 'mapreduce-group') -> KafkaConsumer:
    """Create a Kafka consumer for the specified topic."""
    retries = 12
    kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    logger.info(f"Connecting to Kafka at {kafka_server}")
    
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_server,
                auto_offset_reset='earliest',  # Start reading from the beginning
                enable_auto_commit=False,     # Disable auto-commit to have more control
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000,    # Timeout after 10 seconds of no messages
                max_poll_records=1000,        # Process up to 1000 messages per poll
                session_timeout_ms=30000,     # Increase session timeout
                heartbeat_interval_ms=10000    # Send heartbeats more frequently
            )
            logger.info("Kafka consumer created successfully.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available. Retrying in 5 seconds... ({i+1}/{retries})")
            time.sleep(5)
    
    logger.error("Failed to connect to Kafka broker after multiple retries.")
    raise RuntimeError("Could not connect to Kafka")

def consume_messages(consumer: KafkaConsumer, max_messages: int = 1000) -> List[Dict]:
    """Consume messages from Kafka topic."""
    messages = []
    try:
        logger.info(f"Consuming up to {max_messages} messages from Kafka...")
        logger.info(f"Subscribed to partitions: {consumer.assignment()}")
        
        # Manually seek to beginning of all assigned partitions
        for partition in consumer.assignment():
            consumer.seek_to_beginning(partition)
            logger.info(f"Seeked to beginning of partition {partition}")
        
        # Poll for messages
        batch = consumer.poll(timeout_ms=5000, max_records=max_messages)
        
        for partition, msg_batch in batch.items():
            for msg in msg_batch:
                messages.append(msg.value)
                if len(messages) >= max_messages:
                    break
        
        logger.info(f"Consumed {len(messages)} messages from Kafka")
        
    except Exception as e:
        logger.error(f"Error consuming messages: {e}", exc_info=True)
    finally:
        try:
            consumer.commit()
            consumer.close()
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
    
    return messages

def load_sample_data(size: int = 1000) -> List[str]:
    """Load sample text data."""
    try:
        # Try to load from local file first
        try:
            df = pd.read_csv('data/twitter_training.csv', header=None, 
                           names=['id', 'source', 'sentiment', 'text'])
        except FileNotFoundError:
            # If local file not found, use kaggle API
            try:
                import kaggle
                logger.info("Downloading Kaggle dataset...")
                kaggle.api.dataset_download_files(
                    'saurabhshahane/twitter-sentiment-dataset',
                    path='data',
                    unzip=True
                )
                df = pd.read_csv('data/twitter_training.csv', header=None, 
                               names=['id', 'source', 'sentiment', 'text'])
            except Exception as e:
                logger.error(f"Error downloading Kaggle dataset: {e}")
                raise
        
        texts = df['text'].dropna().astype(str).tolist()
        logger.info(f"Loaded {len(texts)} texts from dataset")
        
        # Ensure we have enough data
        if len(texts) < size:
            logger.warning(f"Requested {size} samples but only {len(texts)} available")
            return texts
        return texts[:size]
    except Exception as e:
        logger.error(f"Error loading Kaggle dataset: {e}")
        logger.info("Falling back to sample data...")
        return ["This is a sample tweet #example #test"] * size

def main():
    """Main function to run the MapReduce analysis with Kafka integration."""
    # Initialize Kafka consumer
    kafka_topic = 'text_data'  # Same topic as producer
    logger.info(f"Initializing Kafka consumer for topic: {kafka_topic}")
    
    try:
        # Create Kafka consumer
        consumer = create_kafka_consumer(kafka_topic)
        logger.info("Successfully created Kafka consumer")
        
        # Check and create topic if needed
        try:
            admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', api_version=(2, 8, 0))
            topics = admin_client.list_topics()
            logger.info(f"Available topics: {topics}")
            
            if kafka_topic not in topics:
                logger.warning(f"Topic '{kafka_topic}' does not exist. Creating it...")
                try:
                    admin_client.create_topics([NewTopic(kafka_topic, 1, 1)])
                    logger.info(f"Created topic: {kafka_topic}")
                except Exception as e:
                    logger.warning(f"Could not create topic: {e}")
        except Exception as e:
            logger.warning(f"Could not check/create topic: {e}")
        
        # Consume messages from Kafka
        logger.info("Starting to consume messages...")
        messages = consume_messages(consumer, max_messages=1000)
        
        if not messages:
            logger.warning("No messages received from Kafka. Make sure the producer is running.")
            logger.info("Using sample data for demonstration...")
            messages = [{"text": "This is a sample tweet #example #test"} for _ in range(10)]
        
        # Extract text from messages (matching producer's format)
        texts = [msg.get('text', '') for msg in messages if isinstance(msg, dict) and 'text' in msg]
        
        if not texts:
            logger.error("No valid text data found in Kafka messages.")
            return
            
        logger.info(f"Processing {len(texts)} messages from Kafka")
        
        # Initialize analyzer with worker processes (using half the available CPUs)
        num_workers = max(1, cpu_count() // 2)
        analyzer = TwitterAnalyzer(num_workers=num_workers)
        
        # Run analysis
        results = analyzer.run_analysis(texts)
        
        # Save results
        with open('mapreduce_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        # Plot results
        plot_results(results)
        
        # Print summary
        print("\n=== Analysis Summary ===")
        print(f"Documents processed: {results['num_documents']}")
        print(f"Execution time: {results['execution_time']:.2f} seconds")
        print(f"Workers used: {results['num_workers']}")
        print("\nTop 5 words:", dict(list(results['word_counts'].items())[:5]))
        print("Sentiment distribution:", results['sentiment'])
        print("Top 5 hashtags:", dict(list(results['top_hashtags'].items())[:5]))
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        # Clean up resources
        try:
            if 'consumer' in locals():
                consumer.close()
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
            
        try:
            if 'admin_client' in locals():
                admin_client.close()
        except Exception as e:
            logger.error(f"Error closing admin client: {e}")

if __name__ == "__main__":
    main()
