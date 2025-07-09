import time
import pandas as pd
import matplotlib.pyplot as plt
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length
from pyspark.sql.types import FloatType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import random
import string
import os
import kagglehub

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('performance_comparison.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize NLTK
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

def load_kaggle_dataset():
    """Load the Kaggle dataset used in the producer"""
    try:
        logger.info("Downloading Kaggle dataset...")
        path = kagglehub.dataset_download("saurabhshahane/twitter-sentiment-dataset")
        csv_path = os.path.join(path, "Twitter_Data.csv")
        
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Clean the data - drop rows with missing text
        df = df.dropna(subset=['clean_text'])
        
        # Convert to list of texts
        texts = df['clean_text'].astype(str).tolist()
        
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
            logger.warning(f"Requested size {size} is larger than dataset size {len(all_texts)}. Using all available texts.")
            return all_texts
        
        # For very large samples, use a more memory-efficient approach
        if size > 10000:
            logger.info(f"Sampling {size} texts (large sample, this might take a moment)...")
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
        logger.info("Falling back to random text generation")
        texts = []
        for _ in range(size):
            text_length = random.randint(50, 200)
            text = ''.join(random.choices(string.ascii_letters + ' ' * 10, k=text_length))
            words = text.split()
            if not words:
                text = 'sample text with some words'
            texts.append(text)
        return texts

# Sequential processing
def process_sequential(texts):
    """Process texts sequentially"""
    sia = SentimentIntensityAnalyzer()
    results = []
    
    for text in texts:
        # Word count
        word_count = len(str(text).split())
        
        # Sentiment analysis
        sentiment = sia.polarity_scores(str(text))['compound']
        
        # Text length
        text_length = len(str(text))
        
        results.append({
            'text': text,
            'word_count': word_count,
            'sentiment': sentiment,
            'text_length': text_length
        })
    
    return pd.DataFrame(results)

# Parallel processing with Spark
def process_parallel(spark, texts):
    """Process texts in parallel using Spark"""
    # Convert texts to DataFrame
    df = spark.createDataFrame([(text,) for text in texts], ['text'])
    
    # Register UDF for sentiment analysis
    @udf(FloatType())
    def get_sentiment_udf(text):
        sia = SentimentIntensityAnalyzer()
        return float(sia.polarity_scores(str(text))['compound'])
    
    # Process in parallel
    result_df = df.withColumn('word_count', udf(lambda x: len(str(x).split()), 'int')(col('text'))) \
                 .withColumn('sentiment', get_sentiment_udf(col('text'))) \
                 .withColumn('text_length', length(col('text')))
    
    return result_df

def run_comparison():
    # Sample sizes to test - from 5000 to 100000 in 5000-tweet intervals (5000, 10000, 15000, ..., 100000)
    sample_sizes = list(range(5000, 100001, 5000))  # 5000 to 100000 in 5K intervals
    
    # Initialize Spark with optimized configurations for larger datasets
    spark = SparkSession.builder \
        .appName("PerformanceComparison") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()
        
    # Set log level to WARN to reduce output noise
    spark.sparkContext.setLogLevel("WARN")
    
    results = []
    
    for size in sample_sizes:
        print(f"\nProcessing sample size: {size}")
        texts = get_sample_data(size)
        
        # Sequential processing
        start_time = time.time()
        _ = process_sequential(texts)  # Process but don't store the result
        seq_time = time.time() - start_time
        
        # Parallel processing
        start_time = time.time()
        par_df = process_parallel(spark, texts)
        # Force execution of the Spark job
        par_df.count()  # This ensures the computation is actually performed
        par_time = time.time() - start_time
        
        # Calculate speedup
        speedup = seq_time / par_time if par_time > 0 else 0
        
        results.append({
            'sample_size': size,
            'sequential_time': seq_time,
            'parallel_time': par_time,
            'speedup': speedup
        })
        
        print(f"Sample size: {size}")
        print(f"Sequential time: {seq_time:.2f} seconds")
        print(f"Parallel time: {par_time:.2f} seconds")
        print(f"Speedup: {speedup:.2f}x")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Save results to CSV
    results_df.to_csv('performance_comparison.csv', index=False)
    
    # Enhanced visualization for larger dataset sizes
    plt.figure(figsize=(12, 6))
    plt.subplot(2, 2, 1)
    plt.plot(results_df['sample_size'], results_df['sequential_time'], 'b-o', label='Sequential')
    plt.plot(results_df['sample_size'], results_df['parallel_time'], 'r-s', label='Parallel')
    plt.xlabel('Number of Tweets')
    plt.ylabel('Execution Time (s)')
    plt.title('Execution Time Comparison')
    plt.legend()
    plt.grid(True)
    
    # 2. Speedup
    # 2. Speedup
    plt.subplot(2, 2, 2)
    plt.plot(results_df['sample_size'], results_df['speedup'], 'g-s', label='Speedup')
    plt.axhline(y=1, color='r', linestyle='--')
    plt.title('Speedup (Sequential / Parallel)')
    plt.xlabel('Number of Tweets')
    plt.ylabel('Speedup (x)')
    plt.grid(True)
    
    # 3. Throughput Comparison (tweets/second)
    plt.subplot(2, 2, 3)
    results_df['seq_throughput'] = results_df['sample_size'] / results_df['sequential_time']
    results_df['par_throughput'] = results_df['sample_size'] / results_df['parallel_time']
    plt.plot(results_df['sample_size'], results_df['seq_throughput'], 'b-o', label='Sequential')
    plt.plot(results_df['sample_size'], results_df['par_throughput'], 'r-s', label='Parallel')
    plt.title('Throughput Comparison')
    plt.xlabel('Number of Tweets')
    plt.ylabel('Tweets Processed per Second')
    plt.yscale('log')  # Log scale for better visualization
    plt.legend()
    plt.grid(True)
    
    # 4. Efficiency (Speedup / Number of Cores)
    plt.subplot(2, 2, 4)
    num_cores = spark.sparkContext.defaultParallelism
    results_df['efficiency'] = (results_df['speedup'] / num_cores) * 100
    plt.plot(results_df['sample_size'], results_df['efficiency'], 'm-D', label='Efficiency')
    plt.axhline(y=100, color='r', linestyle='--', label='Ideal (100%)')
    plt.title(f'Parallel Efficiency (Using {num_cores} Cores)')
    plt.xlabel('Number of Tweets')
    plt.ylabel('Efficiency (%)')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    
    # Save high-resolution figure
    plt.savefig('performance_comparison_large_scale.png', dpi=300, bbox_inches='tight')
    
    # Also save a CSV with detailed metrics
    results_df.to_csv('performance_metrics_large_scale.csv', index=False)
    
    # Log completion
    logger.info("Performance testing completed. Results saved to 'performance_comparison_large_scale.png' and 'performance_metrics_large_scale.csv'")
    
    # Only show plot if running interactively
    import sys
    if 'ipykernel' in sys.modules:
        plt.show()
    
    spark.stop()
    return results_df

if __name__ == "__main__":
    results = run_comparison()
    print("\nPerformance Comparison Results:")
    print(results.to_string())
