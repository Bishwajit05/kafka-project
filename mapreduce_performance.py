"""
Performance analysis of MapReduce implementation with different chunk sizes and worker counts.
Measures throughput, latency, and performance under different loads.
"""
import time
import logging
import pandas as pd
import matplotlib.pyplot as plt
from mapreduce_analysis import TwitterAnalyzer, load_sample_data
from typing import List, Dict, Any
# numpy imported for potential future use
import os
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mapreduce_performance_analysis.log')
    ]
)
logger = logging.getLogger(__name__)

def run_performance_tests(data: List[str], chunk_sizes: List[int], worker_counts: List[int]) -> List[Dict[str, Any]]:
    """
    Run performance tests with different chunk sizes and worker counts.
    
    Args:
        data: List of text documents to process
        chunk_sizes: List of chunk sizes to test
        worker_counts: List of worker counts to test
        
    Returns:
        List of performance metrics for each test
    """
    results = []
    
    for chunk_size in chunk_sizes:
        logger.info(f"\nTesting with chunk size: {chunk_size}")
        
        for num_workers in worker_counts:
            logger.info(f"Testing with {num_workers} worker(s)...")
            analyzer = TwitterAnalyzer(num_workers=num_workers)
            
            # Process data in chunks
            for i in tqdm(range(0, len(data), chunk_size), 
                         desc=f"Chunk {chunk_size}, Workers {num_workers}"):
                chunk = data[i:i + chunk_size]
                chunk_id = i // chunk_size + 1
                
                # Run analysis and measure time
                start_time = time.time()
                try:
                    # Run analysis and store result (result is used for error checking)
                    analyzer.run_analysis(chunk)
                    elapsed = time.time() - start_time
                    
                    metrics = {
                        'chunk_size': chunk_size,
                        'num_workers': num_workers,
                        'chunk_id': chunk_id,
                        'execution_time': elapsed,
                        'throughput': len(chunk) / elapsed if elapsed > 0 else 0,
                        'latency': elapsed / len(chunk) if len(chunk) > 0 else 0,
                        'documents_processed': len(chunk),
                        'success': True
                    }
                    
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_id}: {str(e)}")
                    metrics = {
                        'chunk_size': chunk_size,
                        'num_workers': num_workers,
                        'chunk_id': chunk_id,
                        'execution_time': 0,
                        'throughput': 0,
                        'latency': 0,
                        'documents_processed': len(chunk),
                        'success': False,
                        'error': str(e)
                    }
                
                results.append(metrics)
                
                # Save intermediate results
                if len(results) % 5 == 0 or (i + chunk_size >= len(data) and num_workers == worker_counts[-1]):
                    pd.DataFrame(results).to_csv('mapreduce_performance_results.csv', index=False)
    
    return results

def plot_results(df: pd.DataFrame, output_dir: str = 'performance_plots'):
    """Generate performance plots from test results."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Filter out failed runs
    # Filter out failed runs
    df = df[df['success']]
    
    # Plot 1: Throughput vs Chunk Size for different worker counts
    plt.figure(figsize=(12, 6))
    for workers in df['num_workers'].unique():
        subset = df[df['num_workers'] == workers]
        avg_throughput = subset.groupby('chunk_size')['throughput'].mean()
        plt.plot(avg_throughput.index, avg_throughput.values, 
                marker='o', label=f'{workers} worker(s)')
    
    plt.xlabel('Chunk Size (number of tweets)')
    plt.ylabel('Throughput (tweets/second)')
    plt.title('Throughput vs Chunk Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/throughput_vs_chunk_size.png')
    plt.close()
    
    # Plot 2: Latency vs Chunk Size for different worker counts
    plt.figure(figsize=(12, 6))
    for workers in df['num_workers'].unique():
        subset = df[df['num_workers'] == workers]
        avg_latency = subset.groupby('chunk_size')['latency'].mean() * 1000  # Convert to ms
        plt.plot(avg_latency.index, avg_latency.values, 
                marker='o', label=f'{workers} worker(s)')
    
    plt.xlabel('Chunk Size (number of tweets)')
    plt.ylabel('Latency (milliseconds/tweet)')
    plt.title('Latency vs Chunk Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/latency_vs_chunk_size.png')
    plt.close()
    
    # Plot 3: Speedup vs Number of Workers
    plt.figure(figsize=(12, 6))
    for chunk_size in df['chunk_size'].unique():
        subset = df[df['chunk_size'] == chunk_size]
        baseline = subset[subset['num_workers'] == 1]['execution_time'].mean()
        speedup = []
        for workers in sorted(df['num_workers'].unique()):
            worker_subset = subset[subset['num_workers'] == workers]
            if len(worker_subset) > 0:
                avg_time = worker_subset['execution_time'].mean()
                speedup.append(baseline / avg_time if avg_time > 0 else 0)
            else:
                speedup.append(0)
        
        plt.plot(sorted(df['num_workers'].unique()), speedup, 
                marker='o', label=f'Chunk size: {chunk_size}')
    
    plt.xlabel('Number of Workers')
    plt.ylabel('Speedup (relative to 1 worker)')
    plt.title('Speedup vs Number of Workers')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/speedup_vs_workers.png')
    plt.close()
    
    # Save results to CSV
    df.to_csv('mapreduce_performance_results.csv', index=False)
    return df

def main():
    # Load sample data
    logger.info("Loading sample data...")
    try:
        # Try to load a large dataset (adjust size based on available memory)
        data = load_sample_data(size=100000)  # Load 100,000 tweets
        logger.info(f"Successfully loaded {len(data)} tweets")
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
        logger.info("Falling back to smaller dataset...")
        data = ["This is a sample tweet about big data and analytics."] * 10000
        logger.info(f"Using {len(data)} sample tweets")
    
    # Test parameters
    chunk_sizes = [1000, 5000, 10000, 20000]  # Different chunk sizes to test
    worker_counts = [1, 2, 4, 8]  # Different worker counts to test
    
    # Run performance tests
    logger.info("Starting performance tests...")
    results = run_performance_tests(data, chunk_sizes, worker_counts)
    
    # Generate plots and save results
    logger.info("Generating performance plots...")
    df = pd.DataFrame(results)
    plot_results(df)
    
    # Print summary statistics
    logger.info("\nPerformance Test Summary:")
    if len(df) > 0:
        summary = df.groupby(['chunk_size', 'num_workers']).agg({
            'execution_time': ['mean', 'std'],
            'throughput': ['mean', 'max'],
            'latency': ['mean', 'min']
        }).round(2)
        print(summary)
    else:
        logger.warning("No successful test runs to summarize.")
        logger.info("Performance test completed successfully")

if __name__ == "__main__":
    main()
