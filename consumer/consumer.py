import os
import sys
import csv
import time

# Point Spark to the Python executable in the current virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    split,
    explode,
    length,
    avg,
    count,
    desc,
    udf,
    current_timestamp,
    window,
    lower,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    BooleanType,
    DoubleType,
)
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging
from s3_upload_helper import upload_file_to_s3
from nltk.corpus import stopwords

stopword_set = set(stopwords.words("english"))

from pyspark.sql.functions import udf


def is_not_stopword(word):
    return word not in stopword_set


is_not_stopword_udf = udf(is_not_stopword, BooleanType())


# Configure logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

# The NLTK data is now expected to be pre-downloaded by running setup_nltk.py
# We no longer handle the download within the application code.


# UDF for sentiment analysis
@udf(FloatType())
def get_sentiment(text):
    # Lazy initialization of the sentiment analyzer on the worker.
    # It assumes the vader_lexicon is already available in the NLTK data path.
    if not hasattr(get_sentiment, "sia"):
        get_sentiment.sia = SentimentIntensityAnalyzer()

    try:
        if not isinstance(text, str) or len(text.strip()) == 0:
            return 0.0
        return float(get_sentiment.sia.polarity_scores(str(text))["compound"])
    except Exception as e:
        logger.error(f"Error in sentiment UDF: {e}")
        return 0.0


def process_unified_batch(df, epoch_id):
    import time

    start_time = time.time()
    print(f"\n--- Processing Batch {epoch_id} ---")

    df.persist()
    record_count = df.count()

    if record_count == 0:
        print("Status: No new data in this batch. Waiting for next trigger...")
        df.unpersist()
        return

    print(f"Status: Received {record_count} new records. Starting analysis...")

    # Calculate latency at the start and keep the DataFrame for later CSV writing
    avg_latency = None
    print(df.columns)
    if "produced_at" in df.columns:
        print("hello")
        from pyspark.sql.functions import avg as _avg

        df_with_latency = df.withColumn("latency", (time.time() - col("produced_at")))
        avg_latency = df_with_latency.agg(_avg("latency")).collect()[0][0]
        print(f"Average latency for batch: {avg_latency:.2f} seconds")
    else:
        df_with_latency = df

    # --- Sentiment Analysis (MapReduce Implemented) ---
    print("\n=== Sentiment Analysis Summary ===")
    sentiment_df = df_with_latency.withColumn("sentiment", get_sentiment(col("text")))
    sentiment_summary = sentiment_df.agg(
        avg("sentiment").alias("average_sentiment"), count("*").alias("tweet_count")
    )
    sentiment_summary.show()

    # --- Word Count & Hashtag Trends (MapReduce Implemented) ---
    words_df = df_with_latency.select(
        explode(split(col("text"), " ")).alias("word")
    ).filter(length(col("word")) > 1)
    words_df.persist()

    print("\n=== Top 5 Trending Words ===")
    word_counts = words_df.groupBy("word").count().orderBy(desc("count"))
    word_counts.show(5, truncate=False)

    print("\n=== Top 5 Trending Hashtags ===")
    hashtag_counts = (
        words_df.filter(col("word").startswith("#"))
        .groupBy("word")
        .count()
        .orderBy(desc("count"))
    )
    hashtag_counts.show(5, truncate=False)

    # Add a processing time column
    df_with_time = df_with_latency.withColumn("processing_time", current_timestamp())

    # Explode words and convert to lowercase
    words_df = df_with_time.select(
        explode(split(lower(col("text")), " ")).alias("word"), col("processing_time")
    ).filter(length(col("word")) > 1)

    # Filter out stopwords using UDF
    words_df = words_df.filter(is_not_stopword_udf(col("word")))

    # 5-minute tumbling window (no sliding)
    windowed_word_counts = (
        words_df.groupBy(
            window(col("processing_time"), "5 minutes", "5 minutes"), col("word")
        )
        .count()
        .orderBy(desc("count"))
    )

    print("\n=== Top 5 Words in Last 5 Minutes (Excluding Stopwords) ===")
    windowed_word_counts.show(5, truncate=False)

    df.unpersist()
    end_time = time.time()
    batch_duration = end_time - start_time
    throughput = record_count / batch_duration if batch_duration > 0 else 0
    print(f"Throughput: {throughput:.2f} messages/second")

    # Save results to CSV for plotting
    csv_file = "performance_results.csv"
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(
                [
                    "epoch_id",
                    "record_count",
                    "batch_duration",
                    "throughput",
                    "avg_latency",
                ]
            )
        writer.writerow(
            [epoch_id, record_count, batch_duration, throughput, avg_latency]
        )

    # Upload CSV to S3 after every batch
    upload_file_to_s3(csv_file, "consumer-results", "results/performance_results.csv")

    print(
        f"--- Batch {epoch_id} processing finished in {batch_duration:.2f} seconds ---"
    )


def create_spark_session():
    """Create a resource-constrained and stable Spark session."""
    return (
        SparkSession.builder.appName("StableKafkaConsumer")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")

        # Use environment variable for Kafka connection or default to localhost for local development
        kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        logger.info(f"Connecting to Kafka at {kafka_server}")

        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", "text_data")
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50)
            .load()
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("text", StringType(), True),
                StructField("produced_at", DoubleType(), True),
            ]
        )
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        query = (
            parsed_df.writeStream.foreachBatch(process_unified_batch)
            .trigger(processingTime="15 seconds")
            .option("checkpointLocation", "final_checkpoint")
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    except Exception as e:
        logger.error(f"An error occurred in the main application: {e}", exc_info=True)
    finally:
        if spark:
            print("\nShutting down Spark session...")
            spark.stop()
            print("Spark session stopped successfully.")


if __name__ == "__main__":
    main()
