import os
import sys
import logging
import time
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json as spark_from_json,
    col as spark_col,
    explode as spark_explode,
    split as spark_split,
    avg as spark_avg,
    length as spark_length,
    desc as spark_desc,
    count as spark_count,
    current_timestamp as spark_now,
    udf as spark_udf,
    lower as spark_lower,
    window as spark_window,
    flatten,
    lit,
    from_unixtime,
    unix_timestamp,
    to_timestamp,
    date_format,
)
from pyspark.sql.types import (
    StructType as SparkStructType,
    StructField as SparkStructField,
    StringType as SparkStringType,
    IntegerType as SparkIntType,
    FloatType as SparkFloatType,
    BooleanType as SparkBoolType,
    DoubleType as SparkDoubleType,
)
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from s3_upload_helper import s3_uploader

# Configure Spark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell"
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_stream_consumer")

# Prepare stopword set for filtering airline customer review words
STOPWORDS = set(stopwords.words("english"))


def is_not_stopword(word):
    """True if word is not a stopword."""
    return word not in STOPWORDS


remove_stopwords_udf = spark_udf(is_not_stopword, SparkBoolType())


# Sentiment analysis UDF for airline customer reviews
def compute_sentiment(text):
    if not hasattr(compute_sentiment, "analyzer"):
        compute_sentiment.analyzer = SentimentIntensityAnalyzer()
    try:
        if isinstance(text, str) and text.strip():
            return float(compute_sentiment.analyzer.polarity_scores(text)["compound"])
        return 0.0
    except Exception as exc:
        logger.warning(f"Sentiment UDF error: {exc}")
        return 0.0


sentiment_udf = spark_udf(compute_sentiment, SparkFloatType())


def batch_analysis(df, batch_num):
    """
    Analyze a batch of airline customer reviews for sentiment and trending words.
    """
    start_time = time.time()
    logger.info(f"[Batch {batch_num}] Analysis started.")

    # Persist the DataFrame as it will be used multiple times
    df = df.persist()
    count = df.count()

    if count < 1:
        logger.info("Batch is empty. Skipping...")
        df.unpersist()
        return

    logger.info(f"Processing {count} records...")

    # Calculate latency if timestamp is present
    avg_latency = None
    if "timestamp" in df.columns:
        latency_df = df.withColumn("latency", (time.time() - spark_col("timestamp")))
        avg_latency = latency_df.agg(spark_avg("latency")).collect()[0][0]
        logger.info(f"Average latency: {avg_latency:.2f} s")

        # Add processing time for time window analysis
        enriched_df = df.withColumn("proc_time", spark_now())
    else:
        enriched_df = df

    # Sentiment analysis
    if "Review" in df.columns:
        df = df.withColumn("sentiment", sentiment_udf(spark_col("Review")))
        avg_sentiment = df.agg(spark_avg("sentiment")).collect()[0][0]
        logger.info(f"Mean sentiment: {avg_sentiment:.3f}")

    # Trending terms analysis
    if "Review" in df.columns:
        # Split reviews into words and filter stopwords
        words_df = df.select(
            spark_explode(spark_split(spark_lower(spark_col("Review")), "\\W+")).alias(
                "word"
            )
        ).filter("word != ''")  # Remove empty strings

        words_df = words_df.filter(remove_stopwords_udf(spark_col("word")))

        # Get top 10 words
        top_words = (
            words_df.groupBy("word")
            .count()
            .orderBy(spark_desc("count"))
            .limit(10)
            .collect()
        )
        logger.info(
            "Top 10 words: "
            + ", ".join(f"{row['word']}({row['count']})" for row in top_words)
        )

        # Time window analysis (5-minute windows)
        if "proc_time" in df.columns:
            windowed_words = (
                words_df.withColumn("window", spark_window("proc_time", "5 minutes"))
                .groupBy("window", "word")
                .count()
                .orderBy(spark_desc("count"))
                .limit(5)
            )
            logger.info("Top 5 trending words in 5-minute windows:")
            windowed_words.show(truncate=False)

    # Calculate metrics
    elapsed = time.time() - start_time
    throughput = count / elapsed if elapsed > 0 else 0

    # Save batch to a single CSV file
    output_path = "all_batches_results.csv"
    try:
        # Check if file exists to determine if we need to write header
        write_header = (
            not os.path.exists(output_path) or os.path.getsize(output_path) == 0
        )

        # Append data to the CSV file
        df.toPandas().to_csv(
            output_path,
            mode="a",  # append mode
            header=write_header,  # write header only if file doesn't exist
            index=False,
        )

        # Upload to cloud storage in the batch_results bucket
        s3_path = f"batch_results/{output_path}"
        s3_uploader.upload_file(
            output_path, os.environ.get("S3_BUCKET", "batch_results"), s3_path
        )
    except Exception as e:
        logger.error(f"Failed to save or upload results: {str(e)}")

    # Log metrics to CSV
    metrics_file = "airline_customer_review_metrics.csv"
    write_header = (
        not os.path.exists(metrics_file) or os.path.getsize(metrics_file) == 0
    )

    try:
        with open(metrics_file, "a", newline="") as out_csv:
            writer = csv.writer(out_csv)
            if write_header:
                writer.writerow(
                    ["batch_num", "num_records", "elapsed", "throughput", "avg_latency"]
                )
            writer.writerow([batch_num, count, elapsed, throughput, avg_latency])
    except Exception as e:
        logger.error(f"Failed to write metrics: {str(e)}")

    # Clean up
    df.unpersist()
    logger.info(f"[Batch {batch_num}] Analysis complete in {elapsed:.2f}s.")
    return {
        "batch_num": batch_num,
        "num_records": count,
        "elapsed": elapsed,
        "throughput": throughput,
        "avg_latency": avg_latency,
    }


def build_spark_session():
    """Instantiate a Spark session with resource constraints."""
    return (
        SparkSession.builder.appName("AirlineCustomerReviewStreamConsumer")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def main_stream():
    """
    Main entrypoint for the airline customer review streaming consumer.
    Reads from Kafka, parses airline customer review records, and launches the analysis pipeline.
    """
    spark = None
    try:
        spark = build_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        # Kafka connection (topic and server name refactored for airline customer review context)
        kafka_broker = os.environ.get("KAFKA_AIRLINE_REVIEW_SERVERS", "localhost:9092")
        logger.info(f"Connecting to Kafka at {kafka_broker}")
        review_topic = "airline_customer_review_stream"
        # Define schema for airline reviews
        review_schema = SparkStructType(
            [
                SparkStructField("AirLine_Name", SparkStringType(), True),
                SparkStructField("Rating - 10", SparkStringType(), True),
                SparkStructField("Title", SparkStringType(), True),
                SparkStructField("Name", SparkStringType(), True),
                SparkStructField("Date", SparkStringType(), True),
                SparkStructField("Review", SparkStringType(), True),
                SparkStructField("Recommond", SparkStringType(), True),
                SparkStructField("timestamp", SparkDoubleType(), True),
            ]
        )
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", review_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50)
            .load()
        )
        parsed_reviews = kafka_stream.select(
            spark_from_json(spark_col("value").cast("string"), review_schema).alias(
                "review"
            )
        ).select("review.*")
        # Start streaming analysis
        query = (
            parsed_reviews.writeStream.foreachBatch(batch_analysis)
            .trigger(processingTime="15 seconds")
            .option("checkpointLocation", "airline_customer_review_stream_checkpoint")
            .start()
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    except Exception as err:
        logger.error(f"Fatal error in stream: {err}", exc_info=True)
    finally:
        if spark:
            logger.info("Closing Spark session...")
            spark.stop()
            spark.stop()


if __name__ == "__main__":
    main_stream()
