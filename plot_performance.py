import pandas as pd
import matplotlib.pyplot as plt
from consumer.s3_upload_helper import s3_uploader

# Load performance data for airline customer review streaming pipeline
perf_data = pd.read_csv("airline_customer_review_metrics.csv")

plt.style.use("ggplot")
fig, plots = plt.subplots(1, 2, figsize=(13, 5))

# Plot streaming throughput
plots[0].plot(
    perf_data["batch_num"],
    perf_data["throughput"],
    marker="o",
    color="teal",
    linewidth=2,
)
plots[0].set_title("Batch-wise Stream Throughput")
plots[0].set_xlabel("Batch Number")
plots[0].set_ylabel("Records per Second")
plots[0].grid(True, linestyle="-.", alpha=0.7)

# Plot average ingestion delay
plots[1].plot(
    perf_data["batch_num"],
    perf_data["avg_latency"],
    marker="x",
    color="orange",
    linewidth=2,
)
plots[1].set_title("Mean Ingestion Delay per Batch")
plots[1].set_xlabel("Batch Number")
plots[1].set_ylabel("Average Delay (seconds)")
plots[1].grid(True, linestyle=":", alpha=0.5)

plt.suptitle(
    "Airline Customer Review Streaming Pipeline: Performance Overview",
    fontsize=15,
    fontweight="bold",
)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])

# Save and upload the plot
filename = "stream_performance_summary.png"
bucket_name = "airline-customer-review-streaming-pipeline"

# Use the new S3Uploader to save and upload the plot
success = s3_uploader.upload_plot(
    fig=plt,
    filename=filename,
    bucket=bucket_name,
    s3_key=filename
)

if success:
    print(f"✅ Performance plot saved and uploaded successfully")
else:
    print(f"❌ Failed to save or upload performance plot")
