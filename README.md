# Real-time Text Data Processing System

This project implements a scalable text data processing system that performs real-time analysis of streaming text data using Apache Kafka and Apache Spark.

## Features

- Real-time text data ingestion using Kafka
- Parallel processing with PySpark
- Text analysis including:
  - Word count
  - Sentiment analysis
  - Trending topics
- Real-time visualization of results

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Java 8 or higher (for Apache Spark)

## Setup Instructions

### Option 1: Local Setup

**IMPORTANT**: The following steps must be performed in order.

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-project
```

2. **Create and activate virtual environment**:

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
.\venv\Scripts\activate  # On Windows
```

3. **Install Python dependencies**:

```bash
pip install -r requirements.txt
```

4. **Download NLP Data (MANDATORY ONE-TIME STEP)**:
   This script downloads the necessary data for sentiment analysis. It must be run before starting the consumer.

   ```bash
   python setup_nltk.py
   ```

5. **Start Kafka using Docker**:

```bash
docker-compose up -d
```

6. **Run the setup test to verify everything is working**:

```bash
python test_setup.py
```

### Option 2: Docker Compose Setup (Recommended)

This project includes Docker configuration to run both the producer and consumer services along with Kafka and Zookeeper in containers.

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-project
```

2. **Build the Docker images**:

```bash
docker-compose build
```

3. **Start all services**:

```bash
docker-compose up -d
```

4. **View logs from the services**:

```bash
# View all logs
docker-compose logs -f

# View logs from a specific service
docker-compose logs -f producer
docker-compose logs -f consumer
```

5. **Stop all services**:

```bash
docker-compose down
```

## Project Structure

```
kafka-project/
├── producer/
│   └── producer.py       # Kafka producer for data ingestion
├── consumer/
│   └── consumer.py       # Spark Streaming consumer for processing
├── setup_nltk.py        # NLTK data setup script
├── test_setup.py        # System setup verification
├── docker-compose.yml    # Docker configuration for all services
├── Dockerfile           # Single Dockerfile for both producer and consumer
└── requirements.txt      # Python dependencies
```

## Running the Application

### Local Execution

**IMPORTANT**: The consumer application relies on the NLP data downloaded in the setup steps. Ensure you have run `python setup_nltk.py` successfully before starting the consumer.

#### Option 1: Manual Execution

1. **Start the Consumer**:
   Open a terminal and run the consumer application. It will wait for data from Kafka.

   ```bash
   python consumer/consumer.py
   ```

2. **Start the Producer**:
   In a separate terminal, run the producer to start streaming data.
   ```bash
   python producer/producer.py
   ```

#### Option 2: Using the start.sh Script

The project includes a convenient `start.sh` script that automates the startup process and supports parallel execution:

```bash
# Run with default 1 instance of producer and consumer
./start.sh

# Run with multiple parallel instances (e.g., 3)
./start.sh 3
```

This script:
- Kills any existing tmux sessions
- Starts Kafka using Docker Compose
- Sets up the NLTK environment
- Launches the specified number of producer and consumer instances in separate tmux sessions
- Automatically attaches to the first consumer session

This is particularly useful for EC2 deployments where you need to run multiple parallel instances to increase throughput.

### Docker Execution

With Docker Compose, both the producer and consumer services are started automatically along with Kafka and Zookeeper:

```bash
# Build and start all services
docker-compose build
docker-compose up -d

# Monitor the application logs
docker-compose logs -f

# When finished, stop all services
docker-compose down
```

## Components

1. **Data Producer**

   - Streams text data from a dataset to Kafka
   - Configurable data source and streaming rate

2. **Data Consumer**
   - Real-time processing using Spark Streaming
   - Word count analysis
   - Sentiment analysis
   - Trending topics detection

## Monitoring

The application outputs processing results to the console in real-time, showing:

- Word frequencies
- Sentiment scores
- Current trending topics

## Stopping the Application

### Local Execution
1. Stop the producer and consumer applications (Ctrl+C)
2. Stop Kafka:

```bash
docker-compose down
```

### Docker Execution
Stop all containers with a single command:

```bash
docker-compose down
```

## Troubleshooting

### Local Setup Issues

1. Make sure all dependencies are installed:

```bash
pip install -r requirements.txt
```

2. Verify NLTK data is properly downloaded:

```bash
python setup_nltk.py
```

3. Check if Kafka is running:

```bash
docker ps
```

4. Run the setup test:

```bash
python test_setup.py
```

### Docker Setup Issues

1. Check container logs for errors:

```bash
docker-compose logs -f
# Or for a specific service
docker-compose logs -f consumer
```

2. Verify all containers are running:

```bash
docker-compose ps
```

3. Restart the services if needed:

```bash
docker-compose restart
```

4. If problems persist, rebuild the images:

```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```
