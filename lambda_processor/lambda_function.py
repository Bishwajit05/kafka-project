import os
import json
import boto3
import logging
import uuid
from datetime import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize NLTK
nltk.data.path.append("/tmp/")
NLTK_DATA_DIR = "/tmp/"

# Download NLTK data if not already present
def download_nltk_data():
    try:
        nltk.data.find('sentiment/vader_lexicon')
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('vader_lexicon', download_dir=NLTK_DATA_DIR)
        nltk.download('stopwords', download_dir=NLTK_DATA_DIR)
        nltk.data.path.append(NLTK_DATA_DIR)

download_nltk_data()

# Initialize Sentiment Analyzer
sia = SentimentIntensityAnalyzer()

# Initialize AWS clients
s3_client = boto3.client('s3')

# OpenSearch configuration
OPENSEARCH_HOST = os.environ.get('OPENSEARCH_HOST')
OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'tweets')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialize OpenSearch client
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    'es',
    session_token=credentials.token
)

opensearch = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def analyze_sentiment(text):
    """Analyze sentiment of the given text."""
    if not text or not isinstance(text, str):
        return 0.0
    return sia.polarity_scores(text)['compound']

def process_tweet(tweet):
    """Process a single tweet and extract relevant information."""
    try:
        if not isinstance(tweet, dict):
            tweet = json.loads(tweet)
            
        text = tweet.get('text', '')
        tweet_id = tweet.get('id_str', str(uuid.uuid4()))
        
        # Perform sentiment analysis
        sentiment = analyze_sentiment(text)
        
        # Extract hashtags (simple extraction, can be enhanced)
        hashtags = [word.lower() for word in text.split() if word.startswith('#')]
        
        # Create processed record
        processed = {
            'tweet_id': tweet_id,
            'text': text,
            'created_at': tweet.get('created_at', datetime.utcnow().isoformat()),
            'sentiment': sentiment,
            'hashtags': hashtags,
            'user': tweet.get('user', {}).get('screen_name', 'unknown'),
            'location': tweet.get('user', {}).get('location'),
            'processed_at': datetime.utcnow().isoformat()
        }
        
        return processed
    except Exception as e:
        logger.error(f"Error processing tweet: {str(e)}")
        return None

def save_to_s3(bucket, key, data):
    """Save data to S3."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
        return True
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        return False

def index_in_opensearch(doc):
    """Index document in OpenSearch."""
    try:
        response = opensearch.index(
            index=OPENSEARCH_INDEX,
            id=doc.get('tweet_id'),
            body=doc,
            refresh=True
        )
        return response.get('result') == 'created' or response.get('result') == 'updated'
    except Exception as e:
        logger.error(f"Error indexing in OpenSearch: {str(e)}")
        return False

def lambda_handler(event, context):
    """Process Kinesis records."""
    processed_records = []
    
    for record in event['Records']:
        try:
            # Kinesis data is base64 encoded
            payload = json.loads(record['kinesis']['data'].decode('utf-8'))
            
            # Process the tweet
            processed = process_tweet(payload)
            if not processed:
                continue
                
            # Save raw data to S3
            timestamp = datetime.utcnow().strftime('%Y/%m/%d/%H/%M')
            s3_key = f"raw/{timestamp}/{processed['tweet_id']}.json"
            save_to_s3(os.environ['S3_RAW_BUCKET'], s3_key, payload)
            
            # Save processed data to S3
            processed_s3_key = f"processed/{timestamp}/{processed['tweet_id']}.json"
            save_to_s3(os.environ['S3_RESULTS_BUCKET'], processed_s3_key, processed)
            
            # Index in OpenSearch
            index_in_opensearch(processed)
            
            processed_records.append(processed['tweet_id'])
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            continue
    
    logger.info(f"Successfully processed {len(processed_records)} records")
    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(processed_records)} records")
    }
