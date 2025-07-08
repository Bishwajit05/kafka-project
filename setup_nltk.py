import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

def download_nltk_data():
    """
    Download required NLTK data packages locally.

    For AWS EMR deployment:
    1. Run this script locally to download the data into your local nltk_data directory.
    2. Identify the specific resources needed by the application (e.g., 'sentiment/vader_lexicon.zip', 'corpora/stopwords.zip').
    3. Upload these specific resources (or the entire nltk_data directory if preferred, though it can be large)
       to an S3 bucket (e.g., s3://your-bucket-name/nltk_data/).
    4. Configure your EMR cluster's bootstrap actions to copy these resources from S3
       to a standard NLTK data path (e.g., /usr/share/nltk_data or /home/hadoop/nltk_data) on each node.
       Alternatively, package them with your Spark application.
    The consumer.py script will expect these resources to be available in the NLTK search path on EMR nodes.
    """
    required_packages = [
        'punkt',
        'vader_lexicon',
        'averaged_perceptron_tagger',
        'wordnet',
        'stopwords',
    ]
    
    print("Downloading NLTK data packages...")
    for package in required_packages:
        try:
            nltk.download(package, quiet=True)
            print(f"✓ Successfully downloaded {package}")
        except Exception as e:
            print(f"✗ Failed to download {package}: {str(e)}")

if __name__ == "__main__":
    download_nltk_data() 