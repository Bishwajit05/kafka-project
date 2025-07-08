#!/usr/bin/env python3
import sys
import json
import re
from datetime import datetime

def extract_hashtags(text):
    return re.findall(r'#(\w+)', text.lower())

for line in sys.stdin:
    try:
        tweet = json.loads(line)
        if 'text' in tweet:
            timestamp = tweet.get('created_at', datetime.utcnow().isoformat())[:16]
            text = tweet['text'].lower()
            
            # Emit hashtags
            for tag in extract_hashtags(text):
                print(f"{timestamp}\t{tag}\t1")
                
            # Emit words (optional)
            words = re.findall(r'\b\w{3,}\b', text)
            for word in words:
                if not word.startswith('#'):
                    print(f"{timestamp}\t{word}\t1")
                    
    except Exception as e:
        print(f"Error processing line: {e}", file=sys.stderr)
