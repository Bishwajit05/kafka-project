#!/usr/bin/env python3
import sys
from collections import defaultdict

current_key = None
current_count = 0

for line in sys.stdin:
    try:
        timestamp, word, count = line.strip().split('\t')
        key = (timestamp, word)
        count = int(count)
        
        if current_key == key:
            current_count += count
        else:
            if current_key:
                print(f"{current_key[0]}\t{current_key[1]}\t{current_count}")
            current_key = key
            current_count = count
            
    except Exception as e:
        print(f"Error in reducer: {e}", file=sys.stderr)

# Output the last key
if current_key:
    print(f"{current_key[0]}\t{current_key[1]}\t{current_count}")
