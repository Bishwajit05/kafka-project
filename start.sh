#!/bin/bash

# Default to 1 parallel process if not specified
PARALLEL_COUNT=${1:-1}

echo "Running with $PARALLEL_COUNT parallel processes for both producer and consumer"

echo "Removing previous data"

rm -rf final_checkpoint

echo 'killing existing tmux sessions...'
# Kill any existing tmux sessions
for i in $(seq 1 $PARALLEL_COUNT); do
  tmux kill-session -t producer_$i 2>/dev/null
  tmux kill-session -t consumer_$i 2>/dev/null
done

echo 'killing existing docker containers...'
docker compose down

echo 'starting kafka...'
docker compose up -d

echo 'waiting for kafka to start...'
sleep 10

echo 'setup nltk...'
python3 -m venv venv
source venv/bin/activate 
pip3 install -r requirements.txt
python3 setup_nltk.py 

echo "Starting $PARALLEL_COUNT producer instances..."
for i in $(seq 1 $PARALLEL_COUNT); do
  echo "Starting producer instance $i..."
  tmux new-session -d -s producer_$i
  tmux send-keys -t producer_$i "source venv/bin/activate" C-m
  tmux send-keys -t producer_$i "python3 producer/producer.py --instance-id $i" C-m
done

echo "Starting $PARALLEL_COUNT consumer instances..."
for i in $(seq 1 $PARALLEL_COUNT); do
  echo "Starting consumer instance $i..."
  tmux new-session -d -s consumer_$i
  tmux send-keys -t consumer_$i "source venv/bin/activate" C-m
  tmux send-keys -t consumer_$i "python3 consumer/consumer.py --instance-id $i" C-m
done

# Attach to the first consumer session by default
tmux attach-session -t consumer_1 -d

