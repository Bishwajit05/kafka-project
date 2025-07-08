#!/bin/bash

echo 'killing existing tmux sessions...'
tmux kill-session -t producer 2>/dev/null
tmux kill-session -t consumer 2>/dev/null

echo 'killing existing docker containers...'
docker compose down

echo 'starting kafka...'
docker compose up -d

echo 'waiting for kafka to start...'
sleep 10

echo 'setup nltk...'
source venv/bin/activate && python3 setup_nltk.py && pip3 install -r requirements.txt

echo 'starting producer...'
tmux new-session -d -s producer
tmux send-keys -t producer "source venv/bin/activate" C-m
tmux send-keys -t producer "python3 producer/producer.py" C-m

echo 'starting consumer...'
tmux new-session -d -s consumer
tmux send-keys -t consumer "source venv/bin/activate" C-m
tmux send-keys -t consumer "python3 consumer/consumer.py" C-m

tmux attach-session -t consumer -d

