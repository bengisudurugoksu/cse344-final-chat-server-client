#!/bin/bash

SERVER_IP="127.0.0.1"
PORT=12345
FILENAME="test.txt"

# Hedef kullanıcıları başlat
for i in {1..10}; do
  TARGET="target$i"
  (
    {
      sleep 1
      echo "$TARGET"
      sleep 1
      echo "/join room$((i+100))"
      sleep 30
      echo "/exit"
    } | ./chatclient $SERVER_IP $PORT > "receiver_log_$i.txt"
  ) &
done

sleep 2  # hedef kullanıcılar hazır olsun

# Göndericileri kısa aralıklarla başlat
for i in {1..10}; do
  SENDER="user$i"
  TARGET="target$i"
  (
    {
      echo "$SENDER"
      sleep 1
      echo "/join room$i"
      sleep 1
      echo "/sendfile $FILENAME $TARGET"
      sleep 15
      echo "/exit"
    } | ./chatclient $SERVER_IP $PORT > "client_log_$i.txt"
  ) &
  sleep 0.3
done

wait
echo "🚀 All clients attempted to send files in parallel."
