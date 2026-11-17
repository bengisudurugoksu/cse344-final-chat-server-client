#!/bin/bash

SERVER_IP="127.0.0.1"
PORT=12345
CLIENT_BINARY="./chatclient"

USER_COUNT=15
ROOMS=("roomA" "roomB" "roomC")

for i in $(seq 1 $USER_COUNT); do
  room_index=$(( (i - 1) % ${#ROOMS[@]} ))
  room="${ROOMS[$room_index]}"
  (
    {
      sleep 1
      echo "user$i"
      sleep 1
      echo "/join $room"
      sleep 1
      echo "/broadcast Hello from user$i in $room"
      sleep 1
      echo "/exit"
    } | $CLIENT_BINARY $SERVER_IP $PORT > /dev/null
  ) &
done

wait
echo "✅ All $USER_COUNT clients simulated and exited."
