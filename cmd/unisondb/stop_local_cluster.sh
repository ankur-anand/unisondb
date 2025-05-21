#!/bin/bash

PID_DIR="./pids"

echo "Stopping Client UnisonDB instances..."
for pid_file in ${PID_DIR}/unisondb_*.pid; do
  if [ -f "$pid_file" ]; then
    kill "$(cat "$pid_file")" && echo "  → Killed $(basename "$pid_file")"
    rm "$pid_file"
  fi
done

echo "All processes stopped."
rm -rf ./data