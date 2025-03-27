#!/bin/bash

runs=100
engine="$1"

if [ -z "$engine" ]; then
  echo "Usage: $0 <engine-label>"
  exit 1
fi

echo "Running redis-benchmark $runs times for engine: $engine"

for i in $(seq 1 $runs); do
  redis-benchmark -d 1000 -t set,get -r 10000 -n 10000 -p 6380 -q
done

