#!/bin/bash

engine="$1"

if [ -z "$engine" ]; then
  echo "Usage: $0 <engine-label>"
  exit 1
fi

 # 200k ops
TOTAL_REQ=200000              
CONNECTIONS=10
 # 10 pipelined requests per connection          
PIPELINE=10             
THREADS=4                
RUNS=50                 
OUTFILE="benchmark_${engine}_$(date +%Y%m%d%H%M%S).csv"

echo "Benchmarking engine: $engine"
echo "Requests: $TOTAL_REQ, Connections: $CONNECTIONS, Pipeline: $PIPELINE, Threads: $THREADS"
echo "Running $RUNS iterations..."

echo "timestamp,run,command,requests_per_sec,p50_ms" > "$OUTFILE"

for i in $(seq 1 $RUNS); do
  timestamp=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  echo "[$timestamp] Run $i/$RUNS for engine: $engine"

  redis-benchmark \
    -d 1000 \
    -t set,get \
    -r 200000 \
    -n "$TOTAL_REQ" \
    -c "$CONNECTIONS" \
    -P "$PIPELINE" \
    --threads "$THREADS" \
    -p 6380 \
    -q \
    --csv |
  awk -v ts="$timestamp" -v run="$i" -F',' '{print ts "," run "," $1 "," $2 "," $3}' | tee -a "$OUTFILE"
done

echo "All $RUNS runs completed."
echo "Results saved to $OUTFILE"
