#!/usr/bin/env bash
set -euo pipefail

if [[ ! -x "./run.sh" ]]; then
  echo "error: ./run.sh not found or not executable in $(pwd)" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <relayer_count> [output_log_file]" >&2
  exit 1
fi

RELAYER_COUNT="$1"
LOG_FILE="${2:-benchmarks/relayer_bench.log}"

LOG_DIR="$(dirname "$LOG_FILE")"
mkdir -p "$LOG_DIR"

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

{
  echo ""
  echo "### RELAYER_COUNT=${RELAYER_COUNT} START=$(timestamp)"
} >>"$LOG_FILE"

./run.sh "$RELAYER_COUNT" | tee -a "$LOG_FILE"

{
  echo "### RELAYER_COUNT=${RELAYER_COUNT} END=$(timestamp)"
  echo "# Append replication stats block for relayer=${RELAYER_COUNT} here."
} >>"$LOG_FILE"
