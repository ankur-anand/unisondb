#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$ROOT" ]]; then
  ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
fi

LOG_DIR="$SCRIPT_DIR/logs"
PID_FILE="$SCRIPT_DIR/pids"
PORTS_DIR="$SCRIPT_DIR/ports"
DATA_DIR="$SCRIPT_DIR/raftdata"
CLEAN_DATA="${CLEAN_DATA:-1}"
CLEAN_LOGS="${CLEAN_LOGS:-0}"

mkdir -p "$LOG_DIR" "$PORTS_DIR"
: > "$PID_FILE"
rm -f "$PORTS_DIR"/*.json

run_node() {
  local name="$1"
  local cfg="$2"
  local log="$3"
  local ports_file="$4"

  rm -f "$ports_file"
  (cd "$ROOT" && go run ./cmd/unisondb server --ports-file "$ports_file" -c "$cfg") > "$log" 2>&1 &
  local pid=$!
  echo "$pid" >> "$PID_FILE"
  echo "started ${name} (pid=${pid})"
}

read_http_addr() {
  local ports_file="$1"
  grep -m 1 '"http"' "$ports_file" | cut -d '"' -f 4
}

wait_for_http_addr() {
  local ports_file="$1"
  local log_file="$2"
  local timeout_s="${3:-30}"
  local start
  start="$(date +%s)"

  while true; do
    if [[ -s "$ports_file" ]]; then
      local http_addr
      http_addr="$(read_http_addr "$ports_file" 2>/dev/null || true)"
      if [[ -n "$http_addr" ]]; then
        echo "$http_addr"
        return 0
      fi
    fi
    if (( $(date +%s) - start >= timeout_s )); then
      echo "timed out waiting for http addr in ${ports_file}" >&2
      if [[ -n "$log_file" ]] && [[ -f "$log_file" ]]; then
        echo "last 50 log lines from ${log_file}:" >&2
        tail -n 50 "$log_file" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

wait_for_leader_ready() {
  local base_url="$1"
  local namespace="${2:-default}"
  local timeout_s="${3:-30}"
  local start
  start="$(date +%s)"

  while true; do
    if curl -fsS "${base_url}/readyz?namespace=${namespace}" >/dev/null 2>&1; then
      return 0
    fi
    if (( $(date +%s) - start >= timeout_s )); then
      echo "timed out waiting for leader readiness at ${base_url}" >&2
      return 1
    fi
    sleep 0.5
  done
}

print_leader_status() {
  local base_url="$1"
  local namespace="${2:-default}"
  local status
  status="$(curl -fsS "${base_url}/status/leader?namespace=${namespace}" 2>/dev/null || true)"
  if [[ -n "$status" ]]; then
    echo "leader status: ${status}"
  fi
}

cleanup() {
  if [[ -f "$PID_FILE" ]]; then
    while read -r pid; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done < "$PID_FILE"
  fi
  if [[ "$CLEAN_DATA" == "1" ]]; then
    rm -rf "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"
  fi
  if [[ "$CLEAN_LOGS" == "1" ]]; then
    rm -rf "$LOG_DIR"
  fi
}

trap cleanup EXIT INT TERM

run_node "node1" "$SCRIPT_DIR/node1.toml" "$LOG_DIR/node1.log" "$PORTS_DIR/node1.json"
node1_http_addr="$(wait_for_http_addr "$PORTS_DIR/node1.json" "$LOG_DIR/node1.log" 30)"
node1_base_url="http://${node1_http_addr}"
echo "waiting for node1 leader readiness..."
wait_for_leader_ready "$node1_base_url" "default" 30
print_leader_status "$node1_base_url" "default"
run_node "node2" "$SCRIPT_DIR/node2.toml" "$LOG_DIR/node2.log" "$PORTS_DIR/node2.json"
run_node "node3" "$SCRIPT_DIR/node3.toml" "$LOG_DIR/node3.log" "$PORTS_DIR/node3.json"

echo "nodes started; logs in $LOG_DIR"
echo "press Ctrl+C to stop"

wait
