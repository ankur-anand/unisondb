#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
EXAMPLE_DIR="$ROOT_DIR/cmd/examples/blobstore-minio"
TMP_ROOT="/tmp/unisondb/blobstore-minio-raft"
BIN_DIR="$ROOT_DIR/.bin"
BIN_PATH="${UNISONDB_BIN:-$BIN_DIR/unisondb}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/unisondb-gocache}"
GO_MOD_CACHE_DIR="${GO_MOD_CACHE_DIR:-}"

PRODUCER_CFGS=(
  "$EXAMPLE_DIR/producer-raft-node1.local.toml"
  "$EXAMPLE_DIR/producer-raft-node2.local.toml"
  "$EXAMPLE_DIR/producer-raft-node3.local.toml"
)
PRODUCER_NAMES=("node0" "node1" "node2")
PRODUCER_URLS=(
  "http://127.0.0.1:28180"
  "http://127.0.0.1:28280"
  "http://127.0.0.1:28380"
)
PRODUCER_RAFT_ADDRS=(
  "127.0.0.1:28190"
  "127.0.0.1:28290"
  "127.0.0.1:28390"
)
PRODUCER_LOGS=(
  "$TMP_ROOT/node0.log"
  "$TMP_ROOT/node1.log"
  "$TMP_ROOT/node2.log"
)
CONSUMER_CFG="$EXAMPLE_DIR/consumer-raft.local.toml"
CONSUMER_URL="http://127.0.0.1:29480"
CONSUMER_LOG="$TMP_ROOT/consumer.log"

NAMESPACES=("orders" "inventory")
KEYS=("hello-order" "hello-inventory")
VALUES=("blobstore-via-minio-raft-order" "blobstore-via-minio-raft-inventory")

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=1
export GOCACHE="$GO_CACHE_DIR"
if [[ -n "$GO_MOD_CACHE_DIR" ]]; then
  export GOMODCACHE="$GO_MOD_CACHE_DIR"
fi

PRODUCER_PIDS=("" "" "")
CONSUMER_PID=""

log() {
  printf '[blobstore-minio-raft] %s\n' "$*"
}

cleanup() {
  local code=$?
  trap - EXIT INT TERM

  if [[ -n "$CONSUMER_PID" ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi

  for idx in "${!PRODUCER_PIDS[@]}"; do
    pid="${PRODUCER_PIDS[$idx]}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  exit "$code"
}

trap cleanup EXIT INT TERM

wait_for_http() {
  local url=$1
  local name=$2
  for _ in $(seq 1 80); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  log "$name did not become ready: $url"
  return 1
}

wait_for_replication() {
  local namespace=$1
  local key=$2
  local expected_b64=$3
  for _ in $(seq 1 100); do
    local body
    body="$(curl -fsS "$CONSUMER_URL/api/v1/$namespace/kv/$key" || true)"
    if [[ "$body" == *"\"found\":true"* ]] && [[ "$body" == *"\"value\":\"$expected_b64\""* ]]; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 0.25
  done
  return 1
}

wait_for_raft_peers() {
  local namespace=$1

  for _ in $(seq 1 120); do
    for idx in "${!PRODUCER_URLS[@]}"; do
      if [[ -z "${PRODUCER_PIDS[$idx]}" ]]; then
        continue
      fi

      local body
      body="$(curl -fsS "${PRODUCER_URLS[$idx]}/status/peers?namespace=$namespace" || true)"
      if [[ -z "$body" ]]; then
        continue
      fi

      local found_all=1
      for raft_addr in "${PRODUCER_RAFT_ADDRS[@]}"; do
        if [[ "$body" != *"$raft_addr"* ]]; then
          found_all=0
          break
        fi
      done

      if [[ "$found_all" == "1" ]]; then
        return 0
      fi
    done
    sleep 0.25
  done

  log "raft peer set did not converge for namespace=$namespace"
  return 1
}

leader_idx_for_namespace() {
  local namespace=$1
  local exclude_idx=${2:-}

  for _ in $(seq 1 120); do
    for idx in "${!PRODUCER_URLS[@]}"; do
      if [[ -n "$exclude_idx" ]] && [[ "$idx" == "$exclude_idx" ]]; then
        continue
      fi

      if [[ -z "${PRODUCER_PIDS[$idx]}" ]]; then
        continue
      fi

      local status
      status="$(curl -s -o /dev/null -w '%{http_code}' "${PRODUCER_URLS[$idx]}/readyz?namespace=$namespace" || true)"
      if [[ "$status" == "200" ]]; then
        printf '%s\n' "$idx"
        return 0
      fi
    done
    sleep 0.25
  done

  return 1
}

kill_producer() {
  local idx=$1
  local pid="${PRODUCER_PIDS[$idx]}"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  PRODUCER_PIDS[$idx]=""
}

wait_for_service_exit() {
  while true; do
    if [[ -n "$CONSUMER_PID" ]] && ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
      wait "$CONSUMER_PID"
      return $?
    fi

    for idx in "${!PRODUCER_PIDS[@]}"; do
      pid="${PRODUCER_PIDS[$idx]}"
      if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid"
        return $?
      fi
    done

    sleep 1
  done
}

log "building unisondb binary"
mkdir -p "$BIN_DIR"
mkdir -p "$GOCACHE"
if [[ -n "$GO_MOD_CACHE_DIR" ]]; then
  mkdir -p "$GO_MOD_CACHE_DIR"
fi
go build -o "$BIN_PATH" ./cmd/unisondb

log "preparing local state and MinIO bucket"
rm -rf "$TMP_ROOT"
mkdir -p "$TMP_ROOT"
go run ./cmd/examples/blobstore-minio/minio-setup \
  --bucket-url "s3://unisondb-blob-demo?endpoint=http://127.0.0.1:9000&region=us-east-1&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required" \
  --prefix "unisondb/examples/blobstore-minio-raft" \
  --reset

for idx in "${!PRODUCER_CFGS[@]}"; do
  log "starting ${PRODUCER_NAMES[$idx]}"
  "$BIN_PATH" server --config "${PRODUCER_CFGS[$idx]}" >"${PRODUCER_LOGS[$idx]}" 2>&1 &
  PRODUCER_PIDS[$idx]=$!
  wait_for_http "${PRODUCER_URLS[$idx]}/healthz" "${PRODUCER_NAMES[$idx]}"
done

for namespace in "${NAMESPACES[@]}"; do
  log "waiting for raft membership namespace=$namespace"
  wait_for_raft_peers "$namespace"
done

log "starting consumer"
"$BIN_PATH" replica --config "$CONSUMER_CFG" >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
wait_for_http "$CONSUMER_URL/healthz" "consumer"

for idx in "${!NAMESPACES[@]}"; do
  namespace="${NAMESPACES[$idx]}"
  key="${KEYS[$idx]}"
  value="${VALUES[$idx]}"
  value_b64="$(printf '%s' "$value" | base64 | tr -d '\n')"

  leader_idx="$(leader_idx_for_namespace "$namespace")"
  leader_name="${PRODUCER_NAMES[$leader_idx]}"
  leader_url="${PRODUCER_URLS[$leader_idx]}"

  log "writing initial value namespace=$namespace key=$key leader=$leader_name"
  curl -fsS -X PUT "$leader_url/api/v1/$namespace/kv/$key" \
    -H "Content-Type: application/json" \
    -d "{\"value\":\"$value_b64\"}" >/dev/null

  log "waiting for replication namespace=$namespace key=$key"
  body="$(wait_for_replication "$namespace" "$key" "$value_b64")"
  printf 'namespace: %s\n' "$namespace"
  printf 'leader: %s (%s)\n' "$leader_name" "$leader_url"
  printf 'key: %s/%s\n' "$namespace" "$key"
  printf 'consumer response: %s\n' "$body"
done

orders_leader_idx="$(leader_idx_for_namespace "orders")"
orders_leader_name="${PRODUCER_NAMES[$orders_leader_idx]}"
orders_leader_url="${PRODUCER_URLS[$orders_leader_idx]}"

log "stopping current orders leader to force failover leader=$orders_leader_name"
kill_producer "$orders_leader_idx"

new_orders_leader_idx="$(leader_idx_for_namespace "orders" "$orders_leader_idx")"
new_orders_leader_name="${PRODUCER_NAMES[$new_orders_leader_idx]}"
new_orders_leader_url="${PRODUCER_URLS[$new_orders_leader_idx]}"

failover_key="after-failover"
failover_value="blobstore-via-minio-raft-after-failover"
failover_b64="$(printf '%s' "$failover_value" | base64 | tr -d '\n')"

log "writing post-failover value namespace=orders key=$failover_key leader=$new_orders_leader_name"
curl -fsS -X PUT "$new_orders_leader_url/api/v1/orders/kv/$failover_key" \
  -H "Content-Type: application/json" \
  -d "{\"value\":\"$failover_b64\"}" >/dev/null

log "waiting for post-failover replication namespace=orders key=$failover_key"
failover_body="$(wait_for_replication "orders" "$failover_key" "$failover_b64")"

log "raft replication succeeded"
printf 'consumer: %s\n' "$CONSUMER_URL"
printf 'orders leader before failover: %s (%s)\n' "$orders_leader_name" "$orders_leader_url"
printf 'orders leader after failover: %s (%s)\n' "$new_orders_leader_name" "$new_orders_leader_url"
printf 'post-failover consumer response: %s\n' "$failover_body"
for idx in "${!PRODUCER_NAMES[@]}"; do
  printf '%s log: %s\n' "${PRODUCER_NAMES[$idx]}" "${PRODUCER_LOGS[$idx]}"
done
printf 'consumer log: %s\n' "$CONSUMER_LOG"

printf '\n'
printf 'check leaders:\n'
for url in "${PRODUCER_URLS[@]}"; do
  printf 'curl %s/status/leader?namespace=orders\n' "$url"
done
printf 'write more data with automatic leader detection:\n'
printf './cmd/examples/blobstore-minio/write-10-kv-raft.sh\n'
printf 'read from consumer:\n'
printf 'curl %s/api/v1/orders/kv/your-key\n' "$CONSUMER_URL"
printf 'curl %s/api/v1/inventory/kv/your-key\n' "$CONSUMER_URL"

if [[ "${EXIT_AFTER_VERIFY:-0}" == "1" ]]; then
  exit 0
fi

log "services are running; press Ctrl+C to stop"
wait_for_service_exit
