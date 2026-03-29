#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
EXAMPLE_DIR="$ROOT_DIR/cmd/examples/blobstore-minio"
TMP_ROOT="/tmp/unisondb/blobstore-minio"
BIN_DIR="$ROOT_DIR/.bin"
BIN_PATH="${UNISONDB_BIN:-$BIN_DIR/unisondb}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/unisondb-gocache}"
GO_MOD_CACHE_DIR="${GO_MOD_CACHE_DIR:-}"

PRODUCER_CFG="$EXAMPLE_DIR/producer.local.toml"
CONSUMER_CFG="$EXAMPLE_DIR/consumer.local.toml"
PRODUCER_LOG="$TMP_ROOT/producer.log"
CONSUMER_LOG="$TMP_ROOT/consumer.log"

PRODUCER_URL="http://127.0.0.1:28080"
CONSUMER_URL="http://127.0.0.1:29080"
NAMESPACES=("orders" "inventory")
KEYS=("hello-order" "hello-inventory")
VALUES=("blobstore-via-minio-order" "blobstore-via-minio-inventory")

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=1
export GOCACHE="$GO_CACHE_DIR"
if [[ -n "$GO_MOD_CACHE_DIR" ]]; then
  export GOMODCACHE="$GO_MOD_CACHE_DIR"
fi

PRODUCER_PID=""
CONSUMER_PID=""

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  if [[ -n "$CONSUMER_PID" ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  if [[ -n "$PRODUCER_PID" ]] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
    kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true
  fi
  exit "$code"
}

trap cleanup EXIT INT TERM

log() {
  printf '[blobstore-minio] %s\n' "$*"
}

wait_for_http() {
  local url=$1
  local name=$2
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  log "$name did not become ready: $url"
  return 1
}

wait_for_replication() {
  local url=$1
  local expected_b64=$2
  for _ in $(seq 1 80); do
    local body
    body="$(curl -fsS "$url" || true)"
    if [[ "$body" == *"\"found\":true"* ]] && [[ "$body" == *"\"value\":\"$expected_b64\""* ]]; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 0.25
  done
  return 1
}

wait_for_service_exit() {
  while true; do
    if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
      wait "$PRODUCER_PID"
      return $?
    fi
    if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
      wait "$CONSUMER_PID"
      return $?
    fi
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
  --prefix "unisondb/examples/blobstore-minio" \
  --reset

log "starting producer"
"$BIN_PATH" server --config "$PRODUCER_CFG" >"$PRODUCER_LOG" 2>&1 &
PRODUCER_PID=$!
wait_for_http "$PRODUCER_URL/healthz" "producer"

log "starting consumer"
"$BIN_PATH" replica --config "$CONSUMER_CFG" >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
wait_for_http "$CONSUMER_URL/healthz" "consumer"

for idx in "${!NAMESPACES[@]}"; do
  namespace="${NAMESPACES[$idx]}"
  key="${KEYS[$idx]}"
  value="${VALUES[$idx]}"
  value_b64="$(printf '%s' "$value" | base64 | tr -d '\n')"

  log "writing test value through producer namespace=$namespace key=$key"
  curl -fsS -X PUT "$PRODUCER_URL/api/v1/$namespace/kv/$key" \
    -H "Content-Type: application/json" \
    -d "{\"value\":\"$value_b64\"}" >/dev/null

  log "waiting for replicated value on consumer namespace=$namespace key=$key"
  body="$(wait_for_replication "$CONSUMER_URL/api/v1/$namespace/kv/$key" "$value_b64")"
  printf 'namespace: %s\n' "$namespace"
  printf 'key: %s/%s\n' "$namespace" "$key"
  printf 'consumer response: %s\n' "$body"
done

log "replication succeeded"
printf 'producer: %s\n' "$PRODUCER_URL"
printf 'consumer: %s\n' "$CONSUMER_URL"
printf 'producer log: %s\n' "$PRODUCER_LOG"
printf 'consumer log: %s\n' "$CONSUMER_LOG"

printf '\n'
printf 'write more data:\n'
for namespace in "${NAMESPACES[@]}"; do
  printf 'curl -X PUT %s/api/v1/%s/kv/your-key -H "Content-Type: application/json" -d '\''{"value":"%s"}'\''\n' \
    "$PRODUCER_URL" "$namespace" "$(printf '%s' 'your-value' | base64 | tr -d '\n')"
done
printf 'read from consumer:\n'
for namespace in "${NAMESPACES[@]}"; do
  printf 'curl %s/api/v1/%s/kv/your-key\n' "$CONSUMER_URL" "$namespace"
done

if [[ "${EXIT_AFTER_VERIFY:-0}" == "1" ]]; then
  exit 0
fi

log "services are running; press Ctrl+C to stop"
wait_for_service_exit
