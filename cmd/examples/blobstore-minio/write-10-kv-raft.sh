#!/usr/bin/env bash

set -euo pipefail

PRODUCER_URLS_CSV="${PRODUCER_URLS_CSV:-http://127.0.0.1:28180,http://127.0.0.1:28280,http://127.0.0.1:28380}"
CONSUMER_URL="${CONSUMER_URL:-http://127.0.0.1:29480}"
NAMESPACE="${NAMESPACE:-orders}"
COUNT="${COUNT:-10}"
KEY_PREFIX="${KEY_PREFIX:-sample-key}"
VALUE_PREFIX="${VALUE_PREFIX:-sample-value}"
VERIFY_ON_CONSUMER="${VERIFY_ON_CONSUMER:-1}"
VERIFY_TIMEOUT_SECONDS="${VERIFY_TIMEOUT_SECONDS:-10}"

IFS=',' read -r -a PRODUCER_URLS <<<"$PRODUCER_URLS_CSV"

log() {
  printf '[blobstore-minio-write-10-raft] %s\n' "$*"
}

base64_value() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

leader_url() {
  for _ in $(seq 1 80); do
    for url in "${PRODUCER_URLS[@]}"; do
      local status
      status="$(curl -s -o /dev/null -w '%{http_code}' "$url/readyz?namespace=$NAMESPACE" || true)"
      if [[ "$status" == "200" ]]; then
        printf '%s\n' "$url"
        return 0
      fi
    done
    sleep 0.25
  done

  log "could not find raft leader for namespace=$NAMESPACE"
  return 1
}

wait_for_consumer() {
  local key=$1
  local expected_b64=$2
  local max_attempts=$((VERIFY_TIMEOUT_SECONDS * 4))

  for _ in $(seq 1 "$max_attempts"); do
    local body
    body="$(curl -fsS "$CONSUMER_URL/api/v1/$NAMESPACE/kv/$key" || true)"
    if [[ "$body" == *"\"found\":true"* ]] && [[ "$body" == *"\"value\":\"$expected_b64\""* ]]; then
      return 0
    fi
    sleep 0.25
  done

  log "consumer did not observe replicated key: $key"
  return 1
}

for i in $(seq 1 "$COUNT"); do
  suffix="$(printf '%02d' "$i")"
  key="${KEY_PREFIX}-${suffix}"
  value="${VALUE_PREFIX}-${suffix}"
  encoded="$(base64_value "$value")"
  leader="$(leader_url)"

  log "writing $key=$value leader=$leader"
  curl -fsS -X PUT "$leader/api/v1/$NAMESPACE/kv/$key" \
    -H "Content-Type: application/json" \
    -d "{\"value\":\"$encoded\"}" >/dev/null

  if [[ "$VERIFY_ON_CONSUMER" == "1" ]]; then
    wait_for_consumer "$key" "$encoded"
    log "replicated $key"
  fi
done

log "completed count=$COUNT namespace=$NAMESPACE"
