#!/bin/bash

set -euo pipefail

HOSTNAME="unisondb.client"
INSTANCE_COUNT=${INSTANCE_COUNT:-50}
BASE_HTTP_PORT=6000
BASE_GRPC_PORT=5000
BASE_PPROF_PORT=6080

CENTRAL_IP=${CENTRAL_IP:-"127.0.0.1"}
ROLE=${ROLE:-"client"}
PROM_IP=${PROM_IP:-"127.0.0.1"}

CONFIG_DIR="./configs"
DATA_DIR="./data"
LOG_DIR="./logs"
PID_DIR="./pids"
PROM_CONFIG_PATH="./local/prometheus/prometheus.yml"

mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR" "$PID_DIR" ./prometheus_data

# Generate UnisonDB Configs
for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  HTTP_PORT=$(($BASE_HTTP_PORT + $i))
  GRPC_PORT=$(($BASE_GRPC_PORT + $i))
  PPROF_PORT=$(($BASE_PPROF_PORT + $i))

  cat <<EOF > ${CONFIG_DIR}/config_$i.toml
http_port = $HTTP_PORT
pprof_enable = true

[grpc_config]
port = $GRPC_PORT
allow_insecure = true

[storage_config]
base_dir = "${DATA_DIR}/database_$i"
namespaces = ["ad-campaign", "user-targeting-profile", "ad-vector", "audience-segments", "channel-allocation"]
bytes_per_sync = "1MB"
segment_size = "16MB"
disable_entry_type_check = true

[write_notify_config]
enabled = true
max_delay = "20ms"

[storage_config.wal_cleanup_config]
interval = "5m"
enabled = true
max_age = "1h"
min_segments = 5
max_segments = 30

[relayer_config.relayer1]
namespaces = ["ad-campaign", "user-targeting-profile", "ad-vector", "audience-segments", "channel-allocation"]
upstream_address = "${CENTRAL_IP}:4001"
segment_lag_threshold = 10000
allow_insecure = true

[pprof_config]
enabled = true
port = $PPROF_PORT

[log_config]
log_level = "info"
disable_timestamp = true

[log_config.min_level_percents]
debug = 100.0
info  = 100.0
warn  = 100.0
error = 100.0

[wal_io_global_limiter]
enable = false
burst = 5000
rate_limit = 5000
EOF
done

# Generate Prometheus config
cat <<EOF > ${PROM_CONFIG_PATH}
global:
  scrape_interval: 30s
  external_labels:
    instance: "\${id}"

scrape_configs:
  - job_name: "unisondb"
    static_configs:
      - targets: ["192.168.0.103:4000"]
        labels:
          instance: "${HOSTNAME}_fuzzer"
          role: "fuzzer"
EOF

for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  HTTP_PORT=$(($BASE_HTTP_PORT + $i))
  cat <<EOF >> ${PROM_CONFIG_PATH}
      - targets: ["192.168.0.103:${HTTP_PORT}"]
        labels:
          instance: "${HOSTNAME}_relayer_${i}"
          role: "${ROLE}"
EOF
done

#cat <<EOF >> ${PROM_CONFIG_PATH}
#
#remote_write:
#  - url: "http://${PROM_IP}:9090/api/v1/write"
#EOF

# Start UnisonDB instances
echo "Starting $INSTANCE_COUNT UnisonDB instances..."
for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  CONFIG_FILE="${CONFIG_DIR}/config_$i.toml"
  LOG_FILE="${LOG_DIR}/unisondb_$i.log"
  ./unisondb_fuzz --config "$CONFIG_FILE" relayer > "$LOG_FILE" 2>&1 &
  echo $! > "${PID_DIR}/unisondb_$i.pid"
  echo "Instance $i running on port $((BASE_HTTP_PORT + i))"
done

echo "All processes started. Logs are in $LOG_DIR. Use ./stop_local_cluster.sh to stop them."
