#!/bin/bash

HOSTNAME=$(hostname)
INSTANCE_COUNT=${INSTANCE_COUNT:-5}
BASE_HTTP_PORT=4000
BASE_GRPC_PORT=5000
BASE_PPROF_PORT=6060

CENTRAL_IP=${CENTRAL_IP:-"127.0.0.1"}
USERNAME=${USERNAME:-"ankur"}
OB_TOKEN=${OB_TOKEN:-"ob_token"}
OB_USER=${OB_USER:-"ob_user"}
OB_PASS=${OB_PASS:-"ob_pass"}
ROLE=${ROLE:-"client"}
PROM_CONFIG_PATH="/etc/prometheus/prometheus.yml"

mkdir -p /etc/unisondb /etc/systemd/system /etc/prometheus

# Generate UnisonDB Instance Configs
for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  HTTP_PORT=$(($BASE_HTTP_PORT + $i))
  GRPC_PORT=$(($BASE_GRPC_PORT + $i))
  PPROF_PORT=$(($BASE_PPROF_PORT + $i))

  cat <<EOF > /etc/unisondb/config_$i.toml
http_port = $HTTP_PORT
pprof_enable = true

[grpc_config]
port = $GRPC_PORT
allow_insecure = true

[storage_config]
base_dir = "/tmp/unisondb/database_$i"
namespaces = ["default", "tenant_1", "tenant_2"]
bytes_per_sync = "1MB"
segment_size = "16MB"
arena_size = "4MB"

[relayer_config.relayer1]
namespaces = ["default", "tenant_1", "tenant_2"]
upstream_address = "${CENTRAL_IP}:4001"
segment_lag_threshold = 100
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
enable = true
burst = 400
rate_limit = 1200
EOF
done

cat <<EOF > ${PROM_CONFIG_PATH}
global:
  scrape_interval: 30s
  external_labels:
    instance: "\${id}"

scrape_configs:
  - job_name: "unisondb"
    static_configs:
EOF

for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  HTTP_PORT=$(($BASE_HTTP_PORT + $i))
  cat <<EOF >> ${PROM_CONFIG_PATH}
      - targets: ["localhost:${HTTP_PORT}"]
        labels:
          instance: "${HOSTNAME}_relayer_${i}"
          role: "${ROLE}"
EOF
done

cat <<EOF >> ${PROM_CONFIG_PATH}

remote_write:
  - url: "https://api.openobserve.ai/api/${OB_TOKEN}/prometheus/api/v1/write"
    basic_auth:
      username: "${OB_USER}"
      password: "${OB_PASS}"
EOF

cat <<EOF > /etc/systemd/system/unisondb@.service
[Unit]
Description=UnisonDB Server Instance %i
After=network.target

[Service]
ExecStart=/usr/local/bin/unisondb --config /etc/unisondb/config_%i.toml relayer
WorkingDirectory=/opt/unisondb
Restart=on-failure
User=${USERNAME}
LimitNOFILE=65535
StandardOutput=journal
StandardError=journal
SyslogIdentifier=unisondb_%i

[Install]
WantedBy=multi-user.target
EOF

cat <<EOF > /etc/systemd/system/prometheus.service
[Unit]
Description=Prometheus
After=network.target

[Service]
ExecStart=/usr/local/bin/prometheus \\
  --config.file=/etc/prometheus/prometheus.yml \\
  --storage.tsdb.path=/opt/prometheus/data \\
  --web.listen-address=":9090"
Restart=on-failure
User=nobody

[Install]
WantedBy=multi-user.target
EOF

# Enable and Start Instances
systemctl daemon-reload

for i in $(seq 0 $(($INSTANCE_COUNT - 1))); do
  systemctl enable unisondb@$i.service
  systemctl start unisondb@$i.service
done

systemctl restart prometheus
echo "Setup complete. ${INSTANCE_COUNT} instances started."
