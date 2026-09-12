# UnisonDB Object-Store Replication + MinIO Example

This folder has a live MinIO demo for object-store backed WAL replication: 1 producer + 1 consumer.

The demo uses the namespaces `orders` and `inventory`.
Each namespace is written as its own stream under the configured object-store prefix. Producers publish WAL records as immutable segment files plus catalog metadata. Consumers read the finalized history directly from MinIO and apply it to their local read-only engines.

It uses the MinIO defaults from your local command:

- access key: `minioadmin`
- secret key: `minioadmin`
- region: `us-east-1`
- API endpoint: `http://127.0.0.1:9000`

Start MinIO first:

```bash
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Then run the demo:

```bash
./cmd/examples/blobstore-minio/run-live.sh
```

In another terminal, write 10 sample keys and verify them on the consumer:

```bash
./cmd/examples/blobstore-minio/write-10-kv.sh
```

By default that script writes into `orders`. To target `inventory`:

```bash
NAMESPACE=inventory ./cmd/examples/blobstore-minio/write-10-kv.sh
```

What the script does:

1. Builds `./cmd/unisondb`
2. Ensures the MinIO bucket exists and clears this example's prefix
3. Starts the producer with `producer.local.toml`
4. Starts the consumer with `consumer.local.toml`
5. Writes one KV into `orders` and one KV into `inventory` through the producer HTTP API
6. Waits until the consumer serves both replicated values
7. Keeps both processes running until you press `Ctrl+C`

The producer config uses:

```toml
[blob_store_streaming.namespaces.orders]
bucket_url = "s3://unisondb-blob-demo?endpoint=http://127.0.0.1:9000&region=us-east-1&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required"
base_prefix = "unisondb/examples/blobstore-minio"
```

The consumer config uses:

```toml
[relayer_config.blob_demo.blobstore]
bucket_url = "s3://unisondb-blob-demo?endpoint=http://127.0.0.1:9000&region=us-east-1&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required"
prefix = "unisondb/examples/blobstore-minio"
refresh_interval = "250ms"
```

`base_prefix` on the producer and `prefix` on the consumer must point to the same object-store root. There is no local blob cache directory in this path; the consumer reads catalog and segment data from MinIO.

Useful environment overrides:

- `UNISONDB_BIN`: path to an existing `unisondb` binary
- `AWS_ACCESS_KEY_ID`: defaults to `minioadmin`
- `AWS_SECRET_ACCESS_KEY`: defaults to `minioadmin`
- `AWS_REGION`: defaults to `us-east-1`
- `EXIT_AFTER_VERIFY=1`: exit right after the initial replication check instead of staying attached
- `PRODUCER_URL`, `CONSUMER_URL`, `NAMESPACE`, `COUNT`, `KEY_PREFIX`, `VALUE_PREFIX`: overrides for `write-10-kv.sh`
