# UnisonDB Blobstore + MinIO Example

This folder has two live MinIO demos:

- standalone: 1 producer + 1 consumer
- raft: 3 producer nodes + 1 consumer

Both demos use the namespaces `orders` and `inventory`.

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

Then run the standalone demo:

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

What the standalone script does:

1. Builds `./cmd/unisondb`
2. Ensures the MinIO bucket exists and clears this example's prefix
3. Starts the producer with `producer.local.toml`
4. Starts the consumer with `consumer.local.toml`
5. Writes one KV into `orders` and one KV into `inventory` through the producer HTTP API
6. Waits until the consumer serves both replicated values
7. Keeps both processes running until you press `Ctrl+C`

## Raft Demo

Run the 3-node Raft producer demo:

```bash
./cmd/examples/blobstore-minio/run-live-raft.sh
```

In another terminal, write 10 sample keys through the current Raft leader:

```bash
./cmd/examples/blobstore-minio/write-10-kv-raft.sh
```

By default that script writes into `orders`. To target `inventory`:

```bash
NAMESPACE=inventory ./cmd/examples/blobstore-minio/write-10-kv-raft.sh
```

This demo uses Serf membership for Raft peer discovery:

- `node0` bootstraps the cluster
- `node1` and `node2` join through Serf on `127.0.0.1:17001`
- the Raft service adds peers dynamically per namespace after membership converges

What the Raft script does:

1. Builds `./cmd/unisondb`
2. Ensures the MinIO bucket exists and clears the Raft example prefix
3. Starts three producer nodes with Serf-enabled Raft config
4. Starts one consumer with blobstore relaying enabled
5. Waits until both namespaces see all three Raft peers
6. Discovers the current leader for each namespace and writes initial values
7. Verifies both values on the consumer
8. Stops the current `orders` leader to force failover
9. Discovers the new `orders` leader, writes again, and verifies replication still works
10. Keeps the remaining processes running until you press `Ctrl+C`

Because Raft leadership is per namespace, the failover check is pinned to `orders`. `inventory` is still verified during initial replication.

Useful environment overrides:

- `UNISONDB_BIN`: path to an existing `unisondb` binary
- `AWS_ACCESS_KEY_ID`: defaults to `minioadmin`
- `AWS_SECRET_ACCESS_KEY`: defaults to `minioadmin`
- `AWS_REGION`: defaults to `us-east-1`
- `EXIT_AFTER_VERIFY=1`: exit right after the initial replication check instead of staying attached
- `PRODUCER_URL`, `CONSUMER_URL`, `NAMESPACE`, `COUNT`, `KEY_PREFIX`, `VALUE_PREFIX`: overrides for `write-10-kv.sh`
- `PRODUCER_URLS_CSV`, `CONSUMER_URL`, `NAMESPACE`, `COUNT`, `KEY_PREFIX`, `VALUE_PREFIX`: overrides for `write-10-kv-raft.sh`
