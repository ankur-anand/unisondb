# UnisonDB Multi-Datacenter CRDT Example

This example demonstrates how to build a CRDT-based distributed database using UnisonDB with:
- **Two primary datacenters** (DC1 and DC2) accepting independent writes
- **One relayer node** that replicates from both datacenters
- **client** that listens to ZeroMQ notifications and maintains local CRDT state

## Architecture

```
+-----------------------------------------------------------------+
|                    Multi-DC CRDT Architecture                   |
+-----------------------------------------------------------------+

        Writes                                    Writes
          |                                         |
          v                                         v
   +---------------+                         +---------------+
   | Datacenter 1  |                         | Datacenter 2  |
   |   (Primary)   |                         |   (Primary)   |
   |               |                         |               |
   | Namespace: dc1|                         | Namespace: dc2|
   | gRPC: 9001    |                         | gRPC: 9002    |
   | HTTP: 8001    |                         | HTTP: 8002    |
   +-------+-------+                         +-------+-------+
           |                                         |
           | gRPC Replication                        | gRPC Replication
           |                                         |
           +------------------+----------------------+
                              |
                              v
                       +-----------------+
                       |     Relayer     |
                       |   (Read-Only)   |
                       |                 |
                       | Namespaces:     |
                       |   dc1, dc2      |
                       | HTTP: 8003      |
                       | ZMQ dc1: 5555   |-----+
                       | ZMQ dc2: 5556   |-----|
                       +-----------------+     |
                                               | ZeroMQ
                                               | Notifications
                                               |
                                               v
                                      +-------------------+
                                      |     Client        |
                                      |                   |
                                      |   CRDT State      |
                                      |   - LWW-Register  |
                                      |   - G-Counter     |
                                      +-------------------+
```

## Key Concepts

### Primary Servers (DC1 & DC2)
- **Mode**: Replicator (read-write)
- **Purpose**: Accept writes independently in different geographic locations
- **Namespaces**: Each datacenter has its own namespace (`dc1`, `dc2`)

### Relayer Node
- **Mode**: Relayer (read and sync from primary)
- **Purpose**: Replicate from both datacenters and merge changes
- **Namespaces**: Replicates both `dc1` and `dc2` namespaces
- **ZeroMQ**: Publishes notifications for each namespace on separate ports

### CRDT Types Demonstrated

#### 1. LWW-Register (Last-Write-Wins Register)
- Stores a value with a timestamp
- Conflict resolution: Newest timestamp wins
- Tie-breaking: Higher replica ID wins
- Use case: User profiles, configuration settings

#### 2. G-Counter (Grow-Only Counter)
- Maintains per-replica counters
- Merge operation: Take max of each replica's count
- Monotonically increasing
- Use case: Page views, API calls, metrics

## Prerequisites

1. **Build UnisonDB**:
   ```bash
   go build -o unisondb ./cmd/unisondb
   ```

2. **Install Dependencies**:
   - **ZeroMQ**: Required for change notifications
   - https://zeromq.org/download/

## Quick Start

### 1. Start all UnisonDB nodes

```bash
./cmd/unisondb/unisondb -config ./examples/crdt-multi-dc/configs/dc1.toml replicator

./cmd/unisondb/unisondb -config ./examples/crdt-multi-dc/configs/dc2.toml replicator

./cmd/unisondb/unisondb -config ./examples/crdt-multi-dc/configs/relayer.toml relayer
```

This will start:
- **DC1**: Primary server on ports 9001 (gRPC), 8001 (HTTP)
- **DC2**: Primary server on ports 9002 (gRPC), 8002 (HTTP)
- **Relayer**: Read-only replica on port 8003 (HTTP), 5555 & 5556 (ZeroMQ)

### 2. Start the CRDT client

```bash
cd examples/crdt-multi-dc/golang-crdt-client
go run main.go
```

The client will:
- Connect to both ZeroMQ endpoints (dc1: 5555, dc2: 5556)
- Listen for change notifications
- Fetch updated values via HTTP API
- Maintain local CRDT state
- Print merged state after each update

### 4. Run demo writes

In another terminal:

```bash
cd examples/crdt-multi-dc
./curl-examples.sh
```

This demonstrates:
- **LWW-Register**: Concurrent writes with timestamp-based conflict resolution
- **G-Counter**: Distributed counting across datacenters
- **Out-of-order delivery**: Handling stale updates

### 5. Watch the CRDT convergence

Observe the Node.js client output to see:
- Real-time notifications from both datacenters
- CRDT conflict resolution in action
- Eventual consistency across all replicas
