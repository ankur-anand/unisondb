## UnisonDB 🚀

<img src="docs/logo.svg" width="300" alt="UnisonDB" />

> A Hybrid KV Store for Fast Writes, Efficient Reads, and Seamless Replication — with Explicit Transactions, 
> LOB and Wide Column Support, Powered by Logs and Trees.

[![ci-tests](https://github.com/ankur-anand/unisondb/actions/workflows/go.yml/badge.svg)](https://github.com/ankur-anand/unisondb/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/ankur-anand/unisondb/badge.svg?branch=main)](https://coveralls.io/github/ankur-anand/unisondb?branch=main)

## Overview

UnisonDB is a high-performance, replicated key-value store that blends the best of WALs, Memtables, and B-Trees to achieve:

* Blazing-fast writes without LSM compaction overhead.

* Optimized range queries with minimal disk I/O using B-Trees

* Efficient replication via gRPC WAL streaming & B-Tree snapshots

* Seamless multi-region scaling with rapid fail over.

* LOB support via chunked, transactional writes for large object handling.

* Flexible wide-column data modeling for dynamic, nested records

## Core Architecture 

UnisonDB is built on three foundational layers:

1. **WALFS** - Write-Ahead Log File System (mmap-based, designed for reading at scale)
2. **Engine** - Hybrid storage combining WAL, MemTable, and B-Tree
3. **Replication** - WAL-based streaming replication.

## 1. WALFS (Write-Ahead Log)

### Overview

WALFS is a memory-mapped, segmented write-ahead log implementation designed for **both writing AND reading at scale**. 
Unlike traditional WALs that optimize only for sequential writes, WALFS provides efficient random access for replication, and real-time tailing.

<img src="./docs/walnreader.png">

### Segment Structure

Each WALFS segment consists of two regions:

```
+----------------------+-----------------------------+-------------+
|   Segment Header     |        Record 1             |  Record 2   |
|     (64 bytes)       |  Header + Data + Trailer    |     ...     |
+----------------------+-----------------------------+-------------+
```

#### Segment Header (64 bytes)

| Offset | Size | Field           | Description                                  |
|--------|------|-----------------|----------------------------------------------|
| 0      | 4    | Magic           | Magic number (`0x5557414C`)                  |
| 4      | 4    | Version         | Metadata format version                      |
| 8      | 8    | CreatedAt       | Creation timestamp (nanoseconds)             |
| 16     | 8    | LastModifiedAt  | Last modification timestamp (nanoseconds)    |
| 24     | 8    | WriteOffset     | Offset where next chunk will be written      |
| 32     | 8    | EntryCount      | Total number of chunks written               |
| 40     | 4    | Flags           | Segment state flags (e.g. Active, Sealed)    |
| 44     | 12   | Reserved        | Reserved for future use                      |
| 56     | 4    | CRC             | CRC32 checksum of first 56 bytes             |
| 60     | 4    | Padding         | Ensures 64-byte alignment                    |

#### Record Format (8-byte aligned)

Each record is written in its own aligned frame:

| Offset  | Size     | Field   | Description                                      |
|---------|----------|---------|--------------------------------------------------|
| 0       | 4 bytes  | CRC     | CRC32 of `[Length \| Data]`                      |
| 4       | 4 bytes  | Length  | Size of the data payload in bytes                |
| 8       | N bytes  | Data    | User payload (FlatBuffer-encoded LogRecord)      |
| 8 + N   | 8 bytes  | Trailer | Canary marker (`0xDEADBEEFFEEEDFACE`)            |
| ...     | ≥0 bytes | Padding | Zero padding to align to 8-byte boundary         |

## WALFS Reader Capabilities

WALFS provides powerful reading capabilities essential for replication and recovery:

#### 1. **Forward-Only Iterator**

```go
reader := walLog.NewReader()
defer reader.Close()

for {
    data, pos, err := reader.Next()
    if err == io.EOF {
        break
    }
    // Process record
}
```

- **Zero-copy reads** - data is a memory-mapped slice
- **Position tracking** - each record returns its `(SegmentID, Offset)` position
- **Automatic segment traversal** - seamlessly reads across segment boundaries

#### 2. **Offset-Based Reads**

```go
// Read from a specific offset (for replication catch-up)
offset := Offset{SegmentID: 5, Offset: 1024}
reader, err := walLog.NewReaderWithStart(&offset)
```

- **Efficient seek** - jump directly to any offset without scanning
- **Replication-friendly** - followers can resume from their last synced position
- **Recovery-friendly** - start recovery from last checkpoint

#### 3. **Active Tail Following**

```go
// For real-time replication (tailing active WAL)
reader, err := walLog.NewReaderWithTail(&offset)

for {
    data, pos, err := reader.Next()
    if err == ErrNoNewData {
        // No new data yet, can retry or wait
        continue
    }
}
```
- **Returns `ErrNoNewData`** instead of `io.EOF` when caught up

Unlike traditional WALs that are "write-once, read-on-crash", WALFS is optimized for:

1. **Continuous replication** - followers constantly read from primary's WAL
2. **Real-time tailing** - low-latency streaming of new writes
3. **Parallel readers** - multiple replicas can read concurrently without contention

## 2. Engine (dbkernel)

### Overview

The Engine is the core storage layer that orchestrates writes, reads, and persistence. It combines:

- **WAL (WALFS)** - Durability and replication source
- **MemTable (SkipList)** - In-memory write buffer for fast writes
- **B-Tree Store** - Persistent index for efficient reads

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Client Writes                        │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │  Append to WAL (WALFS) │ ◄── Durability
          └────────┬───────────────┘
                   │
                   ▼
          ┌────────────────────────┐
          │  Write to MemTable     │ ◄── Fast writes
          │    (SkipList)          │
          └────────┬───────────────┘
                   │
                   │ (Async flush on rotation)
                   ▼
          ┌────────────────────────┐
          │   Flush to B-Tree      │ ◄── Persistent index
          │   + Checkpoint offset  │
          └────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                    Client Reads                         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │  1. Check MemTable     │ ◄── Active writes
          └────────┬───────────────┘
                   │ (if miss)
                   ▼
          ┌────────────────────────┐
          │  2. Check Sealed Mems  │ ◄── Pending flush
          └────────┬───────────────┘
                   │ (if miss)
                   ▼
          ┌────────────────────────┐
          │  3. Read from B-Tree   │ ◄── Durable data
          └────────────────────────┘
```

### FlatBuffer Schema

UnisonDB uses **FlatBuffers** for zero-copy serialization of WAL records. This provides:

#### Why FlatBuffers?

**Replication efficiency** - No deserialization needed on replicas

<img src="./docs/schema_time.jpg" width="300">
<img src="./docs/schema_mem.jpg" width="300">

### Transaction Support
### LOB (Large Object) Support
### Wide-Column Support

UnisonDB provides **atomic multi-key transactions**:


## Why is Traditional KV Replication Insufficient?

> Most traditional key-value stores were designed for simple, point-in-time key-value operations — and their replication 
models reflect that. While this works for basic use cases, it quickly breaks down under real-world 
demands like multi-key transactions, large object handling, and fine-grained updates.

### Key-Level Replication Only

Replication is often limited to raw key-value pairs. 
There’s no understanding of higher-level constructs like rows, columns, 
or chunks — making it impossible to efficiently replicate partial updates or large structured objects.

### No Transactional Consistency

Replication happens on a per-operation basis, not as part of an atomic unit.
Without multi-key transactional guarantees, systems can fall into inconsistent states across replicas, 
especially during batch operations, network partitions, or mid-transaction failures.

### Chunked LOB Writes Become Risky

When large values are chunked and streamed to the store, traditional replication models expose chunks as they arrive. 
If a transfer fails mid-way, replicas may store incomplete or corrupted objects, with no rollback or recovery mechanism.

### No Awareness of Column-Level Changes

Wide-column data is treated as flat keys or opaque blobs. If only a single column is modified, 
traditional systems replicate the entire row, wasting bandwidth, 
increasing storage overhead, and making efficient synchronization impossible.

### Operational Complexity Falls on the User

Without built-in transactional semantics, developers must implement their own logic for deduplication, 
rollback, consistency checks, and coordination — which adds fragility and complexity to the system.

### Storage Engine Tradeoffs

•	LSM-Trees (e.g., RocksDB) excel at fast writes but suffer from high read amplification and costly background compactions, which hurt latency and predictability.

•	B+Trees (e.g., BoltDB,LMDB) offer efficient point lookups and range scans, but struggle with high-speed inserts and lack native replication support.
 
## How UnisonDB Solves This. :white_check_mark:

UnisonDB combines append-only logs for high-throughput ingest with B-Trees for fast and efficient range reads — while offering:

* Transactional, multi-key replication with commit visibility guarantees.
* Chunked LOB writes that are fully atomic.
* Column-aware replication for efficient syncing of wide-column updates.
* Isolation by default — once a network-aware transaction is started, all intermediate writes are fully isolated and not visible to readers until a successful txn.Commit().
* Built-in replication via gRPC WAL streaming + B-Tree snapshots.
* Zero-compaction overhead, high write throughput, and optimized reads.

## Architecture Overview

![storage architecture](docs/arch.svg)

## Development
```sh
make lint
make test
```

## certificate for Local host

```shell
brew install mkcert

## install local CA
mkcert -install

## Generate gRPC TLS Certificates
## these certificate are valid for hostnames/IPs localhost 127.0.0.1 ::1

mkcert -key-file grpc.key -cert-file grpc.crt localhost 127.0.0.1 ::1

```

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)