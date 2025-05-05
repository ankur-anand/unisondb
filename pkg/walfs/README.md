### walfs

Implements Write Ahead log using the mmap: Designed for Reading At Scale as well.

* Memory-mapped I/O for low-overhead random access 
* 8yte aligned entries to support efficient page caching
* Built-in corruption detection using CRC32 and trailer markers
* Designed for fast recovery and streaming-based replication

## Segment

Each Segment is Divided into Two Region.
1. Top 64 Byte of the file is Header section of the Segment.
2. After that Each Individual Log Entry is written which consist of
   1. header
   2. data
   3. trailer

```markdown
+----------------------+-----------------------------+-------------+
|     Segment Header   |      Record 1               |  Record 2   |
|     (64 bytes)       |  Header + Data + Trailer    |     ...     |
+----------------------+-----------------------------+-------------+
```

## Metadata Section

```markdown
|--------|------|------------------|---------------------------------------------|
| Offset | Size | Field           | Description                                  |
|--------|------|------------------|---------------------------------------------|
| 0      | 4    | Magic            | Magic number (`0x5557414C`)                 |
| 4      | 4    | Version          | Metadata format version                     |
| 8      | 8    | CreatedAt        | Creation timestamp (nanoseconds)            |
| 16     | 8    | LastModifiedAt   | Last modification timestamp (nanoseconds)   |
| 24     | 8    | WriteOffset      | Offset where next chunk will be written     |
| 32     | 8    | EntryCount       | Total number of chunks written              |
| 40     | 4    | Flags            | Segment state flags (e.g. Active, Sealed)   |
| 44     | 12   | Reserved         | Reserved for future use                     |
| 56     | 4    | CRC              | CRC32 checksum of first 56 bytes            |
| 60     | 4    | Padding/Reserved | Ensures 64-byte alignment                   |
|--------|------|------------------|---------------------------------------------|
```

## Record Format (Aligned)
Each record is written in its own aligned frame. All components (header + data + trailer) are padded to the next multiple of 8 bytes.

* Each record is individually check summed and terminates with a trailer marker
* On recovery, scanning stops at the first invalid or torn record
* The segment header includes a CRC for metadata validation

### Alignment Example

If a record is:
* 11-byte data 
* Then total = 8 (header) + 11 (data) + 8 (trailer) = 27 bytes 
* Final aligned frame = 32 bytes (with 5 bytes of padding)

```markdown
|-----------|
| Offset    | Size         | Field    | Description                                                  |
|-----------|--------------|----------|--------------------------------------------------------------|
| 0         | 4 bytes      | CRC      | CRC32 of `[Length | Data]`                                   |
| 4         | 4 bytes      | Length   | Size of the data payload in bytes                            |
| 8         | N bytes      | Data     | User payload                                                 |
| 8 + N     | 8 bytes      | Trailer  | Canary marker (`0xDEADBEEFFEEEDFACE`)                        |
| ...       | ≥0 bytes     | Padding  | Zero padding to align full frame to 8-byte boundary          |
|-----------|
```
