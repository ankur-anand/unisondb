## kvdrivers

This package implements Multi-modal Database on the TOP of Btree.

## Why Multi-Modal?

Modern applications demand flexible storage primitives: sometimes you need simple key-value; other times, 
fine-grained updates to wide-column data, or efficient storage of large binary blobs.

## Supported Data Models

- **Key-Value (KV):** Simple, opaque key-to-value mappings.
- **Wide-Column (Cell):** Row-oriented storage of multiple columns per row, supporting granular cell updates and queries.
- **Chunked Blobs:** Store and retrieve large objects as logically contiguous chunks.
