package storage

import (
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// WalReader defines an interface for reading Write-Ahead Log (WAL) entries.
type WalReader interface {
	NewReader() *wal.Reader
	NewReaderWithStart(startPos *wal.ChunkPosition) (*wal.Reader, error)
}

// NewWalReader returns a new instance of WalReader, allowing the caller to
// access WAL logs for replication, recovery, or log processing.
func (e *Engine) NewWalReader() WalReader {
	return e.wal
}

// recoverWAL starts from the last stored sequence in BoltDB.
func (e *Engine) recoverWAL() (int, error) {
	slog.Info("Recovering WAL...", "namespace", e.namespace)

	chunkPosition, err := loadChunkPosition(e.db)
	if err != nil {
		return 0, err
	}

	var reader *wal.Reader
	ignoreFirstChunk := false
	// if the chunkPosition is nil
	// maybe in first run itself, we never crossed the arena limit, and it was never flushed.
	// or the db file was new, somehow.
	// in both the case try loading the entire wal.
	if chunkPosition == nil {
		slog.Info("WAL Chunk Position is nil. Loading the Complete WAL...")
		reader = e.wal.NewReader()
	}

	if chunkPosition != nil {
		// recovery should happen from last chunk position + 1.
		ignoreFirstChunk = true
		slog.Info("Starting WAL replay from index ..", "segment", chunkPosition.SegmentId, "offset", chunkPosition.ChunkOffset)

		reader, err = e.wal.NewReaderWithStart(chunkPosition)
		if err != nil {
			return 0, err
		}
	}

	recordCount := 0

	for {
		val, pos, err := reader.Next()
		if err == io.EOF {
			break
		}

		if ignoreFirstChunk {
			ignoreFirstChunk = false
			continue
		}

		// decompress the data
		data, err := decompressLZ4(val)
		if err != nil {
			slog.Error("Skipping unreadable WAL entry:", "pos", pos, "err", err)
			continue
		}

		// build the mem-table,
		// decode the data
		recordCount++
		record := decodeWalRecord(data)

		// Store in MemTable
		if record.Operation == OpInsert || record.Operation == OpDelete {
			var memValue []byte
			if int64(len(val)) <= e.storageConfig.ValueThreshold {
				memValue = append([]byte{1}, val...) // Directly store small values
			} else {
				memValue = append([]byte{0}, pos.Encode()...) // Store WAL reference
			}

			err = e.memTableWrite(record.Key, y.ValueStruct{
				Meta:  record.Operation,
				Value: memValue,
			}, pos)
			if err != nil {
				return recordCount, err
			}
		}
	}
	slog.Info("WAL Recovery Completed", "namespace", e.namespace, "record-count", recordCount)
	return recordCount, nil
}
