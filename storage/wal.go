package storage

import (
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// Ensure WAL implements the interface.
var _ WalReader = (*wal.WAL)(nil)

// WalReader defines an interface for reading Write-Ahead Log (WAL) entries.
type WalReader interface {
	NewReader() *wal.Reader
	NewReaderWithStart(startPos *wal.ChunkPosition) (*wal.Reader, error)
}

// NewWalReader returns a new instance of WalReader, allowing the caller to
// access WAL logs for replication, recovery, or log processing.
//
// nolint:ireturn
func (e *Engine) NewWalReader() WalReader {
	return e.wal
}

// recoverWAL starts from the last stored sequence in BoltDB.
func (e *Engine) recoverWAL(metadata Metadata, namespace string) error {
	slog.Info("Recovering WAL...", "namespace", namespace)
	var reader *wal.Reader
	var err error
	ignoreFirstChunk := false

	// if the index is zero
	// maybe in first run itself, we never crossed the arena limit, and it was never flushed.
	// or the db file was new, somehow.
	// in both the case try loading the entire wal.
	if metadata.Index == 0 {
		slog.Info("WAL Index Position is 0. Loading the Complete WAL...", "namespace", namespace)
		reader = e.wal.NewReader()
	}

	if metadata.Index != 0 {
		// recovery should happen from last chunk position + 1.
		ignoreFirstChunk = true
		slog.Info("Starting WAL replay from index ..", "index", metadata.Index, "segment", metadata.Pos.SegmentId, "offset", metadata.Pos.ChunkOffset, "namespace", namespace)

		reader, err = e.wal.NewReaderWithStart(metadata.Pos)
		if err != nil {
			return err
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
		data, err := DecompressLZ4(val)
		if err != nil {
			slog.Error("Skipping unreadable WAL entry:", "pos", pos, "err", err, "namespace", namespace)
			continue
		}

		// build the mem-table,
		// decode the data
		recordCount++
		record := DecodeWalRecord(data)

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
			}, pos, e.globalCounter.Add(1))
			if err != nil {
				return err
			}
		}
	}

	slog.Info("WAL Recovery Completed", "namespace", namespace, "record-count", recordCount)
	e.recoveredEntriesCount = recordCount
	return nil
}
