package storage

import (
	"io"
	"log/slog"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
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

	if reader == nil {
		panic("can't recover from WAL reader")
	}

	recordCount := 0

	for {
		data, pos, err := reader.Next()
		if err == io.EOF {
			break
		}

		if ignoreFirstChunk {
			ignoreFirstChunk = false
			continue
		}

		recordCount++
		record := wrecord.GetRootAsWalRecord(data, 0)

		// Store in MemTable
		if record.Operation() == wrecord.LogOperationOpInsert || record.Operation() == wrecord.LogOperationOpDelete {
			var memValue []byte
			if int64(len(data)) <= e.storageConfig.ValueThreshold {
				memValue = append([]byte{1}, data...) // Directly store small values
			} else {
				memValue = append([]byte{0}, pos.Encode()...) // Store WAL reference
			}

			err = e.memTableWrite(record.KeyBytes(), y.ValueStruct{
				Meta:  byte(record.Operation()),
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
