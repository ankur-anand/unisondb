package storage

import (
	"io"
	"log/slog"

	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

// loadChunkPosition retrieves the WAL checkpoint from BoltDB
func loadChunkPosition(db *bbolt.DB) (*wal.ChunkPosition, error) {
	var pos *wal.ChunkPosition

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("wal_checkpoint"))
		if bucket == nil {
			return nil // No checkpoint saved yet
		}

		data := bucket.Get([]byte("last_position"))
		pos = wal.DecodeChunkPosition(data)

		return nil
	})

	return pos, err
}

// recoverWAL starts from the last stored sequence in BoltDB.
func (se *Engine) recoverWAL() error {
	se.mu.Lock()
	defer se.mu.Unlock()

	slog.Info("Recovering WAL...", "namespace", se.namespace)

	chunkPosition, err := loadChunkPosition(se.db)
	if err != nil {
		return err
	}

	if chunkPosition == nil {
		slog.Info("WAL Chunk Position is nil. Skipping...")
		return nil
	}
	slog.Info("Starting WAL replay from index ..")

	reader, err := se.wal.NewReaderWithStart(chunkPosition)
	if err != nil {
		return err
	}

	recordCount := 0
	for {
		val, pos, err := reader.Next()
		if err == io.EOF {
			break
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
		record, err := decodeWalRecord(data)
		if err != nil {
			slog.Error("Skipping unreadable WAL entry:", "pos", pos, "err", err)
			continue
		}
		err = se.persistKeyValue(record.Key, record.Value, record.Operation, record.BatchID)
		if err != nil {
			return err
		}
	}

	slog.Info("WAL Recovery Completed", "namespace", se.namespace, "record-count", recordCount)
	return nil
}
