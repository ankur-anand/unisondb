package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

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

func (e *Engine) saveBloomFilter() error {
	// Serialize Bloom Filter
	var buf bytes.Buffer
	e.mu.RLock()
	_, err := e.bloom.WriteTo(&buf)
	e.mu.RUnlock()
	if err != nil {
		return err
	}

	return e.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(walCheckPointBucket)

		if bucket == nil {
			return errors.New("walCheckPointBucket not found") // No checkpoint saved yet
		}
		// Save serialized Bloom Filter to BoltDB
		return bucket.Put(bloomFilterKey, buf.Bytes())
	})
}

func (e *Engine) loadBloomFilter() error {
	var result []byte
	err := e.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(walCheckPointBucket)
		if bucket == nil {
			return nil // No existing Bloom filter, create a new one
		}

		data := bucket.Get(bloomFilterKey)
		if data == nil {
			return ErrKeyNotFound
		}

		// copy the value
		result = append([]byte{}, data...)
		return nil
	})

	if len(result) != 0 {
		// Deserialize Bloom Filter
		buf := bytes.NewReader(result)
		_, err = e.bloom.ReadFrom(buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize bloom filter: %w", err)
		}
	}

	return err
}
