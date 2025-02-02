package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

const (
	defaultValueThreshold = 2 * wal.KB // Values below this size are stored directly in MemTable
	defaultArenaSize      = wal.MB
)

// ErrKeyNotFound is a sentinel error for missing keys
var ErrKeyNotFound = errors.New("key not found")

// Engine manages WAL, MemTable (SkipList), and BoltDB for a given namespace
type Engine struct {
	wal       *wal.WAL
	memTable  *skl.Skiplist
	db        *bbolt.DB
	mu        sync.RWMutex
	bloom     *bloom.BloomFilter
	flushSize int64
	namespace string
}

// NewStorageEngine initializes WAL, MemTable, and BoltDB
func NewStorageEngine(baseDir, namespace string, flushSize int64) (*Engine, error) {
	// Define paths for WAL & BoltDB
	nsDir := filepath.Join(baseDir, namespace)
	walDir := filepath.Join(nsDir, "wal")
	dbFile := filepath.Join(nsDir, "db.bolt")

	// Open WAL
	w, err := wal.Open(newWALOptions(walDir))
	if err != nil {
		return nil, err
	}

	// Open BoltDB
	db, err := bbolt.Open(dbFile, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Create namespace bucket in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(namespace))
		return err
	})
	if err != nil {
		return nil, err
	}

	// Initialize SkipList (MemTable)
	memTable := skl.NewSkiplist(defaultArenaSize)            // 1MB
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001) // 1M keys, 0.01% false positives

	// Create storage engine
	engine := &Engine{
		wal:       w,
		memTable:  memTable,
		db:        db,
		flushSize: flushSize,
		bloom:     bloomFilter,
		namespace: namespace,
	}

	// Recover WAL logs on startup
	slog.Info("Recovering WAL...", "namespace", namespace)
	if err := engine.recoverWAL(); err != nil {
		slog.Error("WAL recovery failed", "err", err)
	}

	return engine, nil
}

// persistKeyValue writes a key-value pair to WAL and MemTable, ensuring durability.
// This function follows a multi-step process to persist the key-value pair:
// 1. Encodes and compresses the record.
// 2. Writes the record to the WAL (Write-Ahead Log).
// 3. If it's an insert operation (`OpInsert`):
//   - Stores small values directly in MemTable.
//   - Stores large values in WAL and keeps a reference in MemTable.
//   - Updates the Bloom filter for quick existence checks.
func (se *Engine) persistKeyValue(key []byte, value []byte, op LogOperation, batchID uuid.UUID) error {
	record := &walRecord{
		Operation: op,
		Key:       key,
		Value:     value,
		BatchID:   batchID,
	}

	// Encode and compress WAL record
	encoded := encodeWalRecord(record)
	compressed, err := compressLZ4(encoded)
	if err != nil {
		return err
	}

	// Write to WAL
	chunkPos, err := se.wal.Write(compressed)
	if err != nil {
		return err
	}

	// Store in MemTable
	if op == OpInsert || op == OpDelete {

		var memValue []byte
		if len(value) <= defaultValueThreshold {
			memValue = append([]byte{1}, compressed...) // Directly store small values
		} else {
			memValue = append([]byte{0}, chunkPos.Encode()...) // Store WAL reference
		}

		se.mu.Lock()
		se.memTable.Put(key, y.ValueStruct{
			Meta:  op,
			Value: memValue,
		})
		se.mu.Unlock()

		//3. put inside the bloom filter.
		se.bloom.Add(key)
	}

	return nil
}

// Put inserts a key-value pair into WAL and MemTable
func (se *Engine) Put(key, value []byte) error {
	return se.persistKeyValue(key, value, OpInsert, uuid.New())
}

// Delete removes a key and its value pair from WAL and MemTable
func (se *Engine) Delete(key []byte) error {
	return se.persistKeyValue(key, nil, OpDelete, uuid.New())
}

// Get retrieves a value from MemTable, WAL, or BoltDB.
func (se *Engine) Get(key []byte) ([]byte, error) {
	// fast negative check
	if !se.bloom.Test(key) {
		return nil, ErrKeyNotFound
	}

	// Retrieve entry from MemTable
	se.mu.RLock()
	it := se.memTable.Get(key)
	se.mu.RUnlock()

	if it.Meta == OpNoop {
		var result []byte
		err := se.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(se.namespace))
			if b == nil {
				return fmt.Errorf("bucket %s not found", se.namespace)
			}

			v := b.Get(key)
			if v == nil {
				return ErrKeyNotFound
			}

			// Correct way to copy the value
			result = append([]byte{}, v...) // Makes a copy of `v`
			return nil
		})
		if err != nil {
			return nil, err
		}

		return result, nil
	}

	// key deleted
	if it.Meta == OpDelete {
		return nil, ErrKeyNotFound
	}

	// Decode MemTable entry (ChunkPosition or Value)
	chunkPos, value, err := decodeChunkPositionWithValue(it.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode MemTable entry for key %s: %w", string(key), err)
	}

	// value exists directly in MemTable
	if chunkPos == nil {
		// Decompress data
		data, err := decompressLZ4(value)
		if err != nil {
			return nil, fmt.Errorf("decompression failed for key %s: %w", string(key), err)
		}

		// Decode WAL Record
		record, err := decodeWalRecord(data)
		if err != nil {
			return nil, fmt.Errorf("WAL decoding failed for key %s: %w", string(key), err)
		}
		return record.Value, nil
	}

	// Retrieve from WAL using ChunkPosition
	walValue, err := se.wal.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
	}

	data, err := decompressLZ4(walValue)
	if err != nil {
		return nil, fmt.Errorf("WAL decompression failed for key %s: %w", string(key), err)
	}

	record, err := decodeWalRecord(data)
	if err != nil {
		return nil, fmt.Errorf("WAL record decoding failed for key %s: %w", string(key), err)
	}

	return record.Value, nil
}

// decodeChunkPositionWithValue decodes a MemTable entry into either a ChunkPosition (WAL lookup) or a direct value.
func decodeChunkPositionWithValue(data []byte) (*wal.ChunkPosition, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrKeyNotFound
	}

	flag := data[0] // First byte determines type

	switch flag {
	case 1:
		// Direct value stored
		return nil, data[1:], nil
	case 0:
		// Stored ChunkPosition (WAL lookup required)
		chunkPos := wal.DecodeChunkPosition(data[1:])

		return chunkPos, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid MemTable entry flag: %d", flag)
	}
}
