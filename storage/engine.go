package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

var (
	// ErrKeyNotFound is a sentinel error for missing keys.
	ErrKeyNotFound      = errors.New("key not found")
	ErrInCloseProcess   = errors.New("in-close process")
	ErrDatabaseDirInUse = errors.New("pid.lock is held by another process")
)

// Engine manages WAL, MemTable (SkipList), and BoltDB for a given namespace.
type Engine struct {
	namespace             string
	wal                   *wal.WAL
	db                    *bbolt.DB
	queueChan             chan struct{}
	flushQueue            *flusherQueue
	storageConfig         StorageConfig
	fileLock              *flock.Flock
	recoveredEntriesCount int

	// used in testing
	Callback func()

	shutdown atomic.Bool
	wg       *sync.WaitGroup
	// protect the mem table and bloom filter
	mu       sync.RWMutex
	bloom    *bloom.BloomFilter
	memTable *memTable
}

// NewStorageEngine initializes WAL, MemTable, and BoltDB.
func NewStorageEngine(baseDir, namespace string, sc *StorageConfig) (*Engine, error) {
	// Define paths for WAL & BoltDB
	nsDir := filepath.Join(baseDir, namespace)
	walDir := filepath.Join(nsDir, "wal")
	dbFile := filepath.Join(nsDir, "db.bolt")

	if _, err := os.Stat(nsDir); err != nil {
		if err := os.MkdirAll(nsDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(nsDir, "pid.lock"))
	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	// unable to get the exclusive lock
	if !locked {
		return nil, ErrDatabaseDirInUse
	}

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
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(walCheckPointBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	config := sc
	if config == nil {
		config = DefaultConfig()
	}

	// skiplist itself needs few bytes for initialization
	// and we don't want to keep on trashing writing boltdb often.
	if config.ArenaSize < 100*wal.KB {
		return nil, errors.New("arena size too small min size 2 KB")
	}

	memTable := newMemTable(config.ArenaSize)
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001) // 1M keys, 0.01% false positives
	signal := make(chan struct{}, 2)
	// Create storage engine
	engine := &Engine{
		namespace:     namespace,
		wal:           w,
		db:            db,
		flushQueue:    newFlusherQueue(signal),
		queueChan:     signal,
		storageConfig: *config,
		fileLock:      fileLock,
		memTable:      memTable,
		bloom:         bloomFilter,
		Callback:      func() {},
		wg:            &sync.WaitGroup{},
	}

	// background task:
	engine.processFlushQueue(engine.wg, signal)
	// Recover WAL logs and bloom filter on startup
	err = engine.loadBloomFilter()
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	slog.Info("Recovering WAL...", "namespace", namespace)
	count, err := engine.recoverWAL()
	if err != nil {
		slog.Error("WAL recovery failed", "err", err)
	}
	engine.recoveredEntriesCount = count

	return engine, err
}

// persistKeyValue writes a key-value pair to WAL and MemTable, ensuring durability.
// multistep process to persist the key-value pair:
// 1. Encodes the record.
// 2. Compresses the record.
// 3. Writes the record to the WAL (Write-Ahead Log).
// 4. Write the record to the SKIP LIST as well.
// ->	4.a If it's an insert operation (`OpInsert`):
//   - Stores small values directly in MemTable.
//   - Stores large values in WAL and keeps a reference in MemTable.
//   - Updates the Bloom filter for quick existence checks.
//
// 5. Store the current Chunk Position in the variable.
func (e *Engine) persistKeyValue(key []byte, value []byte, op LogOperation, batchID uuid.UUID) error {
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
	chunkPos, err := e.wal.Write(compressed)
	if err != nil {
		return err
	}

	// Store in MemTable
	if op == OpInsert || op == OpDelete {
		var memValue []byte
		if int64(len(value)) <= e.storageConfig.ValueThreshold {
			memValue = append([]byte{1}, compressed...) // Directly store small values
		} else {
			memValue = append([]byte{0}, chunkPos.Encode()...) // Store WAL reference
		}

		err := e.memTableWrite(key, y.ValueStruct{
			Meta:  op,
			Value: memValue,
		}, chunkPos)

		if err != nil {
			return err
		}
	}

	return nil
}

// RecoveredEntriesCount keeps track of the number of WAL entries successfully recovered
func (e *Engine) RecoveredEntriesCount() int {
	return e.recoveredEntriesCount
}

// MemTableSize returns the number of keys currently stored in the MemTable.
func (e *Engine) MemTableSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.memTable.keysCount()
}

// memTableWrite will write the provided key and value to the memTable.
func (e *Engine) memTableWrite(key []byte, v y.ValueStruct, chunkPos *wal.ChunkPosition) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var err error
	err = e.memTable.put(key, v, chunkPos)
	if err != nil {
		if errors.Is(err, errArenaSizeWillExceed) {
			// put the old table in the queue
			oldTable := e.memTable
			e.memTable = newMemTable(e.storageConfig.ArenaSize)
			e.flushQueue.enqueue(*oldTable)
			select {
			case e.queueChan <- struct{}{}:
			default:
				slog.Warn("queue signal channel full")
			}
			err = e.memTable.put(key, v, chunkPos)
			// :(
			if err != nil {
				return err
			}
		}
	}

	// bloom filter also need to be protected for concurrent ops.
	if err == nil {
		//put inside the bloom filter.
		e.bloom.Add(key)
	}

	return err
}

// Put inserts a key-value pair into WAL and MemTable.
func (e *Engine) Put(key, value []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	return e.persistKeyValue(key, value, OpInsert, uuid.New())
}

// Delete removes a key and its value pair from WAL and MemTable.
func (e *Engine) Delete(key []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	return e.persistKeyValue(key, nil, OpDelete, uuid.New())
}

// Get retrieves a value from MemTable, WAL, or BoltDB.
// 1. check bloom filter for key presence.
// 2. check recent mem-table
// 3. Check BoltDB.
func (e *Engine) Get(key []byte) ([]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}

	checkFunc := func() (y.ValueStruct, error) {
		// Retrieve entry from MemTable
		e.mu.RLock()
		defer e.mu.RUnlock()
		// fast negative check
		if !e.bloom.Test(key) {
			return y.ValueStruct{}, ErrKeyNotFound
		}
		it := e.memTable.get(key)
		return it, nil
	}

	it, err := checkFunc()
	if err != nil {
		return nil, err
	}

	// if the mem table doesn't have this key associated action or log.
	// directly go to the boltdb to fetch the same.
	if it.Meta == OpNoop {
		var result []byte
		err := e.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(e.namespace))
			if b == nil {
				return fmt.Errorf("bucket %s not found", e.namespace)
			}

			v := b.Get(key)
			if v == nil {
				return ErrKeyNotFound
			}

			// copy the value
			result = append([]byte{}, v...)
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
		record := decodeWalRecord(data)
		return record.Value, nil
	}

	// Retrieve from WAL using ChunkPosition
	walValue, err := e.wal.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
	}

	data, err := decompressLZ4(walValue)
	if err != nil {
		return nil, fmt.Errorf("WAL decompression failed for key %s: %w", string(key), err)
	}

	record := decodeWalRecord(data)

	return record.Value, nil
}

// Close all the associated resource.
func (e *Engine) Close() error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	e.shutdown.Store(true)
	close(e.queueChan)

	var errs strings.Builder

	_, err := e.wal.WriteAll()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	if err := e.wal.Sync(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	if err := e.wal.Close(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	if err := e.db.Close(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	// release the lock file.
	if err := e.fileLock.Unlock(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	if !waitWithTimeout(e.wg, 5*time.Second) {
		slog.Error("Timeout reached! Some goroutines are still running")
	}
	if errs.Len() > 0 {
		return errors.New(errs.String())
	}
	return nil
}

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
