package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofrs/flock"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

const (
	dbFileName  = "db.bolt"
	walDirName  = "wal"
	pidLockName = "pid.lock"
)

var (
	// ErrKeyNotFound is a sentinel error for missing keys.
	ErrKeyNotFound      = errors.New("key not found")
	ErrBucketNotFound   = errors.New("bucket not found")
	ErrInCloseProcess   = errors.New("in-close process")
	ErrDatabaseDirInUse = errors.New("pid.lock is held by another process")
	ErrRecordCorrupted  = errors.New("record corrupted")
	ErrInternalError    = errors.New("internal error")
)

// Engine manages WAL, MemTable (SkipList), and BoltDB for a given namespace.
type Engine struct {
	namespace             string
	wal                   *wal.WAL
	globalCounter         atomic.Uint64 // stores the global index.
	db                    *bbolt.DB
	queueChan             chan struct{}
	flushQueue            *flusherQueue
	storageConfig         StorageConfig
	fileLock              *flock.Flock
	recoveredEntriesCount int

	startMetadata Metadata

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
	walDir := filepath.Join(nsDir, walDirName)
	dbFile := filepath.Join(nsDir, dbFileName)

	if _, err := os.Stat(nsDir); err != nil {
		if err := os.MkdirAll(nsDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(nsDir, pidLockName))
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

	if err := createMetadataBucket(db, namespace); err != nil {
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

	mTable := newMemTable(config.ArenaSize)
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
		memTable:      mTable,
		bloom:         bloomFilter,
		Callback:      func() {},
		wg:            &sync.WaitGroup{},
	}

	// background task:
	engine.processFlushQueue(engine.wg, signal)
	if err := engine.loadMetaValues(); err != nil {
		return nil, err
	}

	if err := engine.recoverWAL(engine.startMetadata, namespace); err != nil {
		return nil, err
	}

	return engine, err
}

func createMetadataBucket(db *bbolt.DB, namespace string) error {
	// Create namespace bucket in BoltDB
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(namespace))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(walCheckPointBucket)
		return err
	})
}

// loadMetaValues loads meta value that the engine stores.
func (e *Engine) loadMetaValues() error {
	// Load Global Counter
	metadata, err := LoadMetadata(e.db)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	e.startMetadata = metadata
	// store the global counter
	e.globalCounter.Store(metadata.Index)

	// Recover WAL logs and bloom filter on startup
	err = e.loadBloomFilter()
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	return nil
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
func (e *Engine) persistKeyValue(key []byte, value []byte, op wrecord.LogOperation, batchID []byte, pos *wal.ChunkPosition) error {
	index := e.globalCounter.Add(1)

	record := walRecord{
		index:        index,
		key:          key,
		value:        value,
		op:           op,
		batchID:      batchID,
		lastBatchPos: pos,
	}

	encoded, err := record.fbEncode()

	if err != nil {
		return err
	}

	// Write to WAL
	chunkPos, err := e.wal.Write(encoded)
	if err != nil {
		return err
	}

	var memValue []byte

	switch op {
	case wrecord.LogOperationOpBatchCommit:
		memValue = append(directValuePrefix, encoded...)

	case wrecord.LogOperationOpInsert, wrecord.LogOperationOpDelete:
		if int64(len(value)) <= e.storageConfig.ValueThreshold {
			memValue = append(directValuePrefix, encoded...) // Store directly
		} else {
			memValue = append(walReferencePrefix, chunkPos.Encode()...) // Store reference
		}
	}

	if len(memValue) > 0 {
		err := e.memTableWrite(key, y.ValueStruct{
			Meta:  byte(op),
			Value: memValue,
		}, chunkPos, index)

		if err != nil {
			return err
		}
	}

	return nil
}

// RecoveredEntriesCount keeps track of the number of WAL entries successfully recovered.
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
func (e *Engine) memTableWrite(key []byte, v y.ValueStruct, chunkPos *wal.ChunkPosition, index uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var err error
	err = e.memTable.put(key, v, chunkPos, index)
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
			err = e.memTable.put(key, v, chunkPos, index)
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
	return e.persistKeyValue(key, value, wrecord.LogOperationOpInsert, nil, nil)
}

// Delete removes a key and its value pair from WAL and MemTable.
func (e *Engine) Delete(key []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	return e.persistKeyValue(key, nil, wrecord.LogOperationOpDelete, nil, nil)
}

// LastSeq return Latest Sequence Number of Index.
func (e *Engine) LastSeq() uint64 {
	return e.globalCounter.Load()
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
	if it.Meta == byte(wrecord.LogOperationOpNoop) {
		return e.getFromBoltDB(key)
	}

	// key deleted
	if it.Meta == byte(wrecord.LogOperationOpDelete) {
		return nil, ErrKeyNotFound
	}

	record, err := getWalRecord(it, e.wal)
	if err != nil {
		return nil, err
	}

	if record.Operation() == wrecord.LogOperationOpBatchCommit {
		return e.reconstructBatchValue(key, record)
	}

	decompressed, err := DecompressLZ4(record.ValueBytes())

	if err != nil {
		return nil, fmt.Errorf("failed to decompress value for key %s: %w", string(key), err)
	}

	if crc32.ChecksumIEEE(decompressed) != record.RecordChecksum() {
		return nil, ErrRecordCorrupted
	}

	return decompressed, nil
}

func (e *Engine) reconstructBatchValue(key []byte, record *wrecord.WalRecord) ([]byte, error) {
	lastPos := record.LastBatchPosBytes()
	if len(lastPos) == 0 {
		return nil, ErrRecordCorrupted
	}

	pos, err := decodeChunkPos(lastPos)
	if err != nil {
		return nil, fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
	}

	sum, err := DecompressLZ4(record.ValueBytes())
	if err != nil {
		return nil, fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
	}
	checksum := unmarshalChecksum(sum)

	data, err := readChunksFromWal(e.wal, pos, record.BatchIdBytes(), checksum)
	if err != nil {
		return nil, err
	}

	fullValue := new(bytes.Buffer)
	for _, d := range data {
		fullValue.Write(d)
	}
	return fullValue.Bytes(), nil
}

func decodeChunkPos(data []byte) (pos *wal.ChunkPosition, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode ChunkPosition: Panic recovered %v", r)
		}
	}()
	pos = wal.DecodeChunkPosition(data)

	return
}

// Fetch from BoltDB.
func (e *Engine) getFromBoltDB(key []byte) ([]byte, error) {
	return retrieveFromBoltDB(e.namespace, e.db, key)
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
