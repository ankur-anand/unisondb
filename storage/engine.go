package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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
	dbFileName  = "bTreeStore.bolt"
	walDirName  = "wal"
	pidLockName = "pid.lock"
)

var (
	// ErrKeyNotFound is a sentinel error for missing keys.
	ErrKeyNotFound      = errors.New("key not found")
	ErrBucketNotFound   = errors.New("bucket not found")
	ErrInCloseProcess   = errors.New("in-Close process")
	ErrDatabaseDirInUse = errors.New("pid.lock is held by another process")
	ErrRecordCorrupted  = errors.New("record corrupted")
	ErrInternalError    = errors.New("internal error")
)

// BtreeWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type BtreeWriter interface {
	// Set associates a value with a key.
	Set(key []byte, value []byte) error
	// SetMany associates multiple values with corresponding keys.
	SetMany(keys [][]byte, values [][]byte) error
	// SetChunks stores a value that has been split into chunks, associating them with a single key.
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	// Delete deletes a value with a key.
	Delete(key []byte) error
	// DeleteMany delete multiple values with corresponding keys.
	DeleteMany(keys [][]byte) error

	StoreMetadata(key []byte, value []byte) error
}

// BtreeReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type BtreeReader interface {
	// Get retrieves a value associated with a key.
	Get(key []byte) ([]byte, error)
	// SnapShot writes the complete database to the provided io writer.
	SnapShot(w io.Writer) error
	RetrieveMetadata(key []byte) ([]byte, error)
}

// BTreeStore combines the BtreeWriter and BtreeReader interfaces.
type BTreeStore interface {
	BtreeWriter
	BtreeReader
	Close() error
}

// Engine manages WAL, MemTable (SkipList), and BoltDB for a given namespace.
type Engine struct {
	namespace string

	bTreeStore  BTreeStore
	flushSignal chan struct{}

	storageConfig         StorageConfig
	fileLock              *flock.Flock
	recoveredEntriesCount int

	startMetadata Metadata

	// used in testing
	Callback func()
	// stores the global counter for Hybrid logical clock.
	// it's not the index of wal.
	globalCounter     atomic.Uint64
	opsFlushedCounter atomic.Uint64 // total ops flushed to BoltDB.

	shutdown        atomic.Bool
	wg              *sync.WaitGroup
	mu              sync.RWMutex
	wal             *wal.WAL
	bloom           *bloom.BloomFilter
	currentMemTable *memTable
	sealedMemTables []*memTable
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
	if err := tryFileLock(fileLock); err != nil {
		return nil, err
	}

	// Open WAL
	w, err := wal.Open(newWALOptions(walDir))
	if err != nil {
		return nil, err
	}

	config := sc
	if config == nil {
		config = DefaultConfig()
	}

	var bTreeStore BTreeStore
	switch config.DBEngine {
	case BoltDBEngine, "": // or even default.
		db, err := newBoltdb(dbFile, namespace)
		if err != nil {
			return nil, err
		}

		if err := createMetadataBucket(db.db, namespace); err != nil {
			return nil, err
		}
		bTreeStore = db
	default:
		return nil, fmt.Errorf("unsupported database engine %q", config.DBEngine)
	}

	// skip list itself needs few bytes for initialization
	// and, we don't want to keep on trashing writing boltdb often.
	if config.ArenaSize < 100*wal.KB {
		return nil, errors.New("arena capacity too small min capacity 2 KB")
	}
	mTable := newMemTable(config.ArenaSize, bTreeStore, w, namespace)
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001) // 1M keys, 0.01% false positives
	signal := make(chan struct{}, 2)

	// Create storage engine
	engine := &Engine{
		namespace:       namespace,
		wal:             w,
		bTreeStore:      bTreeStore,
		flushSignal:     signal,
		storageConfig:   *config,
		fileLock:        fileLock,
		currentMemTable: mTable,
		bloom:           bloomFilter,
		Callback:        func() {},
		wg:              &sync.WaitGroup{},
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

func tryFileLock(fileLock *flock.Flock) error {
	locked, err := fileLock.TryLock()
	if err != nil {
		return err
	}
	// unable to get the exclusive lock
	if !locked {
		return ErrDatabaseDirInUse
	}
	return nil
}

func createMetadataBucket(db *bbolt.DB, namespace string) error {
	// Create namespace bucket in BoltDB
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(namespace))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(sysBucketMetaData))
		return err
	})
}

// loadMetaValues loads meta value that the engine stores.
func (e *Engine) loadMetaValues() error {
	// Load Global Counter
	metadata, err := LoadMetadata(e.bTreeStore)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	e.startMetadata = metadata
	// store the global counter
	e.globalCounter.Store(metadata.RecordProcessed)
	e.opsFlushedCounter.Store(metadata.RecordProcessed)
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
	hlc := HLCNow(e.globalCounter.Add(1))
	record := walRecord{
		hlc:          hlc,
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
	e.mu.Lock()
	defer e.mu.Unlock()
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
		}, chunkPos)

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

// PersistenceSnapShot creates a snapshot of the engine's persistent data and writes it to the provided io.Writer.
func (e *Engine) PersistenceSnapShot(w io.Writer) error {
	return e.bTreeStore.SnapShot(w)
}

// MemTableSize returns the number of keys currently stored in the MemTable.
func (e *Engine) MemTableSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentMemTable.opsCount()
}

// memTableWrite will write the provided key and value to the memTable.
func (e *Engine) memTableWrite(key []byte, v y.ValueStruct, chunkPos *wal.ChunkPosition) error {
	var err error
	err = e.currentMemTable.put(key, v, chunkPos)
	if err != nil {
		if errors.Is(err, errArenaSizeWillExceed) {
			// put the old table in the queue
			oldTable := e.currentMemTable
			e.currentMemTable = newMemTable(e.storageConfig.ArenaSize, e.bTreeStore, e.wal, e.namespace)
			e.sealedMemTables = append(e.sealedMemTables, oldTable)
			select {
			case e.flushSignal <- struct{}{}:
			default:
				slog.Debug("queue signal channel full")
			}
			err = e.currentMemTable.put(key, v, chunkPos)
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

// TotalOpsReceived return total number of Put and Del operation received.
func (e *Engine) TotalOpsReceived() uint64 {
	return e.globalCounter.Load()
}

// TotalOpsFlushed return total number of Put and Del operation flushed to BoltDB.
func (e *Engine) TotalOpsFlushed() uint64 {
	return e.opsFlushedCounter.Load()
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
		it := e.currentMemTable.get(key)
		if it.Meta == byte(wrecord.LogOperationOpNoop) {
			for i := len(e.sealedMemTables) - 1; i >= 0; i-- {
				if val := e.sealedMemTables[i].get(key); val.Meta != byte(wrecord.LogOperationOpNoop) {
					return val, nil
				}
			}
		}
		return it, nil
	}

	it, err := checkFunc()
	if err != nil {
		return nil, err
	}

	// if the mem table doesn't have this key associated action or log.
	// directly go to the boltdb to fetch the same.
	if it.Meta == byte(wrecord.LogOperationOpNoop) {
		return e.bTreeStore.Get(key)
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

func (e *Engine) saveBloomFilter() error {
	// Serialize Bloom Filter
	var buf bytes.Buffer
	e.mu.RLock()
	_, err := e.bloom.WriteTo(&buf)
	e.mu.RUnlock()
	if err != nil {
		return err
	}

	return e.bTreeStore.StoreMetadata(sysKeyBloomFilter, buf.Bytes())
}

func (e *Engine) loadBloomFilter() error {
	result, err := e.bTreeStore.RetrieveMetadata(sysKeyBloomFilter)

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
	slog.Info("engine: closing", "namespace", e.namespace, "ops_received", e.globalCounter.Load(), "ops_flushed", e.opsFlushedCounter.Load())
	e.shutdown.Store(true)
	close(e.flushSignal)

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

	if err := e.bTreeStore.Close(); err != nil {
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
