package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/ankur-anand/unisondb/dbkernel/internal/recovery"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dustin/go-humanize"
	"github.com/gofrs/flock"
	"github.com/hashicorp/go-metrics"
)

const (
	minArenaSize = 1024 * 200
)

var (
	mKeyPendingFsyncTotal     = append(packageKey, "btree", "fsync", "pending", "total")
	mKeyFSyncTotal            = append(packageKey, "btree", "fsync", "total")
	mKeyFSyncErrorsTotal      = append(packageKey, "btree", "fsync", "errors", "total")
	mKeyFSyncDurations        = append(packageKey, "btree", "fsync", "durations", "seconds")
	mKeyMemTableRotationTotal = append(packageKey, "active", "mem", "table", "rotation", "total")

	mKeySealedMemTableTotal       = append(packageKey, "sealed", "mem", "table", "pending", "total")
	mKeySealedMemFlushTotal       = append(packageKey, "sealed", "mem", "table", "flush", "total")
	mKeySealedMemFlushDuration    = append(packageKey, "sealed", "mem", "table", "flush", "durations", "seconds")
	mKeySealedMemFlushRecordTotal = append(packageKey, "sealed", "mem", "table", "flush", "record", "total")
	mKeyWalRecoveryDuration       = append(packageKey, "wal", "recovery", "durations", "seconds")
	mKeyWalRecoveryRecordTotal    = append(packageKey, "wal", "recovery", "record", "total")
)

var (
	// ErrWaitTimeoutExceeded is a sentinel error to denotes sync.cond expired due to timeout.
	ErrWaitTimeoutExceeded = errors.New("wait timeout exceeded")
)

// Engine manages WAL, MemTable (SkipList), and BtreeStore for a given namespace.
type Engine struct {
	mu                sync.RWMutex
	writeSeenCounter  atomic.Uint64
	opsFlushedCounter atomic.Uint64
	currentOffset     atomic.Pointer[wal.Offset]
	namespace         string
	dataStore         internal.BTreeStore
	walIO             *wal.WalIO
	config            *EngineConfig
	metricsLabel      []metrics.Label
	fileLock          *flock.Flock
	wg                *sync.WaitGroup
	bloom             *bloom.BloomFilter
	activeMemTable    *memtable.MemTable
	sealedMemTables   []*memtable.MemTable
	flushReqSignal    chan struct{}
	pendingMetadata   *pendingMetadata
	fsyncReqSignal    chan struct{}

	recoveredEntriesCount int
	startMetadata         internal.Metadata
	shutdown              atomic.Bool

	// uses a cond broadcast for notification.
	notifierMu    sync.RWMutex
	notifier      *sync.Cond
	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher

	// used only during testing
	callback func()
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewStorageEngine initializes WAL, MemTable, and BtreeStore and returns an initialized Engine for a namespace.
func NewStorageEngine(dataDir, namespace string, conf *EngineConfig) (*Engine, error) {
	initMonotonic()
	label := []metrics.Label{{Name: "namespace", Value: namespace}}
	signal := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		namespace:       namespace,
		config:          conf,
		wg:              &sync.WaitGroup{},
		metricsLabel:    label,
		flushReqSignal:  signal,
		pendingMetadata: &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:             ctx,
		cancel:          cancel,
		callback:        func() {},
		fsyncReqSignal:  make(chan struct{}, 1),
	}

	if err := engine.initStorage(dataDir, namespace, conf); err != nil {
		return nil, err
	}

	// background task:
	engine.asyncMemTableFlusher(ctx)

	if err := engine.loadMetaValues(); err != nil {
		return nil, err
	}

	if err := engine.recoverWAL(); err != nil {
		return nil, err
	}

	engine.notifier = sync.NewCond(&engine.notifierMu)

	return engine, nil
}

func (e *Engine) initStorage(dataDir, namespace string, conf *EngineConfig) error {
	// Define paths for WAL & BoltDB
	nsDir := filepath.Join(dataDir, namespace)
	walDir := filepath.Join(nsDir, walDirName)
	dbFile := filepath.Join(nsDir, dbFileName)

	if _, err := os.Stat(nsDir); err != nil {
		if err := os.MkdirAll(nsDir, os.ModePerm); err != nil {
			return err
		}
	}

	if _, err := os.Stat(walDir); err != nil {
		if err := os.MkdirAll(walDir, os.ModePerm); err != nil {
			return err
		}
	}

	fileLock := flock.New(filepath.Join(nsDir, pidLockName))
	if err := tryFileLock(fileLock); err != nil {
		return err
	}
	e.fileLock = fileLock

	walIO, err := wal.NewWalIO(walDir, namespace, &conf.WalConfig, metrics.Default())
	if err != nil {
		return err
	}
	e.walIO = walIO

	var bTreeStore internal.BTreeStore
	switch conf.DBEngine {
	case BoltDBEngine:
		db, err := kvdrivers.NewBoltdb(dbFile, conf.BtreeConfig)
		if err != nil {
			return err
		}
		bTreeStore = db
		e.newTxnBatcher = func(maxBatchSize int) internal.TxnBatcher {
			return db.NewTxnQueue(maxBatchSize)
		}
	case LMDBEngine:
		db, err := kvdrivers.NewLmdb(dbFile, conf.BtreeConfig)
		if err != nil {
			return err
		}
		bTreeStore = db
		e.newTxnBatcher = func(maxBatchSize int) internal.TxnBatcher {
			return db.NewTxnQueue(maxBatchSize)
		}
	default:
		return fmt.Errorf("unsupported database engine %s", conf.DBEngine)
	}
	e.dataStore = bTreeStore

	// skip list itself needs few bytes for initialization
	// and, we don't want to keep on trashing writing to btreeStore often.
	if conf.ArenaSize < minArenaSize {
		return errors.New("arena capacity too small min capacity 2 KB")
	}

	mTable := memtable.NewMemTable(conf.ArenaSize, walIO, namespace, e.newTxnBatcher)
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001) // 1M keys, 0.01% false positives
	e.bloom = bloomFilter
	e.activeMemTable = mTable
	return nil
}

func tryFileLock(fileLock *flock.Flock) error {
	locked, err := fileLock.TryLock()
	if err != nil {
		return err
	}
	// unable to get the exclusive lock4
	if !locked {
		return ErrDatabaseDirInUse
	}
	return nil
}

// loadMetaValues loads meta value that the engine stores.
func (e *Engine) loadMetaValues() error {
	data, err := e.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return err
	}
	// there is no value even for bloom filter.
	if errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return nil
	}
	metadata := internal.UnmarshalMetadata(data)
	e.startMetadata = metadata

	// dataStore the global counter
	e.writeSeenCounter.Store(metadata.RecordProcessed)
	e.opsFlushedCounter.Store(metadata.RecordProcessed)

	// Recover WAL logs and bloom filter on startup
	err = e.loadBloomFilter()
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	return nil
}

func (e *Engine) loadBloomFilter() error {
	result, err := e.dataStore.RetrieveMetadata(internal.SysKeyBloomFilter)

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

// recoverWAL recovers the wal if any pending writes are still not visible.
func (e *Engine) recoverWAL() error {
	checkpoint, err := e.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return fmt.Errorf("recover WAL failed %w", err)
	}

	walRecovery := recovery.NewWalRecovery(e.dataStore, e.walIO, e.bloom)
	startTime := time.Now()
	if err := walRecovery.Recover(checkpoint); err != nil {
		return err
	}

	slog.Info("[unisondb.dbkernal] wal recovered",
		"recovered_count", walRecovery.RecoveredCount(),
		"namespace", e.namespace,
		"btree_engine", e.config.DBEngine,
		"durations", humanizeDuration(time.Since(startTime)),
	)
	metrics.IncrCounterWithLabels(mKeyWalRecoveryRecordTotal, float32(walRecovery.RecoveredCount()), e.metricsLabel)
	metrics.MeasureSinceWithLabels(mKeyWalRecoveryDuration, startTime, e.metricsLabel)

	e.writeSeenCounter.Add(uint64(walRecovery.RecoveredCount()))
	e.recoveredEntriesCount = walRecovery.RecoveredCount()
	e.opsFlushedCounter.Add(uint64(walRecovery.RecoveredCount()))
	e.currentOffset.Store(walRecovery.LastRecoveredOffset())

	if walRecovery.RecoveredCount() > 0 {
		// once recovered update the metadata table again.
		e.pendingMetadata.queueMetadata(&flushedMetadata{
			metadata: &internal.Metadata{
				RecordProcessed: uint64(walRecovery.RecoveredCount()),
				Pos:             walRecovery.LastRecoveredOffset(),
			},
			recordProcessed: walRecovery.RecoveredCount(),
		})

		select {
		case e.fsyncReqSignal <- struct{}{}:
		default:
			// this path should not happen in normal code base.
			slog.Error("[unisondb.dbkernal] fsync req signal channel is full while recovering itself")
			panic("[unisondb.dbkernal] fsync req signal channel is full while recovering itself")
		}
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
func (e *Engine) persistKeyValue(keys [][]byte, values [][]byte, op logrecord.LogOperationType) error {

	kvEntries := make([][]byte, 0, len(keys))

	hintSize := 512
	checksum := uint32(0)
	for i, key := range keys {
		var kv []byte
		switch op {
		case logrecord.LogOperationTypeDelete:
			kv = logcodec.SerializeKVEntry(key, nil)
		default:
			kv = logcodec.SerializeKVEntry(key, values[i])
		}

		hintSize += len(kv)
		crc32.Update(checksum, crc32.IEEETable, kv)
		kvEntries = append(kvEntries, kv)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	index := e.writeSeenCounter.Add(1)
	hlc := HLCNow(index)

	record := logcodec.LogRecord{
		LSN:           index,
		HLC:           hlc,
		OperationType: op,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       kvEntries,
	}

	encoded := record.FBEncode(hintSize)

	// Write to WAL
	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return err
	}

	// store the entire value as single

	for i, entry := range kvEntries {
		memValue := getValueStruct(byte(op), internal.EntryTypeKV, entry)

		err = e.memTableWrite(keys[i], memValue)
		if err != nil {
			return err
		}
	}

	e.writeOffset(offset)
	return nil
}

// persistRowColumnAction writes the columnEntries for the given rowKey in the wal and mem-table.
func (e *Engine) persistRowColumnAction(op logrecord.LogOperationType, rowKeys [][]byte, columnsEntries []map[string][]byte) error {
	rowEntries := make([][]byte, 0, len(rowKeys))

	hintSize := 512
	checksum := uint32(0)
	for i, entry := range rowKeys {
		var re []byte
		switch op {
		case logrecord.LogOperationTypeDeleteRowByKey:
			re = logcodec.SerializeRowUpdateEntry(entry, nil)
		default:
			re = logcodec.SerializeRowUpdateEntry(entry, columnsEntries[i])
		}

		hintSize += len(re)
		crc32.Update(checksum, crc32.IEEETable, re)
		rowEntries = append(rowEntries, re)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	index := e.writeSeenCounter.Add(1)
	hlc := HLCNow(index)

	record := logcodec.LogRecord{
		LSN:           index,
		HLC:           hlc,
		OperationType: op,
		CRC32Checksum: checksum,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeRow,
		Entries:       rowEntries,
	}

	encoded := record.FBEncode(hintSize)

	// Write to WAL
	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return err
	}

	for i, entry := range rowEntries {
		memValue := getValueStruct(byte(op), internal.EntryTypeRow, entry)
		err = e.memTableWrite(rowKeys[i], memValue)
		if err != nil {
			return err
		}
	}

	e.writeOffset(offset)
	return nil
}

func (e *Engine) writeOffset(offset *wal.Offset) {
	if offset != nil {
		e.activeMemTable.SetOffset(offset)
		// Signal all waiting routines that a new append has happened
		// Atomically update lastChunkPosition
		e.currentOffset.Store(offset)
		e.notifierMu.Lock()
		e.notifier.Broadcast()
		e.notifierMu.Unlock()
	}
}

// memTableWrite will write the provided key and value to the memTable.
func (e *Engine) memTableWrite(key []byte, v y.ValueStruct) error {

	var err error
	err = e.activeMemTable.Put(key, v)
	if err != nil {
		if errors.Is(err, memtable.ErrArenaSizeWillExceed) {
			metrics.IncrCounterWithLabels(mKeyMemTableRotationTotal, 1, e.metricsLabel)
			e.rotateMemTable()
			err = e.activeMemTable.Put(key, v)
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

// used for testing purposes.
func (e *Engine) rotateMemTableNoFlush() {
	// put the old table in the queue
	oldTable := e.activeMemTable
	e.activeMemTable = memtable.NewMemTable(e.config.ArenaSize, e.walIO, e.namespace, e.newTxnBatcher)
	e.sealedMemTables = append(e.sealedMemTables, oldTable)
	e.callback()
}

func (e *Engine) rotateMemTable() {
	// put the old table in the queue
	oldTable := e.activeMemTable
	e.activeMemTable = memtable.NewMemTable(e.config.ArenaSize, e.walIO, e.namespace, e.newTxnBatcher)
	e.sealedMemTables = append(e.sealedMemTables, oldTable)
	select {
	case e.flushReqSignal <- struct{}{}:
	default:
		slog.Debug("queue signal channel full")
	}
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

	return e.dataStore.StoreMetadata(internal.SysKeyBloomFilter, buf.Bytes())
}

// asyncMemTableFlusher flushes the sealed mem table.
func (e *Engine) asyncMemTableFlusher(ctx context.Context) {
	e.wg.Add(2)
	// check if there is item in queue that needs to be flushed
	// based upon timer, as the process of input of the WAL write could
	// be higher, then what bTreeStore could keep up the pace.
	tick := time.NewTicker(10 * time.Second)

	go func() {
		defer e.wg.Done()
		e.asyncFSync()
	}()

	go func() {
		defer tick.Stop()
		defer e.wg.Done()
		for {
			select {
			case <-e.flushReqSignal:
				e.handleFlush(ctx)
			case <-tick.C:
				e.handleFlush(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// handleFlush flushes the sealed mem-table to btree store.
func (e *Engine) handleFlush(ctx context.Context) {
	var mt *memtable.MemTable
	e.mu.Lock()
	metrics.SetGaugeWithLabels(mKeySealedMemTableTotal, float32(len(e.sealedMemTables)), e.metricsLabel)
	if len(e.sealedMemTables) > 0 {
		mt = e.sealedMemTables[0]
	}
	e.mu.Unlock()
	if mt != nil && !mt.IsEmpty() {
		startTime := time.Now()
		recordProcessed, err := mt.Flush(ctx)
		if err != nil {
			log.Fatal("Failed to flushMemTable MemTable:", "namespace", e.namespace, "err", err)
		}

		fm := &flushedMetadata{
			metadata: &internal.Metadata{
				RecordProcessed: e.opsFlushedCounter.Add(uint64(recordProcessed)),
				Pos:             mt.GetLastOffset(),
			},
			recordProcessed: recordProcessed,
			bytesFlushed:    uint64(mt.GetBytesStored()),
		}
		e.pendingMetadata.queueMetadata(fm)
		e.mu.Lock()
		// Remove it from the list
		e.sealedMemTables = e.sealedMemTables[1:]
		e.mu.Unlock()

		metrics.MeasureSinceWithLabels(mKeySealedMemFlushDuration, startTime, e.metricsLabel)
		metrics.IncrCounterWithLabels(mKeySealedMemFlushRecordTotal, float32(recordProcessed), e.metricsLabel)
		metrics.IncrCounterWithLabels(mKeySealedMemFlushTotal, 1, e.metricsLabel)

		select {
		case e.fsyncReqSignal <- struct{}{}:
		default:
			slog.Info("fsync queue signal channel full")
		}

		slog.Info("[kvalchemy.dbengine] Flushed MemTable",
			"ops_flushed", recordProcessed, "namespace", e.namespace,
			"duration", humanizeDuration(time.Since(startTime)), "bytes_flushed", humanize.Bytes(uint64(mt.GetBytesStored())))
	}
}

type flushedMetadata struct {
	metadata        *internal.Metadata
	recordProcessed int
	bytesFlushed    uint64
}

type pendingMetadata struct {
	mu                    sync.Mutex
	pendingMetadataWrites []*flushedMetadata
}

func (p *pendingMetadata) queueMetadata(metadata *flushedMetadata) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pendingMetadataWrites = append(p.pendingMetadataWrites, metadata)
}

func (p *pendingMetadata) dequeueMetadata() (*flushedMetadata, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingMetadataWrites) == 0 {
		return nil, 0
	}
	m := p.pendingMetadataWrites[0]
	p.pendingMetadataWrites = p.pendingMetadataWrites[1:]
	return m, len(p.pendingMetadataWrites)
}

// asyncFSync call the fsync of btree store once the mem table flushing completes.
// it's made async so it doesn't block the main mem table and too many mem table doesn't
// accumulate over the time in the memory, even if large values are stored inside the mem table,
// configured via value threshold.
func (e *Engine) asyncFSync() {
	slog.Debug("[kvalchemy.dbengine]: FSync Metadata eventloop", "namespace", e.namespace)
	for {
		select {
		case <-e.fsyncReqSignal:
			fm, n := e.pendingMetadata.dequeueMetadata()
			metrics.IncrCounterWithLabels(mKeyPendingFsyncTotal, float32(n), e.metricsLabel)
			startTime := time.Now()
			metrics.IncrCounterWithLabels(mKeyFSyncTotal, 1, e.metricsLabel)
			fmt.Println("saving metadata", fm.metadata.Pos, fm.metadata.RecordProcessed)
			err := internal.SaveMetadata(e.dataStore, fm.metadata.Pos, fm.metadata.RecordProcessed)
			if err != nil {
				log.Fatal("[kvalchemy.dbengine] Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
			}
			err = e.saveBloomFilter()
			if err != nil {
				log.Fatal("[kvalchemy.dbengine] Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
			}
			err = e.dataStore.FSync()
			if err != nil {
				metrics.IncrCounterWithLabels(mKeyFSyncErrorsTotal, 1, e.metricsLabel)
				// There is no way to recover from the underlying Fsync Issue.
				// https://archive.fosdem.org/2019/schedule/event/postgresql_fsync/
				// How is it possible that PostgreSQL used fsync incorrectly for 20 years.
				log.Fatalln(fmt.Errorf("[kvalchemy.dbengine]: FSync operation failed: %w", err))
			}
			metrics.MeasureSinceWithLabels(mKeyFSyncDurations, startTime, e.metricsLabel)

			slog.Debug("[kvalchemy.dbengine]: Flushed mem table and created WAL checkpoint",
				"ops_flushed", fm.recordProcessed, "namespace", e.namespace,
				"duration", humanizeDuration(time.Since(startTime)), "bytes_flushed", humanize.Bytes(fm.bytesFlushed))
			if e.callback != nil {
				e.callback()
			}
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) close(ctx context.Context) error {
	e.shutdown.Store(true)
	// cancel the context:
	e.cancel()

	var errs strings.Builder

	// wait for background routine to close.
	if !waitWithCancel(e.wg, ctx) {
		errs.WriteString("[kvalchemy.dbengine]: WAL check operation timed out")
		errs.WriteString("|")
		slog.Error("Timeout reached! Some goroutines are still running")
	}

	close(e.fsyncReqSignal)

	err := e.walIO.Sync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: wal Fsync error", "error", err)
	}

	err = e.walIO.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: wal close error", "error", err)
	}

	fmt.Println("flushing fsync")
	err = e.dataStore.FSync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: Btree Fsync error", "error", err)
	}

	fmt.Println("closing datastore")
	err = e.dataStore.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: Btree close error", "error", err)
	}

	// release the lock file.
	fmt.Println("release the lock file.")
	if err := e.fileLock.Unlock(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
	}

	if errs.Len() > 0 {
		return errors.New(errs.String())
	}
	return nil
}

func waitWithCancel(wg *sync.WaitGroup, ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
