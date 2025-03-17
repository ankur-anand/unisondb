package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/wal"
	"github.com/ankur-anand/unisondb/dbkernel/wal/walrecord"
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
	dataStore         BTreeStore
	walIO             *wal.WalIO
	config            *EngineConfig
	metricsLabel      []metrics.Label
	fileLock          *flock.Flock
	wg                *sync.WaitGroup
	bloom             *bloom.BloomFilter
	activeMemTable    *memTable
	sealedMemTables   []*memTable
	flushReqSignal    chan struct{}
	pendingMetadata   *pendingMetadata
	fsyncReqSignal    chan struct{}

	recoveredEntriesCount int
	startMetadata         Metadata
	shutdown              atomic.Bool

	// uses a cond broadcast for notification.
	notifierMu sync.RWMutex
	notifier   *sync.Cond

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

	var bTreeStore BTreeStore
	switch conf.DBEngine {
	case BoltDBEngine:
		db, err := kvdrivers.NewBoltdb(dbFile, conf.BtreeConfig)
		if err != nil {
			return err
		}
		bTreeStore = db
	case LMDBEngine:
		db, err := kvdrivers.NewLmdb(dbFile, conf.BtreeConfig)
		if err != nil {
			return err
		}
		bTreeStore = db
	default:
		return fmt.Errorf("unsupported database engine %s", conf.DBEngine)
	}
	e.dataStore = bTreeStore

	// skip list itself needs few bytes for initialization
	// and, we don't want to keep on trashing writing to btreeStore often.
	if conf.ArenaSize < minArenaSize {
		return errors.New("arena capacity too small min capacity 2 KB")
	}

	mTable := newMemTable(conf.ArenaSize, bTreeStore, walIO, namespace)
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
	data, err := e.dataStore.RetrieveMetadata(sysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return err
	}
	// there is no value even for bloom filter.
	if errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return nil
	}
	metadata := UnmarshalMetadata(data)
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
	result, err := e.dataStore.RetrieveMetadata(sysKeyBloomFilter)

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
	recovery := &walRecovery{
		store: e.dataStore,
		walIO: e.walIO,
		bloom: e.bloom,
	}

	startTime := time.Now()
	if err := recovery.recoverWAL(); err != nil {
		return err
	}

	slog.Info("[kvalchemy.dbengine] wal recovered",
		"recovered_count", recovery.recoveredCount,
		"namespace", e.namespace,
		"btree_engine", e.config.DBEngine,
		"durations", humanizeDuration(time.Since(startTime)),
	)
	metrics.IncrCounterWithLabels(mKeyWalRecoveryRecordTotal, float32(recovery.recoveredCount), e.metricsLabel)
	metrics.MeasureSinceWithLabels(mKeyWalRecoveryDuration, startTime, e.metricsLabel)

	e.writeSeenCounter.Add(uint64(recovery.recoveredCount))
	e.recoveredEntriesCount = recovery.recoveredCount
	e.opsFlushedCounter.Add(uint64(recovery.recoveredCount))
	e.currentOffset.Store(recovery.lastRecoveredPos)

	if recovery.recoveredCount > 0 {
		// once recovered update the metadata table again.
		e.pendingMetadata.queueMetadata(&flushedMetadata{
			metadata: &Metadata{
				RecordProcessed: uint64(recovery.recoveredCount),
				Pos:             recovery.lastRecoveredPos,
			},
			recordProcessed: recovery.recoveredCount,
		})

		select {
		case e.fsyncReqSignal <- struct{}{}:
		default:
			// this path should not happen in normal code base.
			slog.Error("[kvalchemy.dbengine] fsync req signal channel is full while recovering itself")
			panic("[kvalchemy.dbengine] fsync req signal channel is full while recovering itself")
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
func (e *Engine) persistKeyValue(key []byte, value []byte, op walrecord.LogOperation) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	index := e.writeSeenCounter.Add(1)
	hlc := HLCNow(index)
	record := walrecord.Record{
		Index:        index,
		Hlc:          hlc,
		Key:          key,
		Value:        value,
		LogOperation: op,
		TxnStatus:    walrecord.TxnStatusTxnNone,
		EntryType:    walrecord.EntryTypeKV,
	}

	encoded, err := record.FBEncode()

	if err != nil {
		return err
	}

	// Write to WAL
	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return err
	}

	var memValue y.ValueStruct
	if int64(len(value)) <= e.config.ValueThreshold {
		memValue = getValueStruct(byte(op), true, encoded)
	} else {
		memValue = getValueStruct(byte(op), false, offset.Encode())
	}

	err = e.memTableWrite(key, memValue, offset)

	if err != nil {
		return err
	}
	return nil
}

// persistRowColumnAction writes the columnEntries for the given rowKey in the wal and mem-table.
func (e *Engine) persistRowColumnAction(op walrecord.LogOperation, rowKey []byte, columnEntries map[string][]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	index := e.writeSeenCounter.Add(1)
	hlc := HLCNow(index)
	valueSize := 0
	for k, v := range columnEntries {
		valueSize += len(k) + len(v)
	}

	record := walrecord.Record{
		Index:         index,
		Hlc:           hlc,
		Key:           rowKey,
		Value:         nil,
		LogOperation:  op,
		TxnStatus:     walrecord.TxnStatusTxnNone,
		EntryType:     walrecord.EntryTypeRow,
		ColumnEntries: columnEntries,
	}

	encoded, err := record.FBEncode()

	if err != nil {
		return err
	}

	// Write to WAL
	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return err
	}

	var memValue y.ValueStruct
	if int64(valueSize) <= e.config.ValueThreshold {
		memValue = getValueStruct(byte(op), true, encoded)
	} else {
		memValue = getValueStruct(byte(op), false, offset.Encode())
	}

	memValue.UserMeta = entryTypeRow
	err = e.memTableWrite(rowKey, memValue, offset)

	if err != nil {
		return err
	}
	return nil
}

// memTableWrite will write the provided key and value to the memTable.
func (e *Engine) memTableWrite(key []byte, v y.ValueStruct, offset *wal.Offset) error {
	defer func() {
		// Signal all waiting routines that a new append has happened
		// Atomically update lastChunkPosition
		e.currentOffset.Store(offset)
		e.notifierMu.Lock()
		e.notifier.Broadcast()
		e.notifierMu.Unlock()
	}()
	var err error
	err = e.activeMemTable.put(key, v, offset)
	if err != nil {
		if errors.Is(err, errArenaSizeWillExceed) {
			metrics.IncrCounterWithLabels(mKeyMemTableRotationTotal, 1, e.metricsLabel)
			e.rotateMemTable()
			err = e.activeMemTable.put(key, v, offset)
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
	e.activeMemTable = newMemTable(e.config.ArenaSize, e.dataStore, e.walIO, e.namespace)
	e.sealedMemTables = append(e.sealedMemTables, oldTable)
	e.callback()
}

func (e *Engine) rotateMemTable() {
	// put the old table in the queue
	oldTable := e.activeMemTable
	e.activeMemTable = newMemTable(e.config.ArenaSize, e.dataStore, e.walIO, e.namespace)
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

	return e.dataStore.StoreMetadata(sysKeyBloomFilter, buf.Bytes())
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
	var mt *memTable
	e.mu.Lock()
	metrics.SetGaugeWithLabels(mKeySealedMemTableTotal, float32(len(e.sealedMemTables)), e.metricsLabel)
	if len(e.sealedMemTables) > 0 {
		mt = e.sealedMemTables[0]
	}
	e.mu.Unlock()
	if mt != nil && !mt.skipList.Empty() {
		startTime := time.Now()
		recordProcessed, err := mt.flush(ctx)
		if err != nil {
			log.Fatal("Failed to flushMemTable MemTable:", "namespace", e.namespace, "err", err)
		}

		fm := &flushedMetadata{
			metadata: &Metadata{
				RecordProcessed: e.opsFlushedCounter.Add(uint64(recordProcessed)),
				Pos:             mt.lastOffset,
			},
			recordProcessed: recordProcessed,
			bytesFlushed:    uint64(mt.bytesStored),
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
			slog.Debug("fsync queue signal channel full")
		}

		slog.Debug("[kvalchemy.dbengine] Flushed MemTable",
			"ops_flushed", recordProcessed, "namespace", e.namespace,
			"duration", humanizeDuration(time.Since(startTime)), "bytes_flushed", humanize.Bytes(uint64(mt.bytesStored)))
	}
}

type flushedMetadata struct {
	metadata        *Metadata
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
			err := SaveMetadata(e.dataStore, fm.metadata.Pos, fm.metadata.RecordProcessed)
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
	if !waitWithTimeout(e.wg, 10*time.Second) {
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

	err = e.dataStore.FSync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: Btree Fsync error", "error", err)
	}

	err = e.dataStore.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[kvalchemy.dbengine]: Btree close error", "error", err)
	}

	// release the lock file.
	if err := e.fileLock.Unlock(); err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
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
