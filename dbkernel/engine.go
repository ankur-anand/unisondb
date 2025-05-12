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
	walSyncer         walSyncer
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

	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher
	flushPaused   atomic.Bool

	btreeFlushInterval        time.Duration
	btreeFlushIntervalEnabled bool

	// used only during testing
	callback func()
	ctx      context.Context
	cancel   context.CancelFunc

	// used for notification.
	coalesceFlag     atomic.Bool
	coalesceDuration time.Duration
	notifierMu       sync.RWMutex
	appendNotify     chan struct{}
}

// NewStorageEngine initializes WAL, MemTable, and BtreeStore and returns an initialized Engine for a namespace.
func NewStorageEngine(dataDir, namespace string, conf *EngineConfig) (*Engine, error) {
	initMonotonic()
	label := []metrics.Label{{Name: "namespace", Value: namespace}}
	signal := make(chan struct{}, 2)
	btreeFlushInterval, btreeFlushIntervalEnabled := conf.effectiveBTreeFlushInterval()
	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		namespace:                 namespace,
		config:                    conf,
		wg:                        &sync.WaitGroup{},
		metricsLabel:              label,
		flushReqSignal:            signal,
		pendingMetadata:           &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:                       ctx,
		cancel:                    cancel,
		callback:                  func() {},
		fsyncReqSignal:            make(chan struct{}, 1),
		btreeFlushInterval:        btreeFlushInterval,
		btreeFlushIntervalEnabled: btreeFlushIntervalEnabled,
	}

	if err := engine.initStorage(dataDir, namespace, conf); err != nil {
		return nil, err
	}

	// background task:
	engine.asyncMemTableFlusher(ctx)
	engine.syncWalAtInterval(ctx)
	engine.fsyncBtreeAtInterval(ctx)

	if err := engine.loadMetaValues(); err != nil {
		return nil, err
	}

	if err := engine.recoverWAL(); err != nil {
		return nil, err
	}

	engine.appendNotify = make(chan struct{})

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

	err = e.initKVDriver(dbFile, conf)
	if err != nil {
		return err
	}

	// skip list itself needs few bytes for initialization
	// and, we don't want to keep on trashing writing to btreeStore often.
	if conf.ArenaSize < minArenaSize {
		return errors.New("arena capacity too small min capacity 2 KB")
	}

	mTable := memtable.NewMemTable(conf.ArenaSize, walIO, namespace, e.newTxnBatcher)
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001) // 1M keys, 0.01% false positives
	e.bloom = bloomFilter
	e.activeMemTable = mTable
	e.walSyncer = walIO
	return nil
}

func (e *Engine) initKVDriver(dbFile string, conf *EngineConfig) error {
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

	slog.Info("[unisondb.dbkernal]",
		slog.String("event_type", "wal.recovered"),
		slog.Group("wal",
			slog.String("namespace", e.namespace),
			slog.String("btree_engine", string(e.config.DBEngine))),
		slog.Group("recovery",
			slog.String("durations", humanizeDuration(time.Since(startTime))),
			slog.Int("count", walRecovery.RecoveredCount())),
	)
	metrics.IncrCounterWithLabels(mKeyWalRecoveryRecordTotal, float32(walRecovery.RecoveredCount()), e.metricsLabel)
	metrics.MeasureSinceWithLabels(mKeyWalRecoveryDuration, startTime, e.metricsLabel)

	e.writeSeenCounter.Add(uint64(walRecovery.RecoveredCount()))
	e.recoveredEntriesCount = walRecovery.RecoveredCount()
	e.opsFlushedCounter.Add(uint64(walRecovery.RecoveredCount()))
	var offset *wal.Offset
	if len(checkpoint) != 0 {
		metadata := internal.UnmarshalMetadata(checkpoint)
		offset = metadata.Pos
	}
	if walRecovery.LastRecoveredOffset() != nil {
		offset = walRecovery.LastRecoveredOffset()
	}

	e.currentOffset.Store(offset)

	if walRecovery.RecoveredCount() > 0 {
		// once recovered update the metadata table again.
		e.pendingMetadata.queueMetadata(&flushedMetadata{
			metadata: &internal.Metadata{
				RecordProcessed: e.opsFlushedCounter.Load(),
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

func (e *Engine) getEntryTypeForKeyFromMemTable(key []byte) (logrecord.LogEntryType, bool) {
	// fast negative check
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.bloom.Test(key) {
		return 0, false
	}

	valueType, ok := e.activeMemTable.GetEntryTpe(key)
	if !ok {
		// first latest value
		for i := len(e.sealedMemTables) - 1; i >= 0; i-- {
			if valueType, ok = e.sealedMemTables[i].GetEntryTpe(key); ok {
				break
			}
		}
	}

	return valueType, ok
}

func (e *Engine) getEntryTypeForKey(key []byte) (logrecord.LogEntryType, bool) {
	entryType, err := e.dataStore.GetValueType(key)
	if err != nil {
		return 0, false
	}

	switch entryType {
	case kvdrivers.KeyValueValueEntry:
		return logrecord.LogEntryTypeKV, true
	case kvdrivers.ChunkedValueEntry:
		return logrecord.LogEntryTypeKV, true
	case kvdrivers.RowColumnValueEntry:
		return logrecord.LogEntryTypeRow, true
	default:
		return 0, false
	}
}

func (e *Engine) getEntryType(key []byte) (logrecord.LogEntryType, bool) {
	valueType, ok := e.getEntryTypeForKeyFromMemTable(key)
	if !ok {
		valueType, ok = e.getEntryTypeForKey(key)
	}
	return valueType, ok
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
		valueType, ok := e.getEntryType(key)
		if ok && valueType != logrecord.LogEntryTypeKV {
			return fmt.Errorf("%w: expected KV, found %s", ErrMisMatchKeyType, valueType.String())
		}
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
	hlc := HLCNow()

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

	for i, key := range keys {
		var value []byte
		if op != logrecord.LogOperationTypeDelete {
			value = values[i]
		}
		memValue := getValueStruct(byte(op), internal.EntryTypeKV, value)
		err = e.memTableWrite(key, memValue)
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
		valueType, ok := e.getEntryType(entry)
		if ok && valueType != logrecord.LogEntryTypeRow {
			return fmt.Errorf("%w: expected Row, found %s", ErrMisMatchKeyType, valueType.String())
		}
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
	hlc := HLCNow()

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
		e.notifyAppend()
	}
}

func (e *Engine) notifyAppend() {
	if !e.config.WriteNotifyCoalescing.Enabled {
		e.notifierMu.Lock()
		defer e.notifierMu.Unlock()
		if e.appendNotify != nil {
			close(e.appendNotify)
		}
		e.appendNotify = make(chan struct{})
		return
	}
	if !e.coalesceFlag.CompareAndSwap(false, true) {
		// Coalescing timer already running
		return
	}

	go func() {
		select {
		case <-e.ctx.Done():
			return
		case <-time.After(e.coalesceDuration):
		}

		e.notifierMu.Lock()
		defer e.notifierMu.Unlock()

		e.notifierMu.Lock()
		defer e.notifierMu.Unlock()
		if e.appendNotify != nil {
			close(e.appendNotify)
		}
		e.appendNotify = make(chan struct{})
		e.coalesceFlag.Store(false)
	}()
}

func (e *Engine) writeNilOffset() {
	e.activeMemTable.IncrOffset()
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
	tick := time.NewTicker(30 * time.Second)

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

func (e *Engine) syncWalAtInterval(ctx context.Context) {
	if e.config.WalConfig.SyncInterval > 0 {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			ticker := time.NewTicker(e.config.WalConfig.SyncInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					err := e.walSyncer.Sync()
					if errors.Is(err, wal.ErrWalFSync) {
						log.Fatalf("[unisondb.dbkernel] wal fsync failed: %v", err)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (e *Engine) fsyncBtreeAtInterval(ctx context.Context) {
	if !e.btreeFlushIntervalEnabled {
		return
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.btreeFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.fSyncStore()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// handleFlush flushes the sealed mem-table to btree store.
func (e *Engine) handleFlush(ctx context.Context) {
	if e.flushPaused.Load() {
		slog.Warn("[unisondb.dbkernel]: FSync Flush Paused")
		return
	}
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

		if !e.btreeFlushIntervalEnabled {
			select {
			case e.fsyncReqSignal <- struct{}{}:
			default:
				slog.Debug("fsync queue signal channel full")
			}
		}

		slog.Debug("[unisondb.dbkernel]",
			slog.String("event_type", "memtable.flushed"),
			slog.Group("flushed",
				slog.String("namespace", e.namespace),
				slog.String("duration", humanizeDuration(time.Since(startTime))),
				slog.Int("ops", recordProcessed),
				slog.String("bytes", humanize.Bytes(uint64(mt.GetBytesStored())))),
		)
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

func (p *pendingMetadata) dequeueAllMetadata() ([]*flushedMetadata, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingMetadataWrites) == 0 {
		return nil, 0
	}
	old := p.pendingMetadataWrites
	p.pendingMetadataWrites = make([]*flushedMetadata, 0)
	return old, len(old)
}

// asyncFSync call the fsync of btree store once the mem table flushing completes.
// it's made async so it doesn't block the main mem table and too many mem table doesn't
// accumulate over the time in the memory, even if large values are stored inside the mem table,
// configured via value threshold.
func (e *Engine) asyncFSync() {
	slog.Debug("[unisondb.dbkernel]: FSync Metadata eventloop", "namespace", e.namespace)
	for {
		select {
		case <-e.ctx.Done():
			return

		case _, ok := <-e.fsyncReqSignal:
			if !ok {
				return
			}
			if e.ctx.Err() != nil {
				return
			}

			maxCoalesce := 10
			count := 0
		COALESCE:
			for {
				if count > maxCoalesce {
					break COALESCE
				}
				select {
				case _, ok := <-e.fsyncReqSignal:
					if !ok {
						break COALESCE
					}
					// continue coalescing
					count++
					continue
				default:
					break COALESCE
				}
			}
			e.fSyncStore()
		}
	}
}

func (e *Engine) fSyncStore() {
	if e.flushPaused.Load() {
		slog.Warn("[unisondb.dbkernel]: FSync Flush Paused")
		return
	}
	// This queue will have data after the mem table have been flushed, so it's saved to
	// assume all the entries until this point is already in persistent store.
	// So we Can just save the last metadata and call the Flush as single ops.
	all, n := e.pendingMetadata.dequeueAllMetadata()
	if len(all) == 0 {
		return
	}
	fm := all[len(all)-1]

	metrics.IncrCounterWithLabels(mKeyPendingFsyncTotal, float32(n), e.metricsLabel)
	startTime := time.Now()
	metrics.IncrCounterWithLabels(mKeyFSyncTotal, 1, e.metricsLabel)
	err := internal.SaveMetadata(e.dataStore, fm.metadata.Pos, fm.metadata.RecordProcessed)
	if err != nil {
		log.Fatal("[unisondb.dbkernel] Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
	}
	err = e.saveBloomFilter()
	if err != nil {
		log.Fatal("[unisondb.dbkernel] Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
	}

	err = e.dataStore.FSync()
	if err != nil {
		metrics.IncrCounterWithLabels(mKeyFSyncErrorsTotal, 1, e.metricsLabel)
		// There is no way to recover from the underlying Fsync Issue.
		// https://archive.fosdem.org/2019/schedule/event/postgresql_fsync/
		// How is it possible that PostgreSQL used fsync incorrectly for 20 years.
		log.Fatalln(fmt.Errorf("[unisondb.dbkernel]: FSync operation failed: %w", err))
	}
	metrics.MeasureSinceWithLabels(mKeyFSyncDurations, startTime, e.metricsLabel)

	slog.Debug("[unisondb.dbkernel]",
		slog.String("event_type", "btree.fsync"),
		slog.Group("fsync",
			slog.String("namespace", e.namespace),
			slog.String("duration", humanizeDuration(time.Since(startTime))),
			slog.Uint64("record_processed", fm.metadata.RecordProcessed)),
	)

	if e.callback != nil {
		go e.callback()
	}
}

// pauseFlush helps in getting a consistent snapshot of the underlying kv drivers.
// if paused, use resumeFlush to resume.
func (e *Engine) pauseFlush() {
	e.flushPaused.Store(true)
}

//func (e *Engine) resumeFlush() {
//	e.flushPaused.Store(false)
//}

func (e *Engine) close(ctx context.Context) error {
	e.shutdown.Store(true)
	// cancel the context:
	e.cancel()

	var errs strings.Builder
	// wait for background routine to close.
	if !waitWithCancel(e.wg, ctx) {
		errs.WriteString("[unisondb.dbkernel]: WAL check operation timed out")
		errs.WriteString("|")
		slog.Error("Ctx was cancelled! Some goroutines are still running")
	}

	close(e.fsyncReqSignal)

	err := e.walIO.Sync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[unisondb.dbkernel]: wal Fsync error", "error", err)
	}

	err = e.walIO.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[unisondb.dbkernel]: wal close error", "error", err)
	}

	// save if any already flushed entry is still not saved.
	e.fSyncStore()
	err = e.dataStore.FSync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[unisondb.dbkernel]: Btree Fsync error", "error", err)
	}

	err = e.dataStore.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[unisondb.dbkernel]: Btree close error", "error", err)
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
