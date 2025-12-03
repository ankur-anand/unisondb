package dbkernel

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/ankur-anand/unisondb/dbkernel/internal/recovery"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	kvdrivers2 "github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/pkg/umetrics"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dustin/go-humanize"
	"github.com/gofrs/flock"
	"github.com/uber-go/tally/v4"
)

const (
	minArenaSize            = 1024 * 200
	keyVersionEvictInterval = time.Minute
)

var (
	mKeyPendingFsyncTotal     = "btree_fsync_pending_total"
	mKeyFSyncTotal            = "btree_fsync_total"
	mKeyFSyncErrorsTotal      = "btree_fsync_errors_total"
	mKeyFSyncDurations        = "btree_fsync_durations_seconds"
	mKeyMemTableRotationTotal = "active_mem_table_rotation_total"

	mKeySealedMemTableTotal       = "sealed_mem_table_pending_total"
	mKeySealedMemFlushTotal       = "sealed_mem_table_flush_total"
	mKeySealedMemFlushDuration    = "sealed_mem_table_flush_durations_seconds"
	mKeySealedMemFlushRecordTotal = "sealed_mem_table_flush_record_total"

	mKeyWalRecoveryDuration    = "wal_recovery_durations_seconds"
	mKeyWalRecoveryRecordTotal = "wal_recovery_record_total"
)

var (
	recoveryHistBucket = tally.DurationBuckets{
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
	}

	memFlushHistBuckets = tally.DurationBuckets{
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		5 * time.Minute,
	}

	fSyncHistBuckets = tally.DurationBuckets{
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}
)

var (
	// ErrWaitTimeoutExceeded is a sentinel error to denotes sync.cond expired due to timeout.
	ErrWaitTimeoutExceeded = errors.New("wait timeout exceeded")
)

// ChangeNotifier is called on successful local writes.
type ChangeNotifier interface {
	Notify(ctx context.Context, key []byte, op logrecord.LogOperationType, entryType logrecord.LogEntryType)
}

// NopNotifier is a no-op implementation.
type NopNotifier struct{}

func (NopNotifier) Notify(ctx context.Context, key []byte, op logrecord.LogOperationType, entryType logrecord.LogEntryType) {
}

// Engine manages WAL, MemTable (SkipList), and BtreeStore for a given namespace.
type Engine struct {
	mu                sync.RWMutex
	writeSeenCounter  atomic.Uint64
	opsFlushedCounter atomic.Uint64
	currentOffset     atomic.Pointer[wal.Offset]
	namespace         string
	keyVersions       map[string]uint64
	activeTxnBegins   map[uint64]int
	dataStore         internal.BTreeStore
	walIO             *wal.WalIO
	walSyncer         walSyncer
	config            *EngineConfig

	fileLock              *flock.Flock
	wg                    *sync.WaitGroup
	activeMemTable        *memtable.MemTable
	sealedMemTables       []*memtable.MemTable
	flushReqSignal        chan struct{}
	pendingMetadata       *pendingMetadata
	fsyncReqSignal        chan struct{}
	disableEntryTypeCheck bool
	readOnly              bool
	dataDir               string
	backupRoot            string
	backupNamespaceRoot   string

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

	taggedScope    umetrics.Scope
	changeNotifier ChangeNotifier
}

// NewStorageEngine initializes WAL, MemTable, and BtreeStore and returns an initialized Engine for a namespace.
func NewStorageEngine(dataDir, namespace string, conf *EngineConfig) (*Engine, error) {
	signal := make(chan struct{}, 2)
	btreeFlushInterval, btreeFlushIntervalEnabled := conf.effectiveBTreeFlushInterval()
	ctx, cancel := context.WithCancel(context.Background())
	initMonotonic(ctx)
	taggedScope := umetrics.AutoScope().Tagged(map[string]string{
		"namespace": namespace,
	})

	chNotifier := conf.ChangeNotifier
	if chNotifier == nil {
		chNotifier = NopNotifier{}
	}

	engine := &Engine{
		dataDir:                   dataDir,
		namespace:                 namespace,
		config:                    conf,
		wg:                        &sync.WaitGroup{},
		flushReqSignal:            signal,
		pendingMetadata:           &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:                       ctx,
		cancel:                    cancel,
		callback:                  func() {},
		fsyncReqSignal:            make(chan struct{}, 1),
		btreeFlushInterval:        btreeFlushInterval,
		btreeFlushIntervalEnabled: btreeFlushIntervalEnabled,
		disableEntryTypeCheck:     conf.DisableEntryTypeCheck,
		readOnly:                  conf.ReadOnly,
		taggedScope:               taggedScope,
		changeNotifier:            chNotifier,
		coalesceDuration:          conf.WriteNotifyCoalescing.Duration,
		keyVersions:               make(map[string]uint64),
		activeTxnBegins:           make(map[uint64]int),
	}
	engine.backupRoot = filepath.Join(dataDir, BackupRootDirName)
	engine.backupNamespaceRoot = filepath.Join(engine.backupRoot, namespace)
	if err := os.MkdirAll(engine.backupNamespaceRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create backup root: %w", err)
	}

	if err := engine.initStorage(dataDir, namespace, conf); err != nil {
		return nil, err
	}

	// background task:
	engine.asyncMemTableFlusher(ctx)
	engine.syncWalAtInterval(ctx)
	engine.fsyncBtreeAtInterval(ctx)
	engine.startKeyVersionEvicter(ctx)

	if err := engine.loadMetaValues(); err != nil {
		return nil, err
	}

	if conf.EventLogMode {
		// EventLogMode, scan WAL to restore LSN/offset but we don't replay to BTree
		if err := engine.restoreLSNFromWAL(); err != nil {
			return nil, err
		}
	} else {
		if err := engine.recoverWAL(); err != nil {
			return nil, err
		}
	}

	engine.appendNotify = make(chan struct{})
	if conf.WalConfig.AutoCleanup {
		predicate := newWalCleanupPredicate(conf.EventLogMode, engine.CurrentOffset, engine.GetWalCheckPoint)
		engine.walIO.RunWalCleanup(ctx, conf.WalConfig.CleanupInterval, predicate)
	}

	return engine, nil
}

func newWalCleanupPredicate(eventLogMode bool, currentOffset func() *wal.Offset, checkpoint func() (*internal.Metadata, error)) func(segID wal.SegID) bool {
	return func(segID wal.SegID) bool {
		if eventLogMode {
			currOff := currentOffset()
			if currOff == nil {
				return false
			}
			// EventLogMode, events don't need BTree persistence,
			// so we can safely delete segments behind currentOffset
			return currOff.SegmentID > segID
		}
		// normal mode, only delete segments behind the checkpoint
		// to ensure data is safely persisted to BTree
		checkpointMeta, err := checkpoint()
		if err != nil || checkpointMeta == nil {
			return false
		}
		return checkpointMeta.Pos.SegmentID > segID
	}
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

	walIO, err := wal.NewWalIO(walDir, namespace, &conf.WalConfig)
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
	e.activeMemTable = mTable
	e.walSyncer = walIO
	return nil
}

func (e *Engine) initKVDriver(dbFile string, conf *EngineConfig) error {
	conf.BtreeConfig.Namespace = e.namespace

	var bTreeStore internal.BTreeStore
	switch conf.DBEngine {
	case BoltDBEngine:
		db, err := kvdrivers2.NewBoltdb(dbFile, conf.BtreeConfig)
		if err != nil {
			return err
		}
		bTreeStore = db
		e.newTxnBatcher = func(maxBatchSize int) internal.TxnBatcher {
			return db.NewTxnQueue(maxBatchSize)
		}
	case LMDBEngine:
		db, err := kvdrivers2.NewLmdb(dbFile, conf.BtreeConfig)
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
	if err != nil && !errors.Is(err, kvdrivers2.ErrKeyNotFound) {
		return err
	}
	// there is no value even for bloom filter.
	if errors.Is(err, kvdrivers2.ErrKeyNotFound) {
		return nil
	}
	metadata := internal.UnmarshalMetadata(data)
	e.startMetadata = metadata

	// dataStore the global counter
	e.writeSeenCounter.Store(metadata.RecordProcessed)
	e.opsFlushedCounter.Store(metadata.RecordProcessed)

	return nil
}

// recoverWAL recovers the wal if any pending writes are still not visible.
func (e *Engine) recoverWAL() error {
	checkpoint, err := e.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdrivers2.ErrKeyNotFound) {
		return fmt.Errorf("recover WAL failed %w", err)
	}

	walRecovery := recovery.NewWalRecovery(e.dataStore, e.walIO)
	startTime := time.Now()
	if err := walRecovery.Recover(checkpoint); err != nil {
		return err
	}

	slog.Info("[dbkernel]",
		slog.String("message", "Recovered WAL segments"),
		slog.Group("wal",
			slog.String("namespace", e.namespace),
			slog.String("db_engine", string(e.config.DBEngine)),
		),
		slog.Group("recovery",
			slog.String("duration", humanizeDuration(time.Since(startTime))),
			slog.Int("record_count", walRecovery.RecoveredCount()),
		),
	)
	e.taggedScope.Counter(mKeyWalRecoveryRecordTotal).Inc(int64(walRecovery.RecoveredCount()))
	e.taggedScope.Histogram(mKeyWalRecoveryDuration, recoveryHistBucket).RecordDuration(time.Since(startTime))

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
			slog.Error("[dbkernel]",
				slog.String("message", "Fsync request signal channel full during recovery"),
			)
			panic("[dbkernal] fsync req signal channel is full while recovering itself")
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
	if e.readOnly {
		return ErrEngineReadOnly
	}

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
	offset, err := e.walIO.Append(encoded, index)
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

	e.recordKeyVersions(keys, index)
	e.writeOffset(offset)
	return nil
}

// persistEvent writes an event to the WAL.
func (e *Engine) persistEvent(event *logcodec.EventEntry) error {
	if e.readOnly {
		return ErrEngineReadOnly
	}

	encodedEvent := logcodec.SerializeEventEntry(event)
	hintSize := len(encodedEvent) + 512
	checksum := crc32.ChecksumIEEE(encodedEvent)

	e.mu.Lock()
	defer e.mu.Unlock()

	index := e.writeSeenCounter.Add(1)
	hlc := HLCNow()

	record := logcodec.LogRecord{
		LSN:           index,
		HLC:           hlc,
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeEvent,
		Entries:       [][]byte{encodedEvent},
		CRC32Checksum: checksum,
	}

	encoded := record.FBEncode(hintSize)

	offset, err := e.walIO.Append(encoded, index)
	if err != nil {
		return err
	}

	e.writeOffset(offset)
	return nil
}

// persistRowColumnAction writes the columnEntries for the given rowKey in the wal and mem-table.
func (e *Engine) persistRowColumnAction(op logrecord.LogOperationType, rowKeys [][]byte, columnsEntries []map[string][]byte) error {
	if e.readOnly {
		return ErrEngineReadOnly
	}

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
	offset, err := e.walIO.Append(encoded, index)
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

	e.recordKeyVersions(rowKeys, index)
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
			e.taggedScope.Counter(mKeyMemTableRotationTotal).Inc(1)
			e.rotateMemTable()
			err = e.activeMemTable.Put(key, v)
			// :(
			if err != nil {
				return err
			}
		}
	}

	e.changeNotifier.Notify(e.ctx, key, decodeOperation(v), decodeEntryType(v))
	return err
}

func decodeOperation(v y.ValueStruct) logrecord.LogOperationType {
	return logrecord.LogOperationType(v.Meta)
}

func decodeEntryType(v y.ValueStruct) logrecord.LogEntryType {
	return logrecord.LogEntryType(v.UserMeta)
}

// recordKeyVersions updates the latest committed LSN for the provided keys.
func (e *Engine) recordKeyVersions(keys [][]byte, lsn uint64) {
	if e.keyVersions == nil {
		e.keyVersions = make(map[string]uint64)
	}
	for _, key := range keys {
		e.keyVersions[string(key)] = lsn
	}
}

// checkKeyConflicts returns an error if any of the provided keys have a committed
// LSN greater than the provided startLSN.
func (e *Engine) checkKeyConflicts(keys [][]byte, startLSN uint64) error {
	for _, key := range keys {
		if lsn, ok := e.keyVersions[string(key)]; ok && lsn > startLSN {
			return ErrTxnConflict
		}
	}
	return nil
}

// registerTxnBegin tracks the begin LSN for an active transaction.
func (e *Engine) registerTxnBegin(lsn uint64) {
	e.activeTxnBegins[lsn]++
}

// unregisterTxnBegin removes the begin LSN for an active transaction.
func (e *Engine) unregisterTxnBegin(lsn uint64) {
	if count, ok := e.activeTxnBegins[lsn]; ok {
		if count <= 1 {
			delete(e.activeTxnBegins, lsn)
			return
		}
		e.activeTxnBegins[lsn] = count - 1
	}
}

// minActiveTxnBegin returns the smallest begin LSN across active txns.
// If there are no active txns, MaxUint64 is returned.
func (e *Engine) minActiveTxnBegin() uint64 {
	if len(e.activeTxnBegins) == 0 {
		return math.MaxUint64
	}
	min := uint64(math.MaxUint64)
	for lsn := range e.activeTxnBegins {
		if lsn < min {
			min = lsn
		}
	}
	return min
}

// evictKeyVersions removes version entries that are older than the oldest active txn.
func (e *Engine) evictKeyVersions() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.keyVersions) == 0 {
		return
	}

	minBegin := e.minActiveTxnBegin()
	if minBegin == math.MaxUint64 {
		// no active txns, clear all to keep the map bounded.
		e.keyVersions = make(map[string]uint64)
		return
	}

	for key, version := range e.keyVersions {
		if version <= minBegin {
			delete(e.keyVersions, key)
		}
	}
}

func (e *Engine) startKeyVersionEvicter(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(keyVersionEvictInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.evictKeyVersions()
			}
		}
	}()
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
		slog.Warn("[dbkernel]",
			slog.String("message", "Paused Fsync flush"),
		)
		return
	}
	var mt *memtable.MemTable
	e.mu.Lock()
	e.taggedScope.Gauge(mKeySealedMemTableTotal).Update(float64(len(e.sealedMemTables)))
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

		e.taggedScope.Histogram(mKeySealedMemFlushDuration, memFlushHistBuckets).RecordDuration(time.Since(startTime))
		e.taggedScope.Counter(mKeySealedMemFlushRecordTotal).Inc(int64(recordProcessed))
		e.taggedScope.Counter(mKeySealedMemFlushTotal).Inc(1)

		if !e.btreeFlushIntervalEnabled {
			select {
			case e.fsyncReqSignal <- struct{}{}:
			default:
				slog.Debug("[dbkernel]",
					slog.String("message", "Fsync signal dropped due to full queue"),
				)
			}
		}

		slog.Debug("[dbkernel]",
			slog.String("message", "MemTable flushed to BtreeStore"),
			slog.Group("flush",
				slog.String("namespace", e.namespace),
				slog.String("duration", humanizeDuration(time.Since(startTime))),
				slog.Int("ops", recordProcessed),
				slog.String("bytes", humanize.Bytes(uint64(mt.GetBytesStored()))),
			),
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
	slog.Debug("[dbkernel]",
		slog.String("message", "Started FSync metadata event loop"),
		slog.String("namespace", e.namespace),
	)
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
		slog.Warn("[dbkernel]",
			slog.String("message", "Paused FSync flush"),
		)
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

	e.taggedScope.Counter(mKeyPendingFsyncTotal).Inc(int64(n))

	startTime := time.Now()
	e.taggedScope.Counter(mKeyFSyncTotal).Inc(1)

	err := internal.SaveMetadata(e.dataStore, fm.metadata.Pos, fm.metadata.RecordProcessed)
	if err != nil {
		log.Fatal("[dbkernel] Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
	}

	err = e.dataStore.FSync()
	if err != nil {
		e.taggedScope.Counter(mKeyFSyncErrorsTotal).Inc(1)
		// There is no way to recover from the underlying Fsync Issue.
		// https://archive.fosdem.org/2019/schedule/event/postgresql_fsync/
		// How is it possible that PostgreSQL used fsync incorrectly for 20 years.
		log.Fatalln(fmt.Errorf("[dbkernel]: FSync operation failed: %w", err))
	}

	e.taggedScope.Histogram(mKeyFSyncDurations, fSyncHistBuckets).RecordDuration(time.Since(startTime))

	slog.Debug("[dbkernel]",
		slog.String("message", "Flushed BTree to disk via FSync"),
		slog.Group("fsync",
			slog.String("namespace", e.namespace),
			slog.String("duration", humanizeDuration(time.Since(startTime))),
			slog.Uint64("record_processed", fm.metadata.RecordProcessed),
		),
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
	if e.shutdown.Swap(true) {
		return nil
	}
	// cancel the context:
	e.cancel()

	var errs strings.Builder
	// wait for background routine to close.
	if !waitWithCancel(e.wg, ctx) {
		errs.WriteString("[dbkernel]: WAL check operation timed out")
		errs.WriteString("|")
		slog.Error("Ctx was cancelled! Some goroutines are still running")
	}

	close(e.fsyncReqSignal)

	err := e.walIO.Sync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[dbkernel]: wal Fsync error", "error", err)
	}

	err = e.walIO.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[dbkernel]: wal close error", "error", err)
	}

	// save if any already flushed entry is still not saved.
	e.fSyncStore()
	err = e.dataStore.FSync()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[dbkernel]: Btree Fsync error", "error", err)
	}

	err = e.dataStore.Close()
	if err != nil {
		errs.WriteString(err.Error())
		errs.WriteString("|")
		slog.Error("[dbkernel]: Btree close error", "error", err)
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

// restoreLSNFromWAL restores LSN counter and offset from WAL segment metadata in EventLogMode.
func (e *Engine) restoreLSNFromWAL() error {
	lastLSN, lastOffset, count, ok := e.walIO.CurrentSegmentInfo()

	if ok {
		e.writeSeenCounter.Store(lastLSN)
		e.opsFlushedCounter.Store(lastLSN)

		if lastOffset != nil {
			e.currentOffset.Store(lastOffset)
		}
	}

	slog.Info("[dbkernel]",
		slog.String("message", "Restored LSN from WAL (EventLogMode)"),
		slog.Group("engine",
			slog.String("namespace", e.namespace),
		),
		slog.Group("restore",
			slog.Uint64("last_lsn", lastLSN),
			slog.Int64("entry_count", count),
		),
	)

	e.recoveredEntriesCount = int(count)
	return nil
}
