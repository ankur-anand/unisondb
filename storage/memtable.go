package storage

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-metrics"
	"github.com/rosedblabs/wal"
)

var (
	errArenaSizeWillExceed = errors.New("arena capacity will exceed the limit")

	directValuePrefix  = []byte{1} // Marks values stored directly in memory
	walReferencePrefix = []byte{0} // Marks values stored as a reference in WAL

	dbBatchSize = 32
)

// Add a margin to avoid boundary issues, arena uses the same pool for itself.
const arenaSafetyMargin = wal.KB

// memTable hold the underlying skip list and
// the last chunk position in the wIO, associated
// with this skip list
//
//nolint:unused
type memTable struct {
	skipList        *skl.Skiplist
	lastWalPosition *wal.ChunkPosition
	capacity        int64
	opCount         int
	bytesStored     int
	db              BTreeStore
	wIO             *walIO
	namespace       string
}

func newMemTable(capacity int64, db BTreeStore, wIO *walIO, namespace string) *memTable {
	return &memTable{
		skipList:  skl.NewSkiplist(capacity),
		capacity:  capacity,
		db:        db,
		wIO:       wIO,
		namespace: namespace,
	}
}

// skip list arena has a fixed capacity, if adding the given key-value pair would exceed its capacity
// the skip list panic. verify before to avoid the panic.
func (table *memTable) canPut(key []byte, val y.ValueStruct) bool {
	return table.skipList.MemSize()+
		int64(len(y.KeyWithTs(key, 0)))+
		int64(val.EncodedSize())+arenaSafetyMargin <= table.capacity
}

func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.ChunkPosition) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}
	table.skipList.Put(y.KeyWithTs(key, uint64(time.Now().UnixNano())), val)

	table.lastWalPosition = pos
	table.opCount++
	table.bytesStored = table.bytesStored + len(key) + len(val.Value)
	return nil
}

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.skipList.Get(y.KeyWithTs(key, uint64(time.Now().UnixNano())))
}

func (table *memTable) opsCount() int {
	return table.opCount
}

// processFlushQueue flushes the pending mem table in the queue to the persistence storage
// when a signal is generated or ticker comes.
//
//nolint:unused
func (e *Engine) processFlushQueue(wg *sync.WaitGroup, signal chan struct{}) {
	wg.Add(1)
	// check if there is item in queue that needs to be flushed
	// based upon timer, as the process of input of the WAL write could
	// be higher, then what bolt-bTreeStore could keep up the pace.
	tick := time.NewTicker(1 * time.Second)
	go func() {
		defer tick.Stop()
		defer wg.Done()
		for {
			select {
			case _, ok := <-signal:
				if !ok {
					return
				}
				e.handleFlush()
			case <-tick.C:
				e.handleFlush()
			}
		}
	}()
}

func (e *Engine) handleFlush() {
	var mt *memTable
	e.mu.Lock()
	metrics.SetGaugeWithLabels([]string{"kvalchemy", "sealed", "memtable", "count"}, float32(len(e.sealedMemTables)), e.label)
	if len(e.sealedMemTables) > 0 {
		mt = e.sealedMemTables[0]
	}
	e.mu.Unlock()

	if mt != nil && !mt.skipList.Empty() {
		startTime := time.Now()
		recordProcessed, err := mt.flush()
		if err != nil {
			log.Fatal("Failed to flushMemTable MemTable:", "namespace", e.namespace, "err", err)
		}
		// Create WAL checkpoint
		err = SaveMetadata(e.bTreeStore, mt.lastWalPosition, e.opsFlushedCounter.Add(uint64(recordProcessed)))
		if err != nil {
			log.Fatal("Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
		}
		err = e.saveBloomFilter()
		if err != nil {
			log.Fatal("Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
		}
		if e.Callback != nil {
			e.Callback()
		}
		e.mu.Lock()
		// Remove it from the list
		e.sealedMemTables = e.sealedMemTables[1:]
		e.mu.Unlock()

		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "flush", "duration", "msec"}, startTime, e.label)
		metrics.IncrCounterWithLabels([]string{"kvalchemy", "flush", "record", "count"}, float32(recordProcessed), e.label)

		slog.Info("Flushed MemTable to BoltDB & Created WAL Checkpoint",
			"ops_flushed", recordProcessed, "namespace", e.namespace,
			"duration", time.Since(startTime), "bytes_flushed", humanize.Bytes(uint64(mt.bytesStored)))
	}
}

// flush writes all entries from MemTable to BoltDB.
//
//nolint:unused
func (table *memTable) flush() (int, error) {
	slog.Debug("Flushing MemTable to BoltDB...", "namespace", table.namespace)

	// Create an iterator for the MemTable
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	count := 0

	var setKeys, setValues, deleteKeys [][]byte

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
		key := y.ParseKey(it.Key())
		entry := it.Value()

		if entry.Meta == byte(wrecord.LogOperationOpDelete) {
			deleteKeys = append(deleteKeys, key)
			continue
		}

		record, err := getWalRecord(entry, table.wIO)
		if err != nil {
			return 0, err
		}

		if record.Operation() == wrecord.LogOperationOpBatchCommit {
			n, err := table.flushBatchCommit(record)
			if err != nil {
				return 0, err
			}
			// total number of batch record + one for batch start marker that was not part of the record.
			count += n + 1

			continue
		}

		setKeys = append(setKeys, record.KeyBytes())
		setValues = append(setValues, record.ValueBytes())

		// Flush when batch capacity is reached
		if len(deleteKeys) >= dbBatchSize || len(setKeys) >= dbBatchSize {
			if err := table.processBatch(&setKeys, &setValues, &deleteKeys); err != nil {
				return 0, err
			}
		}
	}

	return count, table.processBatch(&setKeys, &setValues, &deleteKeys)
}

func (table *memTable) processBatch(setKeys, setValues, deleteKeys *[][]byte) error {
	if len(*deleteKeys) > 0 {
		if err := table.db.DeleteMany(*deleteKeys); err != nil {
			return err
		}
		*deleteKeys = nil
	}
	if len(*setKeys) > 0 {
		if err := table.db.SetMany(*setKeys, *setValues); err != nil {
			return err
		}
		*setKeys = nil
		*setValues = nil
	}
	return nil
}

// Note: We are only compressing the value not the wIO record. So not decompression involved.
func getWalRecord(entry y.ValueStruct, wIO *walIO) (*wrecord.WalRecord, error) {
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk position: %w", err)
	}

	if chunkPos == nil {
		return wrecord.GetRootAsWalRecord(value, 0), nil
	}

	walValue, err := wIO.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return wrecord.GetRootAsWalRecord(walValue, 0), nil
}

// flushBatchCommit returns the number of batch record that was inserted.
func (table *memTable) flushBatchCommit(record *wrecord.WalRecord) (int, error) {
	data, checksum, err := table.readCompleteBatch(record)
	if err != nil {
		return 0, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	return len(data), table.db.SetChunks(record.KeyBytes(), data, checksum)
}

func (table *memTable) readCompleteBatch(record *wrecord.WalRecord) ([][]byte, uint32, error) {
	lastPos := record.LastBatchPosBytes()
	if len(lastPos) == 0 {
		return nil, 0, ErrRecordCorrupted
	}

	pos, err := decodeChunkPos(lastPos)
	if err != nil {
		return nil, 0, err
	}

	sum, err := DecompressLZ4(record.ValueBytes())
	if err != nil {
		return nil, 0, err
	}
	checksum := unmarshalChecksum(sum)

	data, err := readChunks(table.wIO, pos)

	if err != nil {
		return nil, 0, err
	}

	return data, checksum, nil
}

// readChunksFromWal from Wal without decompressing.
func readChunks(wIO *walIO, startPos *wal.ChunkPosition) ([][]byte, error) {
	if startPos == nil {
		return nil, ErrInternalError
	}

	var values [][]byte

	nextPos := startPos

	for {
		// read the next pos data
		record, err := wIO.Read(nextPos)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL at position %+v: %wIO", nextPos, err)
		}
		wr := wrecord.GetRootAsWalRecord(record, 0)
		value := wr.ValueBytes()

		if wr.Operation() == wrecord.LogOperationOPBatchInsert {
			values = append(values, value)
		}

		next := wr.LastBatchPosBytes()
		// We hit the last record.
		if next == nil {
			break
		}

		nextPos = wal.DecodeChunkPosition(next)
	}

	// as the data-are read in reverse
	slices.Reverse(values)

	return values, nil
}
