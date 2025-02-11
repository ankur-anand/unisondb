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
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

var (
	errArenaSizeWillExceed = errors.New("arena size will exceed the limit")

	directValuePrefix  = []byte{1} // Marks values stored directly in memory
	walReferencePrefix = []byte{0} // Marks values stored as a reference in WAL

	readBatchSize = 20
)

// Add a margin to avoid boundary issues, arena uses the same pool for itself.
const arenaSafetyMargin = wal.KB

// memTable hold the underlying skip list and
// the last chunk position in the wal, associated
// with this skip list
//
//nolint:unused
type memTable struct {
	sList       *skl.Skiplist
	currentPost *wal.ChunkPosition
	size        int64
	opsCount    int
	bytesStored int
}

func newMemTable(size int64) *memTable {
	return &memTable{
		sList: skl.NewSkiplist(size),
		size:  size,
	}
}

// skiplist arena has a fixed size, if adding the given key-value pair would exceed its capacity
// the skiplist panic. verify before to avoid the panic.
func (table *memTable) canPut(key []byte, val y.ValueStruct) bool {
	return table.sList.MemSize()+
		int64(len(y.KeyWithTs(key, 0)))+
		int64(val.EncodedSize())+arenaSafetyMargin <= table.size
}

func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.ChunkPosition) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}
	table.sList.Put(y.KeyWithTs(key, uint64(time.Now().UnixNano())), val)

	table.currentPost = pos
	table.opsCount++
	table.bytesStored = table.bytesStored + len(key) + len(val.Value)
	return nil
}

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.sList.Get(y.KeyWithTs(key, uint64(time.Now().UnixNano())))
}

func (table *memTable) keysCount() int {
	return table.opsCount
}

//nolint:unused
type flushQueue struct {
	mTable *memTable
	next   *flushQueue
}

// flusherQueue holds all the pending mem table that needs to be flushed to persistence store.
// queue is used to make the flushing in order the mem-table is written.
//
//nolint:unused
type flusherQueue struct {
	mu         sync.Mutex
	head, tail *flushQueue
	signal     chan struct{}
}

// newFlusherQueue returns an initialized flusherQueue.
func newFlusherQueue(signal chan struct{}) *flusherQueue {
	return &flusherQueue{
		signal: signal,
	}
}

//nolint:unused
func (fq *flusherQueue) enqueue(m memTable) {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	node := &flushQueue{mTable: &m}
	if fq.tail != nil {
		fq.tail.next = node
	}

	fq.tail = node
	if fq.head == nil {
		fq.head = node
	}
}

//nolint:unused
func (fq *flusherQueue) dequeue() *memTable {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if fq.head == nil {
		return nil
	}

	memTable := fq.head.mTable
	fq.head = fq.head.next
	if fq.head == nil {
		fq.tail = nil
	}
	return memTable
}

// processFlushQueue flushes the pending mem table in the queue to the persistence storage
// when a signal is generated or ticker comes.
//
//nolint:unused
func (e *Engine) processFlushQueue(wg *sync.WaitGroup, signal chan struct{}) {
	wg.Add(1)
	// check if there is item in queue that needs to be flushed
	// based upon timer, as the process of input of the WAL write could
	// be higher, then what bolt-db could keep up the pace.
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
	mt := e.flushQueue.dequeue()
	if mt != nil && !mt.sList.Empty() {
		startTime := time.Now()
		recordProcessed, err := flushMemTable(e.namespace, mt.sList, e.db, e.wal)
		if err != nil {
			log.Fatal("Failed to flushMemTable MemTable:", "namespace", e.namespace, "err", err)
		}
		// Create WAL checkpoint
		err = SaveMetadata(e.db, mt.currentPost, e.opsFlushedCounter.Add(uint64(recordProcessed)))
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
		slog.Info("Flushed MemTable to BoltDB & Created WAL Checkpoint",
			"ops_flushed", recordProcessed, "namespace", e.namespace,
			"duration", time.Since(startTime), "bytes_flushed", mt.bytesStored)
	}
}

// flushMemTable writes all entries from MemTable to BoltDB.
//
//nolint:unused
func flushMemTable(namespace string, currentTable *skl.Skiplist, db *bbolt.DB, wal *wal.WAL) (int, error) {
	slog.Debug("Flushing MemTable to BoltDB...", "namespace", namespace)

	// Create an iterator for the MemTable
	it := currentTable.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	count := 0

	var records []*wrecord.WalRecord
	var deleteKeys [][]byte

	processBatch := func() error {
		if len(deleteKeys) > 0 {
			if err := flushDeletes(namespace, db, deleteKeys); err != nil {
				return err
			}
			deleteKeys = nil
		}
		if len(records) > 0 {
			if err := flushRecords(namespace, db, records); err != nil {
				return err
			}
			records = nil
		}
		return nil
	}

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
		key := y.ParseKey(it.Key())
		entry := it.Value()

		if entry.Meta == byte(wrecord.LogOperationOpDelete) {
			deleteKeys = append(deleteKeys, key)
			continue
		}

		record, err := getWalRecord(entry, wal)

		if err != nil {
			return 0, err
		}

		if record.Operation() == wrecord.LogOperationOpBatchCommit {
			n, err := flushBatchCommit(namespace, db, record, wal)
			if err != nil {
				return 0, err
			}
			count += n // total number of batch record
			count++    // one for batch start marker that was not part of the record.
			continue
		}

		records = append(records, record)
		// Flush when batch size is reached
		if len(deleteKeys) >= readBatchSize || len(records) >= readBatchSize {
			if err := processBatch(); err != nil {
				return 0, err
			}
		}
	}

	return count, processBatch()
}

func getWalRecord(entry y.ValueStruct, wal *wal.WAL) (*wrecord.WalRecord, error) {
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk position: %w", err)
	}

	if chunkPos == nil {
		return wrecord.GetRootAsWalRecord(value, 0), nil
	}

	walValue, err := wal.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return wrecord.GetRootAsWalRecord(walValue, 0), nil
}

func flushDeletes(namespace string, db *bbolt.DB, keys [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return ErrBucketNotFound
		}

		for _, key := range keys {
			err := bucket.Delete(key)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func flushRecords(namespace string, db *bbolt.DB, records []*wrecord.WalRecord) error {
	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return ErrBucketNotFound
		}
		for _, record := range records {
			key := record.KeyBytes()
			value := record.ValueBytes()
			// to indicate this is a full value, not chunked
			storedValue := append([]byte{FullValueFlag}, value...)
			err := bucket.Put(key, storedValue)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// flushBatchCommit returns the number of batch record that was inserted.
func flushBatchCommit(namespace string, db *bbolt.DB, record *wrecord.WalRecord, wal *wal.WAL) (int, error) {
	data, checksum, err := reconstructBatchValue(record, wal)
	if err != nil {
		return 0, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	return len(data), insertChunkIntoBoltDB(namespace, db, record.KeyBytes(), data, checksum)
}

func reconstructBatchValue(record *wrecord.WalRecord, wal *wal.WAL) ([][]byte, uint32, error) {
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

	data, err := readChunks(wal, pos)

	if err != nil {
		return nil, 0, err
	}

	return data, checksum, nil
}

// readChunksFromWal from Wal without decompressing.
func readChunks(w *wal.WAL, startPos *wal.ChunkPosition) ([][]byte, error) {
	if startPos == nil {
		return nil, ErrInternalError
	}

	var values [][]byte

	nextPos := startPos

	for {
		// read the next pos data
		record, err := w.Read(nextPos)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL at position %+v: %w", nextPos, err)
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
