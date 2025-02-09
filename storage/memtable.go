package storage

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
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
	firstPos    *wal.ChunkPosition
	index       uint64
	size        int64
	kCount      int
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

func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.ChunkPosition, index uint64) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}
	table.sList.Put(y.KeyWithTs(key, 0), val)
	if table.firstPos == nil {
		table.firstPos = pos
	}
	table.currentPost = pos
	table.kCount++
	table.index = index
	return nil
}

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.sList.Get(y.KeyWithTs(key, 0))
}

func (table *memTable) keysCount() int {
	return table.kCount
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
		err := flushMemTable(e.namespace, mt.sList, e.db, e.wal)
		if err != nil {
			log.Fatal("Failed to flushMemTable MemTable:", "namespace", e.namespace, "err", err)
		}
		// Create WAL checkpoint
		err = SaveMetadata(e.db, mt.currentPost, mt.index)
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
		slog.Info("Flushed MemTable to BoltDB & Created WAL Checkpoint")
	}
}

// flushMemTable writes all entries from MemTable to BoltDB.
//
//nolint:unused
func flushMemTable(namespace string, currentTable *skl.Skiplist, db *bbolt.DB, wal *wal.WAL) error {
	slog.Info("Flushing MemTable to BoltDB...", "namespace", namespace)

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

		// Decode entry (could be a direct value or ChunkPosition)
		chunkPos, data, err := decodeChunkPositionWithValue(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to decode MemTable entry for key %s: %w", string(key), err)
		}

		// If value is directly in MemTable
		if chunkPos == nil {
			record := wrecord.GetRootAsWalRecord(data, 0)
			records = append(records, record)
			continue
		}

		// If value is in WAL (ChunkPosition)
		walValue, err := wal.Read(chunkPos)
		if err != nil {
			return fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
		}

		record := wrecord.GetRootAsWalRecord(walValue, 0)
		records = append(records, record)
		// Flush when batch size is reached
		if len(deleteKeys) >= readBatchSize || len(records) >= readBatchSize {
			if err := processBatch(); err != nil {
				return err
			}
		}
	}

	return processBatch()
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
			err := bucket.Put(key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
