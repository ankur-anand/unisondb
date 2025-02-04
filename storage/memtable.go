package storage

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

var (
	errArenaSizeWillExceed = errors.New("arena size will exceed the limit")
)

// memTable hold the underlying skip list and
// the last chunk position in the wal, associated
// with this skip list
//
//nolint:unused
type memTable struct {
	sList *skl.Skiplist
	pos   *wal.ChunkPosition
	size  int64
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
		int64(val.EncodedSize()) <= table.size
}

func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.ChunkPosition) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}
	table.sList.Put(y.KeyWithTs(key, 0), val)
	table.pos = pos
	return nil
}

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.sList.Get(y.KeyWithTs(key, 0))
}

//nolint:unused
type flushNode struct {
	mTable *memTable
	next   *flushNode
}

// flusherQueue holds all the pending mem table that needs to be flushed to persistence store.
// queue is used to make the flushing in order the mem-table is written.
//
//nolint:unused
type flusherQueue struct {
	mu         sync.Mutex
	head, tail *flushNode
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
	node := &flushNode{mTable: &m}
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
		err = saveChunkPosition(e.db, mt.pos)
		if err != nil {
			log.Fatal("Failed to Create WAL checkpoint:", "namespace", e.namespace, "err", err)
		}
		if e.callback != nil {
			e.callback()
		}
		slog.Info("Flushed MemTable to BoltDB & Created WAL Checkpoint")
	}
}

// flushMemTable writes all entries from MemTable to BoltDB.
//
//nolint:unused
func flushMemTable(namespace string, currentTable *skl.Skiplist, db *bbolt.DB, wal *wal.WAL) error {
	slog.Info("Flushing MemTable to BoltDB...", "namespace", namespace)

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func(tx *bbolt.Tx) {
		_ = tx.Rollback()
	}(tx)

	bucket := tx.Bucket([]byte(namespace))
	if bucket == nil {
		return errors.New("bucket not found")
	}

	// Create an iterator for the MemTable
	it := currentTable.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	count := 0
	keysProcessed := false

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
		key := y.ParseKey(it.Key())
		entry := it.Value()
		keysProcessed = true
		err := processMemTableEntry(bucket, key, entry, wal)
		if err != nil {
			slog.Error("Skipping entry due to error:", "namespace", namespace, "err", err)
			continue
		}

		// Batch writes every 100 keys
		if count%100 == 0 {
			if err := tx.Commit(); err != nil {
				return err
			}

			tx, err = db.Begin(true)
			if err != nil {
				return err
			}
			bucket = tx.Bucket([]byte(namespace))
			if bucket == nil {
				return errors.New("bucket not found")
			}
		}
	}

	// **Ensure final commit for remaining keys**
	if keysProcessed {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return err
}

// processMemTableEntry handles a single MemTable entry and writes it to BoltDB.
//
//nolint:unused
func processMemTableEntry(bucket *bbolt.Bucket, key []byte, entry y.ValueStruct, wal *wal.WAL) error {
	// Handle deletion
	if entry.Meta == OpDelete {
		if err := bucket.Delete(key); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", string(key), err)
		}
		return nil
	}

	// Decode entry (could be a direct value or ChunkPosition)
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return fmt.Errorf("failed to decode MemTable entry for key %s: %w", string(key), err)
	}

	// If value is directly in MemTable
	if chunkPos == nil {
		data, err := decompressLZ4(value)
		if err != nil {
			return fmt.Errorf("failed to decompress MemTable entry for key %s: %w", string(key), err)
		}

		record, err := decodeWalRecord(data)
		if err != nil {
			return fmt.Errorf("failed to decode WAL record for key %s: %w", string(key), err)
		}

		return bucket.Put(record.Key, record.Value)
	}

	// If value is in WAL (ChunkPosition)
	walValue, err := wal.Read(chunkPos)
	if err != nil {
		return fmt.Errorf("WAL read failed for key %s: %w", string(key), err)
	}

	data, err := decompressLZ4(walValue)
	if err != nil {
		return fmt.Errorf("failed to decompress WAL entry for key %s: %w", string(key), err)
	}

	record, err := decodeWalRecord(data)
	if err != nil {
		return fmt.Errorf("failed to decode WAL record for key %s: %w", string(key), err)
	}

	return bucket.Put(record.Key, record.Value)
}
