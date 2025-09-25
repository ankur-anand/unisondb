package kvdrivers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

var _ txnWriter = (*LMDBTxnQueue)(nil)

// LMDBTxnQueue encapsulates LMDB Transactions and provides an API that allow multiple
// operation to be put in queue that is supposed to be Executed in the same orders.
// It flushes the batch automatically if configured max batch Size threshold is reached.
// Caller should call Commit in the end to finish any pending Txn not flushed via maxBatchSize.
// Single instance of Txn are not concurrent safe.
type LMDBTxnQueue struct {
	mt              *MetricsTracker
	db              lmdb.DBI
	env             *lmdb.Env
	err             error
	opsQueue        []func(*lmdb.Txn) error
	maxBatchSize    int
	entriesModified float32
	putOps          float32
	deleteOps       float32
	stats           *TxnStats
}

// NewTxnQueue returns an initialized LMDBTxnQueue for Batch API queuing and commit.
func (l *LmdbEmbed) NewTxnQueue(maxBatchSize int) *LMDBTxnQueue {
	return &LMDBTxnQueue{
		env:          l.env,
		db:           l.dataDB,
		maxBatchSize: maxBatchSize,
		mt:           l.mt,
		opsQueue:     make([]func(*lmdb.Txn) error, 0, maxBatchSize),
		stats:        &TxnStats{},
	}
}

// BatchPutKV queue one or more key-value pairs inside a transaction that will be commited upon Commit or max batch size
// threshold breach. If the value exists, it replaces the existing value, else sets a new value associated with the key.
// Caller need to take care to not upsert the row column value, else there is no guarantee for consistency in storage.
func (lq *LMDBTxnQueue) BatchPutKV(keys, values [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for i, key := range keys {
		i, key := i, key
		// append the set function to queue for processing
		lq.opsQueue = append(lq.opsQueue, func(t *lmdb.Txn) error {
			typedKey := KeyKV(key)
			err := t.Put(lq.db, typedKey, values[i], 0)
			if err != nil {
				return err
			}
			lq.putOps++
			lq.entriesModified++

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDeleteKV queue one or more key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
// Caller need to call BatchDeleteRows or BatchDeleteRowsColumns to work with rows and columns type value.
func (lq *LMDBTxnQueue) BatchDeleteKV(keys [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for _, key := range keys {
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			typedKey := KeyKV(key)

			err := txn.Del(lq.db, typedKey, nil)
			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil
				}
				return err
			}

			lq.deleteOps++
			lq.entriesModified++
			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// SetLobChunks stores a value that has been split into chunks, associating them with a single key.
// It queues the operation in Txn, which is flushed when either max size threshold is reached or during commit call.
func (lq *LMDBTxnQueue) SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if lq.err != nil {
		return lq.err
	}

	metaData := make([]byte, 9)
	metaData[0] = chunkedValue
	binary.LittleEndian.PutUint32(metaData[1:], uint32(len(chunks)))
	binary.LittleEndian.PutUint32(metaData[5:], checksum)

	typedKey := KeyBlobChunk(key, 0)
	lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(lq.db, typedKey)

		if err == nil && len(storedValue) > 0 && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			oldChunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := 1; i <= int(oldChunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := txn.Del(lq.db, chunkKey, nil); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
			}
		}

		lq.putOps++
		lq.entriesModified++
		// Store chunks
		for i, chunk := range chunks {
			lq.entriesModified++
			chunkKey := KeyBlobChunk(key, i+1)
			err := txn.Put(lq.db, chunkKey, chunk, 0)
			if err != nil {
				return err
			}
		}

		// Store metadata
		if err := txn.Put(lq.db, typedKey, metaData, 0); err != nil {
			return err
		}

		return nil
	})

	// We are saving chunk ASAP as this is already LOB, divided into chunks.
	lq.err = lq.flushBatch()

	return lq.err
}

// BatchDeleteLobChunks queues the deletion of all chunks associated with each LOB key.
func (lq *LMDBTxnQueue) BatchDeleteLobChunks(keys [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for _, key := range keys {
		key := key
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			typedKey := KeyBlobChunk(key, 0)
			storedValue, err := txn.Get(lq.db, typedKey)
			if lmdb.IsNotFound(err) || storedValue == nil {
				return nil
			}
			if err != nil {
				return err
			}
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				_ = txn.Del(lq.db, chunkKey, nil)
				lq.entriesModified++
			}
			if err := txn.Del(lq.db, typedKey, nil); err != nil && !lmdb.IsNotFound(err) {
				return err
			}
			lq.entriesModified++
			lq.deleteOps++
			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}
	return lq.err
}

// BatchSetCells queues updates or inserts of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lq *LMDBTxnQueue) BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lq.err = ErrInvalidArguments
		return lq.err
	}

	for i, rowKey := range rowKeys {
		i, rowKey := i, rowKey
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			lq.putOps++

			for key, value := range columnEntries {
				columnKey := KeyColumn(rowKey, unsafeStringToBytes(key))
				lq.entriesModified++
				if err := txn.Put(lq.db, columnKey, value, 0); err != nil {
					return err
				}
			}

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDeleteCells queues deletes of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lq *LMDBTxnQueue) BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lq.err = ErrInvalidArguments
		return lq.err
	}

	for i, rowKey := range rowKeys {
		i, rowKey := i, rowKey
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			for key := range columnEntries {
				columnKey := KeyColumn(rowKey, unsafeStringToBytes(key))
				if err := txn.Del(lq.db, columnKey, nil); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
				} else {
					lq.entriesModified++
				}
			}
			lq.deleteOps++
			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDeleteRows  queue deletes of the row and all it's associated Columns from the database.
func (lq *LMDBTxnQueue) BatchDeleteRows(rowKeys [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for _, rowKey := range rowKeys {
		rowKey := rowKey
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			c, err := txn.OpenCursor(lq.db)
			if err != nil {
				return err
			}
			defer c.Close()
			lq.deleteOps++

			pKey := RowKey(rowKey)
			// http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
			// MDB_SET_RANGE
			// Position at first key greater than or equal to specified key.
			k, _, err := c.Get(pKey, nil, lmdb.SetRange)

			for err == nil {
				if !bytes.HasPrefix(k, pKey) {
					break // Stop if key is outside the prefix range
				}

				if err := c.Del(0); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
				// Move to next key
				lq.entriesModified++
				k, _, err = c.Get(nil, nil, lmdb.Next)
			}

			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil
				}
				return err
			}

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

func (lq *LMDBTxnQueue) flushBatch() error {
	if lq.err != nil || len(lq.opsQueue) == 0 {
		return lq.err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	startTime := time.Now()

	txn, err := lq.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	for _, op := range lq.opsQueue {
		if err := op(txn); err != nil {
			lq.err = err
			break
		}
	}

	if lq.err != nil {
		txn.Abort()
		return lq.err
	}

	// no errors occurred commit txn
	err = txn.Commit()

	if lq.err == nil {
		lq.mt.RecordFlush(len(lq.opsQueue), startTime)
		lq.mt.RecordBatchOps(OpSet, int(lq.putOps))
		lq.mt.RecordBatchOps(OpDelete, int(lq.deleteOps))
		lq.mt.RecordWriteUnits(int(lq.entriesModified))
		lq.stats.EntriesModified += lq.entriesModified
		lq.stats.DeleteOps += lq.deleteOps
		lq.stats.PutOps += lq.putOps
		lq.opsQueue = nil
		lq.putOps = 0
		lq.deleteOps = 0
		lq.entriesModified = 0
	} else {
		lq.mt.RecordError(TxnCommit)
	}
	return err
}

func (lq *LMDBTxnQueue) Stats() TxnStats {
	return *lq.stats
}

// Commit all the pending operation in queue.
func (lq *LMDBTxnQueue) Commit() error {
	if lq.err != nil {
		return lq.err
	}

	return lq.flushBatch()
}
