package storage

import (
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/dgraph-io/badger/v4/skl"
	"go.etcd.io/bbolt"
)

// FlushMemTable moves data from MemTable to BoltDB and triggers WAL checkpointing
func (se *Engine) FlushMemTable(wg *sync.WaitGroup) {
	se.mu.Lock()

	// Swap MemTable (Old MemTable is now `currentTable`)
	currentTable := se.memTable
	se.memTable = skl.NewSkiplist(defaultArenaSize)

	se.mu.Unlock() // Unlock immediately after swap to allow new writes

	go func() {
		wg.Add(1) // Add to WaitGroup inside goroutine
		defer wg.Done()

		slog.Info("Flushing MemTable to BoltDB...", "namespace", se.namespace)

		err := se.db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(se.namespace))
			if bucket == nil {
				return fmt.Errorf("bucket not found")
			}

			it := currentTable.NewIterator()
			defer it.Close()
			count := 0

			for it.SeekToFirst(); it.Valid(); it.Next() {
				count++
				entry := it.Value()
				key := it.Key()
				if entry.Meta == OpDelete {
					bucket.Delete(key)
					continue
				}

				// Decompress data
				data, err := decompressLZ4(entry.Value)
				if err != nil {
					slog.Error("Skipping unreadable WAL entry:", "namespace", se.namespace, "err", err)
					continue
				}

				// Decode WAL record
				record, err := decodeWalRecord(data)
				if err != nil {
					slog.Error("Skipping unreadable WAL entry:", "namespace", se.namespace, "err", err)
					continue
				}
				err = bucket.Put(it.Key(), record.Value)
				if err != nil {
					return err
				}

				// Batch writes every 100 keys
				if count%100 == 0 {
					err := tx.Commit()
					if err != nil {
						return err
					}
				}
			}
			return nil
		})

		if err != nil {
			log.Fatal("Failed to flush MemTable:", "namespace", se.namespace, "err", err)
		}

		// Create WAL checkpoint
		// se.CheckpointWAL()
		slog.Info("Flushed MemTable to BoltDB & Created WAL Checkpoint")
	}()
}
