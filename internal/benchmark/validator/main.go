package main

import (
	"flag"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
)

const (
	dbFileName = "db.bolt"
	walDirName = "wal"
)

var (
	dataDir   = flag.String("dir", "./../data", "data directory")
	namespace = flag.String("namespace", "default", "namespace")
)

// Default permission values.
const (
	defaultBytesPerSync = 1 * wal.MB  // 1MB
	defaultSegmentSize  = 16 * wal.MB // 16MB
)

func newWALOptions(dirPath string) wal.Options {
	return wal.Options{
		DirPath:        dirPath,
		SegmentSize:    defaultSegmentSize,
		SegmentFileExt: ".seg.wal",
		// writes are buffered to OS.
		// Logs will be only lost, if the machine itself crashes.
		// Not the process, data will be still safe as os buffer persists.
		Sync:         false,
		BytesPerSync: defaultBytesPerSync,
	}
}

func main() {
	nsDir := filepath.Join(*dataDir, *namespace)
	walDir := filepath.Join(nsDir, walDirName)
	dbFile := filepath.Join(nsDir, dbFileName)

	if _, err := os.Stat(nsDir); err != nil {
		panic("err: " + err.Error())
	}

	// Open BoltDB
	db, err := bbolt.Open(dbFile, 0600, nil)
	if err != nil {
		panic("boltdb err: " + err.Error())
	}

	// Open WAL
	w, err := wal.Open(newWALOptions(walDir))
	if err != nil {
		panic("wal err: " + err.Error())
	}

	tx, err := db.Begin(false)
	if err != nil {
		panic("boltdb err: " + err.Error())
	}

	defer tx.Rollback()
	bucket := tx.Bucket([]byte(*namespace))
	if bucket == nil {
		log.Println(storage.ErrBucketNotFound)
	}

	flushedKeys := 0

	bucket.ForEach(func(k, v []byte) error {
		flushedKeys++
		return nil
	})

	slog.Info("flushed keys", "count", flushedKeys)

	startTime := time.Now()
	slog.Info("reading WAL...", "namespace", *namespace)

	reader := w.NewReader()
	uniqueKeys := make(map[string]struct{})
	notFlushedRecordCount := 0
	notFlushedKeysUnique := make(map[string]struct{})

	totalWalCount := 0
	for {
		data, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			panic("wal reader err: " + err.Error())
		}
		totalWalCount++
		record := wrecord.GetRootAsWalRecord(data, 0)
		key := record.KeyBytes()
		uniqueKeys[string(key)] = struct{}{}

		ts, counter := storage.DecodeHLC(record.Hlc())
		switch record.Operation() {
		case wrecord.LogOperationOpInsert:
			val := bucket.Get(key)
			if val == nil {
				notFlushedRecordCount++
				notFlushedKeysUnique[string(key)] = struct{}{}
				slog.Debug("expected Key: Not Found", "key", key, "ts", ts, "counter", counter)
			}
		case wrecord.LogOperationOpDelete:
			// TODO: Build for Batch Ops.
		}

	}

	if len(uniqueKeys) != len(notFlushedKeysUnique)+flushedKeys {
		slog.Error("validation of wal record failed.")
	}

	slog.Info("reading WAL... Finished", "namespace", *namespace, "duration", time.Since(startTime),
		"wal_count", totalWalCount,
		"uniqueKeys", len(uniqueKeys), "notFlushedRecordCount",
		notFlushedRecordCount, "notFlushedKeysUnique", len(notFlushedKeysUnique))
}
