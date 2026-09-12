package dbkernel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/require"
)

func TestStorageEngineReopensAfterInitializationFailure(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		for _, failure := range []string{"arena size", "unsupported engine", "WAL open", "B-tree open", "WAL recovery"} {
			t.Run(string(backend)+"/"+failure, func(t *testing.T) {
				dir, namespace := t.TempDir(), "initialization-failure"
				conf := NewDefaultEngineConfig()
				conf.DBEngine = backend
				nsDir := filepath.Join(dir, namespace)
				dbPath := filepath.Join(nsDir, dbFileName)
				walDir := filepath.Join(nsDir, walDirName)
				repair := func() {}
				switch failure {
				case "arena size":
					conf.ArenaSize = minArenaSize - 1
					repair = func() { conf.ArenaSize = minArenaSize }
				case "unsupported engine":
					conf.DBEngine = "unsupported"
					repair = func() { conf.DBEngine = backend }
				case "WAL open":
					conf.WalConfig.SegmentSize = 1 << 33
					repair = func() { conf.WalConfig.SegmentSize = wal.NewDefaultConfig().SegmentSize }
				case "B-tree open":
					require.NoError(t, os.MkdirAll(nsDir, 0755))
					if backend == BoltDBEngine {
						require.NoError(t, os.Mkdir(dbPath, 0755))
					} else {
						require.NoError(t, os.WriteFile(dbPath, []byte("obstruction"), 0644))
					}
					repair = func() { require.NoError(t, os.Remove(dbPath)) }
				case "WAL recovery":
					w, err := wal.NewWalIO(walDir, namespace, &conf.WalConfig)
					require.NoError(t, err)
					// A valid WAL frame with an invalid empty KV batch fails after
					// recovery has acquired a segment reader.
					record := logcodec.LogRecord{
						LSN: 1, OperationType: logrecord.LogOperationTypeInsert,
						TxnState: logrecord.TransactionStateNone, EntryType: logrecord.LogEntryTypeKV,
					}
					_, err = w.Append(record.FBEncode(256), 1)
					require.NoError(t, err)
					require.NoError(t, w.Close())
					repair = func() { require.NoError(t, os.Remove(walfs.SegmentFileName(walDir, ".seg", 1))) }
				}

				engine, err := NewStorageEngine(dir, namespace, conf)
				require.Error(t, err)
				require.Nil(t, engine)
				require.NotErrorIs(t, err, ErrDatabaseDirInUse)
				if failure == "arena size" || failure == "unsupported engine" {
					entries, err := os.ReadDir(dir)
					require.NoError(t, err)
					require.Empty(t, entries, "invalid configuration must not create storage")
				}
				if failure == "WAL recovery" {
					require.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
				}
				if _, err := os.Stat(nsDir); err == nil {
					lock := flock.New(filepath.Join(nsDir, pidLockName))
					locked, err := lock.TryLock()
					require.NoError(t, err)
					require.True(t, locked, "failed initialization must release pid.lock")
					require.NoError(t, lock.Unlock())
				}
				repair()
				engine, err = NewStorageEngine(dir, namespace, conf)
				require.NoError(t, err, "corrected configuration/storage must reopen in the same process")
				t.Cleanup(func() { require.NoError(t, engine.Close(context.Background())) })
				require.NoError(t, engine.PutKV([]byte("key"), []byte("value")))
				value, err := engine.GetKV([]byte("key"))
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			})
		}
	}
}

func TestStorageEngineRejectsNilConfig(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewStorageEngine(dir, "nil-config", nil)
	require.ErrorContains(t, err, "configuration is nil")
	require.Nil(t, engine)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
