package dbkernel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func openTxnIndexEngine(t *testing.T, dir string, backend DBEngine, size int64, raftMode bool) (*Engine, *raftwalfs.LogStore) {
	t.Helper()
	conf := NewDefaultEngineConfig()
	conf.DBEngine, conf.ArenaSize = backend, minArenaSize
	conf.WalConfig.SegmentSize, conf.WalConfig.AutoCleanup, conf.WalConfig.RaftMode = size, false, raftMode
	e, err := NewStorageEngine(dir, "txn-index", conf)
	require.NoError(t, err)
	e.cancel()
	e.wg.Wait()
	e.SetRaftMode(raftMode)
	var store *raftwalfs.LogStore
	if raftMode {
		store, err = raftwalfs.NewLogStore(e.WAL(), 0, raftwalfs.WithCodec(raftwalfs.BinaryCodecV1{DataMutator: raftwalfs.LogRecordMutator{}}))
		require.NoError(t, err)
		e.SetPositionLookup(store.GetPosition)
		e.SetWALCommitCallback(store.CommitPosition)
	}
	t.Cleanup(func() {
		if store != nil {
			require.NoError(t, store.Close())
		}
		if !e.shutdown.Load() {
			require.NoError(t, e.Close(context.Background()))
		}
	})
	return e, store
}

// Assign actual Raft indexes and use the production WAL codec. Each follower
// persists the same command bytes into its own, differently segmented WAL.
type txnIndexApplier struct {
	engine *Engine
	store  *raftwalfs.LogStore
	logs   []*raft.Log
}

func (a *txnIndexApplier) Apply(data []byte) (uint64, error) {
	entry := &raft.Log{Index: uint64(len(a.logs) + 1), Term: 1, Type: raft.LogCommand, Data: bytes.Clone(data)}
	return a.append(entry)
}

func (a *txnIndexApplier) append(entry *raft.Log) (uint64, error) {
	if err := a.store.StoreLog(entry); err != nil {
		return 0, err
	}
	a.logs = append(a.logs, entry)
	if err, ok := a.engine.Apply(entry).(error); ok {
		return 0, err
	}
	return entry.Index, nil
}

type txnIndexWriter interface {
	AppendKVTxn([]byte, []byte) error
	AppendColumnTxn([]byte, map[string][]byte) error
	Commit() error
}

func newTxnIndexWriter(t *testing.T, e *Engine, kind logrecord.LogEntryType) txnIndexWriter {
	t.Helper()
	if e.raftState.raftMode {
		txn, err := e.NewRaftTxn(logrecord.LogOperationTypeInsert, kind)
		require.NoError(t, err)
		return txn
	}
	txn, err := e.NewTxn(logrecord.LogOperationTypeInsert, kind)
	require.NoError(t, err)
	return txn
}

func appendTxnIndexValue(t *testing.T, txn txnIndexWriter, kind logrecord.LogEntryType, key string, i int, value []byte) {
	t.Helper()
	switch kind {
	case logrecord.LogEntryTypeKV:
		require.NoError(t, txn.AppendKVTxn([]byte(fmt.Sprintf("%s%d", key, i)), value))
	case logrecord.LogEntryTypeRow:
		require.NoError(t, txn.AppendColumnTxn([]byte(key), map[string][]byte{fmt.Sprint(i): value}))
	case logrecord.LogEntryTypeChunked:
		require.NoError(t, txn.AppendKVTxn([]byte(key), value))
	}
}

func requireTxnIndexInvisible(t *testing.T, e *Engine, kind logrecord.LogEntryType) {
	t.Helper()
	var err error
	switch kind {
	case logrecord.LogEntryTypeKV:
		_, err = e.GetKV([]byte("main0"))
	case logrecord.LogEntryTypeRow:
		_, err = e.GetRowColumns("main", nil)
	case logrecord.LogEntryTypeChunked:
		_, err = e.GetLOB([]byte("main"))
	}
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTransactionIndexesAcrossWALLayouts(t *testing.T) {
	for _, mode := range []struct {
		name                     string
		sourceRaft, followerRaft bool
	}{
		{"standalone", false, false},
		{"raft", true, true},
		{"raft_to_isr", true, false},
	} {
		for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
			for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow, logrecord.LogEntryTypeChunked} {
				t.Run(fmt.Sprintf("%s/%s/%s", mode.name, backend, kind), func(t *testing.T) {
					source, sourceStore := openTxnIndexEngine(t, t.TempDir(), backend, 4096, mode.sourceRaft)
					dir := t.TempDir()
					follower, followerStore := openTxnIndexEngine(t, dir, backend, 1024, mode.followerRaft)
					applier := &txnIndexApplier{engine: source, store: sourceStore}
					if mode.sourceRaft {
						source.SetRaftApplier(applier)
						_, err := applier.append(&raft.Log{Index: 1, Term: 1, Type: raft.LogNoop})
						require.NoError(t, err)
					}
					mainTxn := newTxnIndexWriter(t, source, kind)
					otherTxn := newTxnIndexWriter(t, source, kind)
					value := bytes.Repeat([]byte("v"), 600)
					for i := range 2 {
						appendTxnIndexValue(t, mainTxn, kind, "main", i, value)
						appendTxnIndexValue(t, otherTxn, kind, "other", i, []byte("other"))
					}
					require.NoError(t, otherTxn.Commit())
					requireTxnIndexInvisible(t, source, kind)
					require.NoError(t, mainTxn.Commit())
					checkLSNTestValue(t, source, kind, "main", value)
					expectedLast := source.OpsReceivedCount()
					if mode.sourceRaft {
						expectedLast = uint64(len(applier.logs))
					}
					var last uint64
					if mode.followerRaft {
						for i, entry := range applier.logs {
							if i == len(applier.logs)-1 {
								requireTxnIndexInvisible(t, follower, kind)
							}
							require.NoError(t, followerStore.StoreLog(entry))
							require.Nil(t, follower.Apply(entry))
							last = entry.Index
						}
					} else {
						handler := NewReplicaWALHandler(follower)
						reader, err := source.NewReader()
						require.NoError(t, err)
						defer reader.Close()
						for {
							data, _, err := reader.Next()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							last = logrecord.GetRootAsLogRecord(data, 0).Lsn()
							if last == expectedLast {
								requireTxnIndexInvisible(t, follower, kind)
							}
							require.NoError(t, handler.ApplyRecordsLSNOnly([][]byte{bytes.Clone(data)}))
						}
					}
					require.Equal(t, expectedLast, last)
					leaderPos, err := source.WAL().PositionForIndex(last)
					require.NoError(t, err)
					followerPos, err := follower.WAL().PositionForIndex(last)
					require.NoError(t, err)
					require.NotEqual(t, leaderPos, followerPos, "fixture must exercise distinct physical layouts")
					checkLSNTestValue(t, follower, kind, "main", value)
					checkLSNTestValue(t, follower, kind, "other", []byte("other"))
					// First reopen before flushing, rebuild the local index and replay.
					// Then flush and reopen again to verify the persisted values.
					for restart := range 2 {
						if followerStore != nil {
							require.NoError(t, followerStore.Close())
						}
						require.NoError(t, follower.Close(context.Background()))
						follower, followerStore = openTxnIndexEngine(t, dir, backend, 2048, mode.followerRaft)
						if mode.followerRaft {
							for index := uint64(1); index <= last; index++ {
								var entry raft.Log
								require.NoError(t, followerStore.GetLog(index, &entry))
								require.Nil(t, follower.Apply(&entry))
							}
						}
						checkLSNTestValue(t, follower, kind, "main", value)
						checkLSNTestValue(t, follower, kind, "other", []byte("other"))
						if restart == 0 {
							checkpointLSNTestEngine(t, follower)
							checkLSNTestValue(t, follower, kind, "main", value)
						}
					}
				})
			}
		}
	}
}

func TestRaftTransactionRejectsForeignChain(t *testing.T) {
	for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow, logrecord.LogEntryTypeChunked} {
		t.Run(kind.String(), func(t *testing.T) {
			e, store := openTxnIndexEngine(t, t.TempDir(), BoltDBEngine, 1024, true)
			applier := &txnIndexApplier{engine: e, store: store}
			e.SetRaftApplier(applier)
			txn := newTxnIndexWriter(t, e, kind).(*RaftTxn)
			appendTxnIndexValue(t, txn, kind, "main", 0, []byte("foreign-value"))
			forged := logcodec.LogRecord{LSN: 1, TxnID: []byte("different-transaction"), EntryType: kind,
				TxnState: logrecord.TransactionStateCommit, OperationType: logrecord.LogOperationTypeInsert, PrevTxnIndex: txn.prevIndex}
			_, err := applier.Apply(forged.FBEncode(128))
			require.ErrorIs(t, err, wal.ErrInvalidTxnChain)
			requireTxnIndexInvisible(t, e, kind)
		})
	}
}
