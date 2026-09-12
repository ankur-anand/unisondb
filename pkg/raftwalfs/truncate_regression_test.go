package raftwalfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestRaftOverwriteClearedSealedIndex(t *testing.T) {
	for _, clearIndex := range []bool{false, true} {
		for _, reopen := range []bool{false, true} {
			for _, sidecar := range []string{"intact", "missing", "corrupt"} {
				for _, keep := range []uint64{0, 1, 3, 4, 11} {
					t.Run(fmt.Sprintf("clear=%t/reopen=%t/index=%s/keep=%d", clearIndex, reopen, sidecar, keep), func(t *testing.T) {
						dir := t.TempDir()
						opts := []walfs.WALogOptions{walfs.WithMaxSegmentSize(1024)}
						if clearIndex {
							opts = append(opts, walfs.WithClearIndexOnFlush())
						}
						w, err := walfs.NewWALog(dir, ".wal", opts...)
						require.NoError(t, err)
						store, err := raftwalfs.NewLogStore(w, 0)
						require.NoError(t, err)
						for i := uint64(1); i <= 12; i++ {
							require.NoError(t, store.StoreLog(&raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: bytes.Repeat([]byte{byte(i)}, 250)}))
						}
						for _, seg := range w.Segments() {
							seg.WaitForIndexFlush()
						}
						for id, seg := range w.Segments() {
							if !seg.IsSealed() {
								continue
							}
							path := walfs.SegmentIndexFileName(dir, ".wal", id)
							switch sidecar {
							case "missing":
								require.NoError(t, os.Remove(path))
							case "corrupt":
								require.NoError(t, os.WriteFile(path, make([]byte, seg.GetEntryCount()*16), 0644))
							}
						}
						if reopen {
							require.NoError(t, store.Close())
							require.NoError(t, w.Close())
							w, err = walfs.NewWALog(dir, ".wal", opts...)
							require.NoError(t, err)
							store, err = raftwalfs.NewLogStore(w, 0)
							require.NoError(t, err)
						}
						for range 3 {
							require.NoError(t, store.StoreLog(&raft.Log{Index: keep + 1, Term: 2, Type: raft.LogCommand, Data: []byte("replacement")}))
						}
						require.NoError(t, store.StoreLog(&raft.Log{Index: keep + 2, Term: 2, Type: raft.LogCommand, Data: []byte("next")}))
						require.NoError(t, store.Close())
						require.NoError(t, w.Close())
						w, err = walfs.NewWALog(dir, ".wal", opts...)
						require.NoError(t, err)
						defer w.Close()
						store, err = raftwalfs.NewLogStore(w, 0)
						require.NoError(t, err)
						defer store.Close()
						last, err := store.LastIndex()
						require.NoError(t, err)
						require.Equal(t, keep+2, last)
						for i := uint64(1); i <= keep; i++ {
							var log raft.Log
							require.NoError(t, store.GetLog(i, &log))
							require.Equal(t, uint64(1), log.Term)
							require.Equal(t, bytes.Repeat([]byte{byte(i)}, 250), log.Data)
						}
						for i, data := range []string{"replacement", "next"} {
							var log raft.Log
							require.NoError(t, store.GetLog(keep+1+uint64(i), &log))
							require.Equal(t, uint64(2), log.Term)
							require.Equal(t, data, string(log.Data))
						}
						var discarded raft.Log
						require.ErrorIs(t, store.GetLog(keep+3, &discarded), raft.ErrLogNotFound)
					})
				}
			}
		}
	}
}

func TestRaftDeleteRangeFailurePreservesIndex(t *testing.T) {
	dir := t.TempDir()
	w, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024), walfs.WithClearIndexOnFlush())
	require.NoError(t, err)
	defer w.Close()
	store, err := raftwalfs.NewLogStore(w, 0)
	require.NoError(t, err)
	defer store.Close()
	for i := uint64(1); i <= 12; i++ {
		require.NoError(t, store.StoreLog(&raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: bytes.Repeat([]byte("a"), 250)}))
	}
	reader := w.Current().NewReader()
	require.NotNil(t, reader)
	defer reader.Close()
	require.Error(t, store.DeleteRange(2, 12))
	last, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(12), last)
	var log raft.Log
	require.NoError(t, store.GetLog(12, &log))
	reader.Close()
	require.NoError(t, store.DeleteRange(2, 12))
	require.NoError(t, store.StoreLog(&raft.Log{Index: 2, Term: 2, Type: raft.LogCommand, Data: []byte("replacement")}))
	require.NoError(t, store.GetLog(2, &log))
	require.Equal(t, uint64(2), log.Term)
}

type truncateRejectCodec struct{ raftwalfs.BinaryCodecV1 }

func (c truncateRejectCodec) Encode(log *raft.Log) ([]byte, error) {
	if log.Term == 2 {
		return nil, errors.New("injected codec failure")
	}
	return c.BinaryCodecV1.Encode(log)
}

func TestRaftOverwriteFailureLeavesRetryableBounds(t *testing.T) {
	for _, failure := range []string{"encode", "append"} {
		t.Run(failure, func(t *testing.T) {
			w, err := walfs.NewWALog(t.TempDir(), ".wal", walfs.WithMaxSegmentSize(1024))
			require.NoError(t, err)
			defer w.Close()
			store, err := raftwalfs.NewLogStore(w, 0, raftwalfs.WithCodec(truncateRejectCodec{}))
			require.NoError(t, err)
			defer store.Close()
			for i := uint64(1); i <= 3; i++ {
				require.NoError(t, store.StoreLog(&raft.Log{Index: i, Term: 1, Data: []byte("original")}))
			}
			failed := &raft.Log{Index: 2, Term: 2, Data: []byte("replacement")}
			wantLast := uint64(3)
			if failure == "append" {
				failed.Term = 3
				failed.Data = make([]byte, 2048)
				wantLast = 1 // truncation succeeded; replacement append was rejected
			}
			require.Error(t, store.StoreLog(failed))
			last, err := store.LastIndex()
			require.NoError(t, err)
			require.Equal(t, wantLast, last)
			require.NoError(t, store.StoreLog(&raft.Log{Index: 2, Term: 3, Data: []byte("retry")}))
			var log raft.Log
			require.NoError(t, store.GetLog(2, &log))
			require.Equal(t, "retry", string(log.Data))
		})
	}
}
