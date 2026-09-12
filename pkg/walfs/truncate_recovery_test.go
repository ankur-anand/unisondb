package walfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func newTruncationFixture(t *testing.T, dir string, opts ...WALogOptions) *WALog {
	t.Helper()
	opts = append([]WALogOptions{WithMaxSegmentSize(1024), WithClearIndexOnFlush()}, opts...)
	w, err := NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)
	for i := uint64(1); i <= 12; i++ {
		_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 250), i)
		require.NoError(t, err)
	}
	require.Len(t, w.Segments(), 4)
	for _, seg := range w.Segments() {
		seg.WaitForIndexFlush()
	}
	return w
}

func assertTruncationPrefix(t *testing.T, w *WALog, end uint64) {
	t.Helper()
	first, last := w.GetBounds()
	require.Equal(t, end, last)
	if end == 0 {
		require.Zero(t, first)
	} else {
		require.Equal(t, uint64(1), first)
	}
	r := w.NewReader()
	defer r.Close()
	for i := uint64(1); i <= end; i++ {
		data, pos, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i)}, 250), data)
		indexed, err := w.PositionForIndex(i)
		require.NoError(t, err)
		require.Equal(t, pos, indexed)
	}
	_, _, err := r.Next()
	require.True(t, errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData), "unexpected tail: %v", err)
	for i := end + 1; i <= 12; i++ {
		_, err := w.PositionForIndex(i)
		require.Error(t, err)
	}
}

func TestTruncationFailureRequiresRecovery(t *testing.T) {
	// Directory syncs occur after intent publication, target-index replacement,
	// each suffix removal, and intent removal. Fail at every boundary.
	for stage := 1; stage <= 5; stage++ {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			dir := t.TempDir()
			var armed atomic.Bool
			var calls atomic.Int64
			injected := errors.New("injected directory sync failure")
			syncer := DirectorySyncFunc(func(dir string) error {
				if armed.Load() && calls.Add(1) == int64(stage) {
					return injected
				}
				return syncDir(dir)
			})
			w := newTruncationFixture(t, dir, WithDirectorySyncer(syncer))
			pos, err := w.PositionForIndex(1)
			require.NoError(t, err)
			armed.Store(true)
			err = w.Truncate(4)
			require.ErrorIs(t, err, ErrRecoveryRequired)
			require.ErrorIs(t, err, injected)
			_, err = w.Write([]byte("forbidden"), 5)
			require.ErrorIs(t, err, ErrRecoveryRequired)
			_, err = w.WriteBatch([][]byte{[]byte("forbidden")}, []uint64{5})
			require.ErrorIs(t, err, ErrRecoveryRequired)
			require.ErrorIs(t, w.RotateSegment(), ErrRecoveryRequired)
			require.ErrorIs(t, w.Truncate(4), ErrRecoveryRequired)
			require.ErrorIs(t, w.Sync(), ErrRecoveryRequired)
			_, err = w.Read(pos)
			require.ErrorIs(t, err, ErrRecoveryRequired)
			reader := w.NewReader()
			_, _, err = reader.Next()
			require.ErrorIs(t, err, ErrRecoveryRequired)
			reader.Close()
			armed.Store(false)
			require.NoError(t, w.Close())
			w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(512), WithClearIndexOnFlush())
			require.NoError(t, err)
			assertTruncationPrefix(t, w, 4)
			require.NoError(t, w.Truncate(4))
			_, err = w.Write(bytes.Repeat([]byte{5}, 250), 5)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(2048), WithClearIndexOnFlush())
			require.NoError(t, err)
			defer w.Close()
			assertTruncationPrefix(t, w, 5)
		})
	}
}

func TestTruncationPreparationDoesNotDeleteSuffix(t *testing.T) {
	for _, failure := range []string{"future_index", "corrupt_prefix", "intent_publication", "reader"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			w := newTruncationFixture(t, dir)
			defer w.Close()
			index := uint64(4)
			var reader *SegmentReader
			switch failure {
			case "future_index":
				index = 100
			case "corrupt_prefix":
				w.segments[2].mmapData[segmentHeaderSize+recordHeaderSize] ^= 1
			case "intent_publication":
				require.NoError(t, os.Mkdir(w.truncateIntentPath(), 0755))
			case "reader":
				reader = w.Current().NewReader()
				defer reader.Close()
			}
			before := make(map[SegmentID][]byte)
			for id, seg := range w.Segments() {
				before[id] = bytes.Clone(seg.mmapData)
			}
			require.Error(t, w.Truncate(index))
			require.NoError(t, w.RecoveryError())
			require.Len(t, w.Segments(), 4)
			require.Equal(t, SegmentID(4), w.Current().ID())
			for id, seg := range w.Segments() {
				require.Equal(t, before[id], []byte(seg.mmapData))
			}
			_, err := w.Write([]byte("still usable"), 13)
			require.NoError(t, err)
		})
	}
}

// This helper exits without Close or test cleanup, exercising real process-crash
// recovery. The optional torn header/tail is injected only after the intent is durable.
func TestTruncationCrashHelper(t *testing.T) {
	dir := os.Getenv("UNISONDB_TRUNCATE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	stage, err := strconv.Atoi(os.Getenv("UNISONDB_TRUNCATE_CRASH_STAGE"))
	require.NoError(t, err)
	reset := os.Getenv("UNISONDB_TRUNCATE_RESET") == "1"
	var armed atomic.Bool
	var calls atomic.Int64
	syncer := DirectorySyncFunc(func(dir string) error {
		if err := syncDir(dir); err != nil {
			return err
		}
		if !armed.Load() || calls.Add(1) != int64(stage) {
			return nil
		}
		if damage := os.Getenv("UNISONDB_TRUNCATE_DAMAGE"); damage != "" {
			file, err := os.OpenFile(SegmentFileName(dir, ".wal", 2), os.O_RDWR, 0)
			require.NoError(t, err)
			if damage == "header" {
				_, err = file.WriteAt(bytes.Repeat([]byte{0xff}, 37), 0)
			} else {
				// Target retains its first 272-byte framed record; tear part of its suffix.
				_, err = file.WriteAt(make([]byte, 100), segmentHeaderSize+recordOverhead(250)+16)
			}
			require.NoError(t, err)
			require.NoError(t, file.Sync())
			require.NoError(t, file.Close())
		}
		os.Exit(73)
		return nil
	})
	w := newTruncationFixture(t, dir, WithDirectorySyncer(syncer))
	if os.Getenv("UNISONDB_TRUNCATE_COMPACTED") == "1" {
		require.NoError(t, w.deleteSegments([]SegmentID{1, 2}))
	}
	armed.Store(true)
	keep := uint64(4)
	if reset {
		keep = 0
	}
	require.NoError(t, w.Truncate(keep))
	t.Fatal("crash point was not reached")
}

func TestTruncationCrashRecovery(t *testing.T) {
	for _, reset := range []bool{false, true} {
		maxStage := 5
		if reset {
			maxStage = 7
		} // intent, four removals, new segment, intent removal
		for stage := 1; stage <= maxStage; stage++ {
			t.Run(fmt.Sprintf("reset=%t/stage=%d", reset, stage), func(t *testing.T) {
				runTruncationCrash(t, stage, reset, "")
			})
		}
	}
	for _, damage := range []string{"header", "tail"} {
		t.Run("torn_"+damage, func(t *testing.T) { runTruncationCrash(t, 1, false, damage) })
	}
}

func TestTruncationResetAfterPrefixGC(t *testing.T) {
	t.Setenv("UNISONDB_TRUNCATE_COMPACTED", "1")
	for stage := 1; stage <= 5; stage++ {
		t.Run(fmt.Sprint(stage), func(t *testing.T) { runTruncationCrash(t, stage, true, "") })
	}
}

func runTruncationCrash(t *testing.T, stage int, reset bool, damage string) {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTruncationCrashHelper$")
	cmd.Env = append(os.Environ(), "UNISONDB_TRUNCATE_CRASH_DIR="+dir, "UNISONDB_TRUNCATE_CRASH_STAGE="+strconv.Itoa(stage), "UNISONDB_TRUNCATE_DAMAGE="+damage)
	if reset {
		cmd.Env = append(cmd.Env, "UNISONDB_TRUNCATE_RESET=1")
	}
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "%s", output)
	require.Equal(t, 73, exitErr.ExitCode(), "%s", output)
	keep := uint64(4)
	if reset {
		keep = 0
	}
	for reopen := range 2 {
		w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(512), WithClearIndexOnFlush())
		require.NoError(t, err)
		assertTruncationPrefix(t, w, keep)
		_, err = os.Stat(w.truncateIntentPath())
		require.ErrorIs(t, err, os.ErrNotExist)
		if reopen == 1 {
			_, err = w.Write(bytes.Repeat([]byte{byte(keep + 1)}, 250), keep+1)
			require.NoError(t, err)
			assertTruncationPrefix(t, w, keep+1)
		}
		require.NoError(t, w.Close())
	}
}

func TestTruncationRejectsCorruptIntent(t *testing.T) {
	for _, damage := range []string{"checksum", "boundary", "prefix"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			w := newTruncationFixture(t, dir)
			intent, _, err := w.prepareTruncation(4)
			require.NoError(t, err)
			if damage == "boundary" {
				binary.LittleEndian.PutUint64(intent.Header[24:32], uint64(intent.Size+8))
			}
			_, err = w.persistTruncation(intent)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			if damage == "checksum" {
				require.NoError(t, os.WriteFile(w.truncateIntentPath(), []byte(`{"payload":{},"crc":0}`), 0644))
			}
			if damage == "prefix" {
				file, err := os.OpenFile(SegmentFileName(dir, ".wal", 2), os.O_RDWR, 0)
				require.NoError(t, err)
				_, err = file.WriteAt([]byte{0xff}, segmentHeaderSize+recordHeaderSize)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			_, err = NewWALog(dir, ".wal")
			require.Error(t, err)
			for id := SegmentID(1); id <= 4; id++ {
				_, err := os.Stat(SegmentFileName(dir, ".wal", id))
				require.NoError(t, err)
			}
			_, err = os.Stat(w.truncateIntentPath())
			require.NoError(t, err)
		})
	}
}

func TestTruncationUnqueuesCleanupTarget(t *testing.T) {
	w := newTruncationFixture(t, t.TempDir(), WithAutoCleanupPolicy(0, 1, 1, true))
	defer w.Close()
	w.MarkSegmentsForDeletion()
	require.Len(t, w.QueuedSegmentsForDeletion(), 3)
	// Queueing must not remove anything before the predicate is evaluated.
	w.cleanPendingSegments(func(SegmentID) bool { return false })
	require.Len(t, w.Segments(), 4)
	require.NoError(t, w.Truncate(4))
	_, queued := w.QueuedSegmentsForDeletion()[2]
	require.False(t, queued)
	w.cleanPendingSegments(func(id SegmentID) bool { return id == 2 })
	_, err := w.Write(bytes.Repeat([]byte{5}, 250), 5)
	require.NoError(t, err)
	assertTruncationPrefix(t, w, 5)
}

func TestTruncationRemovesEmptyTail(t *testing.T) {
	dir := t.TempDir()
	w := newTruncationFixture(t, dir)
	require.NoError(t, w.RotateSegment())
	require.Len(t, w.Segments(), 5)
	require.NoError(t, w.Truncate(12))
	require.Equal(t, SegmentID(4), w.Current().ID())
	require.NoError(t, w.Close())
	w, err := NewWALog(dir, ".wal", WithClearIndexOnFlush())
	require.NoError(t, err)
	defer w.Close()
	assertTruncationPrefix(t, w, 12)
	_, err = w.Write(bytes.Repeat([]byte{13}, 250), 13)
	require.NoError(t, err)
	assertTruncationPrefix(t, w, 13)
}

func TestCleanupQueueHonorsRetentionAcrossTicks(t *testing.T) {
	w := newTruncationFixture(t, t.TempDir(), WithAutoCleanupPolicy(0, 2, 2, true))
	defer w.Close()
	for range 3 {
		w.MarkSegmentsForDeletion()
	}
	require.Len(t, w.QueuedSegmentsForDeletion(), 2)
	reader := w.Segments()[1].NewReader()
	require.NotNil(t, reader)
	defer reader.Close()
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.Len(t, w.Segments(), 3)
	reader.Close()
	// Releasing a reader must not initiate deletion outside the WAL lock.
	require.Len(t, w.Segments(), 3)
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.Len(t, w.Segments(), 2)
}

func TestTruncationWaitsForPendingIndexFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var block atomic.Bool
		flushing := make(chan struct{})
		release := make(chan struct{})
		syncer := DirectorySyncFunc(func(dir string) error {
			if block.CompareAndSwap(true, false) {
				close(flushing)
				<-release
			}
			return syncDir(dir)
		})
		w, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(1024), WithClearIndexOnFlush(), WithDirectorySyncer(syncer))
		require.NoError(t, err)
		defer w.Close()
		for i := uint64(1); i <= 3; i++ {
			_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 250), i)
			require.NoError(t, err)
		}
		block.Store(true)
		require.NoError(t, w.Current().SealSegment())
		<-flushing
		done := make(chan error, 1)
		go func() { done <- w.Truncate(2) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("truncation returned before pending flush completed: %v", err)
		default:
		}
		close(release)
		require.NoError(t, <-done)
		require.Len(t, w.Current().IndexEntries(), 2)
		_, err = w.Write(bytes.Repeat([]byte{3}, 250), 3)
		require.NoError(t, err)
		require.Len(t, w.Current().IndexEntries(), 3)
		assertTruncationPrefix(t, w, 3)
	})
}
