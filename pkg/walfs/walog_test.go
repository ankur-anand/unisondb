package walfs_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentManager_RecoverSegments_Sealing(t *testing.T) {
	dir := t.TempDir()
	ext := ".wal"

	seg1, err := walfs.OpenSegmentFile(dir, ext, 1)
	assert.NoError(t, err)
	_, err = seg1.Write([]byte("data1"), 0)
	assert.NoError(t, err)
	err = seg1.SealSegment()
	assert.NoError(t, err)
	assert.True(t, walfs.IsSealed(seg1.GetFlags()))
	assert.NoError(t, seg1.Close())

	seg2, err := walfs.OpenSegmentFile(dir, ext, 2)
	assert.NoError(t, err)
	_, err = seg2.Write([]byte("data2"), 0)
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg2.GetFlags()))
	assert.NoError(t, seg2.Close())

	seg3, err := walfs.OpenSegmentFile(dir, ext, 3)
	assert.NoError(t, err)
	_, err = seg3.Write([]byte("data3"), 0)
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg3.GetFlags()))
	assert.NoError(t, seg3.Close())

	manager, err := walfs.NewWALog(dir, ext, walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, manager.Close())
	})

	assert.Len(t, manager.Segments(), 3)

	assert.True(t, walfs.IsSealed(manager.Segments()[1].GetFlags()))
	assert.True(t, walfs.IsSealed(manager.Segments()[2].GetFlags()))
	assert.Equal(t, walfs.SegmentID(3), manager.Current().ID())
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()))
}

func TestSegmentManager_EmptyDirectory_CreatesInitialSegment(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)
	segments := manager.Segments()
	assert.Len(t, segments, 1, "should create one initial segment")
	current := manager.Current()
	assert.NotNil(t, current, "current segment should not be nil")
	assert.Equal(t, walfs.SegmentID(1), current.ID(), "initial segment ID should be 1")
	assert.False(t, walfs.IsSealed(current.GetFlags()), "initial segment should not be sealed")

	expectedPath := filepath.Join(tmpDir, "000000001.wal")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "expected WAL segment file to exist on disk")
}

func TestSegmentManager_SkipNonNumericSegments(t *testing.T) {
	tmpDir := t.TempDir()

	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "foo.wal"), []byte("dummy"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "123abc.wal"), []byte("dummy"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "!!invalid.wal"), []byte("dummy"), 0644))

	seg, err := walfs.OpenSegmentFile(tmpDir, ".wal", 42)
	assert.NoError(t, err)
	defer seg.Close()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	segments := manager.Segments()
	assert.Len(t, segments, 1, "only numeric segment should be recovered")

	current := manager.Current()
	assert.Equal(t, walfs.SegmentID(42), current.ID(), "numeric segment ID should be recovered")
}

func TestSegmentManager_RotateSegment(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("initial-data"), 0)
	assert.NoError(t, err)

	initial := manager.Current()
	initialID := initial.ID()
	assert.False(t, walfs.IsSealed(initial.GetFlags()))

	err = manager.RotateSegment()
	assert.NoError(t, err)

	assert.True(t, walfs.IsSealed(initial.GetFlags()), "previous segment should be sealed")
	assert.Equal(t, initialID+1, manager.Current().ID(), "new segment ID should be incremented")
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()), "new segment should not be sealed")
}

func TestSegmentManager_NewReader(t *testing.T) {
	dir := t.TempDir()

	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1<<10))
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	reader := manager.NewReader()
	assert.NotNil(t, reader)

	var seen []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		seen = append(seen, string(data))
	}

	assert.Equal(t, []string{"segment-1", "segment-2", "segment-3"}, seen)
}

func TestSegmentManager_NewReaderWithStart(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)

	for i := 1; i <= 3; i++ {
		seg := manager.Current()
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-1", i)), 0)
		assert.NoError(t, err)
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-2", i)), 0)
		assert.NoError(t, err)

		if i < 3 {
			assert.NoError(t, manager.RotateSegment())
		}
	}

	start := walfs.RecordPosition{SegmentID: 2, Offset: 0}
	reader, err := manager.NewReaderWithStart(start)
	assert.NotNil(t, reader)
	assert.NoError(t, err)

	var results []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		results = append(results, string(data))
	}

	expected := []string{
		"segment-2-entry-1",
		"segment-2-entry-2",
		"segment-3-entry-1",
		"segment-3-entry-2",
	}
	assert.Equal(t, expected, results)
}

func TestSegmentManager_NewReaderWithStart_Errors(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("record")
	pos, err := manager.Current().Write(data, 0)
	assert.NoError(t, err)

	badOffset := pos.Offset + 4096*1024
	_, err = manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: pos.SegmentID,
		Offset:    badOffset,
	})

	assert.ErrorIs(t, err, walfs.ErrOffsetOutOfBounds)

	_, err = manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: 9999,
		Offset:    0,
	})
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)

	reader, err := manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: pos.SegmentID,
		Offset:    0,
	})
	assert.NoError(t, err)

	val, next, readErr := reader.Next()
	assert.NoError(t, readErr)
	assert.Equal(t, data, val)
	assert.NotNil(t, next)
}

func TestSegmentManager_WriteWithRotation(t *testing.T) {
	tmpDir := t.TempDir()

	maxSegmentSize := int64(528 + 64 + 1)

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(maxSegmentSize))
	assert.NoError(t, err)

	data := make([]byte, 512)

	pos1, err := manager.Write(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, walfs.SegmentID(1), pos1.SegmentID)

	pos2, err := manager.Write(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, walfs.SegmentID(2), pos2.SegmentID)

	segments := manager.Segments()
	assert.Len(t, segments, 2)

	assert.True(t, walfs.IsSealed(segments[walfs.SegmentID(1)].GetFlags()))
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()))
	assert.Equal(t, walfs.SegmentID(2), manager.Current().ID())
}

func TestSegmentManager_Read_Errors(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("record")
	pos, err := manager.Write(data, 0)
	assert.NoError(t, err)

	_, err = manager.Read(walfs.RecordPosition{SegmentID: 9999, Offset: 0})
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)

	_, err = manager.Read(walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: 0})
	assert.ErrorIs(t, err, walfs.ErrOffsetBeforeHeader)

	badOffset := pos.Offset + 4096*1024
	_, err = manager.Read(walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: badOffset})
	assert.ErrorIs(t, err, walfs.ErrOffsetOutOfBounds)

	val, err := manager.Read(pos)
	assert.NoError(t, err)
	assert.Equal(t, data, val)
}

func TestSegmentManager_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("sync-test")
	_, err = manager.Write(data, 0)
	assert.NoError(t, err)

	err = manager.Sync()
	assert.NoError(t, err)
}

func TestSegmentManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("close-test")
	_, err = manager.Write(data, 0)
	assert.NoError(t, err)

	err = manager.Close()
	assert.NoError(t, err)

	//for _, seg := range manager.Segments() {
	//	assert.True(t, seg())
	//}
}

func TestSegmentManager_WithMSyncEveryWrite(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1024*1024),
		walfs.WithMSyncEveryWrite(true),
	)
	assert.NoError(t, err)

	data := []byte("msync-on-write")
	_, err = manager.Write(data, 0)
	assert.NoError(t, err)
}

func TestSegmentManager_WriteFailsOnClosedSegment(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	err = manager.Current().Close()
	assert.NoError(t, err)

	_, err = manager.Write([]byte("should fail"), 0)
	assert.Error(t, err)
}

func TestSegmentManager_Rotation_NoDataLoss(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(128))
	assert.NoError(t, err)

	entries := []string{}
	for i := 0; i < 50; i++ {
		payload := fmt.Sprintf("data-%d", i)
		entries = append(entries, payload)
		_, err := manager.Write([]byte(payload), 0)
		assert.NoError(t, err)
	}

	reader := manager.NewReader()
	var results []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		results = append(results, string(data))
	}

	assert.Equal(t, entries, results)
}

func TestSegmentManager_WriteRecordTooLarge(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer manager.Close()

	data := make([]byte, 1024)

	_, err = manager.Write(data, 0)
	assert.ErrorIs(t, err, walfs.ErrRecordTooLarge, "should fail with ErrRecordTooLarge")
}

func TestSegmentManager_WriteRecordWithByteSync(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(32*1024),
	)
	assert.NoError(t, err)
	data := make([]byte, 1024)
	for i := 0; i < 150; i++ {
		_, err = manager.Write(data, 0)
		assert.NoError(t, err)
	}

	minCall := (150 * (1024 + 8 + 8)) / (32 * 1024)
	assert.Equal(t, int64(minCall), manager.BytesPerSyncCallCount())
}

func TestSegmentManager_ConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(4096))
	assert.NoError(t, err)

	var wg sync.WaitGroup
	numWriters := 5
	numReaders := 3
	numRecords := 50
	writeData := []byte("stress-data")

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numRecords; j++ {
				_, err := manager.Write([]byte(fmt.Sprintf("%s-%d-%d", writeData, id, j)), 0)
				assert.NoError(t, err)
			}
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := manager.NewReader()
			for {
				data, _, err := reader.Next()
				if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
					break
				}
				assert.NoError(t, err)
				_ = data
			}
		}()
	}

	wg.Wait()
}

func TestWALog_ConcurrentWriteRead_WithSegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(
		tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<18),
		walfs.WithBytesPerSync(16*1024),
	)
	assert.NoError(t, err)

	const totalRecords = 50000
	dataTemplate := "entry-%05d"

	var written []string
	var writtenMu sync.Mutex

	go func() {
		for i := 0; i < totalRecords; i++ {
			payload := []byte(fmt.Sprintf(dataTemplate, i))
			_, err := manager.Write(payload, 0)
			assert.NoError(t, err)

			writtenMu.Lock()
			written = append(written, string(payload))
			writtenMu.Unlock()

			if i%200 == 0 {
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}
	}()

	var (
		readEntries  []string
		lastPosition walfs.RecordPosition
		retries      int
	)

retryRead:
	for {
		var reader *walfs.Reader
		if lastPosition.Offset == 0 {
			reader = manager.NewReader()
		} else {
			var err error
			reader, err = manager.NewReaderWithStart(lastPosition)
			assert.NoError(t, err)
			assert.NoError(t, reader.SeekNext())
		}

		for {
			data, pos, err := reader.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
				retries++
				time.Sleep(10 * time.Millisecond)
				continue retryRead
			}
			assert.NoError(t, err)
			assert.NotNil(t, pos)
			readEntries = append(readEntries, string(data))
			lastPosition = pos
			if len(readEntries) >= totalRecords {
				break retryRead
			}
		}
	}

	writtenMu.Lock()
	defer writtenMu.Unlock()

	assert.Equal(t, written, readEntries, "read data should match written in order")

	singleRecordSize := recordOverhead(int64(len([]byte(fmt.Sprintf(dataTemplate, totalRecords)))))
	totalSize := singleRecordSize * totalRecords
	rotationExpected := totalSize / (1 << 18)

	assert.Equal(t, rotationExpected, manager.SegmentRotatedCount())
	assert.GreaterOrEqual(t, retries, int(rotationExpected))
}

func TestManagerReader_LastRecordPosition(t *testing.T) {
	dir := t.TempDir()

	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer manager.Close()

	entriesSeg1 := [][]byte{
		[]byte("s1-record1"),
		[]byte("s1-record2"),
	}

	entriesSeg2 := [][]byte{
		[]byte("s2-record1"),
		[]byte("s2-record2"),
	}

	for _, entry := range entriesSeg1 {
		_, err := manager.Write(entry, 0)
		assert.NoError(t, err)
	}

	assert.NoError(t, manager.RotateSegment())

	for _, entry := range entriesSeg2 {
		_, err := manager.Write(entry, 0)
		assert.NoError(t, err)
	}

	reader := manager.NewReader()
	defer reader.Close()

	var allEntries [][]byte
	allEntries = append(allEntries, entriesSeg1...)
	allEntries = append(allEntries, entriesSeg2...)

	for i := 0; i < len(allEntries); i++ {
		data, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, allEntries[i], data)

		last := reader.LastRecordPosition()
		assert.NotNil(t, last)

		assert.Equal(t, pos.SegmentID, last.SegmentID)
		assert.LessOrEqual(t, last.Offset, pos.Offset)
	}

	data, pos, err := reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, walfs.NilRecordPosition)
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData))
}

func TestWALog_NewReaderAfter(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	var positions []*walfs.RecordPosition
	for i := 1; i <= 3; i++ {
		payload := []byte(fmt.Sprintf("entry-%d", i))
		pos, err := wal.Write(payload, 0)
		assert.NoError(t, err)
		positions = append(positions, &pos)
	}

	reader, err := wal.NewReaderAfter(*positions[0])
	assert.NoError(t, err)
	defer reader.Close()

	var actual []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		actual = append(actual, string(data))
	}

	expected := []string{"entry-2", "entry-3"}
	assert.Equal(t, expected, actual)
}

func TestReader_SeekNext(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	var positions []walfs.RecordPosition
	for i := 1; i <= 3; i++ {
		pos, err := wal.Write([]byte(fmt.Sprintf("entry-%d", i)), 0)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	reader, err := wal.NewReaderWithStart(positions[0])
	assert.NoError(t, err)
	defer reader.Close()

	err = reader.SeekNext()
	assert.NoError(t, err)

	data, pos, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, "entry-2", string(data))
	assert.Equal(t, positions[1], pos)
}

func TestReader_NextClosesOlderSegmentReaders(t *testing.T) {
	dir := t.TempDir()

	walog, err := walfs.NewWALog(
		dir, ".wal",
		walfs.WithMaxSegmentSize(1024),
		walfs.WithBytesPerSync(512),
	)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("entry-%03d", i))
		_, err := walog.Write(data, 0)
		assert.NoError(t, err)
	}

	segmentRefs := walog.Segments()
	currSegID := walog.Current().ID()

	reader := walog.NewReader()
	defer reader.Close()

	for {
		data, pos, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		assert.NotNil(t, pos)
		assert.True(t, bytes.HasPrefix(data, []byte("entry-")))
	}

	for _, seg := range segmentRefs {
		if seg.ID() != currSegID {
			assert.False(t, seg.HasActiveReaders())
		}
	}
}

func TestWALog_MarkSegmentsForDeletion(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("honour_max_segment", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 3, true))
		assert.NoError(t, err)
		for i := 0; i < 50; i++ {
			_, err := wal.Write(make([]byte, 1024*1024), 0)
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Len(t, segments, 47, "only 3 segment is allowed to be kept")
	})

	t.Run("honour_min_segment", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 10, 40, true))
		assert.NoError(t, err)
		for i := 0; i < 50; i++ {
			_, err := wal.Write(make([]byte, 1024*1024), 0)
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.GreaterOrEqual(t, len(segments), 10, "at least 10 segment is allowed to be kept")
	})

	t.Run("cleanup_disabled", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 3, false))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err := wal.Write(make([]byte, 1024*1024), 0)
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Len(t, segments, 0, "No segments should be queued when cleanup is disabled")
	})

	t.Run("latest_segment_not_deleted", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 2, true))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err := wal.Write(make([]byte, 1024*1024), 0)
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		currentID := wal.Current().ID()
		segments := wal.QueuedSegmentsForDeletion()

		_, queued := segments[currentID]
		assert.False(t, queued, "Current active segment should never be queued for deletion")
	})

	t.Run("age_based_deletion", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*10, 1, 100, true))
		assert.NoError(t, err)

		for i := 0; i < 5; i++ {
			_, err := wal.Write(make([]byte, 1024*1024), 0)
			assert.NoError(t, err)
		}
		time.Sleep(20 * time.Millisecond)

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Greater(t, len(segments), 0, "Old segments should be queued based on age")
	})
}

func TestWALog_StartPendingSegmentCleaner(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(2<<20),
		walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 5, true))
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := wal.Write(make([]byte, 1024*1024), 0)
		assert.NoError(t, err)
	}

	wal.MarkSegmentsForDeletion()

	canDeleteFn := func(segID walfs.SegmentID) bool {
		return true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal.StartPendingSegmentCleaner(ctx, 50*time.Millisecond, canDeleteFn)

	time.Sleep(500 * time.Millisecond)

	for id := range wal.QueuedSegmentsForDeletion() {
		segmentPath := walfs.SegmentFileName(tmpDir, ".wal", id)
		_, err := os.Stat(segmentPath)
		assert.True(t, os.IsNotExist(err), fmt.Sprintf("Segment file %s should have been deleted", segmentPath))
	}
}

func TestWALog_CleanupStalePendingSegments(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(2<<20),
		walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 5, true))
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := wal.Write(make([]byte, 1024*1024), 0)
		assert.NoError(t, err)
	}
	wal.MarkSegmentsForDeletion()

	deletionQueued := wal.QueuedSegmentsForDeletion()
	wal.CleanupStalePendingSegments()

	for segID := range deletionQueued {
		_, stillPending := wal.QueuedSegmentsForDeletion()[segID]
		assert.False(t, stillPending, "segment should be removed from pendingDeletion")
		_, stillInSegments := wal.Segments()[segID]
		assert.False(t, stillInSegments, "segment should be removed from segments map")
	}
}

func TestWALog_SegmentRotationCallback(t *testing.T) {
	tmpDir := t.TempDir()
	called := make(chan struct{})
	callback := func() {
		close(called)
	}

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithOnSegmentRotated(callback))
	assert.NoError(t, err)
	err = wal.RotateSegment()
	assert.NoError(t, err, "rotate segment should not return an error")
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Errorf("error waiting for the RotateSegment Callback")
	}

}

func TestWALog_SealedSegmentReturnsEOFAndHasNoActiveReaders(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024))
	assert.NoError(t, err)
	defer wal.Close()

	payload := []byte("hello-world")
	pos, err := wal.Write(payload, 0)
	assert.NoError(t, err)
	reader, err := wal.NewReaderWithStart(pos)
	assert.NoError(t, err)

	data, next, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.NotNil(t, next)

	data, next, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, next, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, walfs.ErrNoNewData)

	err = wal.RotateSegment()
	assert.NoError(t, err)

	data, next, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, next, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, io.EOF)

	reader.Close()

	seg1 := wal.Current().ID() - 1
	segMap := wal.Segments()
	seg, ok := segMap[seg1]
	assert.True(t, ok, "Segment 1 should still exist")
	assert.False(t, seg.HasActiveReaders(), "Segment 1 should have no active readers after Close")
	assert.True(t, walfs.IsSealed(seg.GetFlags()), "Segment 1 should be sealed after rotation")
}

func TestWALogReader_ErrNoNewDataOnActiveTail(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal")
	assert.NoError(t, err)
	defer wal.Close()

	payload := []byte("test-record")
	pos, err := wal.Write(payload, 0)
	assert.NoError(t, err)

	reader, err := wal.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: 0,
	})
	assert.NoError(t, err)
	defer reader.Close()

	data, current, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.Equal(t, pos.SegmentID, current.SegmentID)

	data, current, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, current, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, walfs.ErrNoNewData)
}

func TestRotateSegment_MarksInMemorySealed(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	assert.NoError(t, err)

	record := []byte("hello-wal")
	_, err = wal.Write(record, 0)
	assert.NoError(t, err)

	initialSegment := wal.Current()
	assert.NotNil(t, initialSegment)
	err = wal.RotateSegment()
	assert.NoError(t, err)

	rotated := wal.Current()
	assert.NotEqual(t, initialSegment.ID(), rotated.ID())
	assert.True(t, walfs.IsSealed(initialSegment.GetFlags()), "expected original segment to be sealed")
	assert.True(t, initialSegment.IsInMemorySealed(), "expected original segment to be in memory sealed")
}

func BenchmarkSegmentManager_Write_NoSync(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(0),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Write_WithMSyncEveryWrite(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(true),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Write_WithBytesPerSync(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(32*1024),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Read(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(b, err)

	payload := []byte("bench-read")
	var positions []walfs.RecordPosition
	for i := 0; i < b.N; i++ {
		pos, err := manager.Write(payload, 0)
		assert.NoError(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := manager.Read(positions[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func recordOverhead(dataLen int64) int64 {
	return alignUp(dataLen) + 8 + 8
}

func alignUp(n int64) int64 {
	return (n + 8) & ^7
}

func TestWALog_WriteBatch(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2-longer"),
		[]byte("record3-even-longer"),
		[]byte("r4"),
		[]byte("record5-medium-size"),
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	// Verify all records can be read back
	for i, pos := range positions {
		data, readErr := wal.Read(pos)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}
}

func TestWALog_WriteBatch_WithRotation(t *testing.T) {
	tmpDir := t.TempDir()
	// Create a small segment that will force rotation
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer wal.Close()

	// Create records that won't all fit in one segment
	records := make([][]byte, 20)
	for i := range records {
		records[i] = bytes.Repeat([]byte("x"), 50)
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions), "all records should be written across multiple segments")

	// Verify segment rotation occurred
	assert.Greater(t, wal.SegmentRotatedCount(), int64(0), "should have rotated at least once")

	// Verify all records can be read back from different segments
	for i, pos := range positions {
		data, readErr := wal.Read(pos)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}
}

func TestWALog_WriteBatch_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal")
	assert.NoError(t, err)
	defer wal.Close()

	positions, err := wal.WriteBatch([][]byte{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, positions)
}

func TestWALog_WriteBatch_RecordExceedsSegmentCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	// Create a small WALog with 1KB segment size
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024))
	assert.NoError(t, err)
	defer wal.Close()

	// Try to write a record that's too large to ever fit in any segment
	oversizedRecord := make([]byte, 2048)
	for i := range oversizedRecord {
		oversizedRecord[i] = byte(i % 256)
	}

	records := [][]byte{
		[]byte("small record that fits"),
		oversizedRecord, // This one is too large
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.ErrorIs(t, err, walfs.ErrRecordTooLarge)
	assert.Nil(t, positions, "no records should be written when one exceeds capacity")
}

func TestWALog_WriteBatch_EachRecordValidated(t *testing.T) {
	tmpDir := t.TempDir()
	// Create a small WALog
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(2048))
	assert.NoError(t, err)
	defer wal.Close()

	// Create multiple records where one in the middle is too large
	records := [][]byte{
		bytes.Repeat([]byte("a"), 50),
		bytes.Repeat([]byte("b"), 50),
		bytes.Repeat([]byte("c"), 5000), // Too large for the segment
		bytes.Repeat([]byte("d"), 50),
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.ErrorIs(t, err, walfs.ErrRecordTooLarge)
	assert.Nil(t, positions, "should fail early before writing anything")
}

func TestWALog_WriteBatch_WithBytesPerSync(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithBytesPerSync(1024))
	assert.NoError(t, err)
	defer wal.Close()

	// Create a batch that will trigger sync
	records := make([][]byte, 10)
	for i := range records {
		records[i] = bytes.Repeat([]byte("x"), 200)
	}

	_, err = wal.WriteBatch(records, nil)
	assert.NoError(t, err)

	// Verify sync was called
	assert.Greater(t, wal.BytesPerSyncCallCount(), int64(0))
}

func TestWALog_WriteBatch_SequentialReading(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer wal.Close()

	// Write multiple batches that will span segments
	batch1 := [][]byte{
		[]byte("batch1-record1"),
		[]byte("batch1-record2"),
		[]byte("batch1-record3"),
	}

	batch2 := [][]byte{
		[]byte("batch2-record1"),
		[]byte("batch2-record2"),
	}

	_, err = wal.WriteBatch(batch1, nil)
	assert.NoError(t, err)

	_, err = wal.WriteBatch(batch2, nil)
	assert.NoError(t, err)

	// Read all records sequentially
	reader := wal.NewReader()
	defer reader.Close()

	var allRecords [][]byte
	allRecords = append(allRecords, batch1...)
	allRecords = append(allRecords, batch2...)

	for i := 0; i < len(allRecords); i++ {
		data, _, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, allRecords[i], data)
	}

	// Verify no more records
	_, _, err = reader.Next()
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData))
}

func TestWALog_WriteBatch_ReadbackAcrossRotation(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024))
	assert.NoError(t, err)
	defer wal.Close()

	records := make([][]byte, 30)
	for i := range records {
		records[i] = bytes.Repeat([]byte{byte(i)}, 60)
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions), "all records should be written")

	assert.Greater(t, wal.SegmentRotatedCount(), int64(0), "should have rotated at least once")

	for i, pos := range positions {
		data, readErr := wal.Read(pos)
		assert.NoError(t, readErr, "record %d should be readable after rotation", i)
		assert.Equal(t, records[i], data, "record %d data should match after rotation", i)
	}

	segmentIDs := make(map[walfs.SegmentID]bool)
	for _, pos := range positions {
		segmentIDs[pos.SegmentID] = true
	}
	assert.Greater(t, len(segmentIDs), 1, "records should span multiple segments")
}

func TestWALog_WriteBatch_HeaderConsistencyAcrossSegments(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(800))
	assert.NoError(t, err)
	defer wal.Close()

	records := make([][]byte, 25)
	for i := range records {
		records[i] = []byte(fmt.Sprintf("record-%03d", i))
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	segmentRecords := make(map[walfs.SegmentID][]walfs.RecordPosition)
	for _, pos := range positions {
		segmentRecords[pos.SegmentID] = append(segmentRecords[pos.SegmentID], pos)
	}

	segments := wal.Segments()
	for segID, segPositions := range segmentRecords {
		seg := segments[segID]
		assert.NotNil(t, seg, "segment %d should exist", segID)

		entryCount := seg.GetEntryCount()
		assert.Equal(t, int64(len(segPositions)), entryCount,
			"segment %d entry count should match records written to it", segID)

		writeOffset := seg.WriteOffset()
		assert.Greater(t, writeOffset, int64(64), "segment %d write offset should be past header", segID)

		lastModified := seg.GetLastModifiedAt()
		assert.Greater(t, lastModified, int64(0), "segment %d should have valid timestamp", segID)

		for _, pos := range segPositions {
			_, _, err := seg.Read(pos.Offset)
			assert.NoError(t, err, "record at segment %d offset %d should be readable", segID, pos.Offset)
		}

		if segID != wal.Current().ID() {
			assert.True(t, walfs.IsSealed(seg.GetFlags()),
				"segment %d should be sealed since it's not current", segID)
		}
	}
}

func TestWALog_WriteBatch_SequentialReadAcrossRotation(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer wal.Close()

	records := make([][]byte, 20)
	for i := range records {
		records[i] = []byte(fmt.Sprintf("seq-record-%03d", i))
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	reader := wal.NewReader()
	defer reader.Close()

	for i := 0; i < len(records); i++ {
		data, pos, err := reader.Next()
		assert.NoError(t, err, "sequential read %d should succeed", i)
		assert.Equal(t, records[i], data, "sequential read %d data should match", i)
		assert.Equal(t, positions[i], pos, "sequential read %d position should match", i)
	}

	_, _, err = reader.Next()
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData),
		"should get EOF/ErrNoNewData after all records")
}

func TestWALog_WriteBatch_ReadbackAfterMultipleBatches(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024))
	assert.NoError(t, err)
	defer wal.Close()

	var allRecords [][]byte
	var allPositions []walfs.RecordPosition

	for batchNum := 0; batchNum < 5; batchNum++ {
		batch := make([][]byte, 8)
		for i := range batch {
			batch[i] = []byte(fmt.Sprintf("batch%d-rec%d", batchNum, i))
		}
		allRecords = append(allRecords, batch...)

		positions, err := wal.WriteBatch(batch, nil)
		assert.NoError(t, err, "batch %d should write successfully", batchNum)
		assert.Equal(t, len(batch), len(positions), "batch %d should write all records", batchNum)
		allPositions = append(allPositions, positions...)
	}

	for i, pos := range allPositions {
		data, err := wal.Read(pos)
		assert.NoError(t, err, "record %d from all batches should be readable", i)
		assert.Equal(t, allRecords[i], data, "record %d data should match original", i)
	}

	reader := wal.NewReader()
	defer reader.Close()

	count := 0
	for {
		_, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		count++
	}
	assert.Equal(t, len(allRecords), count, "reader should see all records")
}

func TestWALog_WriteBatch_ReadbackAtSegmentRotationBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	segmentSize := int64(600)
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(segmentSize))
	assert.NoError(t, err)
	defer wal.Close()

	records := make([][]byte, 15)
	for i := range records {
		records[i] = bytes.Repeat([]byte{byte(i)}, 70)
	}

	initialSegID := wal.Current().ID()
	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	finalSegID := wal.Current().ID()
	assert.Greater(t, finalSegID, initialSegID, "should have rotated to new segment")

	var boundaryIdx int = -1
	for i := 1; i < len(positions); i++ {
		if positions[i].SegmentID != positions[i-1].SegmentID {
			boundaryIdx = i
			break
		}
	}
	assert.NotEqual(t, -1, boundaryIdx, "should have found rotation boundary")

	beforeData, err := wal.Read(positions[boundaryIdx-1])
	assert.NoError(t, err, "record before boundary should be readable")
	assert.Equal(t, records[boundaryIdx-1], beforeData, "record before boundary should match")

	afterData, err := wal.Read(positions[boundaryIdx])
	assert.NoError(t, err, "record after boundary should be readable")
	assert.Equal(t, records[boundaryIdx], afterData, "record after boundary should match")

	oldSegID := positions[boundaryIdx-1].SegmentID
	oldSeg := wal.Segments()[oldSegID]
	assert.True(t, walfs.IsSealed(oldSeg.GetFlags()), "old segment should be sealed")

	oldSegRecordCount := 0
	for _, pos := range positions {
		if pos.SegmentID == oldSegID {
			oldSegRecordCount++
		}
	}
	assert.Equal(t, int64(oldSegRecordCount), oldSeg.GetEntryCount(),
		"sealed segment entry count should match actual records")
}

func TestWALog_WriteBatch_ReadbackWithBytesPerSync(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(2048),
		walfs.WithBytesPerSync(512))
	assert.NoError(t, err)
	defer wal.Close()

	records := make([][]byte, 15)
	for i := range records {
		records[i] = bytes.Repeat([]byte{byte(i)}, 100)
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	assert.Greater(t, wal.BytesPerSyncCallCount(), int64(0), "bytesPerSync should have triggered")

	for i, pos := range positions {
		data, err := wal.Read(pos)
		assert.NoError(t, err, "record %d should be readable after sync", i)
		assert.Equal(t, records[i], data, "record %d data should match", i)
	}
}

func TestWALog_WriteBatch_ReadbackEmptyBatch(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal")
	assert.NoError(t, err)
	defer wal.Close()

	initialRecords := [][]byte{[]byte("initial")}
	initialPos, err := wal.WriteBatch(initialRecords, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(initialPos))

	initialSegID := wal.Current().ID()

	emptyPos, err := wal.WriteBatch([][]byte{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, emptyPos)

	assert.Equal(t, initialSegID, wal.Current().ID(), "segment should not change for empty batch")

	data, err := wal.Read(initialPos[0])
	assert.NoError(t, err)
	assert.Equal(t, initialRecords[0], data)
}

func TestWALog_WriteBatch_ReadbackLargeRecords(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	records := [][]byte{
		bytes.Repeat([]byte("A"), 10000),
		bytes.Repeat([]byte("B"), 20000),
		bytes.Repeat([]byte("C"), 30000),
		bytes.Repeat([]byte("D"), 40000),
	}

	positions, err := wal.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), len(positions))

	for i, pos := range positions {
		data, err := wal.Read(pos)
		assert.NoError(t, err, "large record %d should be readable", i)
		assert.Equal(t, len(records[i]), len(data), "large record %d size should match", i)
		assert.Equal(t, records[i], data, "large record %d data should match", i)
	}
}

func TestWALog_WriteBatch_ReadbackAfterReopen(t *testing.T) {
	tmpDir := t.TempDir()

	records := [][]byte{
		[]byte("persistent-1"),
		[]byte("persistent-2"),
		[]byte("persistent-3"),
	}

	var positions []walfs.RecordPosition
	{
		wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
		assert.NoError(t, err)

		positions, err = wal.WriteBatch(records, nil)
		assert.NoError(t, err)
		assert.Equal(t, len(records), len(positions))

		assert.NoError(t, wal.Close())
	}

	{
		wal2, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
		assert.NoError(t, err)
		defer wal2.Close()

		for i, pos := range positions {
			data, err := wal2.Read(pos)
			assert.NoError(t, err, "record %d should be readable after reopen", i)
			assert.Equal(t, records[i], data, "record %d should persist after reopen", i)
		}

		reader := wal2.NewReader()
		defer reader.Close()

		for i := 0; i < len(records); i++ {
			data, _, err := reader.Next()
			assert.NoError(t, err, "sequential read %d should work after reopen", i)
			assert.Equal(t, records[i], data, "sequential read %d data should match", i)
		}
	}
}

func TestSegmentManager_BackupLastRotatedSegment(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("segment-1-data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())
	_, err = manager.Current().Write([]byte("segment-2-data"), 0)
	assert.NoError(t, err)

	backupDir := filepath.Join(t.TempDir(), "backups")
	backupPath, err := manager.BackupLastRotatedSegment(backupDir)
	assert.NoError(t, err)

	expectedPath := filepath.Join(backupDir, fmt.Sprintf("%09d.wal", 1))
	assert.Equal(t, expectedPath, backupPath)

	originalPath := filepath.Join(dir, fmt.Sprintf("%09d.wal", 1))
	original, err := os.ReadFile(originalPath)
	assert.NoError(t, err)

	backup, err := os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, original, backup, "backup contents should match original segment")
}

func TestSegmentManager_BackupLastRotatedSegment_NoRotation(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	backupDir := filepath.Join(t.TempDir(), "backups")
	_, err = manager.BackupLastRotatedSegment(backupDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)
}

func TestSegmentManager_BackupSegmentsAfter(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	for i := 0; i < 2; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")
	backups, err := manager.BackupSegmentsAfter(1, backupDir)
	assert.NoError(t, err)
	assert.Len(t, backups, 1)

	backupPath, ok := backups[2]
	assert.True(t, ok, "expected segment 2 to be backed up")

	originalPath := filepath.Join(dir, fmt.Sprintf("%09d.wal", 2))
	original, err := os.ReadFile(originalPath)
	assert.NoError(t, err)

	copied, err := os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, original, copied)
}

func TestSegmentManager_BackupSegmentsAfter_NoMatches(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("segment-1-data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "backups")
	_, err = manager.BackupSegmentsAfter(1, backupDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)
}

func BenchmarkBackupLastRotatedSegment(b *testing.B) {
	const segmentSizeBytes = 16 * 1024 * 1024

	dir := b.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(segmentSizeBytes))
	if err != nil {
		b.Fatalf("new walog: %v", err)
	}

	payload := bytes.Repeat([]byte("a"), 1024)
	seg := manager.Current()
	for !seg.WillExceed(len(payload)) {
		if _, err := seg.Write(payload, 0); err != nil {
			b.Fatalf("prepare segment: %v", err)
		}
	}

	if err := manager.RotateSegment(); err != nil {
		b.Fatalf("rotate segment: %v", err)
	}

	backupDir := filepath.Join(b.TempDir(), "backups")
	b.ReportAllocs()
	b.SetBytes(segmentSizeBytes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := manager.BackupLastRotatedSegment(backupDir); err != nil {
			b.Fatalf("backup: %v", err)
		}
	}
}

func TestBackupLastRotatedSegment_EmptyBackupDir(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	_, err = manager.BackupLastRotatedSegment("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backup directory cannot be empty")
}

func TestBackupLastRotatedSegment_SameDirectory(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	_, err = manager.BackupLastRotatedSegment(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backup destination must differ from WAL directory")
}

func TestBackupLastRotatedSegment_ReadOnlyBackupDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod doesn't work the same on Windows")
	}

	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "readonly")
	assert.NoError(t, os.MkdirAll(backupDir, 0o555))
	defer os.Chmod(backupDir, 0o755)

	_, err = manager.BackupLastRotatedSegment(backupDir)
	assert.Error(t, err)
}

func TestBackupSegmentsAfter_InvalidAfterID(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "backups")
	_, err = manager.BackupSegmentsAfter(999, backupDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)
}

func TestBackupLastRotatedSegment_MultipleRotations(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")
	backupPath, err := manager.BackupLastRotatedSegment(backupDir)
	assert.NoError(t, err)

	expectedPath := filepath.Join(backupDir, fmt.Sprintf("%09d.wal", 5))
	assert.Equal(t, expectedPath, backupPath)

	originalPath := filepath.Join(dir, fmt.Sprintf("%09d.wal", 5))
	original, err := os.ReadFile(originalPath)
	assert.NoError(t, err)

	backup, err := os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, original, backup)
}

func TestBackupSegmentsAfter_AllSegments(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	numSegments := 5
	for i := 0; i < numSegments; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d-data", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")

	backups, err := manager.BackupSegmentsAfter(0, backupDir)
	assert.NoError(t, err)

	assert.Len(t, backups, numSegments)

	for i := 1; i <= numSegments; i++ {
		segID := walfs.SegmentID(i)
		backupPath, ok := backups[segID]
		assert.True(t, ok, "segment %d should be in backup map", i)

		originalPath := filepath.Join(dir, fmt.Sprintf("%09d.wal", i))
		original, err := os.ReadFile(originalPath)
		assert.NoError(t, err)

		backup, err := os.ReadFile(backupPath)
		assert.NoError(t, err)
		assert.Equal(t, original, backup, "segment %d content mismatch", i)
	}
}

func TestBackupSegmentsAfter_RangeSelection(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")

	backups, err := manager.BackupSegmentsAfter(5, backupDir)
	assert.NoError(t, err)
	assert.Len(t, backups, 5)

	for i := 6; i <= 10; i++ {
		_, ok := backups[walfs.SegmentID(i)]
		assert.True(t, ok, "segment %d should be backed up", i)
	}

	for i := 1; i <= 5; i++ {
		_, ok := backups[walfs.SegmentID(i)]
		assert.False(t, ok, "segment %d should not be backed up", i)
	}
}

func TestBackup_OriginalSegmentUnmodified(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("important-data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	originalPath := filepath.Join(dir, fmt.Sprintf("%09d.wal", 1))
	originalInfo, err := os.Stat(originalPath)
	assert.NoError(t, err)
	originalModTime := originalInfo.ModTime()

	time.Sleep(10 * time.Millisecond)

	backupDir := filepath.Join(t.TempDir(), "backups")
	_, err = manager.BackupLastRotatedSegment(backupDir)
	assert.NoError(t, err)

	newInfo, err := os.Stat(originalPath)
	assert.NoError(t, err)
	assert.Equal(t, originalModTime.Unix(), newInfo.ModTime().Unix(),
		"original segment should not be modified")
}

func TestBackup_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("segment-1"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "backups")
	var wg sync.WaitGroup
	e := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := manager.BackupLastRotatedSegment(backupDir)
		if err != nil {
			e <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := manager.Current().Write([]byte(fmt.Sprintf("data-%d", i)), 0)
			if err != nil {
				e <- err
				return
			}
		}
	}()

	wg.Wait()
	close(e)

	for err := range e {
		t.Errorf("concurrent operation error: %v", err)
	}
}

func TestBackup_ConcurrentBackups(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	var wg sync.WaitGroup
	e := make(chan error, 3)

	for i := 0; i < 3; i++ {
		backupDir := filepath.Join(t.TempDir(), fmt.Sprintf("backups-%d", i))
		wg.Add(1)
		go func(dir string) {
			defer wg.Done()
			_, err := manager.BackupLastRotatedSegment(dir)
			if err != nil {
				e <- err
			}
		}(backupDir)
	}

	wg.Wait()
	close(e)

	for err := range e {
		t.Errorf("concurrent backup error: %v", err)
	}
}

func TestBackupSegmentsAfter_ConcurrentWithRotation(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)), 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")
	var wg sync.WaitGroup
	e := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := manager.BackupSegmentsAfter(0, backupDir)
		if err != nil {
			e <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			_, err := manager.Current().Write([]byte(fmt.Sprintf("new-segment-%d", i)), 0)
			if err != nil {
				e <- err
				return
			}
			if err := manager.RotateSegment(); err != nil {
				e <- err
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(e)

	for err := range e {
		t.Errorf("concurrent operation error: %v", err)
	}
}

func TestBackup_OverwriteExistingBackup(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("first-data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "backups")

	backupPath1, err := manager.BackupLastRotatedSegment(backupDir)
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("second-data"), 0)
	assert.NoError(t, err)
	assert.NoError(t, manager.RotateSegment())

	backupPath2, err := manager.BackupLastRotatedSegment(backupDir)
	assert.NoError(t, err)

	assert.NotEqual(t, backupPath1, backupPath2)
	_, err = os.Stat(backupPath1)
	assert.NoError(t, err)
	_, err = os.Stat(backupPath2)
	assert.NoError(t, err)
}

func TestBackup_LargeSegment(t *testing.T) {
	dir := t.TempDir()
	segmentSize := 10 * 1024 * 1024
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(int64(segmentSize)))
	assert.NoError(t, err)

	payload := bytes.Repeat([]byte("x"), 1024)
	seg := manager.Current()
	for !seg.WillExceed(len(payload)) {
		_, err := seg.Write(payload, 0)
		assert.NoError(t, err)
	}

	assert.NoError(t, manager.RotateSegment())

	backupDir := filepath.Join(t.TempDir(), "backups")
	start := time.Now()
	backupPath, err := manager.BackupLastRotatedSegment(backupDir)
	duration := time.Since(start)

	assert.NoError(t, err)
	t.Logf("Backed up %d MB in %v", segmentSize/(1024*1024), duration)

	info, err := os.Stat(backupPath)
	assert.NoError(t, err)
	minExpectedSize := int64(segmentSize * 9 / 10)
	assert.Greater(t, info.Size(), minExpectedSize, "backup should be near segment size")
}

func TestBackup_EmptySegment(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	assert.NoError(t, manager.RotateSegment())
	_, err = manager.Current().Write([]byte("data-in-segment-2"), 0)
	assert.NoError(t, err)

	backupDir := filepath.Join(t.TempDir(), "backups")
	backupPath, err := manager.BackupLastRotatedSegment(backupDir)

	if err == nil {
		info, err := os.Stat(backupPath)
		assert.NoError(t, err)
		t.Logf("Empty segment backup size: %d bytes", info.Size())
	}
}

func TestBackupSegmentsAfter_VerifyOrdering(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	numSegments := 10
	for i := 0; i < numSegments; i++ {
		data := []byte(fmt.Sprintf("segment-%02d-content", i+1))
		_, err = manager.Current().Write(data, 0)
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	backupDir := filepath.Join(t.TempDir(), "backups")
	backups, err := manager.BackupSegmentsAfter(0, backupDir)
	assert.NoError(t, err)

	var segmentIDs []int
	for id := range backups {
		segmentIDs = append(segmentIDs, int(id))
	}
	sort.Ints(segmentIDs)

	for i := 0; i < len(segmentIDs)-1; i++ {
		assert.Less(t, segmentIDs[i], segmentIDs[i+1], "segments should be in order")
	}
}

type recordingSyncer struct {
	mu    sync.Mutex
	calls []string
}

func newRecordingSyncer() *recordingSyncer {
	return &recordingSyncer{}
}

func (r *recordingSyncer) SyncDir(dir string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, dir)
	return nil
}

func (r *recordingSyncer) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recordingSyncer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func TestWALogSyncsDirectoryOnlyForNewSegments(t *testing.T) {
	dir := t.TempDir()

	syncer := newRecordingSyncer()

	wal, err := walfs.NewWALog(dir, ".wal", walfs.WithDirectorySyncer(syncer))
	require.NoError(t, err)
	require.Equal(t, []string{dir}, syncer.Calls())

	require.NoError(t, wal.RotateSegment())
	require.Equal(t, []string{dir, dir}, syncer.Calls())
	require.NoError(t, wal.Close())
	require.Equal(t, []string{dir, dir, dir}, syncer.Calls())

	syncer.Reset()

	walRecovered, err := walfs.NewWALog(dir, ".wal", walfs.WithDirectorySyncer(syncer))
	require.NoError(t, err)
	require.Len(t, syncer.Calls(), 0)
	require.NoError(t, walRecovered.Close())
	require.Equal(t, []string{dir}, syncer.Calls())
}

func TestSegmentDeletionSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	syncer := newRecordingSyncer()

	wal, err := walfs.NewWALog(dir, ".wal", walfs.WithDirectorySyncer(syncer))
	require.NoError(t, err)
	defer wal.Close()

	_, err = wal.Write([]byte("hello"), 0)
	require.NoError(t, err)
	require.NoError(t, wal.RotateSegment())

	before := len(syncer.Calls())

	segments := wal.Segments()
	seg := segments[1]
	seg.MarkForDeletion()

	require.Equal(t, before+1, len(syncer.Calls()))
	require.Equal(t, dir, syncer.Calls()[len(syncer.Calls())-1])
}

func TestBackupSyncsDestinationDirectory(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "backup")
	syncer := newRecordingSyncer()

	wal, err := walfs.NewWALog(dir, ".wal", walfs.WithDirectorySyncer(syncer))
	require.NoError(t, err)
	defer wal.Close()

	_, err = wal.Write([]byte("hello"), 0)
	require.NoError(t, err)
	require.NoError(t, wal.RotateSegment())

	before := len(syncer.Calls())

	_, err = wal.BackupLastRotatedSegment(backupDir)
	require.NoError(t, err)

	calls := syncer.Calls()
	require.Equal(t, before+1, len(calls))
	require.Equal(t, backupDir, calls[len(calls)-1])
}

func TestCloseSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	syncer := newRecordingSyncer()

	wal, err := walfs.NewWALog(dir, ".wal", walfs.WithDirectorySyncer(syncer))
	require.NoError(t, err)
	require.Equal(t, []string{dir}, syncer.Calls())

	require.NoError(t, wal.Close())

	require.Equal(t, []string{dir, dir}, syncer.Calls())
}

func TestWALSegmentIndexCreationAndRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal")
	require.NoError(t, err)

	payloads := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
	}

	for _, data := range payloads {
		_, err := wal.Write(data, 0)
		require.NoError(t, err)
	}

	sealed := wal.Segments()[1]
	require.NotNil(t, sealed)
	require.NoError(t, wal.RotateSegment())
	sealed.WaitForIndexFlush()
	require.NoError(t, wal.Close())

	indexPath := walfs.SegmentIndexFileName(tmpDir, ".wal", 1)
	info, err := os.Stat(indexPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))

	indexBytes, err := os.ReadFile(indexPath)
	require.NoError(t, err)
	require.NotZero(t, len(indexBytes))
	perEntry := len(indexBytes) / len(payloads)
	assert.Equal(t, 16, perEntry, "each index entry should be 16 bytes")
	firstOffset := binary.LittleEndian.Uint64(indexBytes[:8])
	assert.Equal(t, uint64(64), firstOffset, "first record should start after header")

	require.NoError(t, os.Remove(indexPath))

	wal2, err := walfs.NewWALog(tmpDir, ".wal")
	require.NoError(t, err)
	require.NoError(t, wal2.Close())

	info, err = os.Stat(indexPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(payloads)*16), info.Size(), "rebuild should recreate index entries")
}

func TestSegmentIndexCreatedOnlyAfterSeal(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	_, err = wal.Write([]byte("hot"), 0)
	require.NoError(t, err)
	require.NoError(t, wal.Close())

	idxPath := walfs.SegmentIndexFileName(dir, ".wal", 1)
	_, err = os.Stat(idxPath)
	require.ErrorIs(t, err, os.ErrNotExist, "active segment should not flush index")

	wal2, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	_, err = wal2.Write([]byte("seal-me"), 0)
	require.NoError(t, err)
	require.NoError(t, wal2.RotateSegment())
	wal2.Segments()[1].WaitForIndexFlush()
	require.NoError(t, wal2.Close())

	_, err = os.Stat(idxPath)
	require.NoError(t, err, "sealed segment must flush index file")
}

func TestSegmentIndexRebuildWithoutFile(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	_, err = wal.Write([]byte("alpha"), 0)
	require.NoError(t, err)
	require.NoError(t, wal.RotateSegment())
	wal.Segments()[1].WaitForIndexFlush()
	require.NoError(t, wal.Close())

	idxPath := walfs.SegmentIndexFileName(dir, ".wal", 1)
	require.NoError(t, os.Remove(idxPath), "simulate missing index file")

	wal2, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	wal2.Segments()[1].WaitForIndexFlush()
	require.NoError(t, wal2.Close())

	info, err := os.Stat(idxPath)
	require.NoError(t, err, "recovery should rebuild missing index")
	assert.Greater(t, info.Size(), int64(0))
}

func TestSegmentIndexAPIExposesEntriesForAllSegments(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)

	payloads := [][]byte{[]byte("first"), []byte("second")}
	for _, data := range payloads {
		_, err := wal.Write(data, 0)
		require.NoError(t, err)
	}

	index, err := wal.SegmentIndex(1)
	require.NoError(t, err)
	require.Len(t, index, len(payloads))
	assert.Equal(t, uint32(len(payloads[0])), index[0].Length)
	assert.Equal(t, walfs.SegmentID(1), index[0].SegmentID)
	assert.Greater(t, index[1].Offset, index[0].Offset)

	require.NoError(t, wal.RotateSegment())

	_, err = wal.Write([]byte("third"), 0)
	require.NoError(t, err)

	indexNew, err := wal.SegmentIndex(2)
	require.NoError(t, err)
	require.Len(t, indexNew, 1)
	assert.Equal(t, uint32(len("third")), indexNew[0].Length)
	assert.Equal(t, walfs.SegmentID(2), indexNew[0].SegmentID)

	require.NoError(t, wal.Close())
}

func TestSegmentIndexEntriesAllowReadingFromOffsets(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)

	payloads := [][]byte{[]byte("alpha"), []byte("beta")}
	for _, data := range payloads {
		_, err := wal.Write(data, 0)
		require.NoError(t, err)
	}

	index, err := wal.SegmentIndex(1)
	require.NoError(t, err)
	require.Len(t, index, len(payloads))

	seg := wal.Segments()[1]
	for i, entry := range index {
		buf, _, err := seg.Read(entry.Offset)
		require.NoError(t, err)
		assert.Equal(t, payloads[i], append([]byte(nil), buf...))
	}

	require.NoError(t, wal.RotateSegment())

	_, err = wal.Write([]byte("gamma"), 0)
	require.NoError(t, err)

	index2, err := wal.SegmentIndex(2)
	require.NoError(t, err)
	require.Len(t, index2, 1)
	seg2 := wal.Segments()[2]
	buf, _, err := seg2.Read(index2[0].Offset)
	require.NoError(t, err)
	assert.Equal(t, []byte("gamma"), append([]byte(nil), buf...))

	require.NoError(t, wal.Close())
}

func TestSegmentIndexEntriesMatchWriteOrder(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)

	var positions []walfs.RecordPosition
	for i := 0; i < 5; i++ {
		pos, err := wal.Write([]byte(fmt.Sprintf("value-%d", i)), 0)
		require.NoError(t, err)
		positions = append(positions, pos)
	}

	index, err := wal.SegmentIndex(1)
	require.NoError(t, err)
	require.Len(t, index, len(positions))

	for i, entry := range index {
		assert.Equal(t, positions[i].SegmentID, entry.SegmentID)
		assert.Equal(t, positions[i].Offset, entry.Offset)
	}

	require.NoError(t, wal.Close())
}

func TestSegmentIndexConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)

	done := make(chan struct{})
	var wg sync.WaitGroup

	reader := func(segID walfs.SegmentID) {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				idx, err := wal.SegmentIndex(segID)
				if err == nil && len(idx) > 0 {
					require.Equal(t, segID, idx[len(idx)-1].SegmentID)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go reader(1)
	}

	for i := 0; i < 200; i++ {
		_, err := wal.Write([]byte(fmt.Sprintf("entry-%d", i)), 0)
		require.NoError(t, err)
	}

	close(done)
	wg.Wait()
	require.NoError(t, wal.Close())
}

func TestSegmentCleanupRemovesDataAndIndex(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := wal.Write([]byte(fmt.Sprintf("entry-%d", i)), 0)
		require.NoError(t, err)
	}

	require.NoError(t, wal.RotateSegment())
	wal.Segments()[1].WaitForIndexFlush()

	segmentPath := walfs.SegmentFileName(dir, ".wal", 1)
	indexPath := walfs.SegmentIndexFileName(dir, ".wal", 1)

	seg := wal.Segments()[1]
	seg.MarkForDeletion()

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist, "segment data file should be deleted")

	_, err = os.Stat(indexPath)
	require.ErrorIs(t, err, os.ErrNotExist, "segment index file should be deleted")

	require.NoError(t, wal.Close())
}

func TestSegmentForIndexReturnsCorrectEntry(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	payloads := [][]byte{[]byte("idx-1"), []byte("idx-2"), []byte("idx-3")}
	for i, data := range payloads[:2] {
		_, err := wal.Write(data, uint64(i+1))
		require.NoError(t, err)
	}
	require.NoError(t, wal.RotateSegment())

	_, err = wal.Write(payloads[2], 3)
	require.NoError(t, err)

	tests := []struct {
		index   uint64
		payload []byte
	}{
		{1, payloads[0]},
		{2, payloads[1]},
		{3, payloads[2]},
	}

	for _, tt := range tests {
		segID, slot, err := wal.SegmentForIndex(tt.index)
		require.NoError(t, err)
		idxEntries, err := wal.SegmentIndex(segID)
		require.NoError(t, err)
		require.Greater(t, len(idxEntries), slot)

		entry := idxEntries[slot]
		seg := wal.Segments()[segID]
		data, _, err := seg.Read(entry.Offset)
		require.NoError(t, err)
		assert.Equal(t, tt.payload, append([]byte(nil), data...))
	}

	segID, slot, err := wal.SegmentForIndex(2)
	require.NoError(t, err)
	idx := wal.Segments()[segID]
	offset := idx.IndexEntries()[slot].Offset
	record, _, err := idx.Read(offset)
	require.NoError(t, err)
	assert.Equal(t, payloads[1], append([]byte(nil), record...))
}

func TestSegmentForIndexConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx := uint64(1)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = wal.Write([]byte("data"), idx)
				idx++
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		_, _, _ = wal.SegmentForIndex(1)
	}

	close(stop)
	wg.Wait()
}
