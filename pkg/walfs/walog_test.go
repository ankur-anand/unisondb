package walfs_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
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
	_, err = seg1.Write([]byte("data1"))
	assert.NoError(t, err)
	err = seg1.SealSegment()
	assert.NoError(t, err)
	assert.True(t, walfs.IsSealed(seg1.GetFlags()))
	assert.NoError(t, seg1.Close())

	seg2, err := walfs.OpenSegmentFile(dir, ext, 2)
	assert.NoError(t, err)
	_, err = seg2.Write([]byte("data2"))
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg2.GetFlags()))
	assert.NoError(t, seg2.Close())

	seg3, err := walfs.OpenSegmentFile(dir, ext, 3)
	assert.NoError(t, err)
	_, err = seg3.Write([]byte("data3"))
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg3.GetFlags()))
	assert.NoError(t, seg3.Close())

	manager, err := walfs.NewWALog(dir, ext, walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

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

	_, err = manager.Current().Write([]byte("initial-data"))
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
		_, err := manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)))
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	reader := manager.NewReader()
	assert.NotNil(t, reader)

	var seen []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) {
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
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-1", i)))
		assert.NoError(t, err)
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-2", i)))
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
		if err == io.EOF {
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
	pos, err := manager.Current().Write(data)
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

	pos1, err := manager.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, walfs.SegmentID(1), pos1.SegmentID)

	pos2, err := manager.Write(data)
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
	pos, err := manager.Write(data)
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
	_, err = manager.Write(data)
	assert.NoError(t, err)

	err = manager.Sync()
	assert.NoError(t, err)
}

func TestSegmentManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("close-test")
	_, err = manager.Write(data)
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
	_, err = manager.Write(data)
	assert.NoError(t, err)
}

func TestSegmentManager_WriteFailsOnClosedSegment(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	err = manager.Current().Close()
	assert.NoError(t, err)

	_, err = manager.Write([]byte("should fail"))
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
		_, err := manager.Write([]byte(payload))
		assert.NoError(t, err)
	}

	reader := manager.NewReader()
	var results []string
	for {
		data, _, err := reader.Next()
		if err == io.EOF {
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

	_, err = manager.Write(data)
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
		_, err = manager.Write(data)
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
				_, err := manager.Write([]byte(fmt.Sprintf("%s-%d-%d", writeData, id, j)))
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
				if err == io.EOF {
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
	require.NoError(t, err)

	const totalRecords = 50000
	dataTemplate := "entry-%05d"

	var written []string
	var writtenMu sync.Mutex

	go func() {
		for i := 0; i < totalRecords; i++ {
			payload := []byte(fmt.Sprintf(dataTemplate, i))
			_, err := manager.Write(payload)
			require.NoError(t, err)

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
		lastPosition *walfs.RecordPosition
		retries      int
	)

retryRead:
	for {
		var reader *walfs.Reader
		if lastPosition == nil {
			reader = manager.NewReader()
		} else {
			var err error
			reader, err = manager.NewReaderWithStart(*lastPosition)
			require.NoError(t, err)
		}

		for {
			data, next, err := reader.Next()
			if errors.Is(err, io.EOF) {
				retries++
				time.Sleep(10 * time.Millisecond)
				continue retryRead
			}
			require.NoError(t, err)
			readEntries = append(readEntries, string(data))
			lastPosition = next
			if len(readEntries) >= totalRecords {
				break retryRead
			}
		}
	}

	writtenMu.Lock()
	defer writtenMu.Unlock()

	require.Equal(t, written, readEntries, "read data should match written in order")

	singleRecordSize := recordOverhead(int64(len([]byte(fmt.Sprintf(dataTemplate, totalRecords)))))
	totalSize := singleRecordSize * totalRecords
	rotationExpected := totalSize / (1 << 18)

	assert.Equal(t, rotationExpected, manager.SegmentRotatedCount())
	assert.GreaterOrEqual(t, retries, int(rotationExpected))
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
		if _, err := manager.Write(data); err != nil {
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
		if _, err := manager.Write(data); err != nil {
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
		if _, err := manager.Write(data); err != nil {
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
		pos, err := manager.Write(payload)
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
