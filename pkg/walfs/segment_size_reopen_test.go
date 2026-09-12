package walfs

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReopenPreservesSegmentSizes(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		for _, size := range []int64{1024, 4096, 8192} {
			t.Run(fmt.Sprintf("sealed=%t/size=%d", sealed, size), func(t *testing.T) {
				dir := t.TempDir()
				w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(4096), WithClearIndexOnFlush())
				require.NoError(t, err)
				for i := uint64(1); i <= 12; i++ {
					_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 250), i)
					require.NoError(t, err)
				}
				if sealed {
					require.NoError(t, w.RotateSegment())
				}
				require.NoError(t, w.Close())
				w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(size), WithClearIndexOnFlush())
				require.NoError(t, err)
				for _, seg := range w.Segments() {
					require.Equal(t, int64(4096), seg.GetSegmentSize())
					info, err := os.Stat(seg.path)
					require.NoError(t, err)
					require.Equal(t, int64(4096), info.Size())
				}
				assertTruncationPrefix(t, w, 12)
				require.NoError(t, w.RotateSegment())
				require.Equal(t, size, w.Current().GetSegmentSize())
				_, err = w.Write(bytes.Repeat([]byte{13}, 250), 13)
				require.NoError(t, err)
				require.NoError(t, w.Close())
				w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(4096), WithClearIndexOnFlush())
				require.NoError(t, err)
				defer w.Close()
				assertTruncationPrefix(t, w, 13)
			})
		}
	}
}

func TestReopenUsesExistingActiveCapacity(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(4096))
			require.NoError(t, err)
			_, err = w.Write([]byte("first"), 1)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
			require.NoError(t, err)
			defer w.Close()
			data := bytes.Repeat([]byte("a"), 2000)
			if batch {
				_, err = w.WriteBatch([][]byte{data}, []uint64{2})
			} else {
				_, err = w.Write(data, 2)
			}
			require.NoError(t, err)
			require.Equal(t, SegmentID(1), w.Current().ID())
			if batch {
				_, err = w.WriteBatch([][]byte{data}, []uint64{3})
			} else {
				_, err = w.Write(data, 3)
			}
			require.ErrorIs(t, err, ErrRecordTooLarge)
			require.Equal(t, SegmentID(1), w.Current().ID())
			require.NoError(t, w.RotateSegment())
			require.Equal(t, int64(1024), w.Current().GetSegmentSize())
		})
	}
}

func TestOpenRejectsInvalidExistingSizeWithoutResizing(t *testing.T) {
	for _, size := range []int{0, 20, segmentHeaderSize - 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			path := SegmentFileName(dir, ".wal", 1)
			original := bytes.Repeat([]byte{0x7a}, size)
			require.NoError(t, os.WriteFile(path, original, 0644))
			_, err := OpenSegmentFile(dir, ".wal", 1, WithSegmentSize(1024))
			require.Error(t, err)
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, original, actual)
		})
	}
}
