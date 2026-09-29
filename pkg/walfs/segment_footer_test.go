package walfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// edgeLens are payload lengths around alignment, the sparse interval and the
// record overhead, where off-by-one errors in the reserve would show up.
var edgeLens = []int{0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 4079, 4080, 4081, 4095, 4096, 4097, 8191, 8192, 8193}

// Mirrors pkg/walfs/tla/WalFooterSpace.tla: for any sequence of record sizes
// that the limit accepts, the footer the seal would write fits in the file.
func TestMaxFooterSizeBoundsEveryRecordSequence(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for _, size := range []int64{minSegmentSize(), 128, 512, 1024, 4096, 4096 + 64, 8192 + 1, 64 << 10, 1 << 20, 16 << 20} {
		for trial := 0; trial < 50; trial++ {
			limit := recordLimitFor(size)
			var offsets []int64
			end := int64(segmentHeaderSize)
			for {
				var n int
				switch trial % 4 {
				case 0:
					n = 0 // worst case for sample count
				case 1:
					n = edgeLens[rnd.Intn(len(edgeLens))]
				case 2:
					n = rnd.Intn(64)
				default:
					n = rnd.Intn(int(size / 2))
				}
				if end+recordOverhead(int64(n)) > limit {
					break
				}
				offsets = append(offsets, end)
				end += recordOverhead(int64(n))
			}
			s := buildSparseIndex(uint64(len(offsets)), func(i uint64) int64 { return offsets[i] })
			require.LessOrEqual(t, footerSize(s), maxFooterSize(size), "size %d trial %d", size, trial)
			require.LessOrEqual(t, end+footerSize(s), size, "size %d trial %d", size, trial)
		}
	}
}

// Fill segments of many sizes to the brim with mixed record sizes through the
// WAL, then check every sealed footer and every record after reopening.
func TestFooterReserve_FillSegmentsToTheBrim(t *testing.T) {
	rnd := rand.New(rand.NewSource(2))
	for _, size := range []int64{minSegmentSize(), 128, 512, 4096, 4096 + 64, 64 << 10, 1 << 20} {
		dir := t.TempDir()
		opts := []WALogOptions{WithMaxSegmentSize(size)}
		w, err := NewWALog(dir, ".wal", opts...)
		require.NoError(t, err)

		maxPayload := int(recordLimitFor(size) - segmentHeaderSize - recordOverhead(0))
		var payloads [][]byte
		for lsn := uint64(1); len(w.Segments()) < 5 && lsn < 5000; lsn++ {
			var n int
			switch rnd.Intn(3) {
			case 0:
				n = edgeLens[rnd.Intn(len(edgeLens))]
			case 1:
				n = rnd.Intn(32)
			default:
				n = rnd.Intn(maxPayload + 1)
			}
			if n > maxPayload {
				n = maxPayload
			}
			p := bytes.Repeat([]byte{byte(lsn)}, n)
			_, err := w.Write(p, lsn)
			require.NoError(t, err, "size %d lsn %d len %d", size, lsn, n)
			payloads = append(payloads, p)
		}
		for id, seg := range w.Segments() {
			if !seg.IsSealed() {
				continue
			}
			s := seg.sparse.Load()
			require.NotNil(t, s, "segment %d", id)
			require.LessOrEqual(t, seg.WriteOffset()+footerSize(s), size, "segment %d", id)
			require.LessOrEqual(t, seg.WriteOffset(), recordLimitFor(size), "segment %d", id)
		}
		require.NoError(t, w.Close())

		w, err = NewWALog(dir, ".wal", opts...)
		require.NoError(t, err, "size %d", size)
		for i, want := range payloads {
			pos, err := w.PositionForIndex(uint64(i + 1))
			require.NoError(t, err, "size %d lsn %d", size, i+1)
			got, err := w.Read(pos)
			require.NoError(t, err, "size %d lsn %d", size, i+1)
			require.Equal(t, want, got, "size %d lsn %d", size, i+1)
		}
		require.NoError(t, w.Close())
	}
}

// A record that ends 8 bytes before, exactly at, or 8 bytes past the limit.
func TestFooterReserve_RecordBoundaries(t *testing.T) {
	const size = 4096
	limit := recordLimitFor(size)
	for _, delta := range []int64{-8, 0, 8} {
		seg, err := OpenSegmentFile(t.TempDir(), ".wal", 1, WithSegmentSize(size))
		require.NoError(t, err)
		// One record whose end is limit+delta.
		n := limit + delta - segmentHeaderSize - recordOverhead(0)
		fits := delta <= 0
		require.Equal(t, !fits, seg.WillExceed(int(n)), "delta %d", delta)
		_, err = seg.Write(make([]byte, n), 1)
		if fits {
			require.NoError(t, err, "delta %d", delta)
			require.Equal(t, limit+delta, seg.WriteOffset())
			require.NoError(t, seg.SealSegment())
			require.LessOrEqual(t, seg.WriteOffset()+footerSize(seg.sparse.Load()), int64(size))
		} else {
			require.Error(t, err, "delta %d", delta)
		}
		require.NoError(t, seg.Close())
	}
}

func TestLargestRecordFitsAndOneMoreIsRejected(t *testing.T) {
	const size = 64 << 10
	largest := int(recordLimitFor(size) - segmentHeaderSize - recordOverhead(0))
	for _, batch := range []bool{false, true} {
		w, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(size))
		require.NoError(t, err)
		write := func(n int, lsn uint64) error {
			if batch {
				_, err := w.WriteBatch([][]byte{make([]byte, n)}, []uint64{lsn})
				return err
			}
			_, err := w.Write(make([]byte, n), lsn)
			return err
		}
		require.NoError(t, write(largest, 1), "batch=%t", batch)
		require.ErrorIs(t, write(largest+8, 2), ErrRecordTooLarge, "batch=%t", batch)
		require.NoError(t, write(largest, 2), "batch=%t: rotates into a fresh segment", batch)
		require.NoError(t, w.Close())
	}
}

func TestMinSegmentSize(t *testing.T) {
	min := minSegmentSize()
	require.Equal(t, int64(120), min)

	_, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(min-1))
	require.Error(t, err)

	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(min))
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 3; lsn++ {
		_, err = w.Write(nil, lsn)
		require.NoError(t, err)
	}
	require.Len(t, w.Segments(), 3, "each empty record fills a minimum segment")
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(min))
	require.NoError(t, err)
	defer w.Close()
	_, last := w.GetBounds()
	require.Equal(t, uint64(3), last)
}

// Empty records produce the most samples per byte.
func TestFooterWorstCaseSampleCount(t *testing.T) {
	const size = 1 << 20
	seg, err := OpenSegmentFile(t.TempDir(), ".wal", 1, WithSegmentSize(size))
	require.NoError(t, err)
	defer seg.Close()
	var lsn uint64
	for !seg.WillExceed(0) {
		lsn++
		_, err := seg.Write(nil, lsn)
		require.NoError(t, err)
	}
	require.NoError(t, seg.SealSegment())
	s := seg.sparse.Load()
	require.Equal(t, lsn, s.count)
	require.LessOrEqual(t, footerSize(s), maxFooterSize(size))
	require.Greater(t, footerSize(s), maxFooterSize(size)-2*footerEntrySize, "worst case should approach the bound")
}

func setSealedFlag(t *testing.T, path string, sealed bool) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	h := make([]byte, segmentHeaderSize)
	_, err = f.ReadAt(h, 0)
	require.NoError(t, err)
	flags := binary.LittleEndian.Uint32(h[40:44])
	if sealed {
		flags = flags&^FlagActive | FlagSealed
	} else {
		flags = flags&^FlagSealed | FlagActive
	}
	binary.LittleEndian.PutUint32(h[40:44], flags)
	binary.LittleEndian.PutUint32(h[56:60], crc32.Checksum(h[0:56], crcTable))
	_, err = f.WriteAt(h, 0)
	require.NoError(t, err)
}

// Crash after the footer is durable but before the sealed header: the segment
// is unsealed, and recovery must stop at the footer and clear it.
func TestFooterNeverParsesAsRecord(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 5; lsn++ {
		_, err = seg.Write([]byte("record"), lsn)
		require.NoError(t, err)
	}
	end := seg.WriteOffset()
	require.NoError(t, seg.SealSegment())
	require.NoError(t, seg.Close())
	setSealedFlag(t, SegmentFileName(dir, ".wal", 1), false)

	seg, err = OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	require.False(t, seg.IsSealed())
	require.Equal(t, end, seg.WriteOffset(), "scan stops exactly at the footer")
	require.Equal(t, uint64(5), seg.indexedCount())
	require.Equal(t, make([]byte, footerHeadSize), []byte(seg.mmapData[end:end+footerHeadSize]), "footer cleared")
	require.NoError(t, seg.Close())
}

// Same crash on a segment that is no longer last: recovery seals it again.
func TestUnsealedMiddleSegmentWithFooterIsResealed(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 3; lsn++ {
		_, err = w.Write([]byte("seg-1"), lsn)
		require.NoError(t, err)
	}
	require.NoError(t, w.RotateSegment())
	_, err = w.Write([]byte("seg-2"), 4)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	setSealedFlag(t, SegmentFileName(dir, ".wal", 1), false)

	for range 2 {
		w, err = NewWALog(dir, ".wal")
		require.NoError(t, err)
		require.True(t, w.Segments()[1].IsSealed())
		first, last := w.GetBounds()
		require.Equal(t, uint64(1), first)
		require.Equal(t, uint64(4), last)
		got, err := readAllRecords(w)
		require.NoError(t, err)
		require.Equal(t, []string{"seg-1", "seg-1", "seg-1", "seg-2"}, got)
		require.NoError(t, w.Close())
	}
}

// A torn or damaged footer on a sealed segment is corruption, never a
// shorter log.
func TestFooterCorruptionIsLoud(t *testing.T) {
	cases := map[string]func(data []byte, end int64){
		"magic":         func(d []byte, e int64) { d[e] ^= 0xFF },
		"sentinel":      func(d []byte, e int64) { d[e+4] ^= 0xFF },
		"record_count":  func(d []byte, e int64) { d[e+8] ^= 0x01 },
		"entry_count":   func(d []byte, e int64) { d[e+16] ^= 0x01 },
		"entries_crc":   func(d []byte, e int64) { d[e+20] ^= 0xFF },
		"version":       func(d []byte, e int64) { d[e+24] ^= 0xFF },
		"head_crc":      func(d []byte, e int64) { d[e+28] ^= 0xFF },
		"entry":         func(d []byte, e int64) { d[e+footerHeadSize+4] ^= 0x01 },
		"zeroed_footer": func(d []byte, e int64) { clear(d[e : e+footerHeadSize+footerEntrySize]) },
		"header_count": func(d []byte, e int64) {
			binary.LittleEndian.PutUint64(d[32:40], binary.LittleEndian.Uint64(d[32:40])+1)
			binary.LittleEndian.PutUint32(d[56:60], crc32.Checksum(d[0:56], crcTable))
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWALog(dir, ".wal")
			require.NoError(t, err)
			for lsn := uint64(1); lsn <= 3; lsn++ {
				_, err = w.Write([]byte("record"), lsn)
				require.NoError(t, err)
			}
			require.NoError(t, w.RotateSegment())
			require.NoError(t, w.Close())

			path := SegmentFileName(dir, ".wal", 1)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			damage(data, int64(binary.LittleEndian.Uint64(data[24:32])))
			require.NoError(t, os.WriteFile(path, data, 0o644))

			_, err = NewWALog(dir, ".wal")
			require.ErrorIs(t, err, ErrSegmentCorrupt)
		})
	}
}

// Truncating into a sealed segment unseals it and removes its footer; the
// segment is sealed again with a new footer when it next fills.
func TestTruncateIntoSealedSegmentRemovesFooter(t *testing.T) {
	dir := t.TempDir()
	opts := []WALogOptions{WithMaxSegmentSize(4096)}
	w, err := NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 60; lsn++ {
		_, err = w.Write(bytes.Repeat([]byte{byte(lsn)}, 200), lsn)
		require.NoError(t, err)
	}
	require.Greater(t, len(w.Segments()), 2)
	require.True(t, w.Segments()[1].IsSealed())

	require.NoError(t, w.Truncate(5))
	cur := w.Current()
	require.Equal(t, SegmentID(1), cur.ID())
	require.False(t, cur.IsSealed())
	end := cur.WriteOffset()
	require.Equal(t, make([]byte, footerHeadSize), []byte(cur.mmapData[end:end+footerHeadSize]), "footer removed")

	for lsn := uint64(6); lsn <= 40; lsn++ {
		_, err = w.Write(bytes.Repeat([]byte{byte(lsn + 100)}, 200), lsn)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.Segments()[1].IsSealed())
	for lsn := uint64(1); lsn <= 40; lsn++ {
		pos, err := w.PositionForIndex(lsn)
		require.NoError(t, err, "lsn %d", lsn)
		got, err := w.Read(pos)
		require.NoError(t, err)
		want := byte(lsn)
		if lsn > 5 {
			want = byte(lsn + 100)
		}
		require.Equal(t, bytes.Repeat([]byte{want}, 200), got, "lsn %d", lsn)
	}
}

func TestUnsupportedSegmentVersionIsRejected(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	_, err = w.Write([]byte("x"), 1)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	path := SegmentFileName(dir, ".wal", 1)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(data[4:8], 1)
	binary.LittleEndian.PutUint32(data[56:60], crc32.Checksum(data[0:56], crcTable))
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = NewWALog(dir, ".wal")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unsupported WAL segment format"), err.Error())
}

// The seal-time check refuses to write a footer that would not fit, rather
// than overrun the segment. The limit makes this unreachable via writes.
func TestWriteFooterRefusesToOverrun(t *testing.T) {
	seg, err := OpenSegmentFile(t.TempDir(), ".wal", 1, WithSegmentSize(4096))
	require.NoError(t, err)
	defer seg.Close()
	s := &sparseIndex{count: 1000, ordinals: make([]uint32, 1000), offsets: make([]uint32, 1000)}
	before := bytes.Clone(seg.mmapData)
	require.Error(t, seg.writeFooterLocked(s, segmentHeaderSize))
	require.Equal(t, before, []byte(seg.mmapData), "nothing written")
}

func TestNoSidecarFilesAreCreated(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 50; lsn++ {
		_, err = w.Write(bytes.Repeat([]byte{1}, 100), lsn)
		require.NoError(t, err)
	}
	require.NoError(t, w.Truncate(20))
	require.NoError(t, w.Close())
	w, err = NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".idx"), e.Name())
	}
}

// A failed fsync while sealing leaves the durable state unknown (footer only,
// or footer and sealed header). The WAL must report it as ErrFsync, refuse
// further work until reopened, and recover every record either way.
func TestSealSyncFailureRequiresRecovery(t *testing.T) {
	for _, failAt := range []int{1, 2} { // 1: footer sync, 2: sealed-header sync
		t.Run(map[int]string{1: "footer_sync", 2: "header_sync"}[failAt], func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWALog(dir, ".wal")
			require.NoError(t, err)
			for lsn := uint64(1); lsn <= 5; lsn++ {
				_, err = w.Write([]byte("record"), lsn)
				require.NoError(t, err)
			}

			injected := errors.New("injected fsync failure")
			var calls int
			fsyncFile = func(f *os.File) error {
				calls++
				if calls == failAt {
					return injected
				}
				return f.Sync()
			}
			err = w.RotateSegment()
			fsyncFile = func(f *os.File) error { return f.Sync() }

			require.ErrorIs(t, err, ErrFsync, "dbkernel treats ErrFsync as fatal")
			require.ErrorIs(t, err, ErrRecoveryRequired)
			require.ErrorIs(t, err, injected)
			_, err = w.Write([]byte("forbidden"), 6)
			require.ErrorIs(t, err, ErrRecoveryRequired)
			require.NoError(t, w.Close())

			for reopen := 0; reopen < 2; reopen++ {
				w, err = NewWALog(dir, ".wal")
				require.NoError(t, err)
				first, last := w.GetBounds()
				require.Equal(t, uint64(1), first)
				require.Equal(t, uint64(5+reopen), last)
				if reopen == 0 {
					_, err = w.Write([]byte("record"), 6)
					require.NoError(t, err)
				}
				require.NoError(t, w.Close())
			}
		})
	}
}

// A crash after rotation sealed the last segment but before the next one was
// created leaves no writable segment; reopening must open one.
func TestReopenWithSealedLastSegmentAcceptsWrites(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	for lsn := uint64(1); lsn <= 3; lsn++ {
		_, err = w.Write([]byte("record"), lsn)
		require.NoError(t, err)
	}
	require.NoError(t, w.Current().SealSegment()) // sealed, successor never created
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	require.Equal(t, SegmentID(2), w.Current().ID())
	require.False(t, w.Current().IsSealed())
	_, err = w.Write([]byte("record"), 4)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer w.Close()
	first, last := w.GetBounds()
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(4), last)
}
