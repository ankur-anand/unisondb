package walfs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

var (
	ErrClosed              = errors.New("the Segment file is closed")
	ErrInvalidCRC          = errors.New("invalid crc, the data may be corrupted")
	ErrCorruptHeader       = errors.New("corrupt record header, invalid length")
	ErrIncompleteChunk     = errors.New("incomplete or torn write detected at record trailer")
	ErrSegmentSealed       = errors.New("cannot write to sealed segment")
	ErrSegmentReaderClosed = errors.New("segment reader is closed")
	ErrNoNewData           = errors.New("no new data yet")
	ErrSegmentFull         = errors.New("segment is full, cannot write more records")
)

var (
	// NilRecordPosition is a sentinel value representing an nil RecordPosition.
	NilRecordPosition = RecordPosition{}

	crcTable = crc32.MakeTable(crc32.Castagnoli)
	// marker written after every WAL record to detect torn/incomplete writes.
	trailerMarker = []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0xED, 0xFA, 0xCE}
)

const trailerWord uint64 = 0xCEFAEDFEEFBEADDE

const (
	StateOpen = iota
	StateClosing

	// 4 GiB.
	maxSegmentSize = 4 * 1024 * 1024 * 1024

	FlagActive uint32 = 1 << iota
	FlagSealed uint32 = 1 << 1

	segmentHeaderSize = 64
	// just a string of "UWAL"
	// 'U' = 0x55 and so on. Unison Write ahead log.
	segmentMagicNumber   = 0x5557414C
	segmentHeaderVersion = 1

	// layout: 4 (checksum) + 4 (length) = 8 bytes
	recordHeaderSize = 8
	// default Segment size of 16MB.
	segmentSize  = 16 * 1024 * 1024
	fileModePerm = 0644
	// each index entry stores offset + length (16 bytes).
	indexEntrySize = 16

	// size of the trailer used to detect torn writes.
	// We are writing this to detect torn or partial writes caused by unexpected shutdowns or disk failures.
	// This is inspired by a real-world issue observed in etcd v2.3:
	// SEE: https://github.com/etcd-io/etcd/issues/6191#issuecomment-240268979
	// By adding a known trailer marker (e.g., 0xDEADBEEF), we can explicitly validate that a record entry.
	// was fully persisted, and safely stop recovery at the first missing or corrupted trailer.
	recordTrailerMarkerSize = 8
	// alignSize defines the boundary (in bytes) to which all WAL entries (headers, payloads, trailers) are aligned.
	// helps us reduce the chance of partially written headers/trailers across page boundaries during crashes.
	// atomic sector writes are not used for the correctness but gives us better chance for recovery.
	// SEE: https://github.com/boltdb/bolt/issues/548
	alignSize int64 = 8
	alignMask int64 = alignSize - 1
)

type MsyncOption int

const (
	// MsyncNone skips msync after write.
	MsyncNone MsyncOption = iota

	// MsyncOnWrite calls msync (Flush) after every write.
	MsyncOnWrite
)

type SegmentID = uint32

// SegmentHeader encodes all the necessary information about the segment file at the top of the file.
// Its Size is 64 byte once encoded.
type SegmentHeader struct {
	// at 0
	Magic uint32
	// at 4
	Version uint32
	// at 8
	CreatedAt int64
	// at 16
	LastModifiedAt int64
	// at 24
	WriteOffset int64
	// at 32
	EntryCount int64
	// at 40
	Flags uint32

	// at 44 -51
	FirstLogIndex uint64
	// - Reserved for future use
	// 52-55
	_ [4]byte

	// at 56 byte: - CRC32 of first 56 bytes
	CRC uint32
	// at 60 - padding to align to 64B
	_ uint32
}

/* Record Layout:
┌──────────────────────────────────────────────────────────────┐
│ 0..3   CRC32C(header[4:8] || data)                           │
│ 4..7   u32 length                                            │
│ 8..(8+len-1)   data                                          │
│ (8+len)..(16+len-1)  trailer 0xDEADBEEFFEEDFACE              │
│ ... zero padding to next 8-byte boundary                     │
└──────────────────────────────────────────────────────────────┘
*/

func decodeSegmentHeader(buf []byte) (*SegmentHeader, error) {
	if len(buf) < 64 {
		return nil, io.ErrUnexpectedEOF
	}

	crc := binary.LittleEndian.Uint32(buf[56:60])
	computed := crc32.Checksum(buf[0:56], crcTable)
	if crc != computed {
		return nil, fmt.Errorf("segment metadata CRC mismatch: expected %08x, got %08x", crc, computed)
	}

	meta := &SegmentHeader{
		Magic:          binary.LittleEndian.Uint32(buf[0:4]),
		Version:        binary.LittleEndian.Uint32(buf[4:8]),
		CreatedAt:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		LastModifiedAt: int64(binary.LittleEndian.Uint64(buf[16:24])),
		WriteOffset:    int64(binary.LittleEndian.Uint64(buf[24:32])),
		EntryCount:     int64(binary.LittleEndian.Uint64(buf[32:40])),
		Flags:          binary.LittleEndian.Uint32(buf[40:44]),
		FirstLogIndex:  binary.LittleEndian.Uint64(buf[44:52]),
	}
	return meta, nil
}

// RecordPosition is the logical location of a record entry within a WAL Segment.
type RecordPosition struct {
	SegmentID SegmentID
	Offset    int64
}

func (rp RecordPosition) String() string {
	return fmt.Sprintf("SegmentID=%d, Offset=%d", rp.SegmentID, rp.Offset)
}

// Encode serializes the RecordPosition into a fixed-length byte slice.
func (rp RecordPosition) Encode() []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], rp.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(rp.Offset))
	return buf
}

// EncodeRecordPositionTo serializes a RecordPosition into the provided buffer.
// The buffer must be at least 12 bytes long. If it's shorter, a new 12-byte slice is allocated.
func EncodeRecordPositionTo(pos RecordPosition, buf []byte) []byte {
	if len(buf) < 12 {
		buf = make([]byte, 12)
	} else {
		buf = buf[:12]
	}
	binary.LittleEndian.PutUint32(buf[0:4], pos.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(pos.Offset))
	return buf
}

// IsZero returns true if the RecordPosition is uninitialized,
// meaning both SegmentID and Offset are zero.
func (rp RecordPosition) IsZero() bool {
	return rp.SegmentID == 0 && rp.Offset == 0
}

// DecodeRecordPosition deserializes a byte slice into a RecordPosition.
func DecodeRecordPosition(data []byte) (RecordPosition, error) {
	if len(data) < 12 {
		return RecordPosition{}, io.ErrUnexpectedEOF
	}
	cp := RecordPosition{
		SegmentID: binary.LittleEndian.Uint32(data[0:4]),
		Offset:    int64(binary.LittleEndian.Uint64(data[4:12])),
	}
	return cp, nil
}

type segmentIndexEntry struct {
	Offset uint64
	Length uint32
}

// IndexEntry exposes a record's physical location within a WAL segment.
type IndexEntry struct {
	SegmentID SegmentID
	Offset    int64
	Length    uint32
}

// Segment represents a single WAL segment backed by a memory-mapped file.
type Segment struct {
	path        string
	id          SegmentID
	fd          *os.File
	mmapData    mmap.MMap
	mmapSize    int64
	writeOffset atomic.Int64
	closed      atomic.Bool
	header      []byte

	refCount          atomic.Int64
	state             atomic.Int64
	markedForDeletion atomic.Bool
	readerIDCounter   atomic.Uint64
	activeReaders     *readerTracker
	closeCond         *sync.Cond

	isSealed       atomic.Bool
	inMemorySealed atomic.Bool
	writeMu        sync.RWMutex
	syncOption     MsyncOption
	dirSyncer      DirectorySyncer

	indexPath         string
	indexEntries      []segmentIndexEntry
	indexFlush        sync.WaitGroup
	firstLogIndex     uint64
	clearIndexOnFlush bool
}

// WithSyncOption sets the sync option for the Segment.
func WithSyncOption(opt MsyncOption) func(*Segment) {
	return func(s *Segment) {
		s.syncOption = opt
	}
}

// WithSegmentDirectorySyncer sets the directory syncer used after destructive operations.
func WithSegmentDirectorySyncer(syncer DirectorySyncer) func(*Segment) {
	return func(s *Segment) {
		if syncer != nil {
			s.dirSyncer = syncer
		}
	}
}

// withClearIndexOnFlush enables clearing the in-memory index after it's flushed to disk.
// This is useful when an external index is maintained.
func withClearIndexOnFlush() func(*Segment) {
	return func(s *Segment) {
		s.clearIndexOnFlush = true
	}
}

// WithSegmentSize sets the size for the Segment.
func WithSegmentSize(size int64) func(*Segment) {
	return func(s *Segment) {
		s.mmapSize = size
	}
}

// OpenSegmentFile opens an existing segment file or create a new one if not present.
// If SegmentFile is sealed it doesn't scan its content while opening.
func OpenSegmentFile(dirPath, extName string, id uint32, opts ...func(*Segment)) (*Segment, error) {
	path := SegmentFileName(dirPath, extName, id)
	isNew, err := isNewSegment(path)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		path:          path,
		id:            id,
		header:        make([]byte, recordHeaderSize),
		mmapSize:      segmentSize,
		syncOption:    MsyncNone,
		activeReaders: newReaderTracker(),
		dirSyncer:     DirectorySyncFunc(syncDir),
	}
	s.state.Store(StateOpen)
	s.closeCond = sync.NewCond(&sync.Mutex{})

	for _, opt := range opts {
		opt(s)
	}

	if s.mmapSize > maxSegmentSize {
		return nil, fmt.Errorf("segment size exceeds 4 GiB limit: %d bytes", s.mmapSize)
	}

	fd, mmapData, err := s.prepareSegmentFile(path)
	if err != nil {
		return nil, err
	}
	s.fd = fd
	s.mmapData = mmapData

	offset := int64(segmentHeaderSize)
	if isNew {
		// for a new Segment file we initialize it with default metadata.
		writeInitialMetadata(mmapData)
	} else {
		// decode the Segment metadata header
		meta, err := decodeSegmentHeader(mmapData[:segmentHeaderSize])
		if err != nil {
			return nil, fmt.Errorf("failed to decode metadata: %w", err)
		}

		if IsSealed(meta.Flags) {
			// for the sealed Segment our active offset is already saved
			offset = meta.WriteOffset
			s.isSealed.Store(true)
		} else {
			// while we trust the write offset we are scanning the Segment
			// to find the true end of valid data for safe appends,
			// while in most cases this would not happen but if crashed
			// we don't know if the written header metadata offset is valid enough.
			offset = s.scanForLastOffset()
		}
	}
	s.writeOffset.Store(offset)

	if !isNew {
		s.firstLogIndex = binary.LittleEndian.Uint64(mmapData[44:52])
	}

	if err := s.setupIndexFile(dirPath, extName, isNew); err != nil {
		_ = mmapData.Unmap()
		_ = fd.Close()
		return nil, err
	}

	return s, nil
}

func (seg *Segment) setupIndexFile(dirPath, extName string, isNew bool) error {
	seg.indexPath = SegmentIndexFileName(dirPath, extName, seg.id)
	seg.indexEntries = make([]segmentIndexEntry, 0)

	if isNew {
		return nil
	}

	if seg.isSealed.Load() {
		if err := seg.loadIndexFromFile(); err == nil {
			return nil
		}
		seg.buildIndexFromSegment()
		return seg.flushIndexToFile(seg.indexEntries)
	}

	seg.buildIndexFromSegment()
	return nil
}

func (seg *Segment) loadIndexFromFile() error {
	file, err := os.Open(seg.indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat index: %w", err)
	}
	if info.Size()%indexEntrySize != 0 {
		return fmt.Errorf("corrupt index file size: %d", info.Size())
	}

	count := int(info.Size() / indexEntrySize)
	seg.indexEntries = make([]segmentIndexEntry, 0, count)
	buf := make([]byte, indexEntrySize)
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(file, buf); err != nil {
			return fmt.Errorf("read index: %w", err)
		}
		entry := segmentIndexEntry{
			Offset: binary.LittleEndian.Uint64(buf[0:8]),
			Length: binary.LittleEndian.Uint32(buf[8:12]),
		}
		seg.indexEntries = append(seg.indexEntries, entry)
	}
	return nil
}

func (seg *Segment) buildIndexFromSegment() {
	seg.indexEntries = seg.indexEntries[:0]
	seg.iterateValidEntries(func(offset int64, length uint32) bool {
		seg.indexEntries = append(seg.indexEntries, segmentIndexEntry{
			Offset: uint64(offset),
			Length: length,
		})
		return true
	})
}

func (seg *Segment) flushIndexToFile(entries []segmentIndexEntry) error {
	if seg.indexPath == "" {
		return nil
	}
	indexDir := filepath.Dir(seg.indexPath)
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return fmt.Errorf("ensure index dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(indexDir, filepath.Base(seg.indexPath)+".tmp")
	if err != nil {
		return fmt.Errorf("open index for flush: %w", err)
	}
	tmpPath := tmpFile.Name()

	writer := bufio.NewWriterSize(tmpFile, 32*1024)
	defer func() {
		_ = writer.Flush()
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	buf := make([]byte, indexEntrySize)
	for _, entry := range entries {
		binary.LittleEndian.PutUint64(buf[0:8], entry.Offset)
		binary.LittleEndian.PutUint32(buf[8:12], entry.Length)
		for i := 12; i < indexEntrySize; i++ {
			buf[i] = 0
		}
		if _, err := writer.Write(buf); err != nil {
			return fmt.Errorf("write index entry: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush index writer: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync index file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close index file: %w", err)
	}

	if err := os.Rename(tmpPath, seg.indexPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename index file: %w", err)
	}

	if seg.dirSyncer != nil {
		if err := seg.dirSyncer.SyncDir(indexDir); err != nil {
			return fmt.Errorf("fsync index directory: %w", err)
		}
	}
	return nil
}

func (seg *Segment) appendIndexEntry(offset int64, length uint32) {
	seg.indexEntries = append(seg.indexEntries, segmentIndexEntry{
		Offset: uint64(offset),
		Length: length,
	})
}

// IndexEntries returns a copy of the index metadata for this segment.
func (seg *Segment) IndexEntries() []IndexEntry {
	entries := make([]IndexEntry, len(seg.indexEntries))
	for i, entry := range seg.indexEntries {
		entries[i] = IndexEntry{
			SegmentID: seg.id,
			Offset:    int64(entry.Offset),
			Length:    entry.Length,
		}
	}
	return entries
}

// ClearIndexFromMemory releases the in-memory index entries to free memory.
// This can be called after the index has been copied to an external data structure.
// Note: After calling this, IndexEntries() will return an empty slice.
// Only sealed segments can be cleared; active segments are ignored.
func (seg *Segment) ClearIndexFromMemory() {
	if !seg.isSealed.Load() {
		return
	}
	seg.indexEntries = nil
}

func (seg *Segment) flushIndexAsync() {
	entriesCopy := append([]segmentIndexEntry(nil), seg.indexEntries...)
	clearOnFlush := seg.clearIndexOnFlush
	seg.indexFlush.Add(1)
	go func(path string) {
		defer seg.indexFlush.Done()
		if err := seg.flushIndexToFile(entriesCopy); err != nil {
			slog.Error("[walfs]",
				slog.String("message", "failed to flush index"),
				slog.Uint64("segment_id", uint64(seg.id)),
				slog.Any("error", err))
			return
		}
		// clear in-memory index after successful flush if option is enabled
		if clearOnFlush {
			seg.indexEntries = nil
		}
	}(seg.indexPath)
}

// WaitForIndexFlush blocks until any pending index flush operations complete.
func (seg *Segment) WaitForIndexFlush() {
	seg.indexFlush.Wait()
}

// IsSealed returns if teh provided flag has sealed bit set.
func IsSealed(flags uint32) bool {
	return flags&FlagSealed != 0
}

func IsActive(flags uint32) bool {
	return flags&FlagActive != 0
}

// SealSegment seals the given segment.
func (seg *Segment) SealSegment() error {
	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	if seg.closed.Load() {
		return ErrClosed
	}

	mmapData := seg.mmapData

	now := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(mmapData[16:24], now)
	binary.LittleEndian.PutUint64(mmapData[24:32], uint64(seg.writeOffset.Load()))
	flags := binary.LittleEndian.Uint32(mmapData[40:44])
	// clear 'active' bit
	flags &^= FlagActive
	// set 'sealed' bit
	flags |= FlagSealed
	binary.LittleEndian.PutUint32(mmapData[40:44], flags)

	crc := crc32.Checksum(mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(mmapData[56:60], crc)
	seg.isSealed.Store(true)

	seg.flushIndexAsync()
	return nil
}

// MarkSealedInMemory marks the segment as sealed in memory.
func (seg *Segment) MarkSealedInMemory() {
	seg.inMemorySealed.Store(true)
}

// IsInMemorySealed returns true if the segment has been marked as sealed in memory.
func (seg *Segment) IsInMemorySealed() bool {
	return seg.inMemorySealed.Load()
}

// IsSealed returns true if the segment is sealed (on-disk flag).
func (seg *Segment) IsSealed() bool {
	return seg.isSealed.Load()
}

func isNewSegment(path string) (bool, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat error: %w", err)
	}
	return false, nil
}

func (seg *Segment) prepareSegmentFile(path string) (*os.File, mmap.MMap, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileModePerm)
	if err != nil {
		return nil, nil, err
	}
	if err := fd.Truncate(seg.mmapSize); err != nil {
		fd.Close()
		return nil, nil, fmt.Errorf("truncate error: %w", err)
	}
	mmapData, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		fd.Close()
		return nil, nil, fmt.Errorf("mmap error: %w", err)
	}
	return fd, mmapData, nil
}

func writeInitialMetadata(mmapData mmap.MMap) {
	binary.LittleEndian.PutUint32(mmapData[0:4], segmentMagicNumber)
	binary.LittleEndian.PutUint32(mmapData[4:8], segmentHeaderVersion)
	now := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(mmapData[8:16], now)
	binary.LittleEndian.PutUint64(mmapData[16:24], now)
	binary.LittleEndian.PutUint64(mmapData[24:32], segmentHeaderSize)
	binary.LittleEndian.PutUint64(mmapData[32:40], 0)
	binary.LittleEndian.PutUint32(mmapData[40:44], FlagActive)
	crc := crc32.Checksum(mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(mmapData[56:60], crc)
}

func (seg *Segment) scanForLastOffset() int64 {
	return seg.iterateValidEntries(nil)
}

func (seg *Segment) iterateValidEntries(visitor func(offset int64, length uint32) bool) int64 {
	var offset int64 = segmentHeaderSize

	for offset+recordHeaderSize <= seg.mmapSize {
		offset = alignUp(offset)
		if offset+recordHeaderSize > seg.mmapSize {
			break
		}

		header := seg.mmapData[offset : offset+recordHeaderSize]
		length := binary.LittleEndian.Uint32(header[4:8])
		entrySize := alignUp(int64(recordHeaderSize) + int64(length) + recordTrailerMarkerSize)

		if offset+entrySize > seg.mmapSize {
			break
		}

		data := seg.mmapData[offset+recordHeaderSize : offset+recordHeaderSize+int64(length)]
		trailer := seg.mmapData[offset+recordHeaderSize+int64(length) : offset+recordHeaderSize+int64(length)+recordTrailerMarkerSize]

		savedSum := binary.LittleEndian.Uint32(header[:4])
		computedSum := crc32Checksum(header[4:], data)

		if savedSum == 0 && length == 0 {
			break
		}
		if savedSum == 0 || savedSum != computedSum || !bytes.Equal(trailer, trailerMarker) {
			slog.Warn("[walfs]",
				slog.String("message", "Failed to recover segment: checksum mismatch"),
				slog.Int64("offset", offset),
				slog.Uint64("saved", uint64(savedSum)),
				slog.Uint64("computed", uint64(computedSum)),
				slog.String("Segment", seg.path),
				slog.Bool("trailer_corrupted", !bytes.Equal(trailer, trailerMarker)),
			)
			break
		}

		if visitor != nil {
			if !visitor(offset, length) {
				offset += entrySize
				break
			}
		}

		offset += entrySize
	}

	return offset
}

// alignUp returns the next multiple of alignSize greater than or equal to n.
//
//go:inline
func alignUp(n int64) int64 {
	return (n + alignMask) & ^alignMask
}

// Write writes the provided slice of bytes to the open mmap file.
// It appends data to the segment and returns the offset where
// the record was written in the given segment.
func (seg *Segment) Write(data []byte, logIndex uint64) (RecordPosition, error) {
	if seg.closed.Load() || seg.state.Load() != StateOpen {
		return NilRecordPosition, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	flags := binary.LittleEndian.Uint32(seg.mmapData[40:44])
	if IsSealed(flags) {
		return NilRecordPosition, ErrSegmentSealed
	}

	offset := seg.writeOffset.Load()

	seg.writeFirstIndexEntry(logIndex)

	headerSize := int64(recordHeaderSize)
	dataSize := int64(len(data))
	trailerSize := int64(recordTrailerMarkerSize)
	rawSize := headerSize + dataSize + trailerSize
	entrySize := alignUp(rawSize)

	if offset+entrySize > seg.mmapSize {
		return NilRecordPosition, errors.New("write exceeds Segment size")
	}

	binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
	sum := crc32Checksum(seg.header[4:], data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	copy(seg.mmapData[offset:], seg.header[:])
	copy(seg.mmapData[offset+recordHeaderSize:], data)

	canaryOffset := offset + headerSize + dataSize
	copy(seg.mmapData[canaryOffset:], trailerMarker)

	paddingStart := offset + rawSize
	paddingEnd := offset + entrySize
	// ensuring alignment to 8 bytes
	for i := paddingStart; i < paddingEnd; i++ {
		seg.mmapData[i] = 0
	}

	newOffset := offset + entrySize
	seg.writeOffset.Store(newOffset)

	binary.LittleEndian.PutUint32(seg.mmapData[24:32], uint32(newOffset))
	prevCount := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], prevCount+1)
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)

	seg.appendIndexEntry(offset, uint32(len(data)))

	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return NilRecordPosition, fmt.Errorf("mmap flush error after write: %w", err)
		}
	}

	return RecordPosition{
		SegmentID: seg.id,
		Offset:    offset,
	}, nil
}

func (seg *Segment) writeFirstIndexEntry(logIndex uint64) {
	if seg.firstLogIndex == 0 {
		seg.firstLogIndex = logIndex
		binary.LittleEndian.PutUint64(seg.mmapData[44:52], logIndex)
		crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
		binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)
	}
}

// WriteBatch writes multiple records to the segment in a single operation.
// Returns a slice of RecordPositions for successfully written records and the number written.
// If the segment fills up mid-batch, it returns positions for records that fit,
// the count of records written, and ErrSegmentFull.
// Callers should retry remaining records in a new segment.
// nolint: funlen
func (seg *Segment) WriteBatch(records [][]byte, logIndexes []uint64) ([]RecordPosition, int, error) {
	if len(records) == 0 {
		return nil, 0, nil
	}

	if seg.closed.Load() || seg.state.Load() != StateOpen {
		return nil, 0, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	flags := binary.LittleEndian.Uint32(seg.mmapData[40:44])
	if IsSealed(flags) {
		return nil, 0, ErrSegmentSealed
	}

	// firstLogIndex if this is the first write to the segment
	if seg.firstLogIndex == 0 && logIndexes != nil && len(logIndexes) > 0 {
		seg.writeFirstIndexEntry(logIndexes[0])
	}

	startOffset := seg.writeOffset.Load()
	currentOffset := startOffset
	positions := make([]RecordPosition, 0, len(records))
	lengths := make([]uint32, 0, len(records))

	headerSize := int64(recordHeaderSize)
	trailerSize := int64(recordTrailerMarkerSize)

	// determine how many records we can fit
	var recordsToWrite int
	for i, data := range records {
		dataSize := int64(len(data))
		rawSize := headerSize + dataSize + trailerSize
		entrySize := alignUp(rawSize)

		if entrySize > seg.mmapSize-segmentHeaderSize {
			return nil, 0, fmt.Errorf("record at index %d (size %d bytes) exceeds maximum segment capacity", i, len(data))
		}

		if currentOffset+entrySize > seg.mmapSize {
			// can't fit from this record - stop here
			recordsToWrite = i
			break
		}

		positions = append(positions, RecordPosition{
			SegmentID: seg.id,
			Offset:    currentOffset,
		})
		lengths = append(lengths, uint32(len(data)))
		currentOffset += entrySize
		recordsToWrite = i + 1
	}

	if recordsToWrite == 0 {
		return nil, 0, ErrSegmentFull
	}

	currentOffset = startOffset
	for i := 0; i < recordsToWrite; i++ {
		data := records[i]
		dataSize := int64(len(data))
		rawSize := headerSize + dataSize + trailerSize
		entrySize := alignUp(rawSize)

		// header
		binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
		sum := crc32Checksum(seg.header[4:], data)
		binary.LittleEndian.PutUint32(seg.header[:4], sum)
		copy(seg.mmapData[currentOffset:], seg.header[:])

		// data
		copy(seg.mmapData[currentOffset+recordHeaderSize:], data)

		// trailer
		canaryOffset := currentOffset + headerSize + dataSize
		copy(seg.mmapData[canaryOffset:], trailerMarker)

		// padding
		paddingStart := currentOffset + rawSize
		paddingEnd := currentOffset + entrySize
		for i := paddingStart; i < paddingEnd; i++ {
			seg.mmapData[i] = 0
		}

		currentOffset += entrySize
	}

	// metadata once at the end
	newOffset := currentOffset
	seg.writeOffset.Store(newOffset)

	binary.LittleEndian.PutUint32(seg.mmapData[24:32], uint32(newOffset))
	prevCount := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], prevCount+uint64(recordsToWrite))
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)

	for i := 0; i < recordsToWrite; i++ {
		seg.appendIndexEntry(positions[i].Offset, lengths[i])
	}

	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return positions, recordsToWrite, fmt.Errorf("mmap flush error after batch write: %w", err)
		}
	}

	// Return partial success if we couldn't write all records
	var err error
	if recordsToWrite < len(records) {
		err = ErrSegmentFull
	}

	return positions, recordsToWrite, err
}

// Read reads the record data at the specified offset within the segment.
// IMP: Don't retain any data.
// This method returns a slice of the mmap'd file content corresponding to the record payload.
// so slice becomes invalid immediately after the segment is closed or unmapped.
func (seg *Segment) Read(offset int64) ([]byte, RecordPosition, error) {
	if seg.closed.Load() {
		return nil, NilRecordPosition, ErrClosed
	}
	if offset+recordHeaderSize > seg.mmapSize {
		return nil, NilRecordPosition, io.EOF
	}

	header := seg.mmapData[offset : offset+recordHeaderSize]
	length := binary.LittleEndian.Uint32(header[4:8])
	dataSize := int64(length)

	rawSize := int64(recordHeaderSize) + dataSize + recordTrailerMarkerSize
	entrySize := alignUp(rawSize)

	if length > uint32(seg.WriteOffset()-offset-recordHeaderSize) {
		return nil, NilRecordPosition, ErrCorruptHeader
	}

	if offset+entrySize > seg.WriteOffset() {
		return nil, NilRecordPosition, io.EOF
	}

	// validating  the trailer before reading data
	// we are ensuring no oob access even if length is corrupted.
	trailerOffset := offset + recordHeaderSize + dataSize
	end := trailerOffset + recordTrailerMarkerSize
	if end > seg.mmapSize {
		return nil, NilRecordPosition, ErrIncompleteChunk
	}

	// previously we were doing byte which did show in pprof as runtime.memequal
	// switching to uint64 comparison removed it altogether.
	word := binary.LittleEndian.Uint64(seg.mmapData[trailerOffset:end])
	// validating trailer marker to detect torn/incomplete writes.
	if word != trailerWord {
		return nil, NilRecordPosition, ErrIncompleteChunk
	}

	data := seg.mmapData[offset+recordHeaderSize : offset+recordHeaderSize+dataSize]

	// sealed segments are immutable and may have been recovered
	// from disk after a crash or shutdown. CRC validation ensures that data
	// persisted to disk is still intact and wasn't partially written or corrupted.
	// for active segment, we do one validation at start if not sealed, else it's in the
	// same process memory, so having corruption of the same byte is very unlikely, until
	// done from some external forces.
	// doing this in the hot-path is CPU intensive and most of the read are towards the tail.
	if seg.isSealed.Load() && !seg.inMemorySealed.Load() {
		savedSum := binary.LittleEndian.Uint32(header[:4])
		computedSum := crc32Checksum(header[4:], data)
		if savedSum != computedSum {
			return nil, NilRecordPosition, ErrInvalidCRC
		}
	}

	next := RecordPosition{
		SegmentID: seg.id,
		Offset:    offset + entrySize,
	}

	return data, next, nil
}

// Sync Msync the Memory mapped file and the FSync the underlying file.
func (seg *Segment) Sync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}

	if err := seg.fd.Sync(); err != nil {
		return fmt.Errorf("fsync error: %w", err)
	}

	return nil
}

func (seg *Segment) MSync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}
	return nil
}

// WillExceed returns true if writing a record of the given dataSize would overflow
// the segment's allocated (memory-mapped) size.
func (seg *Segment) WillExceed(dataSize int) bool {
	rawSize := int64(recordHeaderSize + dataSize + recordTrailerMarkerSize)
	entrySize := alignUp(rawSize)
	offset := seg.writeOffset.Load()
	return offset+entrySize > seg.mmapSize
}

// Close gracefully shuts down the segment by waiting for all active readers to complete.
// It unmap the segment file and closes file descriptor.
func (seg *Segment) Close() error {
	if !seg.state.CompareAndSwap(StateOpen, StateClosing) {
		return nil
	}

	seg.closeCond.L.Lock()
	for seg.refCount.Load() > 0 {
		seg.closeCond.Wait()
	}
	seg.closeCond.L.Unlock()

	seg.indexFlush.Wait()

	if err := seg.Sync(); err != nil {
		defer func() {
			_ = seg.mmapData.Unmap()
			_ = seg.fd.Close()
		}()
		return fmt.Errorf("sync error during close: %w", err)
	}
	seg.closed.Store(true)

	if err := seg.mmapData.Unmap(); err != nil {
		_ = seg.fd.Close()
		return fmt.Errorf("unmap error: %w", err)
	}

	if err := seg.fd.Close(); err != nil {
		return fmt.Errorf("file close error: %w", err)
	}

	return nil
}

// HasActiveReaders returns true if there are any currently active readers on the segment.
func (seg *Segment) HasActiveReaders() bool {
	return seg.activeReaders.HasAny()
}

// WriteOffset returns the current write offset of the segment.
//
//go:inline
func (seg *Segment) WriteOffset() int64 {
	return seg.writeOffset.Load()
}

// GetLastModifiedAt returns the last modified time of the segment.
func (seg *Segment) GetLastModifiedAt() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.LastModifiedAt
}

// GetEntryCount returns the total entry count in segment.
func (seg *Segment) GetEntryCount() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.EntryCount
}

func (seg *Segment) FirstLogIndex() uint64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	return seg.firstLogIndex
}

// GetFlags returns the flags stored in segment header.
func (seg *Segment) GetFlags() uint32 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.Flags
}

func (seg *Segment) GetSegmentSize() int64 {
	return seg.mmapSize
}

func (seg *Segment) incrRef() {
	seg.refCount.Add(1)
}

func (seg *Segment) decrRef(id uint64) {
	if ok := seg.activeReaders.Remove(id); ok {
		seg.releaseRef()
	}
}

// releaseRef decrements the reference count and performs any deferred cleanup.
func (seg *Segment) releaseRef() {
	count := seg.refCount.Add(-1)
	if count == 0 {
		seg.closeCond.L.Lock()
		seg.closeCond.Broadcast()
		seg.closeCond.L.Unlock()
		if seg.markedForDeletion.Load() {
			seg.cleanup()
		}
	}
}

// MarkForDeletion marks the segment as candidate for deletion.
// If no active readers, it will immediately call cleanup.
// Otherwise, cleanup will be deferred until the last reference is released.
func (seg *Segment) MarkForDeletion() {
	if seg.markedForDeletion.CompareAndSwap(false, true) {
		if seg.refCount.Load() == 0 {
			seg.cleanup()
		}
	}
}

// cleanup closes and deletes the underlying segment file from disk.
func (seg *Segment) cleanup() {
	if err := seg.Close(); err != nil {
		slog.Error("[walfs]", slog.String("message", "Failed to close segment"), slog.String("path", seg.path), slog.Any("error", err))
	}
	deletedSegment := false
	if err := os.Remove(seg.path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Error("[walfs]", slog.String("message", "Failed to delete segment"), slog.String("path", seg.path), slog.Any("error", err))
		}
	} else {
		deletedSegment = true
	}

	deletedIndex := false
	if seg.indexPath != "" {
		if err := os.Remove(seg.indexPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				slog.Error("[walfs]", slog.String("message", "Failed to delete segment index"), slog.String("path", seg.indexPath), slog.Any("error", err))
			}
		} else {
			deletedIndex = true
		}
	}

	if seg.dirSyncer != nil && (deletedSegment || deletedIndex) {
		dir := filepath.Dir(seg.path)
		if err := seg.dirSyncer.SyncDir(dir); err != nil {
			slog.Error("[walfs]",
				slog.String("message", "Failed to sync WAL directory after deletion"),
				slog.String("path", dir),
				slog.Any("error", err),
			)
		}
	}
	slog.Debug("[walfs]", slog.String("message", "Removed segment"), slog.Int("segment_id", int(seg.id)))
}

// ID returns the unique number of the Segment.
func (seg *Segment) ID() SegmentID {
	return seg.id
}

// SegmentReader is an iterator over records in a WAL segment.
// It maintains its own read offset and provides safe iteration over a Segment.
type SegmentReader struct {
	id               uint64
	segment          *Segment
	readOffset       int64
	lastRecordOffset int64
	closed           atomic.Bool
}

// Close closes the SegmentReader and decrements the segment's reference count.
func (r *SegmentReader) Close() {
	if r.closed.CompareAndSwap(false, true) {
		r.segment.decrRef(r.id)
	}
}

// NewReader creates a new SegmentReader for reading from the segment.
func (seg *Segment) NewReader() *SegmentReader {
	// prevent new readers to segments marked for deletion or not opened
	if seg.markedForDeletion.Load() || seg.state.Load() != StateOpen {
		return nil
	}

	id := seg.readerIDCounter.Add(1)
	seg.activeReaders.Add(id)
	seg.incrRef()

	reader := &SegmentReader{
		segment:    seg,
		readOffset: segmentHeaderSize,
		id:         id,
	}

	// safety net in case caller doesn't call Close()
	runtime.AddCleanup(reader, func(seg *Segment) {
		seg.decrRef(id)
	}, seg)

	return reader
}

// Next reads the next record from the segment and also advances the read position.
// It returns the data, the record's position, or an error.
// Returns io.EOF if the segment is sealed and all data has been read.
// Returns ErrNoNewData if unsealed and no new data is available yet.
func (r *SegmentReader) Next() ([]byte, RecordPosition, error) {
	if r.closed.Load() {
		return nil, NilRecordPosition, ErrSegmentReaderClosed
	}

	isSealed := r.segment.isSealed.Load()
	writeOffset := r.segment.WriteOffset()

	if r.readOffset >= writeOffset {
		if isSealed {
			return nil, NilRecordPosition, io.EOF
		}
		return nil, NilRecordPosition, ErrNoNewData
	}

	currentOffset := r.readOffset
	data, next, err := r.segment.Read(r.readOffset)
	if err != nil {
		// If the read fails due to being too close to write head, treat as "no new data" if unsealed
		if !isSealed && errors.Is(err, io.EOF) {
			return nil, NilRecordPosition, ErrNoNewData
		}

		return nil, NilRecordPosition, err
	}
	r.lastRecordOffset = currentOffset
	r.readOffset = next.Offset

	currentPos := RecordPosition{
		SegmentID: r.segment.ID(),
		Offset:    currentOffset,
	}

	return data, currentPos, nil
}

func (r *SegmentReader) LastRecordPosition() RecordPosition {
	return RecordPosition{
		SegmentID: r.segment.ID(),
		Offset:    r.lastRecordOffset,
	}
}

func crc32Checksum(header []byte, data []byte) uint32 {
	sum := crc32.Checksum(header, crcTable)
	return crc32.Update(sum, crcTable, data)
}

// SegmentFileName returns the file name of a Segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// SegmentIndexFileName returns the file name of the index for a segment.
func SegmentIndexFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName+".idx", id))
}

// TruncateTo truncates the segment to the specified log index.
// All entries after the given log index will be discarded.
// If the log index is not found in this segment, it returns an error.
func (seg *Segment) TruncateTo(logIndex uint64) error {
	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	if seg.closed.Load() {
		return ErrClosed
	}

	if len(seg.indexEntries) == 0 && seg.writeOffset.Load() <= int64(segmentHeaderSize) {
		return fmt.Errorf("segment is empty, cannot truncate to %d", logIndex)
	}

	if logIndex < seg.firstLogIndex {
		return fmt.Errorf("log index %d is before segment start %d", logIndex, seg.firstLogIndex)
	}

	relativeIndex := int(logIndex - seg.firstLogIndex)
	if relativeIndex >= len(seg.indexEntries) {
		return fmt.Errorf("log index %d not found in segment index (max relative %d)", logIndex, len(seg.indexEntries)-1)
	}

	targetEntry := seg.indexEntries[relativeIndex]

	rawSize := int64(recordHeaderSize) + int64(targetEntry.Length) + int64(recordTrailerMarkerSize)
	entrySize := alignUp(rawSize)

	newWriteOffset := int64(targetEntry.Offset) + entrySize
	seg.writeOffset.Store(newWriteOffset)
	seg.indexEntries = seg.indexEntries[:relativeIndex+1]

	newEntryCount := int64(relativeIndex + 1)

	seg.applyTruncateHeader(newWriteOffset, newEntryCount)
	seg.zeroAheadFrom(newWriteOffset)

	if err := seg.MSync(); err != nil {
		return fmt.Errorf("failed to sync truncated segment: %w", err)
	}

	if err := seg.flushIndexToFile(seg.indexEntries); err != nil {
		return fmt.Errorf("failed to flush truncated index: %w", err)
	}

	return nil
}

func (seg *Segment) applyTruncateHeader(newWriteOffset int64, newEntryCount int64) {
	binary.LittleEndian.PutUint64(seg.mmapData[24:32], uint64(newWriteOffset))
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], uint64(newEntryCount))
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	flags := binary.LittleEndian.Uint32(seg.mmapData[40:44])
	if IsSealed(flags) {
		flags &^= FlagSealed
		flags |= FlagActive
		binary.LittleEndian.PutUint32(seg.mmapData[40:44], flags)
		seg.isSealed.Store(false)
		seg.inMemorySealed.Store(false)
	}

	crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)
}

func (seg *Segment) zeroAheadFrom(offset int64) {
	clearEnd := offset + 1024
	if clearEnd > seg.mmapSize {
		clearEnd = seg.mmapSize
	}
	for i := offset; i < clearEnd; i++ {
		seg.mmapData[i] = 0
	}
}

// Remove closes the segment and removes its underlying files (segment and index).
func (seg *Segment) Remove() error {
	if err := seg.Close(); err != nil {
		return fmt.Errorf("failed to close segment %d: %w", seg.id, err)
	}

	dir := filepath.Dir(seg.path)

	if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove segment file %s: %w", seg.path, err)
	}

	if seg.indexPath != "" {
		if err := os.Remove(seg.indexPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove index file %s: %w", seg.indexPath, err)
		}
	}

	if seg.dirSyncer != nil {
		if err := seg.dirSyncer.SyncDir(dir); err != nil {
			return fmt.Errorf("failed to sync directory after removal: %w", err)
		}
	}

	return nil
}
