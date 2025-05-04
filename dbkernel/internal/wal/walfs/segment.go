package walfs

import (
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

const (
	FlagActive uint32 = 1 << iota
	FlagSealed uint32 = 1 << 1
)

const (
	segmentMetadataSize = 64
	// just a string of "UWAL"
	// 'U' = 0x55 and so on. Unison Write ahead log.
	segmentMagicNumber     = 0x5557414C
	currentMetadataVersion = 1
)

type SegmentMetadata struct {
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

	// at 44–55
	// - Reserved for future use
	_ [12]byte
	// at 56 byte: - CRC32 of first 56 bytes
	CRC uint32
	// at 60 - padding to align to 64B
	_ uint32
}

func decodeSegmentMetadata(buf []byte) (*SegmentMetadata, error) {
	if len(buf) < 64 {
		return nil, io.ErrUnexpectedEOF
	}

	crc := binary.LittleEndian.Uint32(buf[56:60])
	computed := crc32.ChecksumIEEE(buf[0:56])
	if crc != computed {
		return nil, fmt.Errorf("segment metadata CRC mismatch: expected %08x, got %08x", crc, computed)
	}

	meta := &SegmentMetadata{
		Magic:          binary.LittleEndian.Uint32(buf[0:4]),
		Version:        binary.LittleEndian.Uint32(buf[4:8]),
		CreatedAt:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		LastModifiedAt: int64(binary.LittleEndian.Uint64(buf[16:24])),
		WriteOffset:    int64(binary.LittleEndian.Uint64(buf[24:32])),
		EntryCount:     int64(binary.LittleEndian.Uint64(buf[32:40])),
		Flags:          binary.LittleEndian.Uint32(buf[40:44]),
	}
	return meta, nil
}

// For now each value is not chunked into smaller blocks, but we will benchmark and see later
// if this improves things we can decide.
// For Now every entry will be of type full.
const (
	ChunkTypeFull   = 1
	ChunkTypeFirst  = 2
	ChunkTypeMiddle = 3
	ChunkTypeLast   = 4
)

type MsyncOption int

const (
	// MsyncNone skips msync after write.
	MsyncNone MsyncOption = iota

	// MsyncOnWrite calls msync (Flush) after every write.
	MsyncOnWrite
)

type SegmentID = uint32

const (
	// layout: 4 (checksum) + 4 (length) + 1 (type) (if we decide to divide into block) = 9 bytes
	chunkHeaderSize = 9
	// fixed segment size of 16MB to avoid accidental different segment in different nodes.
	segmentSize  = 16 * 1024 * 1024
	fileModePerm = 0644
)

var (
	ErrClosed          = errors.New("the segment file is closed")
	ErrInvalidCRC      = errors.New("invalid crc, the data may be corrupted")
	ErrCorruptHeader   = errors.New("corrupt chunk header, invalid length")
	ErrIncompleteChunk = errors.New("incomplete or torn write detected at chunk trailer")
)

const (
	// size of the trailer used to detect torn writes.
	// We are writing this to detect torn or partial writes caused by unexpected shutdowns or disk failures.
	// This is inspired by a real-world issue observed in etcd v2.3:
	// SEE: https://github.com/etcd-io/etcd/issues/6191#issuecomment-240268979
	// By adding a known trailer marker (e.g., 0xDEADBEEF), we can explicitly validate that a chunk
	// was fully persisted, and safely stop recovery at the first missing or corrupted trailer.
	chunkTrailerSize = 4
)

var (
	// marker written after every WAL chunk to detect torn/incomplete writes.
	trailerCanary = []byte{0xDE, 0xAD, 0xBE, 0xEF}
)

type segmentReader struct {
	segment    *segment
	readOffset int64
}

// ChunkPosition is the logical location of a chunk within a WAL segment.
type ChunkPosition struct {
	SegmentID SegmentID
	Offset    int64
}

func (cp ChunkPosition) String() string {
	return fmt.Sprintf("SegmentID=%d, Offset=%d", cp.SegmentID, cp.Offset)
}

// Encode serializes the ChunkPosition into a fixed-length byte slice.
func (cp ChunkPosition) Encode() []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], cp.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(cp.Offset))
	return buf
}

// DecodeChunkPosition deserializes a byte slice into a ChunkPosition.
func DecodeChunkPosition(data []byte) (ChunkPosition, error) {
	if len(data) < 12 {
		return ChunkPosition{}, io.ErrUnexpectedEOF
	}
	cp := ChunkPosition{
		SegmentID: binary.LittleEndian.Uint32(data[0:4]),
		Offset:    int64(binary.LittleEndian.Uint64(data[4:12])),
	}
	return cp, nil
}

type segment struct {
	id           SegmentID
	fd           *os.File
	mmapData     mmap.MMap
	mmapSize     int64
	writeOffset  atomic.Int64
	activeReader atomic.Int32
	closed       atomic.Bool
	header       []byte
	writeMu      sync.RWMutex
	syncOption   MsyncOption
}

// WithSyncOption sets the sync option for the segment.
func WithSyncOption(opt MsyncOption) func(*segment) {
	return func(s *segment) {
		s.syncOption = opt
	}
}

func openSegmentFile(dirPath, extName string, id uint32, opts ...func(*segment)) (*segment, error) {
	path := SegmentFileName(dirPath, extName, id)
	isNew, err := isNewSegment(path)
	if err != nil {
		return nil, err
	}

	fd, mmapData, err := prepareSegmentFile(path)
	if err != nil {
		return nil, err
	}

	s := &segment{
		id:         id,
		fd:         fd,
		header:     make([]byte, chunkHeaderSize),
		mmapData:   mmapData,
		mmapSize:   segmentSize,
		syncOption: MsyncNone,
	}

	for _, opt := range opts {
		opt(s)
	}

	offset := segmentMetadataSize
	if isNew {
		writeInitialMetadata(mmapData)
	} else {
		offset = int(scanForLastOffset(path, mmapData))
	}
	s.writeOffset.Store(int64(offset))

	return s, nil
}

func isNewSegment(path string) (bool, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat error: %w", err)
	}
	return false, nil
}

func prepareSegmentFile(path string) (*os.File, mmap.MMap, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileModePerm)
	if err != nil {
		return nil, nil, err
	}
	if err := fd.Truncate(segmentSize); err != nil {
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
	binary.LittleEndian.PutUint32(mmapData[4:8], currentMetadataVersion)
	now := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(mmapData[8:16], now)
	binary.LittleEndian.PutUint64(mmapData[16:24], now)
	binary.LittleEndian.PutUint64(mmapData[24:32], segmentMetadataSize)
	binary.LittleEndian.PutUint64(mmapData[32:40], 0)
	binary.LittleEndian.PutUint32(mmapData[40:44], FlagActive)
	crc := crc32.ChecksumIEEE(mmapData[0:56])
	binary.LittleEndian.PutUint32(mmapData[56:60], crc)
}

func scanForLastOffset(path string, mmapData mmap.MMap) int64 {
	var offset int64 = segmentMetadataSize

	for offset+chunkHeaderSize <= segmentSize {
		header := mmapData[offset : offset+chunkHeaderSize]
		length := binary.LittleEndian.Uint32(header[4:8])
		entrySize := int64(chunkHeaderSize + length + chunkTrailerSize)

		if offset+entrySize > segmentSize {
			break
		}

		data := mmapData[offset+chunkHeaderSize : offset+chunkHeaderSize+int64(length)]
		trailer := mmapData[offset+chunkHeaderSize+int64(length) : offset+entrySize]
		savedSum := binary.LittleEndian.Uint32(header[:4])
		computedSum := crc32Checksum(header[4:], data)

		if savedSum == 0 && length == 0 {
			break
		}
		if savedSum == 0 || savedSum != computedSum || !bytes.Equal(trailer, trailerCanary) {
			slog.Warn("[unisondb.fswal]",
				slog.String("event_type", "segment.recovery.stopped.checksum.mismatch"),
				slog.Int64("offset", offset),
				slog.Uint64("saved", uint64(savedSum)),
				slog.Uint64("computed", uint64(computedSum)),
				slog.String("segment", path),
				slog.Bool("trailer_corrupted", !bytes.Equal(trailer, trailerCanary)),
			)
			break
		}

		offset += entrySize
	}

	return offset
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed.Load() {
		return nil, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	offset := seg.writeOffset.Load()
	entrySize := int64(chunkHeaderSize + len(data) + chunkTrailerSize)
	if offset+entrySize > seg.mmapSize {
		return nil, errors.New("write exceeds segment size")
	}

	binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
	seg.header[8] = ChunkTypeFull
	sum := crc32Checksum(seg.header[4:], data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	if offset+entrySize > seg.mmapSize {
		return nil, errors.New("write exceeds segment size")
	}

	copy(seg.mmapData[offset:], seg.header[:])
	copy(seg.mmapData[offset+chunkHeaderSize:], data)

	canaryOffset := offset + chunkHeaderSize + int64(len(data))
	copy(seg.mmapData[canaryOffset:], trailerCanary)

	newOffset := offset + entrySize
	seg.writeOffset.Store(newOffset)

	binary.LittleEndian.PutUint32(seg.mmapData[24:32], uint32(newOffset))
	prevCount := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], prevCount+1)
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.ChecksumIEEE(seg.mmapData[0:56])
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)

	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return nil, fmt.Errorf("mmap flush error after write: %w", err)
		}
	}

	return &ChunkPosition{
		SegmentID: seg.id,
		Offset:    offset,
	}, nil
}

func (seg *segment) Read(offset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed.Load() {
		return nil, nil, ErrClosed
	}
	if offset+chunkHeaderSize > seg.mmapSize {
		return nil, nil, io.EOF
	}

	header := seg.mmapData[offset : offset+chunkHeaderSize]
	length := binary.LittleEndian.Uint32(header[4:8])
	if length > uint32(seg.Size()-offset-chunkHeaderSize) {
		return nil, nil, ErrCorruptHeader
	}
	entrySize := chunkHeaderSize + int64(length) + chunkTrailerSize
	if offset+entrySize > seg.Size() {
		return nil, nil, io.EOF
	}

	data := seg.mmapData[offset+chunkHeaderSize : offset+chunkHeaderSize+int64(length)]
	trailer := seg.mmapData[offset+chunkHeaderSize+int64(length) : offset+entrySize]

	savedSum := binary.LittleEndian.Uint32(header[:4])
	computedSum := crc32Checksum(header[4:], data)
	if savedSum != computedSum {
		return nil, nil, ErrInvalidCRC
	}

	if !bytes.Equal(trailer, trailerCanary) {
		return nil, nil, ErrIncompleteChunk
	}

	next := &ChunkPosition{
		SegmentID: seg.id,
		Offset:    offset + entrySize,
	}
	return data, next, nil
}

func (seg *segment) Sync() error {
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

func (seg *segment) MSync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}
	return nil
}

func (seg *segment) WillExceed(dataSize int) bool {
	entrySize := int64(chunkHeaderSize + dataSize)
	offset := seg.writeOffset.Load()
	return offset+entrySize > seg.mmapSize
}

func (seg *segment) Close() error {
	if seg.closed.Load() {
		return nil
	}

	for seg.activeReader.Load() > 0 {
		runtime.Gosched()
	}

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

func (seg *segment) Size() int64 {
	return seg.writeOffset.Load()
}

func (seg *segment) GetLastModifiedAt() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentMetadata(seg.mmapData[:segmentMetadataSize])
	if err != nil {
		return 0 // or panic/log if you want to enforce integrity
	}
	return meta.LastModifiedAt
}

func (seg *segment) GetEntryCount() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentMetadata(seg.mmapData[:segmentMetadataSize])
	if err != nil {
		return 0
	}
	return meta.EntryCount
}

func (seg *segment) GetFlags() uint32 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentMetadata(seg.mmapData[:segmentMetadataSize])
	if err != nil {
		return 0
	}
	return meta.Flags
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:    seg,
		readOffset: segmentMetadataSize,
	}
}

func (r *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if r.readOffset >= r.segment.Size() {
		return nil, nil, io.EOF
	}
	data, next, err := r.segment.Read(r.readOffset)
	if err != nil {
		return nil, nil, err
	}
	r.readOffset = next.Offset
	return data, next, nil
}

func crc32Checksum(header []byte, data []byte) uint32 {
	sum := crc32.ChecksumIEEE(header)
	return crc32.Update(sum, crc32.IEEETable, data)
}

// SegmentFileName returns the file name of a segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}
