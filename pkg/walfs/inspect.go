package walfs

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// SegmentInspection describes the stored metadata and optional record index of
// a segment. Inspection never recovers a segment or reads/writes index sidecars.
type SegmentInspection struct {
	Header       SegmentHeader
	Size         int64
	IndexEntries []InspectedIndexEntry
}

// InspectedIndexEntry is a record's offset and payload length within the inspected file.
type InspectedIndexEntry struct {
	Offset int64
	Length uint32
}

// InspectSegmentFile reads an existing segment using a read-only file handle.
// Metadata is reported as stored, without recovery, resizing, sealing, or sync.
// With includeIndex, records up to the stored write offset are validated and
// indexed in memory. Corruption is returned to the caller without repairing it.
func InspectSegmentFile(path string, includeIndex bool) (*SegmentInspection, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("segment %s is not a regular file", path)
	}
	var header [segmentHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return nil, fmt.Errorf("read segment header: %w", err)
	}
	meta, err := decodeSegmentHeader(header[:])
	if err != nil {
		return nil, err
	}
	meta.CRC = binary.LittleEndian.Uint32(header[56:60])
	if meta.Magic != segmentMagicNumber || meta.Version != segmentHeaderVersion {
		return nil, fmt.Errorf("unsupported segment magic or version: %08x/%d", meta.Magic, meta.Version)
	}
	if meta.WriteOffset < segmentHeaderSize || meta.WriteOffset > info.Size() || meta.WriteOffset != alignUp(meta.WriteOffset) || meta.EntryCount < 0 {
		return nil, fmt.Errorf("invalid segment metadata: write offset %d, entry count %d, file size %d", meta.WriteOffset, meta.EntryCount, info.Size())
	}
	inspection := &SegmentInspection{Header: *meta, Size: info.Size()}
	if !includeIndex {
		return inspection, nil
	}
	inspection.IndexEntries, err = inspectSegmentIndex(file, meta)
	if err != nil {
		return nil, err
	}
	return inspection, nil
}

func inspectSegmentIndex(file *os.File, meta *SegmentHeader) ([]InspectedIndexEntry, error) {
	reader := bufio.NewReader(io.NewSectionReader(file, segmentHeaderSize, meta.WriteOffset-segmentHeaderSize))
	checksum := crc32.New(crcTable)
	copyBuffer := make([]byte, 32*1024)
	var entries []InspectedIndexEntry
	for offset := int64(segmentHeaderSize); offset < meta.WriteOffset; {
		var recordHeader [recordHeaderSize]byte
		if _, err := io.ReadFull(reader, recordHeader[:]); err != nil {
			return nil, fmt.Errorf("read record at %d: %w", offset, err)
		}
		length := binary.LittleEndian.Uint32(recordHeader[4:])
		rawSize := int64(recordHeaderSize) + int64(length) + recordTrailerMarkerSize
		entrySize := alignUp(rawSize)
		if entrySize > meta.WriteOffset-offset {
			return nil, fmt.Errorf("record at %d: %w", offset, ErrCorruptHeader)
		}
		checksum.Reset()
		_, _ = checksum.Write(recordHeader[4:])
		n, err := io.CopyBuffer(checksum, io.LimitReader(reader, int64(length)), copyBuffer)
		if err != nil {
			return nil, fmt.Errorf("read record at %d: %w", offset, err)
		}
		if n != int64(length) {
			return nil, fmt.Errorf("read record at %d: %w", offset, io.ErrUnexpectedEOF)
		}
		if checksum.Sum32() != binary.LittleEndian.Uint32(recordHeader[:4]) {
			return nil, fmt.Errorf("record at %d: %w", offset, ErrInvalidCRC)
		}
		var trailer [recordTrailerMarkerSize]byte
		if _, err := io.ReadFull(reader, trailer[:]); err != nil {
			return nil, fmt.Errorf("read trailer at %d: %w", offset, err)
		}
		if binary.LittleEndian.Uint64(trailer[:]) != trailerWord {
			return nil, fmt.Errorf("record at %d: %w", offset, ErrIncompleteChunk)
		}
		if _, err := reader.Discard(int(entrySize - rawSize)); err != nil {
			return nil, fmt.Errorf("read padding at %d: %w", offset, err)
		}
		entries = append(entries, InspectedIndexEntry{Offset: offset, Length: length})
		offset += entrySize
	}
	if int64(len(entries)) != meta.EntryCount {
		return nil, fmt.Errorf("segment entry count mismatch: header %d, records %d", meta.EntryCount, len(entries))
	}
	return entries, nil
}
