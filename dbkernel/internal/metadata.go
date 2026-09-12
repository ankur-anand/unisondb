package internal

import (
	"encoding/binary"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
)

var (
	SysKeyWalCheckPoint = []byte("sys.kv.unisondb.key.wal.checkpoint")
	SysKeyBloomFilter   = []byte("sys.kv.unisondb.key.bloom-filter")
)

// Metadata represents a checkpoint in the Write-Ahead Log (WAL).
// It encodes the last known chunk position (`Pos`) within the segment file. This is primarily used for
// recovery and replication tracking.
type Metadata struct {
	// Cumulative flushed-record count. This is not a WAL LSN: unfinished
	// transactions also consume sequence numbers without materializing data.
	RecordProcessed uint64
	// Position of the last written chunk in WAL
	Pos *wal.Offset
}

// SaveMetadata saves the WAL checkpoint to BTreeStore.
func SaveMetadata(db BTreeStore, pos *wal.Offset, index uint64) error {
	metaData := Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
	value := metaData.MarshalBinary()

	return db.StoreMetadata(SysKeyWalCheckPoint, value)
}

const posEncodedSize = 12

// encodedMetadataSize is the on-disk checkpoint size: [RecordProcessed:8][Pos:12].
const encodedMetadataSize = 8 + posEncodedSize

// Format: [RecordProcessed:8][Pos:12] = 20 bytes.
func (m *Metadata) MarshalBinary() []byte {
	result := make([]byte, encodedMetadataSize)
	binary.LittleEndian.PutUint64(result[0:8], m.RecordProcessed)
	if m.Pos != nil {
		copy(result[8:20], m.Pos.Encode())
	}

	return result
}

// Format: [RecordProcessed:8][Pos:12] = 20 bytes.
func UnmarshalMetadata(data []byte) Metadata {
	if len(data) < encodedMetadataSize {
		return Metadata{}
	}
	index := binary.LittleEndian.Uint64(data[:8])
	pos := wal.DecodeOffset(data[8:20])

	return Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
}
