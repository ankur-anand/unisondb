package dbkernel

import (
	"encoding/binary"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
)

// Metadata represents a checkpoint in the Write-Ahead Log (WAL).
// It encodes the last known chunk position (`Pos`) within the segment file. This is primarily used for
// recovery and replication tracking.
type Metadata struct {
	RecordProcessed uint64      // (monotonic)
	Pos             *wal.Offset // Position of the last written chunk in WAL
}

// SaveMetadata saves the WAL checkpoint to BTreeStore.
func SaveMetadata(db BTreeStore, pos *wal.Offset, index uint64) error {
	metaData := Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
	value := metaData.MarshalBinary()

	return db.StoreMetadata(sysKeyWalCheckPoint, value)
}

// MarshalBinary encodes a Metadata struct to a byte slice.
func (m *Metadata) MarshalBinary() []byte {
	// Encode the chunk position
	encodedPos := m.Pos.Encode()
	result := make([]byte, len(encodedPos)+8)
	binary.LittleEndian.PutUint64(result, m.RecordProcessed)

	copy(result[8:], encodedPos)
	return result
}
