package dbengine

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/prometheus/common/helpers/templates"
)

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based dataStore.
// extracted in util as both memTable and wal recovery instance uses it.
func handleChunkedValuesTxn(record *walrecord.WalRecord, walIO *wal.WalIO, store BTreeStore) (int, error) {
	checksum := unmarshalChecksum(record.ValueBytes())
	records, err := walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return 0, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, len(preparedRecords))
	for i, record := range preparedRecords {
		values[i] = record.ValueBytes()
	}

	return len(records), store.SetChunks(record.KeyBytes(), values, checksum)
}

func getValueStruct(ops byte, direct bool, value []byte) y.ValueStruct {
	storeValue := make([]byte, len(value)+1)

	switch direct {
	case true:
		storeValue[0] = directValuePrefix
	default:
		storeValue[0] = walReferencePrefix
	}

	copy(storeValue[1:], value)

	return y.ValueStruct{
		Meta:  ops,
		Value: storeValue,
	}
}

// decodeChunkPositionWithValue decodes a MemTable entry into either a ChunkPosition (WAL lookup) or a direct value.
func decodeChunkPositionWithValue(data []byte) (*wal.Offset, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrKeyNotFound
	}

	flag := data[0] // First byte determines type

	switch flag {
	case directValuePrefix:
		// Direct value stored
		return nil, data[1:], nil
	case walReferencePrefix:
		// Stored ChunkPosition (WAL lookup required)
		chunkPos := wal.DecodeOffset(data[1:])

		return chunkPos, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid MemTable entry flag: %d", flag)
	}
}

// getWalRecord returns the underlying wal record.
func getWalRecord(entry y.ValueStruct, wIO *wal.WalIO) (*walrecord.WalRecord, error) {
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk position: %w", err)
	}

	if chunkPos == nil {
		return walrecord.GetRootAsWalRecord(value, 0), nil
	}

	walValue, err := wIO.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return walrecord.GetRootAsWalRecord(walValue, 0), nil
}

func marshalChecksum(checksum uint32) []byte {
	buf := make([]byte, 4) // uint32 takes 4 bytes
	binary.LittleEndian.PutUint32(buf, checksum)
	return buf
}

func unmarshalChecksum(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
