package internal

import (
	"fmt"

	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
)

// HandleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based dataStore.
// extracted in util as both memTable and wal recovery instance uses it.
func HandleChunkedValuesTxn(record *logrecord.LogRecord, walIO *wal.WalIO, store TxnBatcher) (int, error) {
	checksum := record.Crc32Checksum()
	dr := logcodec.DeserializeFBRootLogRecord(record)
	key := dr.Payload.KeyValueBatchEntries.Entries[0].Key
	records, err := walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return 0, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, len(preparedRecords))
	for i, record := range preparedRecords {
		r := logcodec.DeserializeFBRootLogRecord(record)
		values[i] = r.Payload.KeyValueBatchEntries.Entries[0].Value
	}

	return len(records), store.SetChunks(key, values, checksum)
}

// GetWalRecord returns the underlying wal record.
func GetWalRecord(entry y.ValueStruct, wIO *wal.WalIO) (*logrecord.LogRecord, error) {
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk position: %w", err)
	}

	if chunkPos == nil {
		return logrecord.GetRootAsLogRecord(value, 0), nil
	}

	walValue, err := wIO.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return logrecord.GetRootAsLogRecord(walValue, 0), nil
}

// decodeChunkPositionWithValue decodes a MemTable entry into either a ChunkPosition (WAL lookup) or a direct value.
func decodeChunkPositionWithValue(data []byte) (*wal.Offset, []byte, error) {
	if len(data) == 0 {
		return nil, nil, kvdrivers.ErrKeyNotFound
	}

	flag := data[0] // First byte determines type

	switch flag {
	case DirectValuePrefix:
		// Direct value stored
		return nil, data[1:], nil
	case WalReferencePrefix:
		// Stored ChunkPosition (WAL lookup required)
		chunkPos := wal.DecodeOffset(data[1:])

		return chunkPos, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid MemTable entry flag: %d", flag)
	}
}

func ConvertRowFromLogOperationData(logData *logcodec.LogOperationData) ([][]byte, []map[string][]byte) {
	if logData == nil || logData.RowUpdateEntries == nil {
		return nil, nil
	}

	rowKeys := make([][]byte, 0, len(logData.RowUpdateEntries.Entries))
	columnEntriesPerRow := make([]map[string][]byte, 0, len(logData.RowUpdateEntries.Entries))

	for _, rowEntry := range logData.RowUpdateEntries.Entries {
		rowKeys = append(rowKeys, rowEntry.Key)
		columnEntries := make(map[string][]byte)

		for _, col := range rowEntry.Columns {
			columnEntries[col.Name] = col.Value
		}

		columnEntriesPerRow = append(columnEntriesPerRow, columnEntries)
	}

	return rowKeys, columnEntriesPerRow
}
