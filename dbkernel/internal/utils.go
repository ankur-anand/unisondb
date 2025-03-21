package internal

import (
	"fmt"

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
	kv := logcodec.DeserializeKVEntry(dr.Entries[0])
	key := kv.Key
	records, err := walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return 0, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, len(preparedRecords))
	for i, record := range preparedRecords {
		r := logcodec.DeserializeFBRootLogRecord(record)
		kv := logcodec.DeserializeKVEntry(r.Entries[0])
		values[i] = kv.Value
	}

	return len(records), store.SetChunks(key, values, checksum)
}

// GetWalRecord returns the underlying wal record.
func GetWalRecord(entry y.ValueStruct, wIO *wal.WalIO) (*logrecord.LogRecord, error) {
	chunkPos := wal.DecodeOffset(entry.Value)
	walValue, err := wIO.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return logrecord.GetRootAsLogRecord(walValue, 0), nil
}
