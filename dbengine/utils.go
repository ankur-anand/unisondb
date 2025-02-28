package dbengine

import (
	"fmt"

	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
)

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based store.
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
