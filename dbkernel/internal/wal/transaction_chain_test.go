package wal_test

import (
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/stretchr/testify/require"
)

func transactionChainFixture() []logcodec.LogRecord {
	return []logcodec.LogRecord{
		{LSN: 10, TxnID: []byte("txn"), EntryType: logrecord.LogEntryTypeKV, TxnState: logrecord.TransactionStateBegin},
		{LSN: 11, TxnState: logrecord.TransactionStateNone},
		{LSN: 12, TxnID: []byte("txn"), EntryType: logrecord.LogEntryTypeKV, TxnState: logrecord.TransactionStatePrepare,
			OperationType: logrecord.LogOperationTypeInsert, PrevTxnIndex: 10, Entries: [][]byte{logcodec.SerializeKVEntry([]byte("a"), []byte("value"))}},
		{LSN: 13, TxnID: []byte("other"), EntryType: logrecord.LogEntryTypeKV, TxnState: logrecord.TransactionStateBegin},
		{LSN: 14, TxnID: []byte("txn"), EntryType: logrecord.LogEntryTypeKV, TxnState: logrecord.TransactionStatePrepare,
			OperationType: logrecord.LogOperationTypeInsert, PrevTxnIndex: 12, Entries: [][]byte{logcodec.SerializeKVEntry([]byte("b"), []byte("value"))}},
		{LSN: 15, TxnID: []byte("txn"), EntryType: logrecord.LogEntryTypeKV, TxnState: logrecord.TransactionStateCommit,
			OperationType: logrecord.LogOperationTypeInsert, PrevTxnIndex: 14},
	}
}

func TestTransactionChainValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func([]logcodec.LogRecord)
	}{
		{"valid_interleaved", nil},
		{"foreign_prepare", func(r []logcodec.LogRecord) { r[2].TxnID = []byte("other") }},
		{"foreign_begin", func(r []logcodec.LogRecord) { r[0].TxnID = []byte("other") }},
		{"foreign_commit", func(r []logcodec.LogRecord) { r[5].TxnID = []byte("other") }},
		{"missing_txn_id", func(r []logcodec.LogRecord) { r[5].TxnID = nil }},
		{"different_entry_type", func(r []logcodec.LogRecord) { r[2].EntryType = logrecord.LogEntryTypeRow }},
		{"different_operation", func(r []logcodec.LogRecord) { r[2].OperationType = logrecord.LogOperationTypeDelete }},
		{"wrong_index_identity", func(r []logcodec.LogRecord) { r[2].LSN = 99 }},
		{"missing_record", func(r []logcodec.LogRecord) { r[2].PrevTxnIndex = 9 }},
		{"non_transaction_record", func(r []logcodec.LogRecord) { r[2].TxnState = logrecord.TransactionStateNone }},
		{"commit_in_chain", func(r []logcodec.LogRecord) { r[2].TxnState = logrecord.TransactionStateCommit }},
		{"empty_prepare", func(r []logcodec.LogRecord) { r[2].Entries = nil }},
		{"missing_begin", func(r []logcodec.LogRecord) { r[2].PrevTxnIndex = 0 }},
		{"self_link", func(r []logcodec.LogRecord) { r[2].PrevTxnIndex = 12 }},
		{"cycle", func(r []logcodec.LogRecord) { r[2].PrevTxnIndex = 14 }},
		{"begin_has_previous", func(r []logcodec.LogRecord) { r[0].PrevTxnIndex = 9 }},
		{"commit_has_no_previous", func(r []logcodec.LogRecord) { r[5].PrevTxnIndex = 0 }},
		{"commit_points_forward", func(r []logcodec.LogRecord) { r[5].PrevTxnIndex = 16 }},
		{"not_a_commit", func(r []logcodec.LogRecord) { r[5].TxnState = logrecord.TransactionStatePrepare }},
		{"zero_commit_index", func(r []logcodec.LogRecord) { r[5].LSN = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := setupWalTest(t)
			records := transactionChainFixture()
			if tc.change != nil {
				tc.change(records)
			}
			for i := range records[:5] {
				_, err := w.Append(records[i].FBEncode(128), uint64(10+i))
				require.NoError(t, err)
			}
			commit := logrecord.GetRootAsLogRecord(records[5].FBEncode(128), 0)
			got, err := w.GetTransactionRecords(commit)
			if tc.change != nil {
				require.ErrorIs(t, err, wal.ErrInvalidTxnChain)
				require.Nil(t, got, "never return a partially validated chain")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 3)
			for i, index := range []uint64{10, 12, 14} {
				require.Equal(t, index, got[i].Lsn())
			}
		})
	}
}

func TestTransactionChainEmptyAndNil(t *testing.T) {
	w := setupWalTest(t)
	_, err := w.GetTransactionRecords(nil)
	require.ErrorIs(t, err, wal.ErrInvalidTxnChain)
	records := transactionChainFixture()
	_, err = w.Append(records[0].FBEncode(128), 10)
	require.NoError(t, err)
	records[5].PrevTxnIndex = 10
	got, err := w.GetTransactionRecords(logrecord.GetRootAsLogRecord(records[5].FBEncode(128), 0))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, logrecord.TransactionStateBegin, got[0].TxnState())
}
