package walrecord_test

import (
	"sync"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/stretchr/testify/assert"
)

func TestFBEncode_ColumnType(t *testing.T) {
	record := &walrecord.Record{
		Index:        1,
		Hlc:          1234567890,
		Key:          []byte("test-key"),
		Value:        []byte("test-value"),
		LogOperation: walrecord.LogOperationInsert,
		EntryType:    walrecord.EntryTypeRow,
		TxnStatus:    walrecord.TxnStatusCommit,
		TxnID:        []byte("txn-12345"),
		PrevTxnOffset: &wal.Offset{
			SegmentId:   1,
			BlockNumber: 10,
			ChunkOffset: 100,
			ChunkSize:   50,
		},
		ColumnEntries: map[string][]byte{
			"column1": []byte("value1"),
			"column2": []byte("value2"),
		},
	}

	encodedBytes, err := record.FBEncode()
	assert.NoError(t, err)
	assert.NotEmpty(t, encodedBytes)

	buf := walrecord.GetRootAsWalRecord(encodedBytes, 0)

	assert.Equal(t, record.Index, buf.Index())
	assert.Equal(t, record.Hlc, buf.Hlc())
	assert.Equal(t, record.LogOperation, buf.Operation())
	assert.Equal(t, record.TxnStatus, buf.TxnStatus())
	assert.Equal(t, record.EntryType, buf.EntryType())
	assert.Equal(t, string(record.Key), string(buf.KeyBytes()))
	assert.Equal(t, string(record.Value), string(buf.ValueBytes()))
	assert.Equal(t, string(record.TxnID), string(buf.TxnIdBytes()))
	prevTxnBytes := buf.PrevTxnWalIndexBytes()
	assert.NotEmpty(t, prevTxnBytes)

	columns := buf.ColumnsLength()
	assert.Equal(t, len(record.ColumnEntries), columns)
	for i := 0; i < columns; i++ {
		var colEntry walrecord.ColumnEntry
		if buf.Columns(&colEntry, i) {
			value, ok := record.ColumnEntries[string(colEntry.ColumnName())]
			assert.True(t, ok)
			assert.Equal(t, value, colEntry.ColumnValueBytes())
		}
	}
}

func TestFBEncode_ValueType(t *testing.T) {
	record := &walrecord.Record{
		Index:        1,
		Hlc:          1234567890,
		Key:          []byte("test-key"),
		Value:        []byte("test-value"),
		LogOperation: walrecord.LogOperationInsert,
		EntryType:    walrecord.EntryTypeKV,
		TxnStatus:    walrecord.TxnStatusCommit,
		TxnID:        []byte("txn-12345"),
		PrevTxnOffset: &wal.Offset{
			SegmentId:   1,
			BlockNumber: 10,
			ChunkOffset: 100,
			ChunkSize:   50,
		},
		ColumnEntries: map[string][]byte{
			"column1": []byte("value1"),
			"column2": []byte("value2"),
		},
	}

	encodedBytes, err := record.FBEncode()
	assert.NoError(t, err)
	assert.NotEmpty(t, encodedBytes)

	buf := walrecord.GetRootAsWalRecord(encodedBytes, 0)

	assert.Equal(t, record.Index, buf.Index())
	assert.Equal(t, record.Hlc, buf.Hlc())
	assert.Equal(t, record.LogOperation, buf.Operation())
	assert.Equal(t, record.TxnStatus, buf.TxnStatus())
	assert.Equal(t, record.EntryType, buf.EntryType())
	assert.Equal(t, string(record.Key), string(buf.KeyBytes()))
	assert.Equal(t, string(record.Value), string(buf.ValueBytes()))
	assert.Equal(t, string(record.TxnID), string(buf.TxnIdBytes()))
	prevTxnBytes := buf.PrevTxnWalIndexBytes()
	assert.NotEmpty(t, prevTxnBytes)

	columns := buf.ColumnsLength()
	assert.Equal(t, 0, columns)
}

func TestLargeParallelEncode(t *testing.T) {
	numRecords := 10000
	var wg sync.WaitGroup
	wg.Add(numRecords)

	for i := 0; i < numRecords; i++ {
		go func(i int) {
			defer wg.Done()

			record := &walrecord.Record{
				Index:        uint64(i),
				Hlc:          1234567890,
				Key:          []byte("test-key"),
				Value:        []byte("test-value"),
				LogOperation: walrecord.LogOperationInsert,
				EntryType:    walrecord.EntryTypeRow,
				TxnStatus:    walrecord.TxnStatusCommit,
				TxnID:        []byte("txn-12345"),
				PrevTxnOffset: &wal.Offset{
					SegmentId:   1,
					BlockNumber: 10,
					ChunkOffset: 100,
					ChunkSize:   50,
				},
				ColumnEntries: map[string][]byte{
					"column1": []byte("value1"),
					"column2": []byte("value2"),
				},
			}

			encodedBytes, err := record.FBEncode()
			assert.NoError(t, err)
			assert.NotEmpty(t, encodedBytes)

			buf := walrecord.GetRootAsWalRecord(encodedBytes, 0)

			assert.Equal(t, record.Index, buf.Index())
			assert.Equal(t, record.Hlc, buf.Hlc())
			assert.Equal(t, record.LogOperation, buf.Operation())
			assert.Equal(t, record.TxnStatus, buf.TxnStatus())
			assert.Equal(t, record.EntryType, buf.EntryType())
		}(i)
	}

	wg.Wait()
}
