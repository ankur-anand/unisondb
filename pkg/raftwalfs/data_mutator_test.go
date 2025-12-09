package raftwalfs

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

func createTestLogRecord(lsn, hlc uint64) []byte {
	builder := flatbuffers.NewBuilder(256)

	logrecord.LogRecordStart(builder)
	logrecord.LogRecordAddLsn(builder, lsn)
	logrecord.LogRecordAddHlc(builder, hlc)
	logrecord.LogRecordAddOperationType(builder, logrecord.LogOperationTypeInsert)
	logrecord.LogRecordAddTxnState(builder, logrecord.TransactionStateNone)
	logrecord.LogRecordAddEntryType(builder, logrecord.LogEntryTypeKV)
	end := logrecord.LogRecordEnd(builder)
	builder.Finish(end)

	return builder.FinishedBytes()
}

func TestLogRecordMutator_MutateLSN(t *testing.T) {
	mutator := LogRecordMutator{}

	tests := []struct {
		name       string
		initialLSN uint64
		newLSN     uint64
	}{
		{
			name:       "placeholder to actual",
			initialLSN: 1,
			newLSN:     12345,
		},
		{
			name:       "non-zero to different",
			initialLSN: 100,
			newLSN:     200,
		},
		{
			name:       "large value",
			initialLSN: 1,
			newLSN:     ^uint64(0) - 1,
		},
		{
			name:       "small to large",
			initialLSN: 1,
			newLSN:     1_000_000_000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := createTestLogRecord(tt.initialLSN, 999)

			assert.Equal(t, tt.initialLSN, mutator.GetLSN(data))
			err := mutator.MutateLSN(data, tt.newLSN)
			require.NoError(t, err)

			assert.Equal(t, tt.newLSN, mutator.GetLSN(data))
			record := logrecord.GetRootAsLogRecord(data, 0)
			assert.Equal(t, uint64(999), record.Hlc())
			assert.Equal(t, logrecord.LogOperationTypeInsert, record.OperationType())
		})
	}
}

func TestLogRecordMutator_MutateLSN_ZeroNotStored(t *testing.T) {
	mutator := LogRecordMutator{}
	data := createTestLogRecord(0, 999)
	err := mutator.MutateLSN(data, 12345)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field not present")
}

func TestLogRecordMutator_EmptyData(t *testing.T) {
	mutator := LogRecordMutator{}
	err := mutator.MutateLSN([]byte{}, 100)
	assert.NoError(t, err)

	err = mutator.MutateLSN(nil, 100)
	assert.NoError(t, err)
}

func TestLogRecordMutator_GetLSN(t *testing.T) {
	mutator := LogRecordMutator{}
	data := createTestLogRecord(42, 100)
	assert.Equal(t, uint64(42), mutator.GetLSN(data))
	assert.Equal(t, uint64(0), mutator.GetLSN([]byte{}))
	assert.Equal(t, uint64(0), mutator.GetLSN(nil))
}
