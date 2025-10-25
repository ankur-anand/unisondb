package wal_test

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func setupWalTest(t *testing.T) *wal.WalIO {
	dir := t.TempDir()

	walInstance, err := wal.NewWalIO(dir, "test_namespace", wal.NewDefaultConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "closing wal instance failed")
	})

	return walInstance
}

func TestWalIO_Suite(t *testing.T) {

	walInstance := setupWalTest(t)

	appendData := gofakeit.LetterN(10)
	t.Run("wal_append_read", func(t *testing.T) {
		pos, err := walInstance.Append([]byte(appendData))
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		data, err := walInstance.Read(pos)
		assert.NoError(t, err)
		assert.Equal(t, appendData, string(data))
	})

	t.Run("wal_append_reader", func(t *testing.T) {
		reader, err := walInstance.NewReader()
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		value, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, appendData, string(value))
		assert.NotNil(t, pos)

		_, pos, err = reader.Next()
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, pos.IsZero(), true)
	})

	t.Run("wal_reader_with_start", func(t *testing.T) {
		appendData2 := gofakeit.LetterN(10)
		pos, err := walInstance.Append([]byte(appendData2))
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		reader, err := walInstance.NewReaderWithStart(pos)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		value, _, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, appendData2, string(value))

		_, pos1, err := reader.Next()
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, pos1.IsZero(), true)
	})

	t.Run("wal_fsync", func(t *testing.T) {
		err := walInstance.Sync()
		assert.NoError(t, err)
	})

	t.Run("concurrent_read_write", func(t *testing.T) {
		var offsets []*wal.Offset
		var offsetMu sync.Mutex

		numWriters := 4
		numReaders := 4
		numOps := 100

		var wg sync.WaitGroup

		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numOps; j++ {
					data := gofakeit.LetterN(128)
					pos, err := walInstance.Append([]byte(data))
					assert.NoError(t, err, "append should not fail")
					assert.NotNil(t, pos, "append should return a valid position")

					offsetMu.Lock()
					offsets = append(offsets, pos)
					offsetMu.Unlock()
				}
			}()
		}

		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numOps; j++ {
					offsetMu.Lock()
					if len(offsets) == 0 {
						offsetMu.Unlock()
						continue
					}
					pos := offsets[j%len(offsets)] // Read a recent offset
					offsetMu.Unlock()

					data, err := walInstance.Read(pos)
					assert.NoError(t, err, "read should not fail")
					assert.NotEmpty(t, data, "read should return valid data")
				}
			}()
		}

		wg.Wait()
	})

	t.Run("get_transaction_records", func(t *testing.T) {
		var keys []string
		var values []string
		var firstOffset *wal.Offset
		for i := 0; i < 10; i++ {
			key := gofakeit.Name()
			data := gofakeit.LetterN(10)
			keys = append(keys, key)
			values = append(values, data)
			var prevOffset *wal.Offset
			var err error

			encodedKV := logcodec.SerializeKVEntry([]byte(key), []byte(data))

			record := &logcodec.LogRecord{
				LSN:             123456789,
				HLC:             987654321,
				OperationType:   logrecord.LogOperationTypeInsert,
				TxnState:        logrecord.TransactionStateNone,
				EntryType:       logrecord.LogEntryTypeKV,
				TxnID:           []byte("transaction-001"),
				PrevTxnWalIndex: nil,
				Entries:         [][]byte{encodedKV},
			}

			fbRecord := record.FBEncode(len(encodedKV))
			prevOffset, err = walInstance.Append(fbRecord)
			assert.NoError(t, err)
			assert.NotNil(t, prevOffset)
			if firstOffset != nil {
				firstOffset = prevOffset
			}
		}

		records, err := walInstance.GetTransactionRecords(firstOffset)
		assert.NoError(t, err)
		for i, record := range records {
			assert.Equal(t, record.OperationType(), logrecord.LogOperationTypeInsert,
				"expected operation didn't matched")
			assert.Equal(t, uint64(i), record.Lsn(), "expected index didn't match")
			assert.Equal(t, uint64(i), record.Hlc(), "expected hlc didn't match")
			assert.Equal(t, record.EntryType(), logrecord.LogEntryTypeKV,
				"expected value type didn't match")

			deserialized := logcodec.DeserializeFBRootLogRecord(record)
			kvEntry := logcodec.DeserializeKVEntry(deserialized.Entries[0])
			assert.Equal(t, string(kvEntry.Key), keys[i], "expected key didn't match")
			assert.Equal(t, string(kvEntry.Value), values[i], "expected value didn't match")
		}
	})

	t.Run("get_transaction_records_nil_start_offset", func(t *testing.T) {
		records, err := walInstance.GetTransactionRecords(nil)
		assert.NoError(t, err)
		assert.Nil(t, records, "should return nil when start offset is nil")
	})

	t.Run("get_transaction_records_single_entry", func(t *testing.T) {
		key := gofakeit.Name()
		data := gofakeit.LetterN(10)

		encodedKV := logcodec.SerializeKVEntry([]byte(key), []byte(data))

		record := &logcodec.LogRecord{
			LSN:             123456789,
			HLC:             987654321,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateNone,
			EntryType:       logrecord.LogEntryTypeKV,
			TxnID:           []byte("transaction-001"),
			PrevTxnWalIndex: nil,
			Entries:         [][]byte{encodedKV},
		}

		fbRecord := record.FBEncode(len(encodedKV))

		offset, err := walInstance.Append(fbRecord)
		assert.NoError(t, err)
		assert.NotNil(t, offset)

		records, err := walInstance.GetTransactionRecords(offset)
		assert.NoError(t, err)
		assert.Len(t, records, 1, "should return only 1 transaction record")
		deserialized := logcodec.DeserializeFBRootLogRecord(records[0])
		kvEntry := logcodec.DeserializeKVEntry(deserialized.Entries[0])
		assert.Equal(t, key, string(kvEntry.Key), "key should match")
		assert.Equal(t, data, string(kvEntry.Value), "value should match")
	})

	t.Run("get_transaction_records_incorrect_offset", func(t *testing.T) {
		key := gofakeit.Name()
		data := gofakeit.LetterN(10)

		encodedKV := logcodec.SerializeKVEntry([]byte(key), []byte(data))

		prevOffset := &wal.Offset{}

		record := &logcodec.LogRecord{
			LSN:             123456789,
			HLC:             987654321,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateNone,
			EntryType:       logrecord.LogEntryTypeKV,
			TxnID:           []byte("transaction-001"),
			PrevTxnWalIndex: prevOffset.Encode(),
			Entries:         [][]byte{encodedKV},
		}

		fbRecord := record.FBEncode(len(encodedKV))

		offset, err := walInstance.Append(fbRecord)
		assert.NoError(t, err)
		assert.NotNil(t, offset)

		records, err := walInstance.GetTransactionRecords(offset)
		assert.ErrorIs(t, err, wal.ErrWalNextOffset)
		assert.Len(t, records, 0, "should return 0 transaction record")
	})
}

func TestReader_CloseMidway(t *testing.T) {
	dir := t.TempDir()
	walInstance, err := wal.NewWalIO(dir, "test_namespace", wal.NewDefaultConfig())
	assert.NoError(t, err)
	defer walInstance.Close()

	for i := 0; i < 5; i++ {
		_, err := walInstance.Append([]byte(gofakeit.LetterN(10)))
		assert.NoError(t, err)
	}

	reader, err := walInstance.NewReader()
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	_, pos, err := reader.Next()
	assert.NoError(t, err)
	assert.NotNil(t, pos)

	reader.Close()
	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF, "expected io.EOF after reader was closed")
}

func TestReader_AutoCloseOnEOF(t *testing.T) {
	dir := t.TempDir()
	walInstance, err := wal.NewWalIO(dir, "test_namespace", wal.NewDefaultConfig())
	assert.NoError(t, err)
	defer walInstance.Close()

	_, err = walInstance.Append([]byte("single-entry"))
	assert.NoError(t, err)

	reader, err := walInstance.NewReader()
	assert.NoError(t, err)

	data, pos, err := reader.Next()
	assert.NoError(t, err)
	assert.NotNil(t, pos)
	assert.Equal(t, "single-entry", string(data))

	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)

	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestWalIO_BatchAppend(t *testing.T) {
	t.Run("batch_append_basic", func(t *testing.T) {
		walInstance := setupWalTest(t)

		records := [][]byte{
			[]byte(gofakeit.LetterN(10)),
			[]byte(gofakeit.LetterN(20)),
			[]byte(gofakeit.LetterN(30)),
		}

		offsets, err := walInstance.BatchAppend(records)
		assert.NoError(t, err)
		assert.Len(t, offsets, 3, "should return 3 offsets")

		for i, offset := range offsets {
			data, err := walInstance.Read(offset)
			assert.NoError(t, err)
			assert.Equal(t, records[i], data, "record %d should match", i)
		}
	})

	t.Run("batch_append_empty", func(t *testing.T) {
		walInstance := setupWalTest(t)

		offsets, err := walInstance.BatchAppend([][]byte{})
		assert.NoError(t, err)
		assert.Nil(t, offsets, "should return nil for empty batch")
	})

	t.Run("batch_append_single_record", func(t *testing.T) {
		walInstance := setupWalTest(t)

		records := [][]byte{[]byte(gofakeit.LetterN(15))}
		offsets, err := walInstance.BatchAppend(records)
		assert.NoError(t, err)
		assert.Len(t, offsets, 1)

		data, err := walInstance.Read(offsets[0])
		assert.NoError(t, err)
		assert.Equal(t, records[0], data)
	})

	t.Run("batch_append_large_batch", func(t *testing.T) {
		walInstance := setupWalTest(t)

		batchSize := 100
		records := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			records[i] = []byte(gofakeit.LetterN(50))
		}

		offsets, err := walInstance.BatchAppend(records)
		assert.NoError(t, err)
		assert.Len(t, offsets, batchSize)

		testIndices := []int{0, 25, 50, 75, 99}
		for _, i := range testIndices {
			data, err := walInstance.Read(offsets[i])
			assert.NoError(t, err)
			assert.Equal(t, records[i], data, "record %d should match", i)
		}
	})

	t.Run("batch_append_sequential_consistency", func(t *testing.T) {
		walInstance := setupWalTest(t)

		batch1 := [][]byte{
			[]byte("record1"),
			[]byte("record2"),
		}
		batch2 := [][]byte{
			[]byte("record3"),
			[]byte("record4"),
		}

		offsets1, err := walInstance.BatchAppend(batch1)
		assert.NoError(t, err)

		offsets2, err := walInstance.BatchAppend(batch2)
		assert.NoError(t, err)
		
		allOffsets := append(offsets1, offsets2...)
		for i := 1; i < len(allOffsets); i++ {
			prev := allOffsets[i-1]
			curr := allOffsets[i]
			isIncreasing := (curr.SegmentID > prev.SegmentID) ||
				(curr.SegmentID == prev.SegmentID && curr.Offset > prev.Offset)
			assert.True(t, isIncreasing, "offsets should be monotonically increasing")
		}

		allRecords := append(batch1, batch2...)
		for i, offset := range allOffsets {
			data, err := walInstance.Read(offset)
			assert.NoError(t, err)
			assert.Equal(t, allRecords[i], data, "record %d should match", i)
		}
	})

	t.Run("batch_append_concurrent", func(t *testing.T) {
		walInstance := setupWalTest(t)

		var wg sync.WaitGroup
		numWorkers := 2
		batchesPerWorker := 3
		recordsPerBatch := 5

		var totalWritten atomic.Int32

		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for batch := 0; batch < batchesPerWorker; batch++ {
					records := make([][]byte, recordsPerBatch)
					for i := 0; i < recordsPerBatch; i++ {
						records[i] = []byte(gofakeit.LetterN(20))
					}

					offsets, err := walInstance.BatchAppend(records)
					assert.NoError(t, err)
					assert.Len(t, offsets, recordsPerBatch)
					totalWritten.Add(int32(len(offsets)))
				}
			}()
		}

		wg.Wait()

		expectedTotal := int32(numWorkers * batchesPerWorker * recordsPerBatch)
		assert.Equal(t, expectedTotal, totalWritten.Load(), "should have written all records")
	})
}
