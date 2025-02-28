package dbengine

import (
	"errors"
	"fmt"
	"io"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
)

type walRecovery struct {
	store            BTreeStore
	walIO            *wal.WalIO
	recoveredCount   int
	lastRecoveredPos *wal.Offset
}

// recoverWAL recover wal from last check point saved in btree store.
func (wr *walRecovery) recoverWAL() error {
	value, err := wr.store.RetrieveMetadata(sysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdb.ErrKeyNotFound) {
		return fmt.Errorf("recover WAL failed %w", err)
	}

	var offset wal.Offset
	if len(value) != 0 {
		metadata := UnmarshalMetadata(value)
		offset = *metadata.Pos
	}

	reader, err := wr.walIO.NewReaderWithStart(&offset)
	if err != nil {
		return fmt.Errorf("recover WAL failed %w", err)
	}

	if len(value) != 0 {
		// first value will be duplicate, so we can ignore it.
		_, _, err := reader.Next()
		if wr.isFatalError(err) {
			return fmt.Errorf("recover WAL failed %w", err)
		}
	}

	for {
		value, pos, err := reader.Next()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("recover WAL failed %w", err)
		}

		record := walrecord.GetRootAsWalRecord(value, 0)
		err = wr.handleRecord(record)
		wr.lastRecoveredPos = pos
		if err != nil {
			return fmt.Errorf("recover WAL failed %w", err)
		}
	}
	return nil
}

func (wr *walRecovery) handleRecord(record *walrecord.WalRecord) error {
	// we only recover two cases.
	// Individual Insert/Delete
	// Txn Insert/Delete and Chunk. Uncommited Txn are ignored.
	switch record.TxnStatus() {
	case walrecord.TxnStatusTxnNone:
		wr.recoveredCount++
		switch record.Operation() {
		case walrecord.LogOperationInsert:
			return wr.store.Set(record.KeyBytes(), record.ValueBytes())
		case walrecord.LogOperationDelete:
			return wr.store.Delete(record.KeyBytes())
		}
	case walrecord.TxnStatusCommit:
		return wr.handleTxnCommited(record)
	}

	return nil
}

func (wr *walRecovery) isFatalError(err error) bool {
	return err != nil && !errors.Is(err, io.EOF)
}

// handleTxnCommited handles the current commited txn, for chunked, insert and delete ops.
func (wr *walRecovery) handleTxnCommited(record *walrecord.WalRecord) error {
	wr.recoveredCount++
	switch record.ValueType() {
	case walrecord.ValueTypeFull:
		return wr.handleFullValuesTxn(record)
	case walrecord.ValueTypeChunked:
		return wr.handleChunkedValuesTxn(record)
	}

	return nil
}

// handleFullValuesTxn Handles the insert and delete operation of Txn and updates
// the same to the underlying btree bases store.
func (wr *walRecovery) handleFullValuesTxn(record *walrecord.WalRecord) error {
	records, err := wr.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return err
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, len(preparedRecords))
	keys := make([][]byte, len(preparedRecords))
	for i, pRecord := range preparedRecords {
		keys[i] = pRecord.KeyBytes()
		values[i] = pRecord.ValueBytes()
	}

	wr.recoveredCount += len(records)
	switch record.Operation() {
	case walrecord.LogOperationInsert:
		return wr.store.SetMany(keys, values)
	case walrecord.LogOperationDelete:
		return wr.store.DeleteMany(keys)
	}
	return nil
}

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based store.
func (wr *walRecovery) handleChunkedValuesTxn(record *walrecord.WalRecord) error {
	count, err := handleChunkedValuesTxn(record, wr.walIO, wr.store)
	wr.recoveredCount += count
	return err
}
