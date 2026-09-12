package recovery

import (
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/stretchr/testify/require"
)

func TestWalRecoveryBatchedRowDeletesPastEnd(t *testing.T) {
	db, err := kvdrivers.NewLmdb(filepath.Join(t.TempDir(), "db"), kvdrivers.Config{
		Namespace: "row-delete-recovery", NoSync: true, MmapSize: 64 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	walIO, err := wal.NewWalIO(t.TempDir(), "row-delete-recovery", wal.NewDefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, walIO.Close()) })

	rows := [][]byte{[]byte("a"), []byte("b")}
	columns := map[string][]byte{"c1": []byte("one"), "c2": []byte("two")}
	require.NoError(t, db.BatchSetCells(rows, []map[string][]byte{columns, columns}))
	// Replay a missing row, the final stored row, then a lower row in the same record.
	deleteRecord := logcodec.LogRecord{
		LSN:           1,
		OperationType: logrecord.LogOperationTypeDeleteRowByKey,
		EntryType:     logrecord.LogEntryTypeRow,
		TxnState:      logrecord.TransactionStateNone,
		Entries: [][]byte{
			logcodec.SerializeRowUpdateEntry([]byte("z"), nil),
			logcodec.SerializeRowUpdateEntry([]byte("b"), nil),
			logcodec.SerializeRowUpdateEntry([]byte("a"), nil),
		},
	}
	_, err = walIO.Append(deleteRecord.FBEncode(1024), deleteRecord.LSN)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		recovery := NewWalRecovery(db, walIO)
		require.NoError(t, recovery.Recover(nil))
		require.Equal(t, 1, recovery.RecoveredCount())
		for _, row := range rows {
			_, err := db.ScanRowCells(row, nil)
			require.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		}
	}
}
