package internal

import (
	"io"

	"github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

// TxnBatcher batches ops and applies them atomically on Commit.
type TxnBatcher interface {
	BatchPutKV(keys, values [][]byte) error
	BatchDeleteKV(keys [][]byte) error

	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchDeleteLobChunks(keys [][]byte) error

	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) error

	Commit() error
	Stats() kvdrivers.TxnStats
}

type BtreeWriter interface {
	SetKV(key []byte, value []byte) error
	BatchSetKV(keys [][]byte, values [][]byte) error
	DeleteKV(key []byte) error
	BatchDeleteKV(keys [][]byte) error

	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchDeleteLobChunks(keys [][]byte) error

	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) (int, error)

	StoreMetadata(key []byte, value []byte) error
	FSync() error
	Restore(r io.Reader) error
}

type BtreeReader interface {
	GetKV(key []byte) ([]byte, error)

	GetLOBChunks(key []byte) ([][]byte, error)

	ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error)

	GetCell(rowKey []byte, columnName string) ([]byte, error)
	GetCells(rowKey []byte, columns []string) (map[string][]byte, error)

	RetrieveMetadata(key []byte) ([]byte, error)
	Snapshot(w io.Writer) error
}

// BTreeStore combines writer + reader + lifecycle.
type BTreeStore interface {
	BtreeWriter
	BtreeReader
	Close() error
}

var (
	LogOperationDelete         = byte(logrecord.LogOperationTypeDelete)
	LogOperationInsert         = byte(logrecord.LogOperationTypeInsert)
	LogOperationDeleteRowByKey = byte(logrecord.LogOperationTypeDeleteRowByKey)
	EntryTypeRow               = byte(logrecord.LogEntryTypeRow)
	EntryTypeKV                = byte(logrecord.LogEntryTypeKV)
	EntryTypeChunked           = byte(logrecord.LogEntryTypeChunked)
)
