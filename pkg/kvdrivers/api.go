package kvdrivers

import (
	"io"
)

// These interfaces are unexported and exist only to validate that all implementing types
// have the required methods. They are not intended for external use.
type unifiedStorage interface {
	FSync() error
	Close() error
	SetKV(key []byte, value []byte) error
	BatchSetKV(keys [][]byte, value [][]byte) error
	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) (int, error)
	DeleteKV(key []byte) error
	BatchDeleteKV(keys [][]byte) error
	BatchDeleteLobChunks(keys [][]byte) error
	GetKV(key []byte) ([]byte, error)
	GetLOBChunks(key []byte) ([][]byte, error)
	ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error)
	GetCell(rowKey []byte, columnName string) ([]byte, error)
	GetCells(rowKey []byte, columns []string) (map[string][]byte, error)
	StoreMetadata(key []byte, value []byte) error
	RetrieveMetadata(key []byte) ([]byte, error)
	Restore(reader io.Reader) error
	Snapshot(w io.Writer) error
}
type txnWriter interface {
	BatchPutKV(keys, values [][]byte) error
	BatchDeleteKV(keys [][]byte) error
	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchDeleteLobChunks(keys [][]byte) error
	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) error
	Stats() TxnStats
	Commit() error
}
