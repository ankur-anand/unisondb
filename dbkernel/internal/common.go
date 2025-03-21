package internal

import (
	"io"

	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

type TxnBatcher interface {
	BatchPut(keys, values [][]byte) error
	BatchDelete(keys [][]byte) error
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) error
	Commit() error
	Stats() kvdrivers.TxnStats
}

// BtreeWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type BtreeWriter interface {
	// Set associates a value with a key.
	Set(key []byte, value []byte) error
	// SetMany associates multiple values with corresponding keys.
	SetMany(keys [][]byte, values [][]byte) error
	// SetChunks stores a value that has been split into chunks, associating them with a single key.
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	// Delete deletes a value with a key.
	Delete(key []byte) error
	// DeleteMany delete multiple values with corresponding keys.
	DeleteMany(keys [][]byte) error

	SetManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	DeleteManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	DeleteEntireRows(rowKeys [][]byte) (int, error)

	StoreMetadata(key []byte, value []byte) error
	FSync() error
}

// BtreeReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type BtreeReader interface {
	// Get retrieves a value associated with a key.
	Get(key []byte) ([]byte, error)
	GetRowColumns(rowKey []byte, filter func([]byte) bool) (map[string][]byte, error)
	// Snapshot writes the complete database to the provided io writer.
	Snapshot(w io.Writer) error
	RetrieveMetadata(key []byte) ([]byte, error)
}

// BTreeStore combines the BtreeWriter and BtreeReader interfaces.
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
