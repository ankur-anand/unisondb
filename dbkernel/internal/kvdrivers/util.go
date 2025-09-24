package kvdrivers

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// values are non-printable bytes to avoid any user ASCII collision.
const (
	KeyTypeKV         byte = 254
	KeyTypeWideColumn byte = 253
	KeyTypeBlobChunk  byte = 255
	KeyTypeSystem     byte = 252
)

// KeyKind is type markers for multimodal storage.
type KeyKind byte

const (
	KeyKindUnknown    KeyKind = 0
	KeyKindKV         KeyKind = KeyKind(KeyTypeKV)
	KeyKindWideColumn KeyKind = KeyKind(KeyTypeWideColumn)
	KeyKindBlobChunk  KeyKind = KeyKind(KeyTypeBlobChunk)
	KeyKindSystem     KeyKind = KeyKind(KeyTypeSystem)
)

// ParseKeyKind returns the KeyKind associated with the key.
func ParseKeyKind(key []byte) KeyKind {
	if len(key) == 0 {
		return KeyKindUnknown
	}
	switch key[0] {
	case KeyTypeKV:
		return KeyKindKV
	case KeyTypeWideColumn:
		return KeyKindWideColumn
	case KeyTypeBlobChunk:
		return KeyKindBlobChunk
	case KeyTypeSystem:
		return KeyKindSystem
	default:
		return KeyKindUnknown
	}
}

func (k KeyKind) String() string {
	switch k {
	case KeyKindKV:
		return "KV"
	case KeyKindWideColumn:
		return "WideColumn"
	case KeyKindBlobChunk:
		return "BlobChunk"
	case KeyKindSystem:
		return "System"
	default:
		return fmt.Sprintf("Unknown(%d)", k)
	}
}

// KeyKV returns the storage key for a key-value pair.
func KeyKV(k []byte) []byte {
	return append([]byte{KeyTypeKV}, k...)
}

func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// [type][rowLenBE(4)][row][col]
// Why this format?
// - The rowLen tells us where the row ends and the column begins.
// - Big Endian is used so that keys sort in the right order in the B-tree.
// - All columns of the same row share the same prefix, making scans efficient.
//
// Example:
//   Row = "user123", Col = "email"
//   Key = [0xFD][00 00 00 07]["user123"]["email"]
//
// We avoid text separators like "row::col". B-trees compare keys bytewise,
// so string separators break ordering (e.g., "user10..." < "user2..."),
// are ambiguous if row/col contain "::", and are not binary-safe for []byte keys.
// Length-prefixing with Big Endian keeps keys self-delimiting, binary-safe,
// and groups all columns of a row under the shared prefix [type][rowLen][row].

// RowKey constructs a storage key for a wide-column row.
func RowKey(row []byte) []byte {
	b := make([]byte, 1+4+len(row))
	b[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
	copy(b[5:], row)
	return b
}

// KeyColumn returns the storage key for a wide-column cell.
func KeyColumn(row, col []byte) []byte {
	b := make([]byte, 1+4+len(row)+len(col))
	b[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
	copy(b[5:], row)
	copy(b[5+len(row):], col)
	return b
}

// [type][blobIDLenBE(4)][blobID][chunkBE(4)].
func KeyBlobChunk(blobID []byte, chunk int) []byte {
	b := make([]byte, 1+4+len(blobID)+4)
	b[0] = KeyTypeBlobChunk
	binary.BigEndian.PutUint32(b[1:], uint32(len(blobID)))
	copy(b[5:], blobID)
	binary.BigEndian.PutUint32(b[5+len(blobID):], uint32(chunk))
	return b
}

// KeySystem returns the storage key for a system/internal entry.
func KeySystem(name []byte) []byte {
	return append([]byte{KeyTypeSystem}, name...)
}
