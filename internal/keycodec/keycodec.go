package keycodec

import (
	"encoding/binary"
	"fmt"
)

// Key type markers are non-printable bytes to avoid any user ASCII collision.
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

// Format: [0xFE][key].
func KeyKV(k []byte) []byte {
	return append([]byte{KeyTypeKV}, k...)
}

// RowKey constructs a storage key for a wide-column row prefix.
// Format: [0xFD][rowLenBE(4)][row]
//
// Why this format?
// - The rowLen tells us where the row ends and the column begins.
// - Big Endian is used so that keys sort in the right order in the B-tree.
// - All columns of the same row share the same prefix, making scans efficient.
//
// Example:
//
//	Row = "user123"
//	Key = [0xFD][00 00 00 07]["user123"]
//
// We avoid text separators like "row::col". B-trees compare keys bytewise,
// so string separators break ordering (e.g., "user10..." < "user2..."),
// are ambiguous if row/col contain "::", and are not binary-safe for []byte keys.
// Length-prefixing with Big Endian keeps keys self-delimiting, binary-safe,
// and groups all columns of a row under the shared prefix [type][rowLen][row].
func RowKey(row []byte) []byte {
	b := make([]byte, 1+4+len(row))
	b[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
	copy(b[5:], row)
	return b
}

// Format: [0xFD][rowLenBE(4)][row][col].
func KeyColumn(row, col []byte) []byte {
	b := make([]byte, 1+4+len(row)+len(col))
	b[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
	copy(b[5:], row)
	copy(b[5+len(row):], col)
	return b
}

// Format: [0xFF][blobIDLenBE(4)][blobID].
func KeyBlobChunk(blobID []byte) []byte {
	b := make([]byte, 1+4+len(blobID))
	b[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(b[1:], uint32(len(blobID)))
	copy(b[5:], blobID)
	return b
}

// Format: [0xFC][name].
func KeySystem(name []byte) []byte {
	return append([]byte{KeyTypeSystem}, name...)
}
