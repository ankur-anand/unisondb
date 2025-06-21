package kvdrivers

import "fmt"

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

// Separator for logical fields in storage keys.
const rowKeySeparator byte = 0x00

// KeyKV returns the storage key for a key-value pair.
func KeyKV(k []byte) []byte {
	return append([]byte{KeyTypeKV}, k...)
}

// KeyColumn returns the storage key for a wide-column cell.
func KeyColumn(row, col []byte) []byte {
	b := make([]byte, 1+len(row)+1+len(col))
	b[0] = KeyTypeWideColumn
	copy(b[1:], row)
	b[1+len(row)] = rowKeySeparator
	copy(b[2+len(row):], col)
	return b
}

// KeyBlobChunk returns the storage key for a blob chunk.
// Format: [KeyBlobChunk][blobID][sep][chunkNumber].
func KeyBlobChunk(blobID []byte, chunk int) []byte {
	num := itoa(chunk)
	b := make([]byte, 1+len(blobID)+1+len(num))
	b[0] = KeyTypeBlobChunk
	copy(b[1:], blobID)
	b[1+len(blobID)] = rowKeySeparator
	copy(b[2+len(blobID):], num)
	return b
}

// KeySystem returns the storage key for a system/internal entry.
func KeySystem(name []byte) []byte {
	return append([]byte{KeyTypeSystem}, name...)
}

// itoa returns the ASCII bytes for a positive int.
// Faster than fmt.Sprintf in tight loops.
func itoa(i int) []byte {
	if i == 0 {
		return []byte("0")
	}

	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		// what is happening here
		// i%10 gives digit value (0-9)
		// '0' is ASCII 48, so '0' + whatever value is  = ASCII of that number.
		b[pos] = '0' + byte(i%10)
		i /= 10
	}
	return b[pos:]
}
