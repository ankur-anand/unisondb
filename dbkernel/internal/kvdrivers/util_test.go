package kvdrivers

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func toBytes(s string) []byte {
	return []byte(s)
}

func TestKeyKV(t *testing.T) {
	got := KeyKV(toBytes("hello"))
	want := append([]byte{KeyTypeKV}, []byte("hello")...)
	assert.Equal(t, want, got)
}

func TestKeyColumn(t *testing.T) {
	row := toBytes("user:42")
	col := toBytes("email")

	got := KeyColumn(row, col)

	// want = [type][rowLenBE(4)][row][col]
	want := make([]byte, 1+4+len(row)+len(col))
	want[0] = KeyTypeWideColumn
	binary.BigEndian.PutUint32(want[1:], uint32(len(row)))
	copy(want[5:], row)
	copy(want[5+len(row):], col)

	assert.Equal(t, want, got)
}

func TestKeyBlobChunk(t *testing.T) {
	blobID := toBytes("blobid")
	chunk := 42

	got := KeyBlobChunk(blobID, chunk)

	want := make([]byte, 1+4+len(blobID)+4)
	want[0] = KeyTypeBlobChunk
	binary.BigEndian.PutUint32(want[1:], uint32(len(blobID)))
	copy(want[5:], blobID)
	binary.BigEndian.PutUint32(want[5+len(blobID):], uint32(chunk))

	assert.Equal(t, want, got)
}

func TestKeySystem(t *testing.T) {
	got := KeySystem(toBytes("internal:foo"))
	want := append([]byte{KeyTypeSystem}, []byte("internal:foo")...)
	assert.Equal(t, want, got)
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

func TestItoa(t *testing.T) {
	tests := []struct {
		in   int
		want []byte
	}{
		{0, []byte("0")},
		{1, []byte("1")},
		{9, []byte("9")},
		{10, []byte("10")},
		{42, []byte("42")},
		{999, []byte("999")},
		{1000000, []byte("1000000")},
	}
	for _, tt := range tests {
		got := itoa(tt.in)
		assert.Equal(t, tt.want, got)
	}
}

func TestParseKeyKind(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want KeyKind
	}{
		{"KV", []byte{KeyTypeKV}, KeyKindKV},
		{"WideColumn", []byte{KeyTypeWideColumn}, KeyKindWideColumn},
		{"BlobChunk", []byte{KeyTypeBlobChunk}, KeyKindBlobChunk},
		{"System", []byte{KeyTypeSystem}, KeyKindSystem},
		{"Unknown", []byte{42}, KeyKindUnknown},
		{"Empty", []byte{}, KeyKindUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseKeyKind(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKeyKindString(t *testing.T) {
	tests := []struct {
		kind KeyKind
		want string
	}{
		{KeyKindKV, "KV"},
		{KeyKindWideColumn, "WideColumn"},
		{KeyKindBlobChunk, "BlobChunk"},
		{KeyKindSystem, "System"},
		{KeyKindUnknown, "Unknown(0)"},
		{KeyKind(99), fmt.Sprintf("Unknown(%d)", 99)},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.kind.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowKey(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  []byte
	}{
		{
			name:  "non-empty",
			input: []byte("hello"),
			want: func() []byte {
				row := []byte("hello")
				b := make([]byte, 1+4+len(row))
				b[0] = KeyTypeWideColumn
				binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
				copy(b[5:], row)
				return b
			}(),
		},
		{
			name:  "empty",
			input: []byte{},
			want: func() []byte {
				row := []byte{}
				b := make([]byte, 1+4+len(row))
				b[0] = KeyTypeWideColumn
				binary.BigEndian.PutUint32(b[1:], uint32(len(row)))
				return b
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RowKey(tt.input)
			assert.Equal(t, tt.want, got, fmt.Sprintf("RowKey(%q)", tt.input))
		})
	}
}

func bytesStrconv(i int) []byte {
	return []byte(strconv.Itoa(i))
}

var testValues = []int{
	0,
	1,
	42,
	123,
	1234,
	12345,
	123456,
	1234567,
	12345678,
	123456789,
	1234567890,
}

var sinkI []byte

func BenchmarkItoa(b *testing.B) {
	for _, val := range testValues {
		b.Run(fmt.Sprintf("val_%d", val), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkI = itoa(val)
			}
		})
	}
}

var sinkF []byte

func BenchmarkFmtSprintf(b *testing.B) {
	for _, val := range testValues {
		b.Run(fmt.Sprintf("val_%d", val), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkF = []byte(fmt.Sprintf("%d", val))
			}
		})
	}
}

var sinkC []byte

func BenchmarkStrconv(b *testing.B) {
	for _, val := range testValues {
		b.Run(fmt.Sprintf("val_%d", val), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkC = bytesStrconv(val)
			}
		})
	}
}

func BenchmarkComparison(b *testing.B) {
	val := 123456

	b.Run("itoa", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = itoa(val)
		}
	})

	b.Run("fmt.Sprintf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = []byte(fmt.Sprintf("%d", val))
		}
	})

	b.Run("strconv", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = bytesStrconv(val)
		}
	})
}

func BenchmarkItoaAllocs(b *testing.B) {
	val := 123456
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = itoa(val)
	}
}

func BenchmarkFmtSprintfAllocs(b *testing.B) {
	val := 123456
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = []byte(fmt.Sprintf("%d", val))
	}
}

func BenchmarkStrconvAllocs(b *testing.B) {
	val := 123456
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bytesStrconv(val)
	}
}

var sink1 []byte
var sink2 []byte
var sink3 []byte

func BenchmarkKeyKV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink1 = KeyKV(toBytes("this:is:a:long:config:key:with:many:fields"))
	}
}

func BenchmarkKeyColumn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink2 = KeyColumn(toBytes("user:1234567890"), toBytes("really_long_column_name_for_profile_data"))
	}
}

func BenchmarkKeyBlobChunk(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink3 = KeyBlobChunk(toBytes("blob:profilepic:hugeuser:99999"), 123456)
	}
}
