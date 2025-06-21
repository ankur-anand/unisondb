package kvdrivers

import (
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
	got := KeyColumn(toBytes("user:42"), toBytes("email"))
	want := append([]byte{KeyTypeWideColumn}, []byte("user:42")...)
	want = append(want, rowKeySeparator)
	want = append(want, []byte("email")...)
	assert.Equal(t, want, got)
}

func TestKeyBlobChunk(t *testing.T) {
	got := KeyBlobChunk(toBytes("blobid"), 42)
	num := []byte("42")
	want := append([]byte{KeyTypeBlobChunk}, []byte("blobid")...)
	want = append(want, rowKeySeparator)
	want = append(want, num...)
	assert.Equal(t, want, got)
}

func TestKeySystem(t *testing.T) {
	got := KeySystem(toBytes("internal:foo"))
	want := append([]byte{KeyTypeSystem}, []byte("internal:foo")...)
	assert.Equal(t, want, got)
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
