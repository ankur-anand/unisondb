package raftwalfs

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryCodecV1_ID(t *testing.T) {
	codec := BinaryCodecV1{}
	assert.Equal(t, CodecBinaryV1ID, codec.ID())
	assert.Equal(t, uint64(1), codec.ID())
}

func TestBinaryCodecV1_EncodeWithDataMutator(t *testing.T) {
	var mutatedLSN uint64
	var mutatedData []byte
	mockMutator := &mockDataMutator{
		mutateFn: func(data []byte, lsn uint64) error {
			mutatedData = data
			mutatedLSN = lsn
			return nil
		},
	}

	codec := BinaryCodecV1{DataMutator: mockMutator}

	log := &raft.Log{
		Index: 12345,
		Term:  5,
		Type:  raft.LogCommand,
		Data:  []byte("test data"),
	}

	_, err := codec.Encode(log)
	require.NoError(t, err)

	assert.Equal(t, uint64(12345), mutatedLSN)
	assert.Equal(t, []byte("test data"), mutatedData)
}

func TestBinaryCodecV1_EncodeWithoutDataMutator(t *testing.T) {
	codec := BinaryCodecV1{}

	log := &raft.Log{
		Index: 12345,
		Term:  5,
		Type:  raft.LogCommand,
		Data:  []byte("test data"),
	}

	encoded, err := codec.Encode(log)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)
	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, log.Index, decoded.Index)
	assert.Equal(t, log.Data, decoded.Data)
}

func TestBinaryCodecV1_EncodeEmptyDataSkipsMutation(t *testing.T) {
	callCount := 0
	mockMutator := &mockDataMutator{
		mutateFn: func(data []byte, lsn uint64) error {
			callCount++
			return nil
		},
	}

	codec := BinaryCodecV1{DataMutator: mockMutator}

	log := &raft.Log{
		Index: 12345,
		Term:  5,
		Type:  raft.LogNoop,
		Data:  nil,
	}

	_, err := codec.Encode(log)
	require.NoError(t, err)
	assert.Equal(t, 0, callCount)
}

type mockDataMutator struct {
	mutateFn func(data []byte, lsn uint64) error
}

func (m *mockDataMutator) MutateLSN(data []byte, lsn uint64) error {
	if m.mutateFn != nil {
		return m.mutateFn(data, lsn)
	}
	return nil
}

func (m *mockDataMutator) GetLSN(data []byte) uint64 {
	return 0
}

func TestBinaryCodecV1_EncodeDecode(t *testing.T) {
	codec := BinaryCodecV1{}

	tests := []struct {
		name string
		log  *raft.Log
	}{
		{
			name: "minimal log",
			log: &raft.Log{
				Index: 1,
				Term:  1,
				Type:  raft.LogCommand,
			},
		},
		{
			name: "with data",
			log: &raft.Log{
				Index: 100,
				Term:  5,
				Type:  raft.LogCommand,
				Data:  []byte("hello world"),
			},
		},
		{
			name: "with extensions",
			log: &raft.Log{
				Index:      200,
				Term:       10,
				Type:       raft.LogNoop,
				Extensions: []byte("extension data"),
			},
		},
		{
			name: "with data and extensions",
			log: &raft.Log{
				Index:      300,
				Term:       15,
				Type:       raft.LogConfiguration,
				Data:       []byte("config data"),
				Extensions: []byte("extension data"),
			},
		},
		{
			name: "with timestamp",
			log: &raft.Log{
				Index:      400,
				Term:       20,
				Type:       raft.LogCommand,
				Data:       []byte("data"),
				AppendedAt: time.Now().Truncate(time.Nanosecond),
			},
		},
		{
			name: "large data",
			log: &raft.Log{
				Index: 500,
				Term:  25,
				Type:  raft.LogCommand,
				Data:  make([]byte, 64*1024),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := codec.Encode(tt.log)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := codec.Decode(encoded)
			require.NoError(t, err)

			assert.Equal(t, tt.log.Index, decoded.Index)
			assert.Equal(t, tt.log.Term, decoded.Term)
			assert.Equal(t, tt.log.Type, decoded.Type)
			assert.Equal(t, tt.log.Data, decoded.Data)
			assert.Equal(t, tt.log.Extensions, decoded.Extensions)

			if !tt.log.AppendedAt.IsZero() {
				assert.True(t, tt.log.AppendedAt.Equal(decoded.AppendedAt))
			}
		})
	}
}

func TestBinaryCodecV1_DecodeErrors(t *testing.T) {
	codec := BinaryCodecV1{}

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "too short",
			data: make([]byte, 10),
		},
		{
			name: "truncated after header",
			data: make([]byte, 25),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Decode(tt.data)
			assert.Error(t, err)
		})
	}
}

func TestCodecInterface(t *testing.T) {
	var _ Codec = (*BinaryCodecV1)(nil)
}

func BenchmarkBinaryCodecV1_Encode(b *testing.B) {
	codec := BinaryCodecV1{}
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			log := &raft.Log{
				Index: 12345,
				Term:  10,
				Type:  raft.LogCommand,
				Data:  make([]byte, size),
			}

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := codec.Encode(log)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBinaryCodecV1_Decode(b *testing.B) {
	codec := BinaryCodecV1{}
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			log := &raft.Log{
				Index: 12345,
				Term:  10,
				Type:  raft.LogCommand,
				Data:  make([]byte, size),
			}
			encoded, _ := codec.Encode(log)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := codec.Decode(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func formatSize(size int) string {
	if size >= 1024 {
		return string(rune('0'+size/1024)) + "KB"
	}
	return string(rune('0'+size/64)) + "x64B"
}
