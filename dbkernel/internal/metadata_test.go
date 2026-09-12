package internal

import (
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataRoundTrip(t *testing.T) {
	t.Run("with_position", func(t *testing.T) {
		meta := Metadata{
			RecordProcessed: 4242,
			Pos:             &wal.Offset{SegmentID: 7, Offset: 8192},
		}

		encoded := meta.MarshalBinary()
		require.Len(t, encoded, encodedMetadataSize)

		decoded := UnmarshalMetadata(encoded)
		assert.Equal(t, meta.RecordProcessed, decoded.RecordProcessed)
		require.NotNil(t, decoded.Pos)
		assert.Equal(t, meta.Pos.SegmentID, decoded.Pos.SegmentID)
		assert.Equal(t, meta.Pos.Offset, decoded.Pos.Offset)
	})

	// A nil Pos is written as a zeroed position and decodes back to the zero
	// value rather than to nil.
	t.Run("nil_position", func(t *testing.T) {
		meta := Metadata{RecordProcessed: 9}

		encoded := meta.MarshalBinary()
		require.Len(t, encoded, encodedMetadataSize)

		decoded := UnmarshalMetadata(encoded)
		assert.Equal(t, uint64(9), decoded.RecordProcessed)
		require.NotNil(t, decoded.Pos)
		assert.Equal(t, wal.Offset{}, *decoded.Pos)
	})

	t.Run("zero_value", func(t *testing.T) {
		empty := Metadata{}
		decoded := UnmarshalMetadata(empty.MarshalBinary())
		assert.Zero(t, decoded.RecordProcessed)
		require.NotNil(t, decoded.Pos)
		assert.Equal(t, wal.Offset{}, *decoded.Pos)
	})
}

func TestUnmarshalMetadataRejectsShortBuffer(t *testing.T) {
	for _, size := range []int{0, 1, encodedMetadataSize - 1} {
		decoded := UnmarshalMetadata(make([]byte, size))
		assert.Zero(t, decoded.RecordProcessed)
		assert.Nil(t, decoded.Pos)
	}
}

func TestUnmarshalMetadataIgnoresTrailingBytes(t *testing.T) {
	meta := Metadata{RecordProcessed: 5, Pos: &wal.Offset{SegmentID: 1, Offset: 64}}

	decoded := UnmarshalMetadata(append(meta.MarshalBinary(), 0xAA, 0xBB))
	assert.Equal(t, meta.RecordProcessed, decoded.RecordProcessed)
	require.NotNil(t, decoded.Pos)
	assert.Equal(t, meta.Pos.Offset, decoded.Pos.Offset)
}
