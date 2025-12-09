package raftwalfs

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVarintSize(t *testing.T) {
	testCases := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{1, 1},
		{127, 1},

		{128, 2},
		{255, 2},
		{16383, 2},

		{16384, 3},
		{2097151, 3},

		{2097152, 4},
		{1<<28 - 1, 4},

		{1 << 28, 5},
		{1<<35 - 1, 5},

		{1 << 63, 10},
		{^uint64(0), 10},
	}

	buf := make([]byte, binary.MaxVarintLen64)

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := varintSize(tc.value)
			stdSize := binary.PutUvarint(buf, tc.value)

			assert.Equal(t, tc.expected, got, "varintSize(%d)", tc.value)
			assert.Equal(t, stdSize, got, "varintSize(%d) should match binary.PutUvarint", tc.value)
		})
	}
}

func TestVarintSizeMatchesStdlib(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen64)

	for i := 0; i < 64; i++ {
		v := uint64(1) << i
		got := varintSize(v)
		expected := binary.PutUvarint(buf, v)
		assert.Equal(t, expected, got, "varintSize(1<<%d) = varintSize(%d)", i, v)
	}

	for i := 1; i <= 10; i++ {
		if i*7 < 64 {
			maxVal := uint64(1)<<(i*7) - 1
			got := varintSize(maxVal)
			expected := binary.PutUvarint(buf, maxVal)
			assert.Equal(t, expected, got, "varintSize(%d) boundary", maxVal)
			assert.Equal(t, i, got, "max %d-byte value", i)
		}
	}
}
