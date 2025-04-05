package dbkernel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	initMonotonic()
	m.Run()
}

func TestHLCMonotonicity(t *testing.T) {
	seen := make(map[uint64]struct{})

	var last uint64
	for i := 0; i < 1_00_000; i++ {
		hlc := HLCNow()
		if hlc <= last {
			t.Fatalf("HLC not strictly increasing: got %d after %d", hlc, last)
		}
		if _, exists := seen[hlc]; exists {
			t.Fatalf("Duplicate HLC generated: %d", hlc)
		}
		seen[hlc] = struct{}{}
		last = hlc
	}
}

func TestHLCDecode(t *testing.T) {
	hlc := HLCNow()
	ts, counter := HLCDecode(hlc)

	if ts == 0 {
		t.Error("Decoded timestamp should not be zero")
	}
	if counter > logicalMask {
		t.Errorf("Counter overflow: %d > %d", counter, logicalMask)
	}
}

func TestIsConcurrent(t *testing.T) {

	ts := uint64(1_000_000)
	hlc1 := (ts << logicalBits) | 1
	hlc2 := (ts << logicalBits) | 2
	assert.True(t, IsConcurrent(hlc1, hlc2), "should be true for concurrent HLCs")

	hlc3 := ((ts + 1) << logicalBits) | 1

	assert.False(t, IsConcurrent(hlc3, hlc2), "should be false for non concurrent HLCs")

	hlc4 := (ts << logicalBits) | 1
	assert.False(t, IsConcurrent(hlc4, hlc1), "should be false for concurrent HLCs")
}

func BenchmarkHLCNow(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = HLCNow()
		}
	})
}

func BenchmarkHLCDecode(b *testing.B) {
	now := HLCNow()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			HLCDecode(now)
		}
	})
}
