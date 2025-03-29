package memtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMonotonicClock(t *testing.T) {
	// this denotes we have not
	pTSMap := make(map[int64]struct{})
	for i := 0; i < 100; i++ {
		pTSMap[time.Now().UnixNano()] = struct{}{}
	}

	t.Logf("loop faster than unix nano? if len not 100 len: %d", len(pTSMap))
	ts := &tsGenerator{}
	prev := uint64(0)
	for i := 0; i < 100; i++ {
		now := ts.Next()
		assert.Greater(t, now, prev)
		now = prev
	}
}
