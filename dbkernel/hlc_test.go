package dbkernel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHLCNowMonotonicUpdate(t *testing.T) {

	first := HLCNow()
	time.Sleep(2 * time.Millisecond)
	second := HLCNow()
	assert.Greater(t, second, first, "Expected HLCNow to increase, got first=%d second=%d", first, second)
}

func TestMeasureClockDrift(t *testing.T) {
	drift := measureClockDrift()
	if abs := absDuration(drift); abs > 10*time.Millisecond {
		t.Errorf("Expected clock drift to be near zero, got %s", drift)
	}
}

func TestStartClockDriftMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartClockDriftMonitor(ctx, 10*time.Millisecond)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
