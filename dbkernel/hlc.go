package dbkernel

import (
	"context"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/pkg/umetrics"
)

const (
	mClockDriftSeconds   = "monotonic_clock_drift_seconds"
	driftReportThreshold = 5 * time.Millisecond
)

var (
	startDriftOnce sync.Once
)

// wall clock can jump forward or backward by the ntp.
// monotonic time don't.
// the process get monotonic time at the start of the process, so during it's life-time
// https://github.com/golang/go/blob/889abb17e125bb0f5d8de61bb80ef15fbe2a130d/src/runtime/time_nofake.go#L19
var startTime = time.Now()

// StartClockDriftMonitor starts a goroutine that calculates the
// drift between wall time and monotonic time every `interval`.
func StartClockDriftMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				drift := measureClockDrift()
				if drift < 0 {
					drift = -drift
				}
				if drift > driftReportThreshold {
					umetrics.AutoScope().Gauge(mClockDriftSeconds).Update(drift.Seconds())
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// measureClockDrift returns how far time.Now() deviates from the
// monotonic baseline established at process start.
func measureClockDrift() time.Duration {
	now := time.Now()
	monotonicElapsed := now.Sub(startTime)
	expected := startTime.Add(monotonicElapsed)
	return now.Sub(expected)
}

// HLCNow returns the current time in milliseconds since the Unix epoch.
func HLCNow() uint64 {
	return uint64(time.Now().UnixMilli())
}

func initMonotonic(ctx context.Context) {
	startDriftOnce.Do(func() {
		StartClockDriftMonitor(ctx, time.Second)
	})
}
