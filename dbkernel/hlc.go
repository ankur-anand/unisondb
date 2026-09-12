package dbkernel

import (
	"context"
	"time"

	"github.com/ankur-anand/unisondb/pkg/umetrics"
)

const (
	mClockDriftSeconds   = "monotonic_clock_drift_seconds"
	driftReportThreshold = 5 * time.Millisecond
)

// Keep both the initial wall-clock and monotonic readings for this process.
var startTime = time.Now()

// StartClockDriftMonitor starts a goroutine that calculates the
// drift between wall time and monotonic time every `interval`.
// Start it once at application startup with the application's context, not an
// individual engine's context. The returned function stops and waits for the
// monitor; it is safe to call more than once. The interval must be positive.
func StartClockDriftMonitor(ctx context.Context, interval time.Duration) func() {
	return startClockDriftMonitor(ctx, interval, measureClockDrift,
		umetrics.AutoScope().Gauge(mClockDriftSeconds).Update)
}

func startClockDriftMonitor(ctx context.Context, interval time.Duration, measure func() time.Duration, report func(float64)) func() {
	ticker := time.NewTicker(interval)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				drift := measure()
				if drift < 0 {
					drift = -drift
				}
				if drift <= driftReportThreshold {
					drift = 0
				}
				report(drift.Seconds())
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// measureClockDrift returns how far time.Now() deviates from the
// monotonic baseline established at process start.
func measureClockDrift() time.Duration {
	now := time.Now()
	return clockDrift(startTime, now, now.Sub(startTime))
}

func clockDrift(start, now time.Time, monotonicElapsed time.Duration) time.Duration {
	// Round(0) removes monotonic readings, forcing Sub to compare wall time.
	wallElapsed := now.Round(0).Sub(start.Round(0))
	return wallElapsed - monotonicElapsed
}

// HLCNow returns the current time in milliseconds since the Unix epoch.
func HLCNow() uint64 {
	return uint64(time.Now().UnixMilli())
}
