package dbkernel

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var clockDriftGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "unisondb",
	Subsystem: "dbkernel",
	Name:      "monotonic_clock_drift_seconds",
	Help:      "Difference between wall clock and monotonic-derived time since process start.",
})

// wall clock can jump forward or backward by the ntp.
// monotonic time don't.
// the process get monotonic time at the start of the process, so during it's life-time
// https://github.com/golang/go/blob/889abb17e125bb0f5d8de61bb80ef15fbe2a130d/src/runtime/time_nofake.go#L19
var startTime = time.Now()

// currentMilli stores the cached millisecond timestamp (updated every 1ms).
// so instead of invoking time.Now() frequently, we periodically update currentMilli and serve using atomic loads.
// it trades off the sub-millisecond accuracy in exchange for performance gain.
// while it's not ideal for sub-nanosecond accuracy this serves as us well as this is mostly used for measuring
// the replication latency and gives us fast ops in the hot path.
// See the benchmark for the result.
// if the clock is adjusted we can see it in the clock drift measurement.
var currentMilli atomic.Uint64

// StartCachedTicker updates via a goroutine
// `currentMilli` every 1ms using time.Now().UnixMilli().
func StartCachedTicker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case now := <-ticker.C:
				currentMilli.Store(uint64(now.UnixMilli()))
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StartClockDriftMonitor launches a goroutine that calculates the
// drift between wall time and monotonic time every `interval`.
// It exposes the drift (in seconds) via Prometheus.
func StartClockDriftMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				drift := measureClockDrift()
				// report if drift is beyond ±1ms
				if drift < -1*time.Millisecond || drift > 1*time.Millisecond {
					clockDriftGauge.Set(drift.Seconds())
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

// HLCNow returns the latest cached millisecond timestamp.
// It is fast over the repetitive calling of time.Now or monotonic time.Since
// But it sacrifices accuracy below 1ms. The Cache gets updated every 1ms.
// Should only be used for replication latency nothing more.
func HLCNow() uint64 {
	return currentMilli.Load()
}

func initMonotonic(ctx context.Context) {
	StartCachedTicker(ctx)
	StartClockDriftMonitor(ctx, 1*time.Second)
}
