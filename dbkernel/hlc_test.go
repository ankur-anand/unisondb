package dbkernel

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stop := StartClockDriftMonitor(ctx, time.Hour)
		cancel()
		stop()
		stop()
	})
}

func TestClockDriftDetectsWallClockJumps(t *testing.T) {
	start := time.Now()
	elapsed := 10 * time.Second
	for _, jump := range []time.Duration{0, 5 * time.Second, -5 * time.Second, -20 * time.Second, 500 * time.Microsecond} {
		t.Run(jump.String(), func(t *testing.T) {
			// Supply wall and monotonic elapsed time independently to simulate
			// clock adjustments without changing the system clock.
			now := start.Add(elapsed + jump)
			require.Equal(t, jump, clockDrift(start, now, elapsed))
		})
	}
}

func TestClockDriftMonitorReportsAndClearsDrift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var drift atomic.Int64
		reported := make(chan float64, 1)
		stop := startClockDriftMonitor(t.Context(), time.Millisecond,
			func() time.Duration { return time.Duration(drift.Load()) },
			func(value float64) {
				select {
				case reported <- value:
				default:
				}
			})
		defer stop()
		for _, sample := range []struct {
			drift time.Duration
			want  float64
		}{
			{20 * time.Millisecond, 0.020},
			{-15 * time.Millisecond, 0.015},
			{driftReportThreshold, 0},
			{3 * time.Millisecond, 0},
			{time.Second, 1},
			{0, 0},
		} {
			drift.Store(int64(sample.drift))
			time.Sleep(time.Millisecond)
			synctest.Wait()
			require.Len(t, reported, 1)
			require.Equal(t, sample.want, <-reported)
		}
	})
}

func TestClockDriftMonitorShutdown(t *testing.T) {
	for _, cancelContext := range []bool{false, true} {
		name := "explicit stop"
		if cancelContext {
			name = "application context"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var samples atomic.Int64
				stop := startClockDriftMonitor(ctx, time.Millisecond,
					func() time.Duration { return 0 },
					func(float64) { samples.Add(1) })
				defer stop()
				time.Sleep(time.Millisecond)
				synctest.Wait()
				require.Positive(t, samples.Load())
				if cancelContext {
					cancel()
				} else {
					stop()
				}
				synctest.Wait()
				before := samples.Load()
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				require.Equal(t, before, samples.Load())
			})
		})
	}
}

func TestClockDriftMonitorSurvivesEngineRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var samples atomic.Int64
		stop := startClockDriftMonitor(ctx, time.Millisecond,
			func() time.Duration { return 20 * time.Millisecond },
			func(float64) { samples.Add(1) })
		defer stop()
		dir := t.TempDir()
		for range 2 {
			engine, err := NewStorageEngine(dir, "clock-lifetime", NewDefaultEngineConfig())
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
			synctest.Wait()
			require.NoError(t, engine.Close(context.Background()))
			synctest.Wait()
			before := samples.Load()
			time.Sleep(time.Millisecond)
			synctest.Wait()
			require.Greater(t, samples.Load(), before, "closing an engine must not stop the application monitor")
		}
	})
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
