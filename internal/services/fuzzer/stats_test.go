package fuzzer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFuzzStats_BasicUsage(t *testing.T) {
	stats := NewFuzzStats()

	ns := "test-namespace"

	stats.Inc(ns, "Put")
	stats.Inc(ns, "Put")
	stats.Inc(ns, "Delete")
	stats.IncError(ns)

	time.Sleep(10 * time.Millisecond)

	snapshot := stats.Snapshot()
	require.Contains(t, snapshot, ns)

	nsStats := snapshot[ns]
	require.Equal(t, int64(2), nsStats.OpCount["Put"])
	require.Equal(t, int64(1), nsStats.OpCount["Delete"])
	require.Equal(t, int64(1), nsStats.ErrorCount)
	require.Greater(t, nsStats.Uptime, float64(0), "uptime should be positive")
	require.Greater(t, nsStats.OpsRate, float64(0), "ops rate should be positive")
}

func TestFuzzStats_ConcurrentAccess(t *testing.T) {
	stats := NewFuzzStats()

	ns := "concurrent-ns"
	op := "BatchPut"

	done := make(chan struct{})

	go func() {
		for i := 0; i < 1000; i++ {
			stats.Inc(ns, op)
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			stats.IncError(ns)
		}
		done <- struct{}{}
	}()

	<-done
	<-done

	snapshot := stats.Snapshot()
	require.Equal(t, int64(1000), snapshot[ns].OpCount[op])
	require.Equal(t, int64(1000), snapshot[ns].ErrorCount)
}

func TestFuzzStats_HandleMetrics(t *testing.T) {

	stats := NewFuzzStats()
	stats.Inc("test", "Put")
	stats.Inc("test", "Put")
	stats.Inc("test", "Delete")
	stats.IncError("test")
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	stats.HandleMetrics(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var data map[string]NamespaceStats
	err := json.NewDecoder(resp.Body).Decode(&data)
	require.NoError(t, err)

	nsStats, ok := data["test"]
	require.True(t, ok, "namespace 'test' should be present")

	require.Equal(t, int64(2), nsStats.OpCount["Put"])
	require.Equal(t, int64(1), nsStats.OpCount["Delete"])
	require.Equal(t, int64(1), nsStats.ErrorCount)
	require.Greater(t, nsStats.Uptime, float64(0))
}
