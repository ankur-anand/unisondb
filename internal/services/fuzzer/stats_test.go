package fuzzer

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.Contains(t, snapshot, ns)

	nsStats := snapshot[ns]
	assert.Equal(t, int64(2), nsStats.OpCount["Put"])
	assert.Equal(t, int64(1), nsStats.OpCount["Delete"])
	assert.Equal(t, int64(1), nsStats.ErrorCount)
	assert.Greater(t, nsStats.Uptime, float64(0), "uptime should be positive")
	assert.Greater(t, nsStats.OpsRate, float64(0), "ops rate should be positive")
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
	assert.Equal(t, int64(1000), snapshot[ns].OpCount[op])
	assert.Equal(t, int64(1000), snapshot[ns].ErrorCount)
}

func TestFuzzStats_HandleMetrics(t *testing.T) {

	stats := NewFuzzStats()
	stats.Inc("test", "Put")
	stats.Inc("test", "Put")
	stats.Inc("test", "Delete")
	stats.IncError("test")
	stats.ObserveLatency("test", 15*time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	stats.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var data map[string]NamespaceStats
	err := json.NewDecoder(resp.Body).Decode(&data)
	assert.NoError(t, err)

	nsStats, ok := data["test"]
	assert.True(t, ok, "namespace 'test' should be present")

	assert.Equal(t, int64(2), nsStats.OpCount["Put"])
	assert.Equal(t, int64(1), nsStats.OpCount["Delete"])
	assert.Equal(t, int64(1), nsStats.ErrorCount)
	assert.Greater(t, nsStats.Uptime, float64(0))
}

func TestWriteFuzzRunCSV(t *testing.T) {
	dir := t.TempDir()
	tmpFile, err := os.CreateTemp(dir, "fuzzstats_test_*.csv")
	assert.NoError(t, err)

	stats := map[string]NamespaceStats{
		"test-ns": {
			OpCount: map[string]int64{
				"Put": 10,
			},
			ErrorCount: 5,
			OpsRate:    123.45,
			Throughput: 67.89,
			Latency: map[string]float64{
				"p50": 10.1,
				"p90": 20.2,
				"p99": 30.3,
			},
		},
	}

	err = WriteFuzzRunCSV(tmpFile.Name(), stats, "fuzzing-mode", 100)
	assert.NoError(t, err)

	f, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	assert.NoError(t, err)
	assert.Len(t, records, 1)

	row := records[0]
	assert.Len(t, row, 10)

	assert.Equal(t, "fuzzing-mode", row[1])
	assert.Equal(t, "test-ns", row[2])
	assert.Equal(t, "100", row[3])
	assert.Equal(t, "123.45", row[4])
	assert.Equal(t, "67.89", row[5])
	assert.Equal(t, "10.10", row[6])
	assert.Equal(t, "20.20", row[7])
	assert.Equal(t, "30.30", row[8])
	assert.Equal(t, strconv.FormatInt(5, 10), row[9])

	_, err = time.Parse(time.RFC3339, row[0])
	assert.NoError(t, err)
}
