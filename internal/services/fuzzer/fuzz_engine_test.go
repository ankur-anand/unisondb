package fuzzer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockEngine struct {
	mu                 sync.Mutex
	putCalls           int
	batchPutCalls      int
	deleteCalls        int
	batchDeleteCalls   int
	putColumnsCalls    int
	deleteColumnsCalls int
}

func (m *mockEngine) Put(key, value []byte) error {
	m.mu.Lock()
	m.putCalls++
	m.mu.Unlock()
	return nil
}

func (m *mockEngine) BatchPut(keys, values [][]byte) error {
	m.mu.Lock()
	m.batchPutCalls++
	m.mu.Unlock()
	return nil
}

func (m *mockEngine) Delete(key []byte) error {
	m.mu.Lock()
	m.deleteCalls++
	m.mu.Unlock()
	return nil
}

func (m *mockEngine) BatchDelete(keys [][]byte) error {
	m.mu.Lock()
	m.batchDeleteCalls++
	m.mu.Unlock()
	return nil
}

func (m *mockEngine) PutColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	m.mu.Lock()
	m.putColumnsCalls++
	m.mu.Unlock()
	return nil
}

func (m *mockEngine) DeleteColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	m.mu.Lock()
	m.deleteColumnsCalls++
	m.mu.Unlock()
	return nil
}

func TestFuzzEngineOps_Basic(t *testing.T) {
	engine := &mockEngine{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stats := NewFuzzStats()
	FuzzEngineOps(ctx, engine, 50, true, 3, stats, "test")

	engine.mu.Lock()
	defer engine.mu.Unlock()

	snapshot := stats.Snapshot()
	require.Contains(t, snapshot, "test")

	nsStats := snapshot["test"]
	require.Equal(t, int64(engine.putCalls), nsStats.OpCount["Put"])
	require.Equal(t, int64(engine.batchPutCalls), nsStats.OpCount["BatchPut"])
	require.Equal(t, int64(engine.deleteCalls), nsStats.OpCount["Delete"])
	require.Equal(t, int64(engine.batchDeleteCalls), nsStats.OpCount["BatchDelete"])
	require.Equal(t, int64(engine.putColumnsCalls), nsStats.OpCount["PutColumnsForRow"])
	require.Equal(t, int64(engine.deleteColumnsCalls), nsStats.OpCount["DeleteColumnsForRow"])
	require.Greater(t, nsStats.Uptime, float64(0), "uptime should be positive")
	require.GreaterOrEqual(t, nsStats.OpsRate, float64(0), "ops rate should not be negative")
}
