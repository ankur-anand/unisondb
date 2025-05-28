package fuzzer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func (m *mockEngine) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (m *mockEngine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	return nil, nil
}

func TestFuzzEngineOps_Basic(t *testing.T) {
	engine := &mockEngine{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stats := NewFuzzStats()
	FuzzEngineOps(ctx, engine, 50, 3, stats, "test", true)

	engine.mu.Lock()
	defer engine.mu.Unlock()

	snapshot := stats.Snapshot()
	assert.Contains(t, snapshot, "test")

	nsStats := snapshot["test"]
	assert.Equal(t, int64(engine.putCalls), nsStats.OpCount["Put"])
	assert.Equal(t, int64(engine.batchPutCalls), nsStats.OpCount["BatchPut"])
	assert.Equal(t, int64(engine.deleteCalls), nsStats.OpCount["Delete"])
	assert.Equal(t, int64(engine.batchDeleteCalls), nsStats.OpCount["BatchDelete"])
	assert.Equal(t, int64(engine.putColumnsCalls), nsStats.OpCount["PutColumnsForRow"])
	assert.Equal(t, int64(engine.deleteColumnsCalls), nsStats.OpCount["DeleteColumnsForRow"])
	assert.Greater(t, nsStats.Uptime, float64(0), "uptime should be positive")
	assert.GreaterOrEqual(t, nsStats.OpsRate, float64(0), "ops rate should not be negative")
}

type noopEngine struct{}

func (n *noopEngine) Put(key, value []byte) error                                       { return nil }
func (n *noopEngine) BatchPut(keys, values [][]byte) error                              { return nil }
func (n *noopEngine) Delete(key []byte) error                                           { return nil }
func (n *noopEngine) BatchDelete(keys [][]byte) error                                   { return nil }
func (n *noopEngine) PutColumnsForRow(rowKey []byte, column map[string][]byte) error    { return nil }
func (n *noopEngine) DeleteColumnsForRow(rowKey []byte, column map[string][]byte) error { return nil }
func (n *noopEngine) Get(key []byte) ([]byte, error)                                    { return nil, nil }
func (n *noopEngine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	return nil, nil
}

func BenchmarkExecuteRandomOp(b *testing.B) {
	keyPool := NewKeyPool(500, 5, 64)
	rowKeyPool := NewKeyPool(500, 5, 64)
	columnPool := NewColumnPool(50)
	valuePool := NewValuePool([]int{1024, 10 * 1024, 50 * 1024, 100 * 1024}, 1000)
	engine := &noopEngine{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executeRandomOp(engine, keyPool, rowKeyPool, columnPool, NewFuzzStats(), "test", valuePool, false)
	}
}
