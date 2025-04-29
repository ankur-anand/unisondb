package fuzzer

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/prometheus/common/helpers/templates"
	"golang.org/x/sync/errgroup"
)

type Engine interface {
	Put(key, value []byte) error
	BatchPut(keys, values [][]byte) error
	Delete(key []byte) error
	BatchDelete(keys [][]byte) error
	PutColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error
	DeleteColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error
}

type KeyPool struct {
	mu     sync.RWMutex
	keys   [][]byte
	size   int
	minLen int
	maxLen int
}

func NewKeyPool(size, minLen, maxLen int) *KeyPool {
	return &KeyPool{
		keys:   generateFuzzKeyPool(size, minLen, maxLen),
		size:   size,
		minLen: minLen,
		maxLen: maxLen,
	}
}

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

func randKey(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func generateFuzzKeyPool(count, minLen, maxLen int) [][]byte {
	pool := make([][]byte, 0, count)
	for len(pool) < count {
		pool = append(pool, randKey(rand.Intn(maxLen-minLen+1)+minLen))
	}
	return pool
}

const columnPrefix = "col"

func randColumnName() string {
	return columnPrefix + string(randKey(4))
}

func (kp *KeyPool) Get(n int) [][]byte {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		var idx = rand.Intn(len(kp.keys))

		out = append(out, kp.keys[idx])
	}
	return out
}

func (kp *KeyPool) Mutate() {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	// mutate 1-10% of the keys.
	mutations := rand.Intn(kp.size/10) + 1
	for i := 0; i < mutations; i++ {
		idx := rand.Intn(len(kp.keys))
		action := rand.Intn(2)
		switch action {
		case 0:
			newKey := randKey(rand.Intn(kp.maxLen-kp.minLen+1) + kp.minLen)
			kp.keys[idx] = newKey
		case 1:
			dup := kp.keys[rand.Intn(len(kp.keys))]
			kp.keys[idx] = dup
		}
	}
}

type ColumnPool struct {
	mu      sync.RWMutex
	columns []string
	size    int
}

func NewColumnPool(size int) *ColumnPool {
	cp := &ColumnPool{
		columns: make([]string, 0, size),
		size:    size,
	}
	for i := 0; i < size; i++ {
		cp.columns = append(cp.columns, randColumnName())
	}
	return cp
}

func (cp *ColumnPool) Get(n int) map[string][]byte {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	out := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		col := cp.columns[rand.Intn(len(cp.columns))]
		out[col] = []byte(gofakeit.LetterN(256))
	}
	return out
}

func (cp *ColumnPool) Mutate() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	mutations := rand.Intn(cp.size/5) + 1 // mutate 1-20% of columns.
	for i := 0; i < mutations; i++ {
		idx := rand.Intn(len(cp.columns))
		cp.columns[idx] = randColumnName()
	}
}

type ValuePool struct {
	smallValues [][]byte
	largeValues [][]byte
}

func NewValuePool(sizes []int, countPerSize int) *ValuePool {
	var smallValues [][]byte
	var largeValues [][]byte

	for _, sz := range sizes {
		for i := 0; i < countPerSize; i++ {
			b := make([]byte, sz)
			fillRandomBytes(b)
			if sz <= 2048 {
				smallValues = append(smallValues, b)
			} else {
				largeValues = append(largeValues, b)
			}
		}
	}

	return &ValuePool{
		smallValues: smallValues,
		largeValues: largeValues,
	}
}

func fillRandomBytes(b []byte) {
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
}

func (vp *ValuePool) Get() []byte {
	// 90% chance for small value
	if rand.Float64() < 0.9 {
		return vp.smallValues[rand.Intn(len(vp.smallValues))]
	}
	return vp.largeValues[rand.Intn(len(vp.largeValues))]
}

// FuzzEngineOps concurrently runs fuzzing operations against an Engine using multiple worker goroutines.
func FuzzEngineOps(ctx context.Context, e Engine, opsPerSec int,
	numWorkers int, stats *FuzzStats, namespace string) {
	keyPool := NewKeyPool(500, 5, 256)
	rowKeyPool := NewKeyPool(500, 5, 256)
	columnPool := NewColumnPool(50)
	valuePool := NewValuePool([]int{100, 500, 1024, 2048, 10 * 1024, 50 * 1024, 100 * 1024}, 100)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return startMutationLoop(ctx, keyPool, rowKeyPool, columnPool)
	})

	workerInterval := time.Duration(float64(time.Second) / (float64(opsPerSec) / float64(numWorkers)))
	slog.Info("[unisondb.fuzzer]",
		slog.String("event_type", "fuzzing.started"),
		slog.Group("worker",
			slog.String("namespace", namespace),
			slog.Duration("interval", workerInterval)))

	// fuzzing workers
	for i := 0; i < numWorkers; i++ {
		workerID := i
		g.Go(func() error {
			return runFuzzWorker(ctx, e, workerID, workerInterval, keyPool, rowKeyPool, columnPool, stats, namespace, valuePool)
		})
	}

	_ = g.Wait()
	snapshot := stats.Snapshot()
	nameSpacedStats := snapshot[namespace]
	slog.Info("[unisondb.fuzzer]",
		slog.String("event_type", "fuzzing.completed"),
		slog.Group("stats",
			slog.String("namespace", namespace),
			slog.Int64("error_count", nameSpacedStats.ErrorCount),
			slog.String("runtime", humanizeDuration(nameSpacedStats.Duration)),
			slog.Any("op_count", nameSpacedStats.OpCount),
		),
	)
}

func startMutationLoop(ctx context.Context, keyPool, rowKeyPool *KeyPool, columnPool *ColumnPool) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			keyPool.Mutate()
			rowKeyPool.Mutate()
			columnPool.Mutate()
		}
	}
}

func runFuzzWorker(ctx context.Context, e Engine, workerID int, interval time.Duration,
	keyPool, rowKeyPool *KeyPool, columnPool *ColumnPool, stats *FuzzStats, namespace string, valuePool *ValuePool) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			executeRandomOp(e, keyPool, rowKeyPool, columnPool, stats, namespace, valuePool)
		}
	}
}

func executeRandomOp(e Engine, keyPool, rowKeyPool *KeyPool, columnPool *ColumnPool,
	stats *FuzzStats, namespace string, valuePool *ValuePool) {
	keys := keyPool.Get(3)
	values := [][]byte{valuePool.Get(), valuePool.Get(), valuePool.Get()}

	rowKey := rowKeyPool.Get(1)[0]
	columns := columnPool.Get(rand.Intn(5) + 1)

	ops := []struct {
		name string
		fn   func() error
	}{
		{"Put", func() error { return e.Put(keys[0], values[0]) }},
		{"BatchPut", func() error { return e.BatchPut(keys, values) }},
		{"Delete", func() error { return e.Delete(keys[1]) }},
		{"BatchDelete", func() error { return e.BatchDelete(keys) }},
		{"PutColumnsForRow", func() error { return e.PutColumnsForRow(rowKey, columns) }},
		{"DeleteColumnsForRow", func() error { return e.DeleteColumnsForRow(rowKey, columns) }},
	}

	op := ops[rand.Intn(len(ops))]
	if err := op.fn(); err != nil {
		slog.Error("[unisondb.fuzzer] Operation failed", "op", op.name, "err", err)
	}

	if stats != nil {
		stats.Inc(namespace, op.name)
	}
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
