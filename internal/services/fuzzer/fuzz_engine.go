package fuzzer

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
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

func (kp *KeyPool) Get(n int, zipf *rand.Zipf) [][]byte {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		var idx int
		// use Zipfian selection if enabled
		if zipf != nil {
			idx = int(zipf.Uint64())
		} else {
			idx = rand.Intn(len(kp.keys))
		}
		out = append(out, kp.keys[idx])
	}
	return out
}

func (kp *KeyPool) Mutate() {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	// Mutate 1-10% of the keys.
	mutations := rand.Intn(kp.size/10) + 1
	for i := 0; i < mutations; i++ {
		idx := rand.Intn(len(kp.keys))
		action := rand.Intn(2)
		switch action {
		case 0:
			newKey := randKey(rand.Intn(kp.maxLen-kp.minLen+1) + kp.minLen)
			kp.keys[idx] = newKey
			slog.Debug("KeyPool mutation: replaced key", "index", idx)
		case 1:
			dup := kp.keys[rand.Intn(len(kp.keys))]
			kp.keys[idx] = dup
			slog.Debug("KeyPool mutation: duplicated key", "index", idx)
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
		slog.Debug("ColumnPool mutation: changed column name", "index", idx)
	}
}

// FuzzEngineOps concurrently runs fuzzing operations against an Engine using multiple worker goroutines.
func FuzzEngineOps(ctx context.Context, e Engine, opsPerSec int, enableZipf bool,
	numWorkers int, stats *FuzzStats, namespace string) {
	keyPool := NewKeyPool(500, 5, 256)
	rowKeyPool := NewKeyPool(500, 5, 256)
	columnPool := NewColumnPool(50)

	go func() {
		mutTicker := time.NewTicker(1 * time.Second)
		defer mutTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-mutTicker.C:
				keyPool.Mutate()
				rowKeyPool.Mutate()
				columnPool.Mutate()
			}
		}
	}()

	globalRNG := rand.New(rand.NewSource(time.Now().UnixNano()))
	var rngMu sync.Mutex

	workerRate := float64(opsPerSec) / float64(numWorkers)
	workerInterval := time.Duration(float64(time.Second) / workerRate)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(workerInterval)
			defer ticker.Stop()

			// 👇 Each worker gets its own Zipf generator
			var localZipf *rand.Zipf
			if enableZipf {
				rngMu.Lock()
				seed := globalRNG.Int63()
				rngMu.Unlock()
				localRand := rand.New(rand.NewSource(seed))
				localZipf = rand.NewZipf(localRand, 1.2, math.MaxFloat64, uint64(keyPool.size-1))
			}

			for {
				select {
				case <-ctx.Done():
					slog.Info("[unisondb.fuzzer] Worker shutting down", "workerID", workerID)
					return
				case <-ticker.C:
					keys := keyPool.Get(3, localZipf)
					values := [][]byte{
						[]byte(gofakeit.LetterN(uint(rand.Intn(1024)))),
						[]byte(gofakeit.LetterN(uint(rand.Intn(1024 * 500)))),
						[]byte(gofakeit.LetterN(uint(rand.Intn(1024 * 100)))),
					}
					rowKey := rowKeyPool.Get(1, localZipf)[0]
					columns := columnPool.Get(rand.Intn(5) + 1)

					ops := []struct {
						name string
						fn   func()
					}{
						{"Put", func() { _ = e.Put(keys[0], values[0]) }},
						{"BatchPut", func() { _ = e.BatchPut(keys, values) }},
						{"Delete", func() { _ = e.Delete(keys[1]) }},
						{"BatchDelete", func() { _ = e.BatchDelete(keys) }},
						{"PutColumnsForRow", func() { _ = e.PutColumnsForRow(rowKey, columns) }},
						{"DeleteColumnsForRow", func() { _ = e.DeleteColumnsForRow(rowKey, columns) }},
					}

					selected := rand.Intn(len(ops))
					op := ops[selected]
					op.fn()

					if stats != nil {
						stats.Inc(namespace, op.name)
					}
				}
			}
		}(i)
	}

	<-ctx.Done()
	wg.Wait()
	slog.Info("FuzzEngineOps: completed fuzzing")
}
