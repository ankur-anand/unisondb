package fuzzer

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type NamespaceStats struct {
	OpCount    map[string]int64 `json:"op_count"`
	ErrorCount int64            `json:"error_count"`
	OpsRate    float64          `json:"ops_rate"`
	Uptime     float64          `json:"uptime"`
}

type internalNS struct {
	OpCount      map[string]int64
	ErrorCount   int64
	LastTotalOps int64
	LastUpdated  time.Time
}

type FuzzStats struct {
	mu        sync.Mutex
	startTime time.Time
	stats     map[string]*internalNS
	once      sync.Once
}

func NewFuzzStats() *FuzzStats {
	return &FuzzStats{
		startTime: time.Now(),
		stats:     make(map[string]*internalNS),
		once:      sync.Once{},
	}
}

func (fs *FuzzStats) Inc(namespace, op string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ns := fs.ensureNS(namespace)
	ns.OpCount[op]++
}

func (fs *FuzzStats) IncError(namespace string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ns := fs.ensureNS(namespace)
	ns.ErrorCount++
}

func (fs *FuzzStats) ensureNS(namespace string) *internalNS {
	ns, ok := fs.stats[namespace]
	if !ok {
		ns = &internalNS{
			OpCount:     make(map[string]int64),
			LastUpdated: time.Now(),
		}
		fs.stats[namespace] = ns
	}
	return ns
}

func (fs *FuzzStats) Snapshot() map[string]NamespaceStats {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	now := time.Now()
	out := make(map[string]NamespaceStats)

	for ns, current := range fs.stats {
		total := int64(0)
		for _, v := range current.OpCount {
			total += v
		}
		delta := total - current.LastTotalOps
		dt := now.Sub(current.LastUpdated).Seconds()
		if dt == 0 {
			dt = 1
		}
		opsRate := float64(delta) / dt
		uptime := now.Sub(fs.startTime).Seconds()

		out[ns] = NamespaceStats{
			OpCount:    copyMap(current.OpCount),
			ErrorCount: current.ErrorCount,
			OpsRate:    opsRate,
			Uptime:     uptime,
		}
		current.LastTotalOps = total
		current.LastUpdated = now
	}
	return out
}

func copyMap(m map[string]int64) map[string]int64 {
	c := make(map[string]int64)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func (fs *FuzzStats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snapshot := fs.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot)
}

func (fs *FuzzStats) StartStatsMonitor(ctx context.Context, interval time.Duration) {
	fs.once.Do(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snapshot := fs.Snapshot()

				for ns, stats := range snapshot {
					if stats.OpsRate == 0 {
						panic("Fuzzing activity stopped: ops dropped to zero for namespace: " + ns)
					}
					slog.Info("[unisondb.fuzzer] Fuzzing stats",
						"namespace", ns,
						"op_count", stats.OpCount,
						"error_count", stats.ErrorCount,
						"ops_rate", stats.OpsRate,
						"uptime_secs", stats.Uptime,
					)
				}
			}
		}
	})
}
