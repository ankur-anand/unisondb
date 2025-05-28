package fuzzer

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	llhist "github.com/openhistogram/circonusllhist"
)

type NamespaceStats struct {
	OpCount    map[string]int64 `json:"op_count"`
	ErrorCount int64            `json:"error_count"`
	OpsRate    float64          `json:"ops_rate"`
	Uptime     float64          `json:"uptime"`
	Throughput float64          `json:"throughput"`
	Duration   time.Duration
	Latency    map[string]float64 `json:"latency_ms,omitempty"`
}

type internalNS struct {
	OpCount      map[string]int64
	ErrorCount   int64
	LastTotalOps int64
	LastUpdated  time.Time
	LatencyHist  *llhist.Histogram
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
			LatencyHist: llhist.New(),
		}
		fs.stats[namespace] = ns
	}
	return ns
}

func (fs *FuzzStats) ObserveLatency(namespace string, dur time.Duration) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ns := fs.ensureNS(namespace)
	_ = ns.LatencyHist.RecordValue(dur.Seconds())
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
		count := current.LatencyHist.Count()
		throughput := float64(count) / uptime
		out[ns] = NamespaceStats{
			OpCount:    copyMap(current.OpCount),
			ErrorCount: current.ErrorCount,
			OpsRate:    opsRate,
			Uptime:     uptime,
			Throughput: throughput,
			Duration:   time.Since(fs.startTime),
			Latency: map[string]float64{
				"p50": current.LatencyHist.ValueAtQuantile(0.50) * 1000,
				"p90": current.LatencyHist.ValueAtQuantile(0.90) * 1000,
				"p99": current.LatencyHist.ValueAtQuantile(0.99) * 1000,
			},
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
					slog.Info("[unisondb.fuzzer]",
						slog.String("event_type", "fuzzer.stats.report"),
						slog.Group("stats",
							slog.String("namespace", ns),
							slog.Any("op_count", stats.OpCount),
							slog.Int64("error_count", stats.ErrorCount),
							slog.String("uptime", humanizeDuration(stats.Duration)),
							slog.Float64("ops_rate", stats.OpsRate),
						),
					)
				}
			}
		}
	})
}

func WriteFuzzRunCSV(path string, nsStats map[string]NamespaceStats, mode string, readers int) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	for ns, s := range nsStats {
		lat := s.Latency
		_ = writer.Write([]string{
			time.Now().Format(time.RFC3339),
			mode,
			ns,
			strconv.Itoa(readers),
			fmt.Sprintf("%.2f", s.OpsRate),
			fmt.Sprintf("%.2f", s.Throughput),
			fmt.Sprintf("%.2f", lat["p50"]),
			fmt.Sprintf("%.2f", lat["p90"]),
			fmt.Sprintf("%.2f", lat["p99"]),
			strconv.FormatInt(s.ErrorCount, 10),
		})
	}
	return nil
}
